"""End-to-end ForgeFlow pipeline with failure-safe observability finalization."""

from __future__ import annotations

import csv
import io
import json
import math
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, TypeVar
from uuid import UUID, uuid4

import structlog

from forgeflow.config import Settings
from forgeflow.contracts import SOURCE_CONTRACTS, validate_dataset
from forgeflow.dbt_runner import DbtRunner, DbtRunResult
from forgeflow.errors import ForgeFlowError, WarehouseError
from forgeflow.incident import DeterministicExplanationProvider, build_explanation_provider
from forgeflow.models import (
    ContractResult,
    FailureScenario,
    QualityResult,
    RunStatus,
    RunSummary,
    Severity,
)
from forgeflow.object_store import ObjectStore, schema_fingerprint
from forgeflow.service import evidence_from_run
from forgeflow.synthetic import SOURCE_NAMES, generate_dataset
from forgeflow.warehouse import FileRegistration, PostgresRepository

logger = structlog.get_logger(__name__)
StageValue = TypeVar("StageValue")


@dataclass(slots=True)
class PipelineOutcome:
    """Run result plus optional incident identity and human-readable evidence."""

    summary: RunSummary
    human_summary: dict[str, Any]
    incident_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class LandedSource:
    """Ledger outcome retained between the landing and validation phases."""

    registration: FileRegistration
    checksum: str


class PipelineRunner:
    """Coordinate landing, validation, loading, dbt, evidence, and recovery."""

    def __init__(
        self,
        settings: Settings,
        repository: PostgresRepository,
        object_store: ObjectStore,
        *,
        dbt_runner: DbtRunner | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._object_store = object_store
        self._dbt_runner = dbt_runner or DbtRunner(settings, repository)

    def run_generated(
        self,
        scenario: FailureScenario = FailureScenario.CLEAN,
        *,
        batch_date: date | None = None,
        incremental: bool = False,
        run_dbt: bool = True,
        recovery_incident_id: UUID | None = None,
        dbt_variables: Mapping[str, str] | None = None,
        source_bytes: Mapping[str, bytes] | None = None,
    ) -> PipelineOutcome:
        """Generate a deterministic batch and execute it through the full platform."""
        resolved_date = batch_date or (datetime.now(UTC).date() - timedelta(days=2))
        batch_id = build_batch_id(
            scenario=scenario,
            batch_date=resolved_date,
            seed=self._settings.seed,
            incremental=incremental,
        )
        self._settings.ensure_runtime_directories()
        self._repository.initialize()
        summary = RunSummary(batch_id=batch_id, scenario=scenario)
        self._repository.start_run(summary)
        logger.info(
            "pipeline_started",
            run_id=str(summary.run_id),
            batch_id=batch_id,
            scenario=scenario.value,
        )
        try:
            dataset = self._execute_stage(
                summary,
                "source_generation",
                lambda: generate_dataset(
                    scenario,
                    seed=self._settings.seed,
                    generated_days=self._settings.generated_days,
                    batch_date=resolved_date,
                    incremental=incremental,
                ),
                output_counter=lambda generated: sum(len(rows) for rows in generated.values()),
                metadata={"scenario": scenario.value, "incremental": incremental},
            )
        except Exception as error:
            self._finalize_preflight_failure(summary, error)
            if isinstance(error, ForgeFlowError):
                raise
            raise ForgeFlowError(f"Source generation failed for run {summary.run_id}") from error
        return self.run_dataset(
            dataset,
            batch_id=batch_id,
            scenario=scenario,
            run_dbt=run_dbt,
            recovery_incident_id=recovery_incident_id,
            dbt_variables=dbt_variables,
            source_bytes=source_bytes,
            _summary=summary,
            _repository_initialized=True,
        )

    def run_dataset(
        self,
        dataset: Mapping[str, Sequence[Mapping[str, Any]]],
        *,
        batch_id: str,
        scenario: FailureScenario,
        run_dbt: bool = True,
        recovery_incident_id: UUID | None = None,
        dbt_variables: Mapping[str, str] | None = None,
        source_bytes: Mapping[str, bytes] | None = None,
        _summary: RunSummary | None = None,
        _repository_initialized: bool = False,
    ) -> PipelineOutcome:
        """Execute one source batch, finalizing run evidence on every failure path."""
        self._settings.ensure_runtime_directories()
        if not _repository_initialized:
            self._repository.initialize()
        summary = _summary or RunSummary(batch_id=batch_id, scenario=scenario)
        if _summary is None:
            self._repository.start_run(summary)
            logger.info(
                "pipeline_started",
                run_id=str(summary.run_id),
                batch_id=batch_id,
                scenario=scenario.value,
            )
        quality_results: list[QualityResult] = []
        dbt_result = DbtRunResult(succeeded=True, return_code=0)
        landed_sources: dict[str, LandedSource] = {}
        processed_file_ids: set[UUID] = set()

        try:
            observed_columns = self._validate_source_bytes(dataset, source_bytes)

            def land_all_sources() -> dict[str, LandedSource]:
                self._object_store.ensure_bucket()
                for source_name, records in dataset.items():
                    landed_sources[source_name] = self._land_source(
                        summary=summary,
                        source_name=source_name,
                        records=records,
                        raw_content=(source_bytes or {}).get(source_name),
                        raw_columns=observed_columns.get(source_name),
                    )
                return landed_sources

            landed_sources = self._execute_stage(
                summary,
                "raw_landing",
                land_all_sources,
                input_row_count=sum(len(records) for records in dataset.values()),
                output_counter=lambda landed: len(landed),
            )

            def validate_sources() -> dict[str, ContractResult]:
                if observed_columns:
                    return validate_dataset(dataset, observed_columns=observed_columns)
                return validate_dataset(dataset)

            contract_results = self._execute_stage(
                summary,
                "contract_validation",
                validate_sources,
                input_row_count=sum(len(records) for records in dataset.values()),
                output_counter=lambda results: sum(
                    len(result.accepted_records) for result in results.values()
                ),
            )

            def load_validated_sources() -> int:
                for source_name in SOURCE_NAMES:
                    records = dataset.get(source_name)
                    if records is None:
                        continue
                    result = contract_results[source_name]
                    self._process_source(
                        summary=summary,
                        source_name=source_name,
                        records=records,
                        contract_result=result,
                        landed_source=landed_sources[source_name],
                        quality_results=quality_results,
                    )
                    if not landed_sources[source_name].registration.duplicate:
                        processed_file_ids.add(landed_sources[source_name].registration.file_id)
                return summary.accepted_row_count

            self._execute_stage(
                summary,
                "warehouse_load",
                load_validated_sources,
                input_row_count=sum(
                    len(result.accepted_records) for result in contract_results.values()
                ),
                output_counter=lambda accepted_rows: accepted_rows,
            )

            if run_dbt:
                dbt_result = self._execute_stage(
                    summary,
                    "dbt_build_and_freshness",
                    lambda: self._dbt_runner.build(
                        summary.run_id,
                        batch_id,
                        variables=dbt_variables,
                    ),
                    successful=lambda result: result.succeeded,
                    error_selector=lambda result: result.error_message,
                    metadata={"isolated_artifacts": True},
                )
                quality_results.extend(dbt_result.quality_results)
                summary.model_row_counts = dbt_result.model_row_counts
                summary.test_counts = dbt_result.test_counts
                summary.affected_downstream_models = dbt_result.affected_downstream_models
                if dbt_result.error_message:
                    summary.error_message = dbt_result.error_message
            else:
                self._repository.start_stage(
                    summary.run_id,
                    "dbt_build_and_freshness",
                    metadata={"diagnostic_only": True},
                )
                self._repository.finish_stage(
                    summary.run_id,
                    "dbt_build_and_freshness",
                    status="skipped",
                    metadata={"diagnostic_only": True},
                )
                quality_results.append(
                    QualityResult(
                        check_id="dbt_skipped",
                        run_id=summary.run_id,
                        check_name="dbt execution skipped",
                        check_type="execution",
                        scope="dbt",
                        status="warning",
                        severity=Severity.WARNING,
                        observed_value="skipped",
                        expected="canonical runs execute dbt build and source freshness",
                        evidence={"diagnostic_only": True},
                    )
                )

            def finalize_run() -> dict[str, Any]:
                self._repository.record_quality_results(quality_results)
                self._finalize_summary(summary, quality_results, dbt_result)
                finalized_summary = build_human_summary(summary, quality_results)
                self._repository.finish_run(summary, finalized_summary)
                return finalized_summary

            human_summary = self._execute_stage(
                summary,
                "observability_finalization",
                finalize_run,
                input_row_count=len(quality_results),
            )
            incident_id = self._execute_stage(
                summary,
                "incident_linkage",
                lambda: self._finalize_incident_or_recovery(
                    summary,
                    human_summary,
                    recovery_incident_id=recovery_incident_id,
                ),
            )
            logger.info(
                "pipeline_finished",
                run_id=str(summary.run_id),
                status=summary.status.value,
                accepted_rows=summary.accepted_row_count,
                quarantined_rows=summary.quarantined_row_count,
                failed_checks=summary.failed_checks,
            )
            return PipelineOutcome(
                summary=summary,
                human_summary=human_summary,
                incident_id=incident_id,
            )
        except Exception as error:
            self._mark_unfinished_source_files_failed(landed_sources, processed_file_ids)
            self._finalize_summary(summary, quality_results, dbt_result)
            summary.status = RunStatus.FAILED
            summary.error_message = _safe_error_message(error)
            human_summary = build_human_summary(summary, quality_results)
            try:
                self._repository.record_quality_results(quality_results)
            except WarehouseError:
                logger.exception("pipeline_quality_finalization_failed", run_id=str(summary.run_id))
            try:
                self._repository.finish_run(summary, human_summary)
            except WarehouseError:
                logger.exception("pipeline_run_finalization_failed", run_id=str(summary.run_id))
            logger.exception(
                "pipeline_failed",
                run_id=str(summary.run_id),
                error_type=type(error).__name__,
            )
            if isinstance(error, ForgeFlowError):
                raise
            raise ForgeFlowError(f"Pipeline run {summary.run_id} failed") from error

    def _validate_source_bytes(
        self,
        dataset: Mapping[str, Sequence[Mapping[str, Any]]],
        source_bytes: Mapping[str, bytes] | None,
    ) -> dict[str, list[str]]:
        """Bind exact raw CSV objects to parsed rows before any object-store side effect."""
        if not dataset:
            raise ForgeFlowError("Dataset contains no registered source records")
        unknown_dataset_sources = set(dataset).difference(SOURCE_CONTRACTS)
        if unknown_dataset_sources:
            names = ", ".join(sorted(unknown_dataset_sources))
            raise ForgeFlowError(f"Dataset contains unregistered sources: {names}")
        oversized_rows = [
            name
            for name, records in dataset.items()
            if len(records) > self._settings.max_source_rows_per_file
        ]
        if oversized_rows:
            names = ", ".join(sorted(oversized_rows))
            raise ForgeFlowError(f"Dataset sources exceed the configured row limit: {names}")
        if source_bytes is None:
            return {}
        if not source_bytes:
            raise ForgeFlowError("Raw source bytes were supplied without any source files")
        unknown_raw_sources = set(source_bytes).difference(dataset)
        if unknown_raw_sources:
            names = ", ".join(sorted(unknown_raw_sources))
            raise ForgeFlowError(f"Raw source bytes have no matching dataset: {names}")
        missing_raw_sources = set(dataset).difference(source_bytes)
        if missing_raw_sources:
            names = ", ".join(sorted(missing_raw_sources))
            raise ForgeFlowError(f"Dataset sources have no matching raw bytes: {names}")
        oversized = [
            name
            for name, content in source_bytes.items()
            if len(content) > self._settings.max_source_file_bytes
        ]
        if oversized:
            names = ", ".join(sorted(oversized))
            raise ForgeFlowError(f"Raw source bytes exceed the configured limit: {names}")
        observed_columns: dict[str, list[str]] = {}
        for source_name, content in source_bytes.items():
            records = dataset[source_name]
            try:
                with io.StringIO(content.decode("utf-8"), newline="") as handle:
                    reader = csv.DictReader(handle, strict=True)
                    headers = list(reader.fieldnames or [])
                    if not headers:
                        raise ForgeFlowError(f"Raw source CSV has no header: {source_name}")
                    if any(not header for header in headers) or len(set(headers)) != len(headers):
                        raise ForgeFlowError(
                            f"Raw source CSV has empty or duplicate headers: {source_name}"
                        )
                    raw_rows = list(reader)
            except (UnicodeError, csv.Error) as error:
                raise ForgeFlowError(f"Raw source CSV cannot be parsed: {source_name}") from error

            if len(raw_rows) != len(records):
                raise ForgeFlowError(
                    f"Raw source bytes do not match parsed row count: {source_name}"
                )
            contract = SOURCE_CONTRACTS.get(source_name)
            if contract is None:
                raise ForgeFlowError(f"Dataset contains an unregistered source: {source_name}")
            for row_number, (raw_row, record) in enumerate(
                zip(raw_rows, records, strict=True),
                start=2,
            ):
                if None in raw_row or set(record) != set(headers):
                    raise ForgeFlowError(
                        f"Raw source bytes do not match parsed columns on row {row_number}: "
                        f"{source_name}"
                    )
                for column in headers:
                    if not _raw_csv_value_matches(
                        raw_row.get(column),
                        record.get(column),
                        kind=(
                            contract.columns[column].kind if column in contract.columns else None
                        ),
                    ):
                        raise ForgeFlowError(
                            f"Raw source bytes do not match parsed value on row {row_number}, "
                            f"column {column!r}: {source_name}"
                        )
            observed_columns[source_name] = headers
        return observed_columns

    def _execute_stage(
        self,
        summary: RunSummary,
        stage_name: str,
        operation: Callable[[], StageValue],
        *,
        input_row_count: int | None = None,
        output_counter: Callable[[StageValue], int | None] | None = None,
        successful: Callable[[StageValue], bool] | None = None,
        error_selector: Callable[[StageValue], str | None] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> StageValue:
        """Execute one stage and retain timing/status without masking its primary error."""
        self._repository.start_stage(
            summary.run_id,
            stage_name,
            input_row_count=input_row_count,
            metadata=metadata,
        )
        try:
            result = operation()
        except Exception as error:
            try:
                self._repository.finish_stage(
                    summary.run_id,
                    stage_name,
                    status="failed",
                    error_message=_safe_error_message(error),
                )
            except WarehouseError:
                logger.exception(
                    "pipeline_stage_failure_metadata_unavailable",
                    run_id=str(summary.run_id),
                    stage_name=stage_name,
                )
            raise
        stage_succeeded = successful(result) if successful is not None else True
        self._repository.finish_stage(
            summary.run_id,
            stage_name,
            status="succeeded" if stage_succeeded else "failed",
            output_row_count=output_counter(result) if output_counter is not None else None,
            error_message=(
                error_selector(result)
                if not stage_succeeded and error_selector is not None
                else None
            ),
        )
        return result

    def _mark_unfinished_source_files_failed(
        self,
        landed_sources: Mapping[str, LandedSource],
        processed_file_ids: set[UUID],
    ) -> None:
        """Leave retryable file-ledger evidence for sources interrupted mid-processing."""
        for landed_source in landed_sources.values():
            registration = landed_source.registration
            if registration.duplicate or registration.file_id in processed_file_ids:
                continue
            try:
                self._repository.complete_source_file(
                    registration.file_id,
                    accepted_count=0,
                    quarantined_count=0,
                    status="failed",
                )
            except WarehouseError:
                logger.exception(
                    "source_file_failure_status_unavailable",
                    file_id=str(registration.file_id),
                )

    def _finalize_preflight_failure(self, summary: RunSummary, error: Exception) -> None:
        """Retain generation/preflight failures before any source object exists."""
        summary.status = RunStatus.FAILED
        summary.finished_at = datetime.now(UTC)
        summary.duration_seconds = max(
            0.0, (summary.finished_at - summary.started_at).total_seconds()
        )
        summary.error_message = _safe_error_message(error)
        human_summary = build_human_summary(summary, [])
        try:
            self._repository.finish_run(summary, human_summary)
        except WarehouseError:
            logger.exception("pipeline_preflight_finalization_failed", run_id=str(summary.run_id))
        logger.exception(
            "pipeline_preflight_failed",
            run_id=str(summary.run_id),
            error_type=type(error).__name__,
        )

    def _process_source(
        self,
        *,
        summary: RunSummary,
        source_name: str,
        records: Sequence[Mapping[str, Any]],
        contract_result: ContractResult,
        landed_source: LandedSource,
        quality_results: list[QualityResult],
    ) -> None:
        filename = f"{source_name}.csv"
        registration = landed_source.registration
        quality_results.append(
            volume_anomaly_result(
                run_id=summary.run_id,
                source_name=source_name,
                observed_rows=len(records),
                healthy_history=self._repository.source_volume_history(
                    source_name,
                    before=summary.started_at,
                    batch_kind=(
                        "incremental" if "-incremental-" in summary.batch_id else "historical"
                    ),
                    limit=7,
                ),
            )
        )
        if registration.duplicate:
            summary.skipped_file_count += 1
            quality_results.append(
                QualityResult(
                    check_id=f"duplicate_file:{source_name}",
                    run_id=summary.run_id,
                    check_name="duplicate file prevention",
                    check_type="idempotency",
                    scope=source_name,
                    status="passed",
                    severity=Severity.INFO,
                    observed_value="existing checksum",
                    expected="identical content is skipped",
                    evidence={"checksum": landed_source.checksum, "action": "skipped"},
                )
            )
            return

        if registration.changed_logical_file:
            quality_results.append(
                QualityResult(
                    check_id=f"changed_file:{source_name}",
                    run_id=summary.run_id,
                    check_name="changed logical file",
                    check_type="file_integrity",
                    scope=source_name,
                    status="warning",
                    severity=Severity.WARNING,
                    observed_value=landed_source.checksum,
                    expected="a logical file path normally retains its checksum",
                    evidence={"logical_key": f"{summary.batch_id}/{filename}"},
                )
            )

        accepted = _attach_source_row_numbers(source_name, records, contract_result)
        ingested_at = datetime.now(UTC)
        loaded_count, quarantined_count = self._repository.commit_source_result(
            source_name,
            accepted,
            run_id=summary.run_id,
            batch_id=summary.batch_id,
            file_id=registration.file_id,
            ingested_at=ingested_at,
            quarantined_records=contract_result.quarantined_records,
            schema_changes=contract_result.schema_changes,
        )
        summary.accepted_row_count += loaded_count
        summary.quarantined_row_count += quarantined_count
        summary.schema_changes.extend(contract_result.schema_changes)
        quality_results.extend(
            contract_quality_results(
                run_id=summary.run_id,
                source_name=source_name,
                result=contract_result,
            )
        )

    def _land_source(
        self,
        *,
        summary: RunSummary,
        source_name: str,
        records: Sequence[Mapping[str, Any]],
        raw_content: bytes | None = None,
        raw_columns: Sequence[str] | None = None,
    ) -> LandedSource:
        """Preserve source bytes and ledger identity before contract evaluation."""
        serialized, inferred_columns = serialize_source_csv(records)
        columns = list(raw_columns) if raw_columns is not None else inferred_columns
        content = raw_content if raw_content is not None else serialized
        if len(content) > self._settings.max_source_file_bytes:
            raise ForgeFlowError(
                f"Serialized source exceeds the configured byte limit: {source_name}"
            )
        filename = f"{source_name}.csv"
        landed = self._object_store.put_bytes(
            content=content,
            source_name=source_name,
            batch_id=summary.batch_id,
            filename=filename,
        )
        summary.source_file_count += 1
        summary.source_row_count += len(records)
        registration = self._repository.register_source_file(
            run_id=summary.run_id,
            batch_id=summary.batch_id,
            source_name=source_name,
            logical_key=f"{summary.batch_id}/{filename}",
            object_key=landed.object_key,
            checksum=landed.checksum,
            schema_fingerprint=schema_fingerprint(columns),
            size_bytes=landed.size_bytes,
            row_count=len(records),
        )
        return LandedSource(registration=registration, checksum=landed.checksum)

    def _finalize_summary(
        self,
        summary: RunSummary,
        results: Sequence[QualityResult],
        dbt_result: DbtRunResult,
    ) -> None:
        summary.passed_checks = sum(result.status == "passed" for result in results)
        summary.failed_checks = sum(result.status == "failed" for result in results)
        error_failure = any(
            result.status == "failed" and result.severity == Severity.ERROR for result in results
        )
        warning = any(result.status == "warning" for result in results)
        if not dbt_result.succeeded or error_failure:
            summary.status = RunStatus.FAILED
        elif summary.quarantined_row_count or warning:
            summary.status = RunStatus.DEGRADED
        else:
            summary.status = RunStatus.HEALTHY
        freshness_results = [
            result
            for result in results
            if "fresh" in result.check_name.lower() or "fresh" in result.check_type.lower()
        ]
        if any(result.status == "failed" for result in freshness_results):
            summary.freshness_status = "stale"
        elif any(result.status == "warning" for result in freshness_results):
            summary.freshness_status = "warning"
        elif freshness_results:
            summary.freshness_status = "fresh"
        summary.finished_at = datetime.now(UTC)
        summary.duration_seconds = max(
            0.0, (summary.finished_at - summary.started_at).total_seconds()
        )

    def _finalize_incident_or_recovery(
        self,
        summary: RunSummary,
        human_summary: Mapping[str, Any],
        *,
        recovery_incident_id: UUID | None,
    ) -> UUID | None:
        del human_summary
        if summary.scenario == FailureScenario.INCIDENT and summary.status == RunStatus.FAILED:
            failed_run = self._repository.get_run(summary.run_id)
            if failed_run is None:
                raise WarehouseError("Failed run was finalized but could not be read back")
            baseline = self._repository.latest_healthy_run_before(summary.started_at)
            failed_checks, _ = self._repository.list_failed_checks(
                run_id=summary.run_id,
                limit=self._settings.max_page_size,
                offset=0,
            )
            quarantine = self._repository.quarantine_summary(summary.run_id)
            incident_id = uuid4()
            evidence = evidence_from_run(
                incident_id=incident_id,
                failed_run=failed_run,
                baseline_run=baseline,
                failed_checks=failed_checks,
                quarantine_summary=quarantine,
            )
            explanation = DeterministicExplanationProvider().explain(evidence)
            self._repository.create_incident(
                incident_id=incident_id,
                failed_run_id=summary.run_id,
                baseline_run_id=(UUID(str(baseline["run_id"])) if baseline else None),
                title=f"ForgeFlow incident for batch {summary.batch_id}",
                evidence=evidence.model_dump(mode="json"),
                explanation=explanation.model_dump(mode="json"),
            )
            if self._settings.ai_provider == "openai":
                try:
                    enriched = build_explanation_provider(self._settings).explain(evidence)
                    self._repository.update_incident_explanation(
                        incident_id,
                        enriched.model_dump(mode="json"),
                    )
                except ForgeFlowError as error:
                    logger.warning(
                        "incident_explanation_enrichment_failed",
                        incident_id=str(incident_id),
                        error_type=type(error).__name__,
                    )
            return incident_id
        if (
            summary.scenario == FailureScenario.RECOVERY
            and summary.status == RunStatus.HEALTHY
            and recovery_incident_id is not None
        ):
            incident = self._repository.get_incident(recovery_incident_id)
            if incident is None:
                raise WarehouseError(f"Recovery incident {recovery_incident_id} was not found")
            if incident.get("status") != "open":
                raise WarehouseError(f"Recovery incident {recovery_incident_id} is not open")
            self._repository.resolve_incident(recovery_incident_id, summary.run_id)
            return recovery_incident_id
        return None


def contract_quality_results(
    *, run_id: UUID, source_name: str, result: ContractResult
) -> list[QualityResult]:
    """Normalize contract, drift, duplicate, and row failures into quality checks."""
    checks: list[QualityResult] = []
    rejected = len(result.quarantined_records)
    checks.append(
        QualityResult(
            check_id=f"contract_validity:{source_name}",
            run_id=run_id,
            check_name="contract validity",
            check_type="validity",
            scope=source_name,
            status="warning" if rejected else "passed",
            severity=Severity.WARNING if rejected else Severity.INFO,
            observed_value=rejected,
            expected="zero rows violate the ingestion contract",
            evidence={
                "contract_version": SOURCE_CONTRACTS[source_name].version,
                "source_rows": result.source_rows,
                "accepted_rows": len(result.accepted_records),
                "quarantined_rows": rejected,
            },
        )
    )
    reason_counts = Counter(
        reason.code for record in result.quarantined_records for reason in record.reasons
    )
    for reason_code, count in sorted(reason_counts.items()):
        checks.append(
            QualityResult(
                check_id=f"contract_reason:{source_name}:{reason_code}",
                run_id=run_id,
                check_name=reason_code.replace("_", " "),
                check_type="contract",
                scope=source_name,
                status="warning",
                severity=Severity.WARNING,
                observed_value=count,
                expected="zero rejected records",
                evidence={"reason_code": reason_code, "count": count},
            )
        )
    for index, change in enumerate(result.schema_changes):
        breaking = change.change_type == "breaking"
        checks.append(
            QualityResult(
                check_id=f"schema_change:{source_name}:{index}",
                run_id=run_id,
                check_name="schema drift",
                check_type="schema",
                scope=source_name,
                status="failed" if breaking else "warning",
                severity=Severity.ERROR if breaking else Severity.WARNING,
                observed_value=change.change_type,
                expected="source columns match the registered contract",
                evidence=change.model_dump(mode="json"),
            )
        )
    if source_name == "machine_telemetry":
        late_records = [
            record for record in result.accepted_records if _arrival_delay_seconds(record) > 86_400
        ]
        checks.append(
            QualityResult(
                check_id="late_arrival:machine_telemetry",
                run_id=run_id,
                check_name="telemetry arrival freshness",
                check_type="freshness",
                scope=source_name,
                status="warning" if late_records else "passed",
                severity=Severity.WARNING if late_records else Severity.INFO,
                observed_value=len(late_records),
                expected="telemetry is updated within 24 hours of event time",
                evidence={
                    "late_record_count": len(late_records),
                    "example_telemetry_ids": [
                        str(record.get("telemetry_id")) for record in late_records[:5]
                    ],
                    "threshold_hours": 24,
                },
            )
        )
    return checks


def volume_anomaly_result(
    *,
    run_id: UUID,
    source_name: str,
    observed_rows: int,
    healthy_history: Sequence[int],
) -> QualityResult:
    """Evaluate a transparent median/MAD source-volume heuristic."""
    bounded_history = [max(0, int(value)) for value in healthy_history[:7]]
    minimum_samples = 3
    evidence: dict[str, Any] = {
        "method": "median_mad",
        "healthy_history_rows": bounded_history,
        "minimum_samples": minimum_samples,
        "evaluated": len(bounded_history) >= minimum_samples,
    }
    if len(bounded_history) < minimum_samples:
        return QualityResult(
            check_id=f"volume_anomaly:{source_name}",
            run_id=run_id,
            check_name="source volume anomaly",
            check_type="volume",
            scope=source_name,
            status="passed",
            severity=Severity.INFO,
            observed_value=observed_rows,
            expected="collect at least three prior healthy volumes before anomaly evaluation",
            evidence=evidence,
        )

    baseline = float(median(bounded_history))
    absolute_deviations = [abs(value - baseline) for value in bounded_history]
    mad = float(median(absolute_deviations))
    tolerance = max(1.0, baseline * 0.20, 3.0 * 1.4826 * mad)
    lower_bound = max(0.0, baseline - tolerance)
    upper_bound = baseline + tolerance
    anomalous = observed_rows < lower_bound or observed_rows > upper_bound
    evidence.update(
        {
            "baseline_median": baseline,
            "median_absolute_deviation": mad,
            "tolerance": tolerance,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "relative_floor": 0.20,
            "mad_scale": 1.4826,
            "mad_multiplier": 3.0,
        }
    )
    return QualityResult(
        check_id=f"volume_anomaly:{source_name}",
        run_id=run_id,
        check_name="source volume anomaly",
        check_type="volume",
        scope=source_name,
        status="warning" if anomalous else "passed",
        severity=Severity.WARNING if anomalous else Severity.INFO,
        observed_value=observed_rows,
        expected=(
            f"source rows remain between {lower_bound:.2f} and {upper_bound:.2f} "
            "from prior healthy median/MAD evidence"
        ),
        evidence=evidence,
    )


def serialize_source_csv(records: Sequence[Mapping[str, Any]]) -> tuple[bytes, list[str]]:
    """Serialize source rows deterministically while preserving observed column order."""
    columns = list(dict.fromkeys(key for record in records for key in record))
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n", extrasaction="raise")
    writer.writeheader()
    for record in records:
        writer.writerow({column: _csv_value(record.get(column)) for column in columns})
    return buffer.getvalue().encode("utf-8"), columns


def write_dataset(dataset: Mapping[str, Sequence[Mapping[str, Any]]], destination: Path) -> Path:
    """Write generated source CSVs and a checksum-neutral manifest for CLI handoff."""
    destination.mkdir(parents=True, exist_ok=True)
    row_counts: dict[str, int] = {}
    for source_name in SOURCE_NAMES:
        records = dataset.get(source_name)
        if records is None:
            continue
        content, _ = serialize_source_csv(records)
        (destination / f"{source_name}.csv").write_bytes(content)
        row_counts[source_name] = len(records)
    (destination / "manifest.json").write_text(
        json.dumps({"sources": row_counts}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return destination


def build_batch_id(
    *, scenario: FailureScenario, batch_date: date, seed: int, incremental: bool
) -> str:
    """Build a reproducible, reviewer-readable batch identity."""
    kind = "incremental" if incremental else "historical"
    return f"{batch_date.isoformat()}-{kind}-{scenario.value}-s{seed}"


def build_human_summary(summary: RunSummary, results: Sequence[QualityResult]) -> dict[str, Any]:
    """Build evidence-oriented run text without claiming a root cause."""
    failed = [result for result in results if result.status == "failed"]
    warnings = [result for result in results if result.status == "warning"]
    facts = [
        f"Run {summary.run_id} finished with status {summary.status.value}.",
        f"Accepted {summary.accepted_row_count} rows and quarantined "
        f"{summary.quarantined_row_count} rows.",
        f"Recorded {len(failed)} failed and {len(warnings)} warning checks.",
    ]
    return {
        "observed_facts": facts,
        "likely_explanations": [],
        "recommended_next_steps": (
            ["Inspect failed checks, quarantine reasons, and direct lineage parents."]
            if failed or warnings
            else ["No action required; retain the run as the comparison baseline."]
        ),
        "uncertainty_note": "This run summary reports recorded evidence and does not infer causation.",
    }


def _attach_source_row_numbers(
    source_name: str,
    raw_records: Sequence[Mapping[str, Any]],
    result: ContractResult,
) -> list[dict[str, Any]]:
    contract = SOURCE_CONTRACTS[source_name]
    first_row_by_key: dict[Any, int] = {}
    for row_number, record in enumerate(raw_records, start=2):
        key = record.get(contract.primary_key)
        try:
            first_row_by_key.setdefault(key, row_number)
        except TypeError:
            continue
    accepted: list[dict[str, Any]] = []
    for record in result.accepted_records:
        row = dict(record)
        row["_source_row_number"] = first_row_by_key.get(record.get(contract.primary_key), 2)
        accepted.append(row)
    return accepted


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _raw_csv_value_matches(raw_value: str | None, parsed_value: Any, *, kind: str | None) -> bool:
    """Compare one exact CSV cell with its parsed representation without lossy reserialization."""
    if raw_value is None or raw_value == "":
        return parsed_value is None
    if kind == "integer":
        try:
            return bool(int(raw_value) == parsed_value)
        except ValueError:
            return bool(raw_value == parsed_value)
    if kind == "number":
        try:
            numeric = float(raw_value)
        except ValueError:
            return bool(raw_value == parsed_value)
        if not math.isfinite(numeric):
            return bool(raw_value == parsed_value)
        return bool(numeric == parsed_value)
    return bool(raw_value == parsed_value)


def _safe_error_message(error: Exception) -> str:
    message = str(error).strip() or type(error).__name__
    return message[:2_000]


def _arrival_delay_seconds(record: Mapping[str, Any]) -> float:
    try:
        event_time = datetime.fromisoformat(str(record["event_timestamp"]).replace("Z", "+00:00"))
        updated_at = datetime.fromisoformat(str(record["updated_at"]).replace("Z", "+00:00"))
    except (KeyError, TypeError, ValueError):
        return 0.0
    return max(0.0, (updated_at - event_time).total_seconds())
