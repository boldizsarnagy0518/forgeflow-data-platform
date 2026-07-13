"""PostgreSQL warehouse and metadata repository."""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import psycopg
from psycopg import Connection, Cursor, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from forgeflow.config import Settings
from forgeflow.errors import WarehouseError
from forgeflow.models import (
    QualityResult,
    QuarantinedRecord,
    RunSummary,
    SchemaChange,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, slots=True)
class RawTable:
    """Whitelisted source-to-table mapping used for safe dynamic SQL."""

    columns: tuple[str, ...]
    business_key: str


RAW_TABLES: dict[str, RawTable] = {
    "factories": RawTable(
        (
            "factory_id",
            "factory_name",
            "country_code",
            "timezone",
            "opened_on",
            "status",
            "updated_at",
        ),
        "factory_id",
    ),
    "production_lines": RawTable(
        (
            "production_line_id",
            "factory_id",
            "line_name",
            "product_family",
            "nominal_capacity_per_hour",
            "status",
            "updated_at",
        ),
        "production_line_id",
    ),
    "machines": RawTable(
        (
            "machine_id",
            "production_line_id",
            "machine_name",
            "machine_type",
            "manufacturer",
            "model",
            "installed_on",
            "status",
            "updated_at",
        ),
        "machine_id",
    ),
    "shifts": RawTable(
        (
            "shift_id",
            "factory_id",
            "shift_name",
            "started_at",
            "ended_at",
            "operator_id",
            "updated_at",
        ),
        "shift_id",
    ),
    "production_orders": RawTable(
        (
            "production_order_id",
            "production_line_id",
            "product_code",
            "planned_start_at",
            "planned_end_at",
            "actual_start_at",
            "actual_end_at",
            "planned_quantity",
            "actual_quantity",
            "status",
            "updated_at",
        ),
        "production_order_id",
    ),
    "machine_telemetry": RawTable(
        (
            "telemetry_id",
            "machine_id",
            "event_timestamp",
            "temperature_c",
            "vibration_mm_s",
            "pressure_bar",
            "energy_kwh",
            "operating_state",
            "updated_at",
        ),
        "telemetry_id",
    ),
    "downtime_events": RawTable(
        (
            "downtime_event_id",
            "machine_id",
            "started_at",
            "ended_at",
            "downtime_type",
            "reason_code",
            "updated_at",
        ),
        "downtime_event_id",
    ),
    "maintenance_work_orders": RawTable(
        (
            "maintenance_work_order_id",
            "machine_id",
            "created_at",
            "scheduled_for",
            "completed_at",
            "maintenance_type",
            "priority",
            "status",
            "technician_id",
            "updated_at",
        ),
        "maintenance_work_order_id",
    ),
    "quality_inspections": RawTable(
        (
            "quality_inspection_id",
            "production_order_id",
            "inspected_at",
            "sample_size",
            "passed_units",
            "failed_units",
            "result",
            "inspector_id",
            "updated_at",
        ),
        "quality_inspection_id",
    ),
    "product_defects": RawTable(
        (
            "product_defect_id",
            "quality_inspection_id",
            "detected_at",
            "defect_type",
            "severity",
            "defect_count",
            "updated_at",
        ),
        "product_defect_id",
    ),
}

LINEAGE_COLUMNS = (
    "_batch_id",
    "_source_file_id",
    "_source_row_number",
    "_ingested_at",
    "_record_checksum",
)

TERMINAL_SOURCE_FILE_STATUSES = frozenset({"loaded", "quarantined", "skipped"})
DBT_ADVISORY_LOCK_KEY = 7_301_202_507


@dataclass(frozen=True, slots=True)
class FileRegistration:
    """Ledger decision for one landed source file."""

    file_id: UUID
    duplicate: bool
    changed_logical_file: bool
    existing_object_key: str | None = None


class PostgresRepository:
    """Typed PostgreSQL boundary for ingestion and reviewer-facing metadata reads."""

    def __init__(self, settings: Settings) -> None:
        self._dsn = settings.database_url.get_secret_value()

    @contextmanager
    def _connection(self) -> Iterator[Connection[dict[str, Any]]]:
        try:
            with psycopg.connect(self._dsn, row_factory=dict_row) as connection:
                yield connection
        except psycopg.Error as error:
            raise WarehouseError("PostgreSQL operation failed") from error

    def initialize(self, script_path: Path | None = None) -> None:
        """Apply the idempotent local warehouse bootstrap SQL."""
        path = script_path or PROJECT_ROOT / "infra" / "postgres" / "init"
        try:
            sql_paths = sorted(path.glob("*.sql")) if path.is_dir() else [path]
            statements = "\n".join(sql_path.read_text(encoding="utf-8") for sql_path in sql_paths)
        except OSError as error:
            raise WarehouseError(f"Unable to read warehouse bootstrap SQL at {path}") from error
        if not statements.strip():
            raise WarehouseError(f"No warehouse bootstrap SQL was found at {path}")
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(statements)

    def ping(self) -> bool:
        """Return whether PostgreSQL accepts a bounded read."""
        try:
            with self._connection() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1 AS healthy")
                row = cursor.fetchone()
        except WarehouseError:
            return False
        return row == {"healthy": 1}

    def start_run(self, summary: RunSummary) -> None:
        """Persist a running record before any external side effect."""
        query = """
            INSERT INTO observability.pipeline_runs (
                run_id, batch_id, scenario, status, started_at, source_file_count,
                source_row_count, accepted_row_count, quarantined_row_count,
                skipped_file_count, model_row_counts, test_counts, passed_checks,
                failed_checks, freshness_status, schema_changes,
                affected_downstream_models
            ) VALUES (
                %(run_id)s, %(batch_id)s, %(scenario)s, %(status)s, %(started_at)s, 0,
                0, 0, 0, 0, '{}'::jsonb, '{}'::jsonb, 0, 0, 'unknown', '[]'::jsonb,
                '[]'::jsonb
            )
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                query,
                {
                    "run_id": summary.run_id,
                    "batch_id": summary.batch_id,
                    "scenario": summary.scenario.value,
                    "status": summary.status.value,
                    "started_at": summary.started_at,
                },
            )

    def finish_run(self, summary: RunSummary, human_summary: Mapping[str, Any]) -> None:
        """Finalize all run evidence in one update, including failed runs."""
        query = """
            UPDATE observability.pipeline_runs SET
                status = %(status)s,
                finished_at = %(finished_at)s,
                duration_seconds = %(duration_seconds)s,
                source_file_count = %(source_file_count)s,
                source_row_count = %(source_row_count)s,
                accepted_row_count = %(accepted_row_count)s,
                quarantined_row_count = %(quarantined_row_count)s,
                skipped_file_count = %(skipped_file_count)s,
                model_row_counts = %(model_row_counts)s,
                test_counts = %(test_counts)s,
                passed_checks = %(passed_checks)s,
                failed_checks = %(failed_checks)s,
                freshness_status = %(freshness_status)s,
                schema_changes = %(schema_changes)s,
                affected_downstream_models = %(affected_downstream_models)s,
                error_message = %(error_message)s,
                summary = %(summary)s
            WHERE run_id = %(run_id)s
        """
        payload = summary.model_dump(mode="json")
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                query,
                {
                    **payload,
                    "status": summary.status.value,
                    "model_row_counts": Jsonb(summary.model_row_counts),
                    "test_counts": Jsonb(summary.test_counts),
                    "schema_changes": Jsonb(
                        [change.model_dump(mode="json") for change in summary.schema_changes]
                    ),
                    "affected_downstream_models": Jsonb(summary.affected_downstream_models),
                    "summary": Jsonb(dict(human_summary)),
                },
            )
            if cursor.rowcount != 1:
                raise WarehouseError(f"Run {summary.run_id} was not available to finalize")

    def start_stage(
        self,
        run_id: UUID,
        stage_name: str,
        *,
        input_row_count: int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Start or safely restart one named pipeline stage for a run."""
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO observability.pipeline_stages (
                    run_id, stage_name, status, started_at, input_row_count, metadata
                ) VALUES (%s, %s, 'running', CURRENT_TIMESTAMP, %s, %s)
                ON CONFLICT (run_id, stage_name) DO UPDATE SET
                    status = 'running',
                    started_at = CURRENT_TIMESTAMP,
                    finished_at = NULL,
                    duration_seconds = NULL,
                    input_row_count = EXCLUDED.input_row_count,
                    output_row_count = NULL,
                    error_message = NULL,
                    metadata = EXCLUDED.metadata
                """,
                (run_id, stage_name, input_row_count, Jsonb(dict(metadata or {}))),
            )

    def finish_stage(
        self,
        run_id: UUID,
        stage_name: str,
        *,
        status: str,
        output_row_count: int | None = None,
        error_message: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Finalize stage timing, counts, bounded error context, and metadata."""
        if status not in {"succeeded", "failed", "skipped"}:
            raise WarehouseError(f"Unsupported pipeline stage status: {status!r}")
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE observability.pipeline_stages SET
                    status = %s,
                    finished_at = CURRENT_TIMESTAMP,
                    duration_seconds = GREATEST(
                        0,
                        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at))
                    ),
                    output_row_count = %s,
                    error_message = %s,
                    metadata = metadata || %s
                WHERE run_id = %s AND stage_name = %s
                """,
                (
                    status,
                    output_row_count,
                    error_message[:2_000] if error_message else None,
                    Jsonb(dict(metadata or {})),
                    run_id,
                    stage_name,
                ),
            )
            if cursor.rowcount != 1:
                raise WarehouseError(f"Pipeline stage {stage_name!r} was not available to finalize")

    def list_run_stages(self, run_id: UUID) -> list[dict[str, Any]]:
        """Return ordered stage evidence for one run."""
        return self._fetch_all(
            """
            SELECT stage_name, status, started_at, finished_at, duration_seconds,
                   input_row_count, output_row_count, error_message, metadata
            FROM observability.pipeline_stages
            WHERE run_id = %s
            ORDER BY started_at, stage_id
            LIMIT 50
            """,
            (run_id,),
        )

    def register_source_file(
        self,
        *,
        run_id: UUID,
        batch_id: str,
        source_name: str,
        logical_key: str,
        object_key: str,
        checksum: str,
        schema_fingerprint: str,
        size_bytes: int,
        row_count: int,
    ) -> FileRegistration:
        """Record content identity and detect duplicate or changed logical files."""
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT file_id, object_key, status
                FROM observability.source_files
                WHERE source_name = %s AND checksum = %s
                ORDER BY created_at ASC LIMIT 1
                FOR UPDATE
                """,
                (source_name, checksum),
            )
            existing = cursor.fetchone()
            if existing is not None:
                file_id = UUID(str(existing["file_id"]))
                if str(existing["status"]) in TERMINAL_SOURCE_FILE_STATUSES:
                    return FileRegistration(
                        file_id=file_id,
                        duplicate=True,
                        changed_logical_file=False,
                        existing_object_key=str(existing["object_key"]),
                    )

                cursor.execute(
                    """
                    UPDATE observability.source_files SET
                        run_id = %s,
                        batch_id = %s,
                        logical_key = %s,
                        object_key = %s,
                        schema_fingerprint = %s,
                        size_bytes = %s,
                        row_count = %s,
                        accepted_count = 0,
                        quarantined_count = 0,
                        status = 'landed',
                        processed_at = NULL
                    WHERE file_id = %s
                      AND status NOT IN ('loaded', 'quarantined', 'skipped')
                    """,
                    (
                        run_id,
                        batch_id,
                        logical_key,
                        object_key,
                        schema_fingerprint,
                        size_bytes,
                        row_count,
                        file_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise WarehouseError(f"Source file {file_id} could not be prepared for retry")
                return FileRegistration(
                    file_id=file_id,
                    duplicate=False,
                    changed_logical_file=False,
                )

            cursor.execute(
                """
                SELECT checksum FROM observability.source_files
                WHERE logical_key = %s ORDER BY created_at DESC LIMIT 1
                """,
                (logical_key,),
            )
            prior = cursor.fetchone()
            changed = prior is not None and str(prior["checksum"]) != checksum
            file_id = uuid4()
            cursor.execute(
                """
                INSERT INTO observability.source_files (
                    file_id, run_id, batch_id, source_name, logical_key, object_key,
                    checksum, schema_fingerprint, size_bytes, row_count, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'landed')
                """,
                (
                    file_id,
                    run_id,
                    batch_id,
                    source_name,
                    logical_key,
                    object_key,
                    checksum,
                    schema_fingerprint,
                    size_bytes,
                    row_count,
                ),
            )
        return FileRegistration(file_id=file_id, duplicate=False, changed_logical_file=changed)

    def complete_source_file(
        self,
        file_id: UUID,
        *,
        accepted_count: int,
        quarantined_count: int,
        status: str,
    ) -> None:
        """Persist final per-file validation and load counts."""
        with self._connection() as connection, connection.cursor() as cursor:
            self._complete_source_file(
                cursor,
                file_id,
                accepted_count=accepted_count,
                quarantined_count=quarantined_count,
                status=status,
            )

    def record_schema_changes(
        self,
        *,
        run_id: UUID,
        file_id: UUID,
        changes: Sequence[SchemaChange],
    ) -> None:
        """Persist each detected additive or breaking source shape change."""
        if not changes:
            return
        with self._connection() as connection, connection.cursor() as cursor:
            self._record_schema_changes(cursor, run_id=run_id, file_id=file_id, changes=changes)

    def load_records(
        self,
        source_name: str,
        records: Sequence[Mapping[str, Any]],
        *,
        batch_id: str,
        file_id: UUID,
        ingested_at: datetime,
    ) -> int:
        """Idempotently upsert validated source rows into a whitelisted raw table."""
        if not records:
            return 0
        with self._connection() as connection, connection.cursor() as cursor:
            return self._load_records(
                cursor,
                source_name,
                records,
                batch_id=batch_id,
                file_id=file_id,
                ingested_at=ingested_at,
            )

    def quarantine_records(
        self,
        *,
        run_id: UUID,
        file_id: UUID,
        records: Sequence[QuarantinedRecord],
    ) -> int:
        """Persist rejected source rows and all structured reasons."""
        if not records:
            return 0
        with self._connection() as connection, connection.cursor() as cursor:
            return self._quarantine_records(
                cursor,
                run_id=run_id,
                file_id=file_id,
                records=records,
            )

    def commit_source_result(
        self,
        source_name: str,
        accepted_records: Sequence[Mapping[str, Any]],
        *,
        run_id: UUID,
        batch_id: str,
        file_id: UUID,
        ingested_at: datetime,
        quarantined_records: Sequence[QuarantinedRecord],
        schema_changes: Sequence[SchemaChange],
    ) -> tuple[int, int]:
        """Atomically persist every outcome for one non-duplicate landed source file."""
        with self._connection() as connection, connection.cursor() as cursor:
            loaded_count = self._load_records(
                cursor,
                source_name,
                accepted_records,
                batch_id=batch_id,
                file_id=file_id,
                ingested_at=ingested_at,
            )
            quarantined_count = self._quarantine_records(
                cursor,
                run_id=run_id,
                file_id=file_id,
                records=quarantined_records,
            )
            self._record_schema_changes(
                cursor,
                run_id=run_id,
                file_id=file_id,
                changes=schema_changes,
            )
            has_breaking_schema = any(change.change_type == "breaking" for change in schema_changes)
            self._complete_source_file(
                cursor,
                file_id,
                accepted_count=loaded_count,
                quarantined_count=quarantined_count,
                status=(
                    "quarantined"
                    if loaded_count == 0 and (quarantined_count > 0 or has_breaking_schema)
                    else "loaded"
                ),
                require_landed=True,
            )
        return loaded_count, quarantined_count

    @staticmethod
    def _complete_source_file(
        cursor: Cursor[dict[str, Any]],
        file_id: UUID,
        *,
        accepted_count: int,
        quarantined_count: int,
        status: str,
        require_landed: bool = False,
    ) -> None:
        query = (
            """
            UPDATE observability.source_files
            SET accepted_count = %s, quarantined_count = %s, status = %s,
                processed_at = CURRENT_TIMESTAMP
            WHERE file_id = %s AND status = 'landed'
            """
            if require_landed
            else """
            UPDATE observability.source_files
            SET accepted_count = %s, quarantined_count = %s, status = %s,
                processed_at = CURRENT_TIMESTAMP
            WHERE file_id = %s
            """
        )
        cursor.execute(query, (accepted_count, quarantined_count, status, file_id))
        if require_landed and cursor.rowcount != 1:
            raise WarehouseError(
                f"Source file {file_id} was not available for one atomic completion"
            )

    @staticmethod
    def _record_schema_changes(
        cursor: Cursor[dict[str, Any]],
        *,
        run_id: UUID,
        file_id: UUID,
        changes: Sequence[SchemaChange],
    ) -> None:
        if not changes:
            return
        rows = [
            (
                uuid4(),
                run_id,
                file_id,
                change.source_name,
                change.change_type,
                Jsonb(change.expected_columns),
                Jsonb(change.actual_columns),
                Jsonb(change.missing_columns),
                Jsonb(change.unexpected_columns),
            )
            for change in changes
        ]
        cursor.executemany(
            """
            INSERT INTO observability.schema_changes (
                change_id, run_id, file_id, source_name, change_type,
                expected_columns, actual_columns, missing_columns, unexpected_columns
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )

    @staticmethod
    def _load_records(
        cursor: Cursor[dict[str, Any]],
        source_name: str,
        records: Sequence[Mapping[str, Any]],
        *,
        batch_id: str,
        file_id: UUID,
        ingested_at: datetime,
    ) -> int:
        if not records:
            return 0
        table = RAW_TABLES.get(source_name)
        if table is None:
            raise WarehouseError(f"No raw-table mapping is registered for {source_name!r}")
        columns = (*table.columns, *LINEAGE_COLUMNS)
        normalized: list[tuple[Any, ...]] = []
        for fallback_row_number, record in enumerate(records, start=2):
            values = [_normalize_value(record.get(column)) for column in table.columns]
            checksum = _record_checksum(record, table.columns)
            row_number = int(record.get("_source_row_number", fallback_row_number))
            normalized.append((*values, batch_id, file_id, row_number, ingested_at, checksum))

        identifiers = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        update_columns = tuple(column for column in columns if column != table.business_key)
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
            for column in update_columns
        )
        statement = sql.SQL(
            """
            INSERT INTO raw.{table} ({columns}) VALUES ({placeholders})
            ON CONFLICT ({business_key}) DO UPDATE SET {assignments}
            WHERE raw.{table}.{record_checksum} IS DISTINCT FROM EXCLUDED.{record_checksum}
              AND EXCLUDED.{updated_at} >= raw.{table}.{updated_at}
            """
        ).format(
            table=sql.Identifier(source_name),
            columns=identifiers,
            placeholders=placeholders,
            business_key=sql.Identifier(table.business_key),
            assignments=assignments,
            record_checksum=sql.Identifier("_record_checksum"),
            updated_at=sql.Identifier("updated_at"),
        )
        cursor.executemany(statement, normalized)
        return len(normalized)

    @staticmethod
    def _quarantine_records(
        cursor: Cursor[dict[str, Any]],
        *,
        run_id: UUID,
        file_id: UUID,
        records: Sequence[QuarantinedRecord],
    ) -> int:
        if not records:
            return 0
        rows = [
            (
                uuid4(),
                run_id,
                file_id,
                record.source_name,
                record.source_row_number,
                Jsonb(_json_payload(record.raw_payload)),
                Jsonb(
                    _json_payload([reason.model_dump(mode="python") for reason in record.reasons])
                ),
            )
            for record in records
        ]
        cursor.executemany(
            """
            INSERT INTO quarantine.records (
                quarantine_id, run_id, file_id, source_name, source_row_number,
                raw_payload, reasons
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (file_id, source_row_number) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                source_name = EXCLUDED.source_name,
                raw_payload = EXCLUDED.raw_payload,
                reasons = EXCLUDED.reasons,
                quarantined_at = CURRENT_TIMESTAMP
            """,
            rows,
        )
        return len(rows)

    def record_quality_results(self, results: Sequence[QualityResult]) -> None:
        """Persist normalized checks from every quality layer."""
        if not results:
            return
        rows = [
            (
                result.check_id,
                result.run_id,
                result.check_name,
                result.check_type,
                result.scope,
                result.status,
                result.severity.value,
                str(result.observed_value) if result.observed_value is not None else None,
                result.expected,
                Jsonb(result.evidence),
                result.occurred_at,
            )
            for result in results
        ]
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO observability.quality_results (
                    check_id, run_id, check_name, check_type, scope, status, severity,
                    observed_value, expected, evidence, occurred_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (check_id, run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    observed_value = EXCLUDED.observed_value,
                    evidence = EXCLUDED.evidence,
                    occurred_at = EXCLUDED.occurred_at
                """,
                rows,
            )

    def record_dbt_artifact(
        self, run_id: UUID, artifact_type: str, artifact: Mapping[str, Any]
    ) -> None:
        """Retain a bounded dbt artifact required for diagnosis and lineage."""
        encoded = json.dumps(artifact, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > 20_000_000:
            raise WarehouseError(f"dbt artifact {artifact_type!r} exceeded the 20 MB safety limit")
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO observability.dbt_artifacts (run_id, artifact_type, artifact_json)
                VALUES (%s, %s, %s)
                ON CONFLICT (run_id, artifact_type) DO UPDATE
                SET artifact_json = EXCLUDED.artifact_json, captured_at = CURRENT_TIMESTAMP
                """,
                (run_id, artifact_type, Jsonb(dict(artifact))),
            )

    @contextmanager
    def dbt_execution_lock(self) -> Iterator[None]:
        """Serialize local dbt relation mutations with a session advisory lock."""
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            cursor.execute("SELECT pg_advisory_lock(%s)", (DBT_ADVISORY_LOCK_KEY,))
            try:
                yield
            finally:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (DBT_ADVISORY_LOCK_KEY,))

    def upsert_model_metadata(
        self,
        run_id: UUID,
        models: Sequence[Mapping[str, Any]],
        columns: Sequence[Mapping[str, Any]],
        lineage_edges: Sequence[Mapping[str, str]],
    ) -> None:
        """Publish parsed dbt descriptions and lineage for all read surfaces."""
        with self._connection() as connection, connection.cursor() as cursor:
            for model in models:
                cursor.execute(
                    """
                    INSERT INTO observability.model_metadata (
                        run_id, unique_id, model_name, resource_type, database_name,
                        schema_name, relation_name, description, materialization,
                        tags, meta, depends_on
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, unique_id) DO UPDATE SET
                        model_name = EXCLUDED.model_name,
                        resource_type = EXCLUDED.resource_type,
                        database_name = EXCLUDED.database_name,
                        schema_name = EXCLUDED.schema_name,
                        relation_name = EXCLUDED.relation_name,
                        description = EXCLUDED.description,
                        materialization = EXCLUDED.materialization,
                        tags = EXCLUDED.tags,
                        meta = EXCLUDED.meta,
                        depends_on = EXCLUDED.depends_on,
                        captured_at = CURRENT_TIMESTAMP
                    """,
                    (
                        run_id,
                        model["unique_id"],
                        model["model_name"],
                        model["resource_type"],
                        model.get("database_name"),
                        model.get("schema_name"),
                        model.get("relation_name"),
                        model.get("description", ""),
                        model.get("materialization"),
                        Jsonb(list(model.get("tags", []))),
                        Jsonb(dict(model.get("meta", {}))),
                        Jsonb(list(model.get("depends_on", []))),
                    ),
                )
            for column in columns:
                cursor.execute(
                    """
                    INSERT INTO observability.model_columns (
                        run_id, model_unique_id, column_name, data_type,
                        description, tests, ordinal_position
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, model_unique_id, column_name) DO UPDATE SET
                        data_type = EXCLUDED.data_type,
                        description = EXCLUDED.description,
                        tests = EXCLUDED.tests,
                        ordinal_position = EXCLUDED.ordinal_position
                    """,
                    (
                        run_id,
                        column["model_unique_id"],
                        column["column_name"],
                        column.get("data_type"),
                        column.get("description", ""),
                        Jsonb(list(column.get("tests", []))),
                        column.get("ordinal_position"),
                    ),
                )
            for edge in lineage_edges:
                cursor.execute(
                    """
                    INSERT INTO observability.lineage_edges (
                        run_id, parent_unique_id, child_unique_id,
                        parent_name, child_name, edge_type
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, parent_unique_id, child_unique_id) DO NOTHING
                    """,
                    (
                        run_id,
                        edge["parent_unique_id"],
                        edge["child_unique_id"],
                        edge["parent_name"],
                        edge["child_name"],
                        edge.get("edge_type", "depends_on"),
                    ),
                )

    def count_model_rows(self, models: Sequence[Mapping[str, Any]]) -> dict[str, int]:
        """Count bounded manifest-declared dbt relations for canonical run evidence."""
        allowed_schemas = {"staging", "intermediate", "marts"}
        candidates: list[tuple[str, str, str]] = []
        for model in models[:100]:
            if model.get("resource_type") != "model":
                continue
            schema_name = model.get("schema_name")
            relation_identifier = model.get("relation_identifier")
            model_name = model.get("model_name")
            if not isinstance(schema_name, str) or not schema_name or len(schema_name) > 63:
                continue
            if (
                not isinstance(relation_identifier, str)
                or not relation_identifier
                or len(relation_identifier) > 63
            ):
                continue
            if not isinstance(model_name, str) or not model_name or len(model_name) > 63:
                continue
            if schema_name not in allowed_schemas:
                continue
            if (
                not schema_name.replace("_", "").isalnum()
                or not relation_identifier.replace("_", "").isalnum()
            ):
                continue
            candidates.append((model_name, schema_name, relation_identifier))

        counts: dict[str, int] = {}
        with self._connection() as connection, connection.cursor() as cursor:
            for model_name, schema_name, relation_identifier in candidates:
                cursor.execute(
                    "SELECT to_regclass(%s) AS relation",
                    (f'"{schema_name}"."{relation_identifier}"',),
                )
                relation = cursor.fetchone()
                if relation is None or relation.get("relation") is None:
                    continue
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) AS row_count FROM {}.{}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(relation_identifier),
                    )
                )
                row = cursor.fetchone()
                if row is not None:
                    counts[model_name] = int(row["row_count"])
        return counts

    def list_runs(self, *, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        """Return a stable newest-first run page and total count."""
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS count FROM observability.pipeline_runs")
            count_row = cursor.fetchone()
            cursor.execute(
                """
                SELECT * FROM observability.pipeline_runs
                ORDER BY started_at DESC, run_id DESC LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows], int(count_row["count"] if count_row else 0)

    def get_run(self, run_id: UUID) -> dict[str, Any] | None:
        """Return one complete persisted run record."""
        return self._fetch_one(
            "SELECT * FROM observability.pipeline_runs WHERE run_id = %s", (run_id,)
        )

    def get_latest_run(self) -> dict[str, Any] | None:
        """Return the most recently started run."""
        return self._fetch_one(
            """
            SELECT * FROM observability.pipeline_runs
            ORDER BY started_at DESC, run_id DESC LIMIT 1
            """,
            (),
        )

    def latest_healthy_run_before(self, started_at: datetime) -> dict[str, Any] | None:
        """Return the most recent healthy baseline strictly before an incident run."""
        return self._fetch_one(
            """
            SELECT * FROM observability.pipeline_runs
            WHERE status = 'healthy' AND started_at < %s
            ORDER BY started_at DESC, run_id DESC LIMIT 1
            """,
            (started_at,),
        )

    def source_volume_history(
        self,
        source_name: str,
        *,
        before: datetime,
        batch_kind: str,
        limit: int = 7,
    ) -> list[int]:
        """Return bounded source-row volumes from comparable prior healthy runs."""
        if source_name not in RAW_TABLES:
            raise WarehouseError(f"No source-volume history is registered for {source_name!r}")
        if batch_kind not in {"historical", "incremental"}:
            raise WarehouseError(f"Unsupported source-volume batch kind: {batch_kind!r}")
        bounded_limit = max(1, min(limit, 30))
        rows = self._fetch_all(
            """
            SELECT source_file.row_count
            FROM observability.source_files AS source_file
            JOIN observability.pipeline_runs AS pipeline_run
              ON pipeline_run.run_id = source_file.run_id
            WHERE source_file.source_name = %s
              AND source_file.status IN ('loaded', 'quarantined')
              AND pipeline_run.status = 'healthy'
              AND pipeline_run.started_at < %s
              AND pipeline_run.batch_id LIKE %s
            ORDER BY pipeline_run.started_at DESC, source_file.created_at DESC
            LIMIT %s
            """,
            (source_name, before, f"%-{batch_kind}-%", bounded_limit),
        )
        return [int(row["row_count"]) for row in rows]

    def quality_summary(self, run_id: UUID | None = None) -> list[dict[str, Any]]:
        """Return check counts grouped by type, status, and severity."""
        resolved = run_id or self._latest_run_id()
        if resolved is None:
            return []
        return self._fetch_all(
            """
            SELECT check_type, status, severity, COUNT(*) AS count
            FROM observability.quality_results WHERE run_id = %s
            GROUP BY check_type, status, severity
            ORDER BY check_type, status, severity
            """,
            (resolved,),
        )

    def list_failed_checks(
        self, *, run_id: UUID | None, limit: int, offset: int
    ) -> tuple[list[dict[str, Any]], int]:
        """Return bounded failed/warning check evidence without table dumps."""
        resolved = run_id or self._latest_run_id()
        if resolved is None:
            return [], 0
        rows = self._fetch_all(
            """
            SELECT check_id, run_id, check_name, check_type, scope, status, severity,
                   observed_value, expected, evidence, occurred_at
            FROM observability.quality_results
            WHERE run_id = %s AND status IN ('failed', 'warning')
            ORDER BY CASE severity
                         WHEN 'error' THEN 0
                         WHEN 'warning' THEN 1
                         ELSE 2
                     END,
                     occurred_at DESC
            LIMIT %s OFFSET %s
            """,
            (resolved, limit, offset),
        )
        count = self._fetch_one(
            """
            SELECT COUNT(*) AS count FROM observability.quality_results
            WHERE run_id = %s AND status IN ('failed', 'warning')
            """,
            (resolved,),
        )
        return rows, int(count["count"] if count else 0)

    def get_failed_check(self, check_id: str, run_id: UUID | None = None) -> dict[str, Any] | None:
        """Return evidence for one check in a run."""
        resolved = run_id or self._latest_run_id()
        if resolved is None:
            return None
        return self._fetch_one(
            """
            SELECT check_id, run_id, check_name, check_type, scope, status, severity,
                   observed_value, expected, evidence, occurred_at
            FROM observability.quality_results WHERE run_id = %s AND check_id = %s
            """,
            (resolved, check_id),
        )

    def quarantine_summary(self, run_id: UUID | None = None) -> list[dict[str, Any]]:
        """Aggregate quarantine reason codes without exposing raw source payloads."""
        resolved = run_id or self._latest_run_id()
        if resolved is None:
            return []
        return self._fetch_all(
            """
            SELECT q.source_name, reason->>'code' AS reason_code, COUNT(*) AS count
            FROM quarantine.records q
            CROSS JOIN LATERAL jsonb_array_elements(q.reasons) AS reason
            WHERE q.run_id = %s
            GROUP BY q.source_name, reason->>'code'
            ORDER BY count DESC, q.source_name, reason_code
            """,
            (resolved,),
        )

    def list_quarantined_records(
        self, *, run_id: UUID | None, limit: int, offset: int
    ) -> tuple[list[dict[str, Any]], int]:
        """List rejection metadata while deliberately omitting raw payloads."""
        resolved = run_id or self._latest_run_id()
        if resolved is None:
            return [], 0
        rows = self._fetch_all(
            """
            SELECT quarantine_id, run_id, source_name, source_row_number, reasons,
                   quarantined_at
            FROM quarantine.records WHERE run_id = %s
            ORDER BY quarantined_at DESC, quarantine_id LIMIT %s OFFSET %s
            """,
            (resolved, limit, offset),
        )
        count = self._fetch_one(
            "SELECT COUNT(*) AS count FROM quarantine.records WHERE run_id = %s",
            (resolved,),
        )
        return rows, int(count["count"] if count else 0)

    def list_models(self, *, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        """Return documented dbt resources."""
        rows = self._fetch_all(
            """
            SELECT DISTINCT ON (model_name)
                   model_name, unique_id, resource_type, database_name, schema_name,
                   relation_name, description, materialization, tags, meta,
                   depends_on, captured_at
            FROM observability.model_metadata
            ORDER BY model_name, captured_at DESC LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        count = self._fetch_one(
            "SELECT COUNT(DISTINCT model_name) AS count FROM observability.model_metadata", ()
        )
        return rows, int(count["count"] if count else 0)

    def get_model(self, model_name: str) -> dict[str, Any] | None:
        """Return metadata for one exact model name."""
        return self._fetch_one(
            """
            SELECT model_name, unique_id, resource_type, database_name, schema_name,
                   relation_name, description, materialization, tags, meta,
                   depends_on, captured_at
            FROM observability.model_metadata WHERE model_name = %s
            ORDER BY captured_at DESC LIMIT 1
            """,
            (model_name,),
        )

    def get_columns(self, model_name: str) -> list[dict[str, Any]]:
        """Return bounded documented columns for one exact model."""
        return self._fetch_all(
            """
            WITH latest_model AS (
                SELECT run_id, unique_id, model_name
                FROM observability.model_metadata WHERE model_name = %s
                ORDER BY captured_at DESC LIMIT 1
            )
            SELECT latest_model.model_name, columns.column_name, columns.data_type,
                   columns.description, columns.tests, columns.ordinal_position
            FROM latest_model
            JOIN observability.model_columns columns
              ON columns.run_id = latest_model.run_id
             AND columns.model_unique_id = latest_model.unique_id
            ORDER BY columns.ordinal_position NULLS LAST, columns.column_name LIMIT 500
            """,
            (model_name,),
        )

    def get_lineage(self, model_name: str) -> dict[str, Any]:
        """Return direct parents and children for one model."""
        parents = self._fetch_all(
            """
            WITH latest_run AS (
                SELECT run_id FROM observability.lineage_edges
                ORDER BY discovered_at DESC LIMIT 1
            )
            SELECT parent_name AS model FROM observability.lineage_edges
            WHERE run_id = (SELECT run_id FROM latest_run) AND child_name = %s
            ORDER BY parent_name
            """,
            (model_name,),
        )
        children = self._fetch_all(
            """
            WITH latest_run AS (
                SELECT run_id FROM observability.lineage_edges
                ORDER BY discovered_at DESC LIMIT 1
            )
            SELECT child_name AS model FROM observability.lineage_edges
            WHERE run_id = (SELECT run_id FROM latest_run) AND parent_name = %s
            ORDER BY child_name
            """,
            (model_name,),
        )
        return {
            "model_name": model_name,
            "parents": [row["model"] for row in parents],
            "children": [row["model"] for row in children],
        }

    def get_downstream_impact(self, model_name: str) -> list[dict[str, Any]]:
        """Traverse downstream lineage with a cycle-safe recursive query."""
        return self._fetch_all(
            """
            WITH RECURSIVE latest_run AS (
                SELECT run_id FROM observability.lineage_edges
                ORDER BY discovered_at DESC LIMIT 1
            ), downstream(model_name, depth, path) AS (
                SELECT child_name, 1, ARRAY[parent_name, child_name]
                FROM observability.lineage_edges
                WHERE run_id = (SELECT run_id FROM latest_run) AND parent_name = %s
                UNION ALL
                SELECT edge.child_name, downstream.depth + 1,
                       downstream.path || edge.child_name
                FROM downstream
                JOIN observability.lineage_edges edge
                  ON edge.parent_name = downstream.model_name
                 AND edge.run_id = (SELECT run_id FROM latest_run)
                WHERE NOT edge.child_name = ANY(downstream.path)
                  AND downstream.depth < 20
            )
            SELECT model_name, MIN(depth) AS depth FROM downstream
            GROUP BY model_name ORDER BY depth, model_name LIMIT 500
            """,
            (model_name,),
        )

    def freshness(self) -> list[dict[str, Any]]:
        """Return materialized machine/source freshness from the dbt mart when available."""
        try:
            return self._fetch_all(
                """
                SELECT * FROM marts.data_freshness
                ORDER BY freshness_status DESC, latest_recorded_at ASC LIMIT 500
                """,
                (),
            )
        except WarehouseError:
            return []

    def factory_performance(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return actual modeled factory metrics for the dashboard."""
        try:
            return self._fetch_all(
                "SELECT * FROM marts.factory_performance ORDER BY factory_id LIMIT %s",
                (limit,),
            )
        except WarehouseError:
            return []

    def quality_trend(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return newest run-level quality trend points."""
        return self._fetch_all(
            """
            SELECT run_id, started_at, status, passed_checks, failed_checks,
                   quarantined_row_count, freshness_status
            FROM observability.pipeline_runs ORDER BY started_at DESC LIMIT %s
            """,
            (limit,),
        )

    def create_incident(
        self,
        *,
        incident_id: UUID | None = None,
        failed_run_id: UUID,
        baseline_run_id: UUID | None,
        title: str,
        evidence: Mapping[str, Any],
        explanation: Mapping[str, Any],
    ) -> UUID:
        """Persist immutable evidence and its current explanation."""
        resolved_incident_id = incident_id or uuid4()
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO observability.incidents (
                    incident_id, failed_run_id, baseline_run_id, status, title,
                    evidence, explanation
                ) VALUES (%s, %s, %s, 'open', %s, %s, %s)
                """,
                (
                    resolved_incident_id,
                    failed_run_id,
                    baseline_run_id,
                    title,
                    Jsonb(dict(evidence)),
                    Jsonb(dict(explanation)),
                ),
            )
        return resolved_incident_id

    def resolve_incident(self, incident_id: UUID, recovery_run_id: UUID) -> None:
        """Resolve an incident without deleting failed-run evidence."""
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE observability.incidents SET status = 'resolved',
                    recovery_run_id = %s, resolved_at = CURRENT_TIMESTAMP
                WHERE incident_id = %s AND status = 'open'
                """,
                (recovery_run_id, incident_id),
            )
            if cursor.rowcount != 1:
                raise WarehouseError(f"Open incident {incident_id} was not available to resolve")

    def update_incident_explanation(
        self, incident_id: UUID, explanation: Mapping[str, Any]
    ) -> None:
        """Replace only an incident's explanation while retaining its evidence."""
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE observability.incidents
                SET explanation = %s
                WHERE incident_id = %s
                """,
                (Jsonb(dict(explanation)), incident_id),
            )
            if cursor.rowcount != 1:
                raise WarehouseError(f"Incident {incident_id} was not available to update")

    def get_incident(self, incident_id: UUID) -> dict[str, Any] | None:
        """Return one incident and its retained evidence."""
        return self._fetch_one(
            "SELECT * FROM observability.incidents WHERE incident_id = %s", (incident_id,)
        )

    def latest_open_incident(self) -> dict[str, Any] | None:
        """Return the newest unresolved incident."""
        return self._fetch_one(
            """
            SELECT * FROM observability.incidents WHERE status = 'open'
            ORDER BY created_at DESC LIMIT 1
            """,
            (),
        )

    def clean_demo_state(self, *, confirmed: bool = False) -> None:
        """Remove only confirmed synthetic demo data while retaining schemas."""
        if not confirmed:
            raise WarehouseError("Demo cleanup requires explicit confirmation")
        database = urlsplit(self._dsn)
        if (
            database.scheme not in {"postgres", "postgresql"}
            or database.hostname not in {"127.0.0.1", "localhost", "::1"}
            or database.path.removeprefix("/") not in {"forgeflow", "forgeflow_test"}
        ):
            raise WarehouseError(
                "Demo cleanup is restricted to the local PostgreSQL forgeflow demo/test databases"
            )
        raw_tables = sql.SQL(", ").join(
            sql.SQL("raw.{}").format(sql.Identifier(name)) for name in RAW_TABLES
        )
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(raw_tables))
            cursor.execute(
                """
                TRUNCATE quarantine.records,
                    observability.quality_results,
                    observability.dbt_artifacts,
                    observability.lineage_edges,
                    observability.model_columns,
                    observability.model_metadata,
                    observability.incidents,
                    observability.source_files,
                    observability.pipeline_runs
                RESTART IDENTITY CASCADE
                """
            )
            cursor.execute(
                """
                DROP SCHEMA IF EXISTS staging CASCADE;
                DROP SCHEMA IF EXISTS intermediate CASCADE;
                DROP SCHEMA IF EXISTS marts CASCADE;
                CREATE SCHEMA staging;
                CREATE SCHEMA intermediate;
                CREATE SCHEMA marts;
                GRANT USAGE ON SCHEMA observability, quarantine, marts TO forgeflow_reader;
                """
            )

    def _latest_run_id(self) -> UUID | None:
        row = self._fetch_one(
            "SELECT run_id FROM observability.pipeline_runs ORDER BY started_at DESC LIMIT 1", ()
        )
        return UUID(str(row["run_id"])) if row else None

    def _fetch_one(self, query: str, parameters: Sequence[Any]) -> dict[str, Any] | None:
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, parameters)
            row = cursor.fetchone()
        return dict(row) if row is not None else None

    def _fetch_all(self, query: str, parameters: Sequence[Any]) -> list[dict[str, Any]]:
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, parameters)
            return [dict(row) for row in cursor.fetchall()]


def _normalize_value(value: Any) -> Any:
    """Convert pandas/numpy scalar values into psycopg-adaptable Python values."""
    if value is None or isinstance(value, (str, int, float, bool, date, datetime, UUID)):
        return value
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        return value.item()
    return value


def _record_checksum(record: Mapping[str, Any], columns: Iterable[str]) -> str:
    from forgeflow.object_store import sha256_bytes

    canonical = json.dumps(
        {column: _json_value(record.get(column)) for column in columns},
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256_bytes(canonical.encode("utf-8"))


def _json_value(value: Any) -> Any:
    normalized = _normalize_value(value)
    if isinstance(normalized, (datetime, date)):
        if isinstance(normalized, datetime) and normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=UTC)
        return normalized.isoformat()
    if isinstance(normalized, UUID):
        return str(normalized)
    return normalized


def _json_payload(value: Any) -> Any:
    """Normalize untrusted quarantine evidence into strict PostgreSQL JSON values."""
    normalized = _normalize_value(value)
    if isinstance(normalized, float):
        return normalized if math.isfinite(normalized) else "<non-finite numeric value>"
    if isinstance(normalized, Decimal):
        return str(normalized) if normalized.is_finite() else "<non-finite numeric value>"
    if isinstance(normalized, str):
        return normalized.replace("\x00", "<NUL>")
    if isinstance(normalized, Mapping):
        return {
            str(key).replace("\x00", "<NUL>"): _json_payload(item)
            for key, item in normalized.items()
        }
    if isinstance(normalized, (list, tuple)):
        return [_json_payload(item) for item in normalized]
    scalar = _json_value(normalized)
    if scalar is None or isinstance(scalar, (str, int, float, bool)):
        return scalar
    return str(scalar)
