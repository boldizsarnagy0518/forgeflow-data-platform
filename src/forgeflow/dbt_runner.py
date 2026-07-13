"""dbt execution and failure-safe artifact normalization."""

from __future__ import annotations

import json
import os
import subprocess  # nosec B404
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit
from uuid import UUID

from forgeflow.config import Settings
from forgeflow.models import QualityResult, Severity
from forgeflow.warehouse import PostgresRepository


@dataclass(slots=True)
class DbtRunResult:
    """Normalized outcome retained even when dbt returns a failure code."""

    succeeded: bool
    return_code: int
    quality_results: list[QualityResult] = field(default_factory=list)
    model_row_counts: dict[str, int] = field(default_factory=dict)
    test_counts: dict[str, int] = field(default_factory=dict)
    affected_downstream_models: list[str] = field(default_factory=list)
    stdout_tail: str = ""
    stderr_tail: str = ""
    error_message: str | None = None


class DbtRunner:
    """Run dbt without a shell and persist diagnostic artifacts on every outcome."""

    def __init__(self, settings: Settings, repository: PostgresRepository) -> None:
        self._settings = settings
        self._repository = repository

    def build(
        self,
        run_id: UUID,
        batch_id: str,
        *,
        variables: Mapping[str, str] | None = None,
    ) -> DbtRunResult:
        """Serialize relation mutations, execute dbt, and retain failure artifacts."""
        with self._repository.dbt_execution_lock():
            return self._build_locked(run_id, batch_id, variables=variables)

    def _build_locked(
        self,
        run_id: UUID,
        batch_id: str,
        *,
        variables: Mapping[str, str] | None,
    ) -> DbtRunResult:
        """Execute `dbt build` while the PostgreSQL advisory lock is held."""
        target_dir, preparation_error = self._prepare_target_dir(run_id)
        freshness_target_dir = target_dir / "freshness"
        if preparation_error is not None:
            return DbtRunResult(
                succeeded=False,
                return_code=1,
                error_message=preparation_error,
            )
        dbt_variables = {"forgeflow_batch_id": batch_id}
        if variables:
            unsupported = set(variables).difference({"backfill_start", "backfill_end"})
            if unsupported:
                names = ", ".join(sorted(unsupported))
                return DbtRunResult(
                    succeeded=False,
                    return_code=1,
                    error_message=f"Unsupported dbt variables: {names}",
                )
            dbt_variables.update(variables)
        command = [
            "dbt",
            "build",
            "--project-dir",
            str(self._settings.dbt_project_dir),
            "--profiles-dir",
            str(self._settings.dbt_profiles_dir),
            "--target",
            self._settings.dbt_target,
            "--target-path",
            str(target_dir),
            "--vars",
            json.dumps(dbt_variables, separators=(",", ":")),
            "--no-use-colors",
        ]
        environment = self._dbt_environment()
        return_code = 1
        stdout = ""
        stderr = ""
        execution_error: str | None = None
        freshness_return_code = 1
        try:
            completed = subprocess.run(  # noqa: S603  # nosec B603
                command,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=environment,
                timeout=900,
            )
            return_code = completed.returncode
            stdout = completed.stdout
            stderr = completed.stderr
        except FileNotFoundError:
            execution_error = "dbt executable was not found in the active environment"
        except subprocess.TimeoutExpired:
            execution_error = "dbt build exceeded the 900-second safety timeout"

        artifacts = self._capture_artifacts(
            run_id,
            target_dir,
            artifact_names=("manifest", "run_results", "catalog"),
        )
        if execution_error is None:
            freshness_command = [
                "dbt",
                "source",
                "freshness",
                "--project-dir",
                str(self._settings.dbt_project_dir),
                "--profiles-dir",
                str(self._settings.dbt_profiles_dir),
                "--target",
                self._settings.dbt_target,
                "--target-path",
                str(freshness_target_dir),
                "--vars",
                json.dumps(dbt_variables, separators=(",", ":")),
                "--no-use-colors",
            ]
            try:
                freshness = subprocess.run(  # noqa: S603  # nosec B603
                    freshness_command,
                    check=False,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=environment,
                    timeout=300,
                )
                freshness_return_code = freshness.returncode
                stdout = f"{stdout}\n--- source freshness ---\n{freshness.stdout}"
                stderr = f"{stderr}\n--- source freshness ---\n{freshness.stderr}"
            except FileNotFoundError:
                execution_error = "dbt executable was not found while running source freshness"
            except subprocess.TimeoutExpired:
                execution_error = "dbt source freshness exceeded the 300-second safety timeout"

        artifacts.update(
            self._capture_artifacts(
                run_id,
                freshness_target_dir,
                artifact_names=("sources",),
            )
        )
        invalid_artifacts = {
            name
            for name, artifact in artifacts.items()
            if not _artifact_is_structurally_valid(name, artifact)
        }
        if (
            "manifest" not in invalid_artifacts
            and "run_results" not in invalid_artifacts
            and "manifest" in artifacts
            and "run_results" in artifacts
            and not _matching_build_invocations(artifacts["manifest"], artifacts["run_results"])
        ):
            invalid_artifacts.update({"manifest", "run_results"})
        manifest = artifacts.get("manifest", {}) if "manifest" not in invalid_artifacts else {}
        run_results = (
            artifacts.get("run_results", {}) if "run_results" not in invalid_artifacts else {}
        )
        parsed = parse_dbt_artifacts(run_id, manifest=manifest, run_results=run_results)
        sources_artifact = (
            artifacts.get("sources", {}) if "sources" not in invalid_artifacts else {}
        )
        freshness_results = parse_source_freshness(run_id, sources_artifact)
        parsed.quality_results.extend(freshness_results)
        parsed.test_counts.update(
            {
                "freshness_total": len(freshness_results),
                "freshness_passed": sum(result.status == "passed" for result in freshness_results),
                "freshness_failed": sum(result.status == "failed" for result in freshness_results),
                "freshness_warning": sum(
                    result.status == "warning" for result in freshness_results
                ),
            }
        )
        if manifest:
            models, columns, lineage = parse_manifest_metadata(manifest)
            self._repository.upsert_model_metadata(run_id, models, columns, lineage)
            parsed.model_row_counts = self._repository.count_model_rows(models)

        missing_artifacts: list[str] = []
        if return_code == 0:
            missing_artifacts.extend(
                name for name in ("manifest", "run_results") if name not in artifacts
            )
            missing_artifacts.extend(
                name for name in ("manifest", "run_results") if name in invalid_artifacts
            )
        if freshness_return_code == 0 and "sources" not in artifacts:
            missing_artifacts.append("sources")
        if freshness_return_code == 0 and "sources" in invalid_artifacts:
            missing_artifacts.append("sources")

        parsed.return_code = return_code
        parsed.succeeded = (
            return_code == 0
            and freshness_return_code == 0
            and execution_error is None
            and not missing_artifacts
        )
        parsed.stdout_tail = _bounded_tail(stdout)
        parsed.stderr_tail = _bounded_tail(stderr)
        artifact_error = (
            "dbt reported success but required artifacts were missing, oversized, or invalid: "
            + ", ".join(sorted(set(missing_artifacts)))
            if missing_artifacts
            else None
        )
        parsed.error_message = (
            execution_error
            or artifact_error
            or (None if parsed.succeeded else _dbt_error_summary(stderr, run_results))
        )
        return parsed

    def _prepare_target_dir(self, run_id: UUID) -> tuple[Path, str | None]:
        """Create an isolated target and remove artifacts from a retry of the same run."""
        target_dir = (self._settings.artifact_dir / "dbt" / str(run_id)).resolve()
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            return target_dir, "Unable to create the isolated dbt artifact directory"
        for artifact_name in ("manifest", "run_results", "catalog", "sources"):
            path = target_dir / f"{artifact_name}.json"
            try:
                path.unlink(missing_ok=True)
            except OSError:
                return target_dir, (
                    f"Unable to clear stale dbt artifact before execution: {path.name}"
                )
        freshness_target_dir = target_dir / "freshness"
        try:
            freshness_target_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            return target_dir, "Unable to create the isolated dbt freshness artifact directory"
        for artifact_name in ("manifest", "run_results", "catalog", "sources"):
            path = freshness_target_dir / f"{artifact_name}.json"
            try:
                path.unlink(missing_ok=True)
            except OSError:
                return target_dir, (
                    f"Unable to clear stale dbt freshness artifact before execution: {path.name}"
                )
        return target_dir, None

    def _capture_artifacts(
        self,
        run_id: UUID,
        target_dir: Path,
        *,
        artifact_names: tuple[str, ...],
    ) -> dict[str, dict[str, Any]]:
        artifacts: dict[str, dict[str, Any]] = {}
        for artifact_name in artifact_names:
            path = target_dir / f"{artifact_name}.json"
            if not path.is_file():
                continue
            try:
                raw = path.read_bytes()
                if len(raw) > 20_000_000:
                    continue
                artifact = json.loads(raw)
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(artifact, dict):
                artifacts[artifact_name] = artifact
                self._repository.record_dbt_artifact(run_id, artifact_name, artifact)
        return artifacts

    def _dbt_environment(self) -> dict[str, str]:
        allowed_names = {
            "APPDATA",
            "COMSPEC",
            "HOME",
            "LANG",
            "LC_ALL",
            "LOCALAPPDATA",
            "PATH",
            "PATHEXT",
            "SYSTEMROOT",
            "TEMP",
            "TMP",
            "TMPDIR",
            "USERPROFILE",
            "WINDIR",
        }
        environment = {
            name: value for name, value in os.environ.items() if name.upper() in allowed_names
        }
        parsed = urlsplit(self._settings.database_url.get_secret_value())
        environment.update(
            {
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
                "FORGEFLOW_DBT_HOST": parsed.hostname or "127.0.0.1",
                "FORGEFLOW_DBT_PORT": str(parsed.port or 5432),
                "FORGEFLOW_DBT_USER": unquote(parsed.username or "forgeflow"),
                "FORGEFLOW_DBT_PASSWORD": unquote(parsed.password or ""),
                "FORGEFLOW_DBT_DBNAME": parsed.path.lstrip("/") or "forgeflow",
                "FORGEFLOW_DBT_SCHEMA": "staging",
            }
        )
        return environment


def _artifact_is_structurally_valid(name: str, artifact: Mapping[str, Any]) -> bool:
    """Require the minimal dbt artifact schema needed for trustworthy evidence."""
    if name == "manifest":
        nodes = artifact.get("nodes")
        sources = artifact.get("sources")
        child_map = artifact.get("child_map")
        return (
            isinstance(nodes, dict)
            and bool(nodes)
            and isinstance(sources, dict)
            and isinstance(child_map, dict)
        )
    if name in {"run_results", "sources"}:
        results = artifact.get("results")
        return (
            isinstance(results, list)
            and bool(results)
            and all(
                isinstance(result, dict)
                and isinstance(result.get("unique_id"), str)
                and bool(result["unique_id"])
                and isinstance(result.get("status"), str)
                and bool(result["status"])
                for result in results
            )
        )
    if name == "catalog":
        return isinstance(artifact.get("nodes"), dict) and isinstance(artifact.get("sources"), dict)
    return False


def _matching_build_invocations(
    manifest: Mapping[str, Any], run_results: Mapping[str, Any]
) -> bool:
    """Correlate the build manifest and results so freshness cannot overwrite lineage context."""
    manifest_metadata = manifest.get("metadata")
    results_metadata = run_results.get("metadata")
    if not isinstance(manifest_metadata, dict) or not isinstance(results_metadata, dict):
        return False
    manifest_invocation = manifest_metadata.get("invocation_id")
    results_invocation = results_metadata.get("invocation_id")
    return (
        isinstance(manifest_invocation, str)
        and bool(manifest_invocation)
        and manifest_invocation == results_invocation
    )


def parse_dbt_artifacts(
    run_id: UUID,
    *,
    manifest: dict[str, Any],
    run_results: dict[str, Any],
) -> DbtRunResult:
    """Turn dbt artifacts into unified checks, counts, and downstream impact."""
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})
    all_nodes = (
        {**sources, **nodes} if isinstance(nodes, dict) and isinstance(sources, dict) else {}
    )
    results = run_results.get("results", [])
    quality: list[QualityResult] = []
    counts = {"total": 0, "passed": 0, "failed": 0, "warning": 0}
    model_rows: dict[str, int] = {}
    failed_parent_ids: set[str] = set()

    if not isinstance(results, list):
        results = []
    for result in results:
        if not isinstance(result, dict):
            continue
        unique_id = str(result.get("unique_id", "unknown"))
        node = all_nodes.get(unique_id, {}) if isinstance(all_nodes, dict) else {}
        resource_type = str(node.get("resource_type", unique_id.split(".", 1)[0]))
        status_raw = str(result.get("status", "error")).lower()
        name = str(node.get("name", unique_id))
        failures = result.get("failures")
        if resource_type == "test":
            status = _quality_status(status_raw)
            counts["total"] += 1
            counts[status] += 1
            severity = Severity.WARNING if status == "warning" else Severity.ERROR
            if status == "passed":
                severity = Severity.INFO
            dependency_ids = _dependency_ids(node)
            if status in {"failed", "warning"}:
                failed_parent_ids.update(dependency_ids)
            quality.append(
                QualityResult(
                    check_id=unique_id,
                    run_id=run_id,
                    check_name=name,
                    check_type="dbt_test",
                    scope=next(iter(dependency_ids), "project"),
                    status=status,
                    severity=severity,
                    observed_value=failures if isinstance(failures, int) else None,
                    expected="dbt test returns zero failing rows",
                    evidence={
                        "dbt_status": status_raw,
                        "message": str(result.get("message", ""))[:2_000],
                        "execution_time_seconds": result.get("execution_time"),
                        "dependencies": sorted(dependency_ids),
                    },
                )
            )
        elif resource_type in {"model", "snapshot", "seed"}:
            response = result.get("adapter_response", {})
            rows_affected = response.get("rows_affected") if isinstance(response, dict) else None
            if isinstance(rows_affected, int) and rows_affected >= 0:
                model_rows[name] = rows_affected
            if status_raw not in {"success", "pass", "skipped"}:
                failed_parent_ids.add(unique_id)

    affected = _downstream_names(manifest, failed_parent_ids)
    return DbtRunResult(
        succeeded=False,
        return_code=1,
        quality_results=quality,
        model_row_counts=model_rows,
        test_counts=counts,
        affected_downstream_models=affected,
    )


def parse_manifest_metadata(
    manifest: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    """Extract compact model descriptions, columns, and dependency edges."""
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})
    if not isinstance(nodes, dict) or not isinstance(sources, dict):
        return [], [], []
    all_nodes: dict[str, Any] = {**sources, **nodes}
    name_by_id = {
        unique_id: _display_name(node)
        for unique_id, node in all_nodes.items()
        if isinstance(node, dict)
    }
    models: list[dict[str, Any]] = []
    columns: list[dict[str, Any]] = []
    edges: dict[tuple[str, str], dict[str, str]] = {}
    for unique_id, node in all_nodes.items():
        if not isinstance(node, dict):
            continue
        resource_type = str(node.get("resource_type", "unknown"))
        if resource_type not in {"model", "snapshot", "source"}:
            continue
        name = name_by_id[unique_id]
        config = node.get("config", {})
        models.append(
            {
                "model_name": name,
                "unique_id": unique_id,
                "resource_type": resource_type,
                "database_name": node.get("database"),
                "schema_name": node.get("schema"),
                "relation_name": node.get("relation_name"),
                "relation_identifier": node.get("alias") or node.get("name"),
                "description": str(node.get("description", "")),
                "materialization": config.get("materialized") if isinstance(config, dict) else None,
                "tags": node.get("tags", []),
                "meta": node.get("meta", {}),
                "depends_on": sorted(_dependency_ids(node)),
            }
        )
        raw_columns = node.get("columns", {})
        if isinstance(raw_columns, dict):
            for ordinal, (column_name, column) in enumerate(raw_columns.items(), start=1):
                details = column if isinstance(column, dict) else {}
                columns.append(
                    {
                        "model_unique_id": unique_id,
                        "column_name": str(column_name),
                        "data_type": details.get("data_type"),
                        "description": str(details.get("description", "")),
                        "tests": [],
                        "ordinal_position": ordinal,
                    }
                )
        for parent_id in _dependency_ids(node):
            parent_name = name_by_id.get(parent_id)
            if parent_name and parent_name != name:
                edges[(parent_id, unique_id)] = {
                    "parent_unique_id": parent_id,
                    "child_unique_id": unique_id,
                    "parent_name": parent_name,
                    "child_name": name,
                    "edge_type": "depends_on",
                }
    return models, columns, [edges[key] for key in sorted(edges)]


def parse_source_freshness(run_id: UUID, sources_artifact: dict[str, Any]) -> list[QualityResult]:
    """Normalize `dbt source freshness` results into the shared quality schema."""
    raw_results = sources_artifact.get("results", [])
    if not isinstance(raw_results, list):
        return []
    results: list[QualityResult] = []
    for item in raw_results:
        if not isinstance(item, dict):
            continue
        unique_id = str(item.get("unique_id", "unknown_source"))
        raw_status = str(item.get("status", "error")).lower()
        status = _quality_status(raw_status)
        severity = (
            Severity.INFO
            if status == "passed"
            else Severity.WARNING
            if status == "warning"
            else Severity.ERROR
        )
        age_seconds = item.get("max_loaded_at_time_ago_in_s")
        results.append(
            QualityResult(
                check_id=f"freshness:{unique_id}",
                run_id=run_id,
                check_name="dbt source freshness",
                check_type="freshness",
                scope=unique_id,
                status=status,
                severity=severity,
                observed_value=(
                    float(age_seconds) if isinstance(age_seconds, (int, float)) else raw_status
                ),
                expected="source age remains within its documented warn/error thresholds",
                evidence={
                    "dbt_status": raw_status,
                    "max_loaded_at": item.get("max_loaded_at"),
                    "snapshotted_at": item.get("snapshotted_at"),
                    "criteria": item.get("criteria", {}),
                },
            )
        )
    return results


def _quality_status(status: str) -> str:
    if status in {"pass", "success"}:
        return "passed"
    if status == "warn":
        return "warning"
    return "failed"


def _dependency_ids(node: Any) -> set[str]:
    if not isinstance(node, dict):
        return set()
    depends_on = node.get("depends_on", {})
    raw_nodes = depends_on.get("nodes", []) if isinstance(depends_on, dict) else []
    return {str(item) for item in raw_nodes if isinstance(item, str)}


def _display_name(node: dict[str, Any]) -> str:
    resource_type = str(node.get("resource_type", ""))
    source_name = node.get("source_name")
    name = str(node.get("name", node.get("unique_id", "unknown")))
    return f"source:{source_name}.{name}" if resource_type == "source" else name


def _downstream_names(manifest: dict[str, Any], roots: set[str]) -> list[str]:
    child_map = manifest.get("child_map", {})
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})
    all_nodes = (
        {**sources, **nodes} if isinstance(nodes, dict) and isinstance(sources, dict) else {}
    )
    if not isinstance(child_map, dict):
        return []
    visited: set[str] = set()
    pending = list(roots)
    while pending and len(visited) < 500:
        current = pending.pop()
        children = child_map.get(current, [])
        if not isinstance(children, list):
            continue
        for child in children:
            child_id = str(child)
            if child_id not in visited:
                visited.add(child_id)
                pending.append(child_id)
    return sorted(
        {
            str(all_nodes[node_id].get("name", node_id))
            for node_id in visited
            if node_id in all_nodes
            and isinstance(all_nodes[node_id], dict)
            and all_nodes[node_id].get("resource_type") in {"model", "exposure"}
        }
    )


def _bounded_tail(value: str, limit: int = 20_000) -> str:
    return value[-limit:]


def _dbt_error_summary(stderr: str, run_results: dict[str, Any]) -> str:
    results = run_results.get("results", [])
    messages = [
        str(result.get("message", "")).strip()
        for result in results
        if isinstance(result, dict)
        and str(result.get("status", "")).lower() in {"error", "fail", "warn"}
        and result.get("message")
    ]
    if messages:
        return " | ".join(messages)[:2_000]
    tail = _bounded_tail(stderr.strip(), 2_000)
    return tail or "dbt build failed; inspect the persisted run_results artifact"
