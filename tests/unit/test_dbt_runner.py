"""Unit coverage for failure-safe dbt execution and artifact normalization."""

from __future__ import annotations

import json
import subprocess
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from forgeflow.config import Settings
from forgeflow.dbt_runner import (
    DbtRunner,
    parse_dbt_artifacts,
    parse_manifest_metadata,
    parse_source_freshness,
)
from forgeflow.models import Severity
from forgeflow.warehouse import PostgresRepository

BUILD_INVOCATION_ID = "70000000-0000-0000-0000-000000000001"


class ArtifactRepository:
    """Capture only the dbt persistence calls used by ``DbtRunner``."""

    def __init__(self) -> None:
        self.artifacts: list[tuple[UUID, str, dict[str, Any]]] = []
        self.metadata: list[
            tuple[UUID, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]
        ] = []

    def record_dbt_artifact(
        self,
        run_id: UUID,
        artifact_type: str,
        artifact: dict[str, Any],
    ) -> None:
        self.artifacts.append((run_id, artifact_type, artifact))

    def dbt_execution_lock(self) -> AbstractContextManager[None]:
        return nullcontext()

    def upsert_model_metadata(
        self,
        run_id: UUID,
        models: list[dict[str, Any]],
        columns: list[dict[str, Any]],
        lineage: list[dict[str, str]],
    ) -> None:
        self.metadata.append((run_id, models, columns, lineage))

    def count_model_rows(self, models: list[dict[str, Any]]) -> dict[str, int]:
        del models
        return {
            "stg_production_orders": 8,
            "mart_factory_performance": 4,
        }


def _settings(tmp_path: Path) -> Settings:
    project = tmp_path / "dbt-project"
    project.mkdir()
    return Settings(
        dbt_project_dir=project,
        dbt_profiles_dir=project,
        data_dir=tmp_path / "data",
        artifact_dir=tmp_path / "artifacts",
    )


def _runner(tmp_path: Path, repository: ArtifactRepository) -> DbtRunner:
    return DbtRunner(_settings(tmp_path), cast("PostgresRepository", repository))


def _manifest() -> dict[str, Any]:
    return {
        "metadata": {"invocation_id": BUILD_INVOCATION_ID},
        "sources": {
            "source.forgeflow.raw.production_orders": {
                "unique_id": "source.forgeflow.raw.production_orders",
                "resource_type": "source",
                "source_name": "raw",
                "name": "production_orders",
                "database": "forgeflow",
                "schema": "raw",
                "relation_name": '"forgeflow"."raw"."production_orders"',
                "description": "Replayable production orders.",
                "columns": {
                    "production_order_id": {
                        "description": "Stable business identifier.",
                        "data_type": "text",
                    }
                },
                "depends_on": {"nodes": []},
            }
        },
        "nodes": {
            "model.forgeflow.stg_production_orders": {
                "unique_id": "model.forgeflow.stg_production_orders",
                "resource_type": "model",
                "name": "stg_production_orders",
                "database": "forgeflow",
                "schema": "staging",
                "relation_name": '"forgeflow"."staging"."stg_production_orders"',
                "description": "Typed production orders.",
                "config": {"materialized": "incremental"},
                "tags": ["staging"],
                "meta": {"owner": "data-platform"},
                "columns": {
                    "production_order_id": {
                        "description": "Stable business identifier.",
                        "data_type": "text",
                    },
                    "actual_quantity": {
                        "description": "Produced units.",
                        "data_type": "integer",
                    },
                },
                "depends_on": {"nodes": ["source.forgeflow.raw.production_orders"]},
            },
            "model.forgeflow.mart_factory_performance": {
                "unique_id": "model.forgeflow.mart_factory_performance",
                "resource_type": "model",
                "name": "mart_factory_performance",
                "database": "forgeflow",
                "schema": "marts",
                "config": {"materialized": "table"},
                "columns": {},
                "depends_on": {"nodes": ["model.forgeflow.stg_production_orders"]},
            },
            "test.forgeflow.actual_quantity_not_null": {
                "unique_id": "test.forgeflow.actual_quantity_not_null",
                "resource_type": "test",
                "name": "actual_quantity_not_null",
                "depends_on": {"nodes": ["model.forgeflow.stg_production_orders"]},
            },
            "test.forgeflow.overrun_rule": {
                "unique_id": "test.forgeflow.overrun_rule",
                "resource_type": "test",
                "name": "actual_quantity_within_expected_bound",
                "depends_on": {"nodes": ["model.forgeflow.stg_production_orders"]},
            },
            "test.forgeflow.mart_warning": {
                "unique_id": "test.forgeflow.mart_warning",
                "resource_type": "test",
                "name": "mart_has_recent_orders",
                "depends_on": {"nodes": ["model.forgeflow.mart_factory_performance"]},
            },
        },
        "child_map": {
            "source.forgeflow.raw.production_orders": ["model.forgeflow.stg_production_orders"],
            "model.forgeflow.stg_production_orders": [
                "test.forgeflow.actual_quantity_not_null",
                "test.forgeflow.overrun_rule",
                "model.forgeflow.mart_factory_performance",
            ],
            "model.forgeflow.mart_factory_performance": ["test.forgeflow.mart_warning"],
        },
    }


def _run_results() -> dict[str, Any]:
    return {
        "metadata": {"invocation_id": BUILD_INVOCATION_ID},
        "results": [
            {
                "unique_id": "model.forgeflow.stg_production_orders",
                "status": "success",
                "adapter_response": {"rows_affected": 8},
                "execution_time": 0.4,
            },
            {
                "unique_id": "model.forgeflow.mart_factory_performance",
                "status": "success",
                "adapter_response": {"rows_affected": 4},
                "execution_time": 0.2,
            },
            {
                "unique_id": "test.forgeflow.actual_quantity_not_null",
                "status": "pass",
                "failures": 0,
                "execution_time": 0.03,
            },
            {
                "unique_id": "test.forgeflow.overrun_rule",
                "status": "fail",
                "failures": 2,
                "message": "Got 2 results, configured to fail if != 0",
                "execution_time": 0.05,
            },
            {
                "unique_id": "test.forgeflow.mart_warning",
                "status": "warn",
                "failures": 1,
                "message": "One factory is outside the warning SLA",
                "execution_time": 0.02,
            },
        ],
    }


def _source_results() -> dict[str, Any]:
    return {
        "results": [
            {
                "unique_id": "source.forgeflow.raw.production_orders",
                "status": "pass",
                "max_loaded_at_time_ago_in_s": 120,
            }
        ]
    }


def test_build_uses_isolated_target_and_clears_retry_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = ArtifactRepository()
    runner = _runner(tmp_path, repository)
    run_id = uuid4()
    target = tmp_path / "artifacts" / "dbt" / str(run_id)
    target.mkdir(parents=True)
    artifact_paths = [
        target / f"{name}.json" for name in ("manifest", "run_results", "catalog", "sources")
    ]
    freshness_target = target / "freshness"
    freshness_target.mkdir()
    artifact_paths.extend(
        freshness_target / f"{name}.json"
        for name in ("manifest", "run_results", "catalog", "sources")
    )
    for path in artifact_paths:
        path.write_text('{"metadata":"stale"}', encoding="utf-8")
    calls: list[tuple[list[str], dict[str, object]]] = []

    def successful_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        target_index = command.index("--target-path")
        actual_target = Path(command[target_index + 1])
        assert actual_target == (target if len(calls) == 1 else freshness_target)
        variables = json.loads(command[command.index("--vars") + 1])
        assert variables == {
            "forgeflow_batch_id": "fresh-batch",
            "backfill_start": "2025-07-10",
            "backfill_end": "2025-07-11",
        }
        if len(calls) == 1:
            assert all(not path.exists() for path in artifact_paths)
            (actual_target / "manifest.json").write_text(json.dumps(_manifest()), encoding="utf-8")
            (actual_target / "run_results.json").write_text(
                json.dumps(_run_results()), encoding="utf-8"
            )
        else:
            (actual_target / "sources.json").write_text(
                json.dumps(_source_results()), encoding="utf-8"
            )
        return subprocess.CompletedProcess[str](command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", successful_run)
    result = runner.build(
        run_id,
        "fresh-batch",
        variables={"backfill_start": "2025-07-10", "backfill_end": "2025-07-11"},
    )

    assert result.succeeded
    assert len(calls) == 2
    assert calls[0][0][:2] == ["dbt", "build"]
    assert calls[1][0][:3] == ["dbt", "source", "freshness"]
    assert {artifact_type for _, artifact_type, _ in repository.artifacts} == {
        "manifest",
        "run_results",
        "sources",
    }


def test_build_rejects_unregistered_dbt_variables_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, ArtifactRepository())

    def unexpected_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        del command, kwargs
        raise AssertionError("dbt must not run for unregistered variables")

    monkeypatch.setattr(subprocess, "run", unexpected_run)

    result = runner.build(uuid4(), "unsafe-vars", variables={"arbitrary_sql": "select 1"})

    assert not result.succeeded
    assert result.error_message == "Unsupported dbt variables: arbitrary_sql"


def test_failed_build_still_captures_and_parses_new_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = ArtifactRepository()
    runner = _runner(tmp_path, repository)
    run_id = uuid4()

    def failed_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        actual_target = Path(command[command.index("--target-path") + 1])
        if command[1] == "build":
            (actual_target / "manifest.json").write_text(json.dumps(_manifest()), encoding="utf-8")
            (actual_target / "run_results.json").write_text(
                json.dumps(_run_results()), encoding="utf-8"
            )
            return subprocess.CompletedProcess[str](
                command,
                1,
                stdout="compiled project",
                stderr="dbt build reported a data test failure",
            )
        (actual_target / "sources.json").write_text(
            json.dumps(
                {
                    "results": [
                        {
                            "unique_id": "source.forgeflow.raw.production_orders",
                            "status": "pass",
                            "max_loaded_at_time_ago_in_s": 120,
                            "max_loaded_at": "2025-07-10T11:58:00Z",
                            "snapshotted_at": "2025-07-10T12:00:00Z",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess[str](command, 0, stdout="fresh", stderr="")

    monkeypatch.setattr(subprocess, "run", failed_run)
    result = runner.build(run_id, "incident-batch")

    assert not result.succeeded
    assert result.return_code == 1
    assert result.model_row_counts == {
        "stg_production_orders": 8,
        "mart_factory_performance": 4,
    }
    assert result.test_counts == {
        "total": 3,
        "passed": 1,
        "failed": 1,
        "warning": 1,
        "freshness_total": 1,
        "freshness_passed": 1,
        "freshness_failed": 0,
        "freshness_warning": 0,
    }
    assert result.affected_downstream_models == ["mart_factory_performance"]
    assert "Got 2 results" in cast("str", result.error_message)
    assert {artifact_type for _, artifact_type, _ in repository.artifacts} == {
        "manifest",
        "run_results",
        "sources",
    }
    assert repository.metadata
    assert repository.metadata[0][0] == run_id


def test_success_exit_without_required_artifacts_is_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, ArtifactRepository())

    def successful_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess[str](command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", successful_run)

    result = runner.build(uuid4(), "missing-artifacts")

    assert not result.succeeded
    assert result.error_message is not None
    assert "manifest" in result.error_message
    assert "run_results" in result.error_message
    assert "sources" in result.error_message


def test_success_exit_with_structurally_empty_artifacts_is_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, ArtifactRepository())
    run_id = uuid4()

    def successful_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        actual_target = Path(command[command.index("--target-path") + 1])
        artifact_name = "sources" if command[1] == "source" else "manifest"
        (actual_target / f"{artifact_name}.json").write_text("{}", encoding="utf-8")
        if command[1] == "build":
            (actual_target / "run_results.json").write_text("{}", encoding="utf-8")
        return subprocess.CompletedProcess[str](command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", successful_run)

    result = runner.build(run_id, "empty-artifacts")

    assert not result.succeeded
    assert result.error_message is not None
    assert "manifest" in result.error_message
    assert "run_results" in result.error_message
    assert "sources" in result.error_message


def test_success_exit_with_mismatched_build_invocations_is_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, ArtifactRepository())
    run_id = uuid4()

    def successful_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        actual_target = Path(command[command.index("--target-path") + 1])
        if command[1] == "build":
            mismatched_results = _run_results()
            mismatched_results["metadata"] = {
                "invocation_id": "70000000-0000-0000-0000-000000000099"
            }
            (actual_target / "manifest.json").write_text(json.dumps(_manifest()), encoding="utf-8")
            (actual_target / "run_results.json").write_text(
                json.dumps(mismatched_results), encoding="utf-8"
            )
        else:
            (actual_target / "sources.json").write_text(
                json.dumps(_source_results()), encoding="utf-8"
            )
        return subprocess.CompletedProcess[str](command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", successful_run)

    result = runner.build(run_id, "mismatched-invocations")

    assert not result.succeeded
    assert result.error_message is not None
    assert "manifest" in result.error_message
    assert "run_results" in result.error_message


def test_source_freshness_timeout_reports_the_correct_command_and_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, ArtifactRepository())
    run_id = uuid4()

    def timed_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        actual_target = Path(command[command.index("--target-path") + 1])
        if command[1] == "build":
            (actual_target / "manifest.json").write_text(json.dumps(_manifest()), encoding="utf-8")
            (actual_target / "run_results.json").write_text(
                json.dumps(_run_results()), encoding="utf-8"
            )
            return subprocess.CompletedProcess[str](command, 0, stdout="built", stderr="")
        raise subprocess.TimeoutExpired(command, timeout=300)

    monkeypatch.setattr(subprocess, "run", timed_run)

    result = runner.build(run_id, "freshness-timeout")

    assert not result.succeeded
    assert result.error_message == "dbt source freshness exceeded the 300-second safety timeout"


def test_dbt_subprocess_environment_is_allowlisted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = ArtifactRepository()
    runner = _runner(tmp_path, repository)
    run_id = uuid4()
    environments: list[dict[str, str]] = []
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-leak")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-leak")
    monkeypatch.setenv("FORGEFLOW_S3_SECRET_KEY", "must-not-leak")

    def successful_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        environments.append(cast("dict[str, str]", kwargs["env"]))
        actual_target = Path(command[command.index("--target-path") + 1])
        if command[1] == "build":
            (actual_target / "manifest.json").write_text(json.dumps(_manifest()), encoding="utf-8")
            (actual_target / "run_results.json").write_text(
                json.dumps(_run_results()), encoding="utf-8"
            )
        else:
            (actual_target / "sources.json").write_text(
                json.dumps(_source_results()), encoding="utf-8"
            )
        return subprocess.CompletedProcess[str](command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", successful_run)

    result = runner.build(run_id, "environment-test")

    assert result.succeeded
    assert len(environments) == 2
    for environment in environments:
        assert environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "false"
        assert environment["FORGEFLOW_DBT_DBNAME"] == "forgeflow"
        assert "OPENAI_API_KEY" not in environment
        assert "AWS_SECRET_ACCESS_KEY" not in environment
        assert "FORGEFLOW_S3_SECRET_KEY" not in environment


def test_artifact_parser_normalizes_tests_rows_and_downstream_impact() -> None:
    run_id = uuid4()
    result = parse_dbt_artifacts(
        run_id,
        manifest=_manifest(),
        run_results=_run_results(),
    )

    assert result.model_row_counts["stg_production_orders"] == 8
    assert result.test_counts == {"total": 3, "passed": 1, "failed": 1, "warning": 1}
    assert result.affected_downstream_models == ["mart_factory_performance"]
    by_id = {quality.check_id: quality for quality in result.quality_results}
    passed = by_id["test.forgeflow.actual_quantity_not_null"]
    failed = by_id["test.forgeflow.overrun_rule"]
    warned = by_id["test.forgeflow.mart_warning"]
    assert (passed.status, passed.severity) == ("passed", Severity.INFO)
    assert (failed.status, failed.severity, failed.observed_value) == (
        "failed",
        Severity.ERROR,
        2,
    )
    assert (warned.status, warned.severity) == ("warning", Severity.WARNING)
    assert failed.scope == "model.forgeflow.stg_production_orders"
    assert failed.evidence["dependencies"] == ["model.forgeflow.stg_production_orders"]


def test_manifest_metadata_preserves_columns_and_direct_lineage() -> None:
    models, columns, edges = parse_manifest_metadata(_manifest())

    by_id = {model["unique_id"]: model for model in models}
    source_id = "source.forgeflow.raw.production_orders"
    staging_id = "model.forgeflow.stg_production_orders"
    mart_id = "model.forgeflow.mart_factory_performance"
    assert set(by_id) == {source_id, staging_id, mart_id}
    assert by_id[source_id]["model_name"] == "source:raw.production_orders"
    assert by_id[staging_id]["materialization"] == "incremental"
    staging_columns = [column for column in columns if column["model_unique_id"] == staging_id]
    assert [column["column_name"] for column in staging_columns] == [
        "production_order_id",
        "actual_quantity",
    ]
    assert [column["ordinal_position"] for column in staging_columns] == [1, 2]
    assert edges == [
        {
            "parent_unique_id": staging_id,
            "child_unique_id": mart_id,
            "parent_name": "stg_production_orders",
            "child_name": "mart_factory_performance",
            "edge_type": "depends_on",
        },
        {
            "parent_unique_id": source_id,
            "child_unique_id": staging_id,
            "parent_name": "source:raw.production_orders",
            "child_name": "stg_production_orders",
            "edge_type": "depends_on",
        },
    ]


def test_source_freshness_parser_maps_pass_warn_and_error_with_evidence() -> None:
    run_id = uuid4()
    artifact = {
        "results": [
            {
                "unique_id": "source.forgeflow.raw.factories",
                "status": "pass",
                "max_loaded_at_time_ago_in_s": 30,
                "max_loaded_at": "2025-07-10T11:59:30Z",
                "snapshotted_at": "2025-07-10T12:00:00Z",
                "criteria": {"warn_after": {"count": 24, "period": "hour"}},
            },
            {
                "unique_id": "source.forgeflow.raw.machine_telemetry",
                "status": "warn",
                "max_loaded_at_time_ago_in_s": 90_000,
                "criteria": {"warn_after": {"count": 24, "period": "hour"}},
            },
            {
                "unique_id": "source.forgeflow.raw.production_orders",
                "status": "runtime error",
            },
        ]
    }

    parsed = parse_source_freshness(run_id, artifact)

    assert [(result.status, result.severity) for result in parsed] == [
        ("passed", Severity.INFO),
        ("warning", Severity.WARNING),
        ("failed", Severity.ERROR),
    ]
    assert parsed[0].observed_value == 30.0
    assert parsed[1].evidence["criteria"] == {"warn_after": {"count": 24, "period": "hour"}}
    assert parsed[2].observed_value == "runtime error"


def test_invalid_artifact_shapes_are_ignored_instead_of_crashing() -> None:
    run_id = uuid4()
    parsed = parse_dbt_artifacts(
        run_id,
        manifest={"nodes": [], "sources": {}},
        run_results={"results": {"not": "a list"}},
    )

    assert parsed.quality_results == []
    assert parsed.model_row_counts == {}
    assert parsed.test_counts == {"total": 0, "passed": 0, "failed": 0, "warning": 0}
    assert parse_source_freshness(run_id, {"results": "invalid"}) == []
