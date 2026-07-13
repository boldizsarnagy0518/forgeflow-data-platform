"""Unit coverage for the PostgreSQL repository without a running database."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Never, Self
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from forgeflow.config import Settings
from forgeflow.errors import WarehouseError
from forgeflow.models import (
    FailureScenario,
    QualityResult,
    QuarantinedRecord,
    QuarantineReason,
    RunStatus,
    RunSummary,
    SchemaChange,
    Severity,
)
from forgeflow.warehouse import (
    RAW_TABLES,
    PostgresRepository,
    _json_value,
    _normalize_value,
    _record_checksum,
)

RUN_ID = UUID("10000000-0000-0000-0000-000000000001")
FILE_ID = UUID("20000000-0000-0000-0000-000000000002")
BASELINE_ID = UUID("30000000-0000-0000-0000-000000000003")
STARTED_AT = datetime(2025, 7, 10, 8, 0, tzinfo=UTC)
FINISHED_AT = datetime(2025, 7, 10, 8, 1, 30, tzinfo=UTC)


@dataclass(slots=True)
class ScriptedResult:
    """One result made active by one ``execute`` call."""

    one: Mapping[str, Any] | None = None
    all: list[Mapping[str, Any]] = field(default_factory=list)
    rowcount: int = 1


@dataclass(frozen=True, slots=True)
class SqlCall:
    """Captured SQL and adapted parameters."""

    query: str
    parameters: object | None


class ScriptedCursor:
    """Small psycopg cursor double with ordered query results."""

    def __init__(self, results: Sequence[ScriptedResult] = ()) -> None:
        self._results = deque(results)
        self._current = ScriptedResult()
        self.calls: list[SqlCall] = []
        self.many_calls: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.transaction_entries = 0
        self.commits = 0
        self.rollbacks = 0

    @property
    def rowcount(self) -> int:
        return self._current.rowcount

    def execute(self, query: str | sql.Composable, parameters: object | None = None) -> None:
        self.calls.append(SqlCall(_sql_text(query), parameters))
        self._current = self._results.popleft() if self._results else ScriptedResult()

    def executemany(
        self,
        query: str | sql.Composable,
        parameters: Iterable[Sequence[Any]],
    ) -> None:
        self.many_calls.append(
            (_sql_text(query), [tuple(parameter_set) for parameter_set in parameters])
        )

    def fetchone(self) -> Mapping[str, Any] | None:
        return self._current.one

    def fetchall(self) -> list[Mapping[str, Any]]:
        return self._current.all

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class ScriptedConnection:
    """Connection double sharing one cursor across repository transactions."""

    def __init__(self, cursor: ScriptedCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> ScriptedCursor:
        return self._cursor

    def __enter__(self) -> Self:
        self._cursor.transaction_entries += 1
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: object | None,
    ) -> None:
        if exception_type is None:
            self._cursor.commits += 1
        else:
            self._cursor.rollbacks += 1


class UnavailableConnection:
    """Context manager that fails before exposing a database connection."""

    def __enter__(self) -> Never:
        raise WarehouseError("mart is not built")

    def __exit__(self, *_args: object) -> None:
        return None


def _sql_text(query: str | sql.Composable) -> str:
    return query if isinstance(query, str) else query.as_string(None)


def _repository(
    monkeypatch: pytest.MonkeyPatch,
    results: Sequence[ScriptedResult] = (),
) -> tuple[PostgresRepository, ScriptedCursor]:
    repository = PostgresRepository(Settings())
    cursor = ScriptedCursor(results)
    connection = ScriptedConnection(cursor)

    def fake_connection() -> ScriptedConnection:
        return connection

    monkeypatch.setattr(repository, "_connection", fake_connection)
    return repository, cursor


def _summary(*, status: RunStatus = RunStatus.RUNNING) -> RunSummary:
    return RunSummary(
        run_id=RUN_ID,
        batch_id="batch-2025-07-10",
        scenario=FailureScenario.INCIDENT,
        status=status,
        started_at=STARTED_AT,
        finished_at=FINISHED_AT if status is not RunStatus.RUNNING else None,
        duration_seconds=90.0 if status is not RunStatus.RUNNING else None,
        source_file_count=3,
        source_row_count=100,
        accepted_row_count=93,
        quarantined_row_count=7,
        skipped_file_count=1,
        model_row_counts={"mart_factory_performance": 2},
        test_counts={"passed": 9, "failed": 1},
        passed_checks=9,
        failed_checks=1,
        freshness_status="stale",
        schema_changes=[
            SchemaChange(
                source_name="factories",
                change_type="additive",
                expected_columns=["factory_id"],
                actual_columns=["factory_id", "new_column"],
                unexpected_columns=["new_column"],
            )
        ],
        affected_downstream_models=["mart_factory_performance"],
        error_message="contract drift" if status is RunStatus.FAILED else None,
    )


def _json_object(value: object) -> object:
    assert isinstance(value, Jsonb)
    return value.obj


def test_initialize_reads_directory_scripts_in_lexical_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    init_dir = tmp_path / "init"
    init_dir.mkdir()
    (init_dir / "20-observability.sql").write_text("SELECT 'second';", encoding="utf-8")
    (init_dir / "10-schemas.sql").write_text("SELECT 'first';", encoding="utf-8")
    (init_dir / "README.txt").write_text("ignored", encoding="utf-8")
    repository, cursor = _repository(monkeypatch)

    repository.initialize(init_dir)

    assert cursor.calls == [SqlCall("SELECT 'first';\nSELECT 'second';", None)]


def test_pipeline_stage_lifecycle_persists_timing_counts_and_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(
        monkeypatch,
        [ScriptedResult(rowcount=1), ScriptedResult(rowcount=1)],
    )

    repository.start_stage(
        RUN_ID,
        "contract_validation",
        input_row_count=100,
        metadata={"contract_version": "1.0.0"},
    )
    repository.finish_stage(
        RUN_ID,
        "contract_validation",
        status="succeeded",
        output_row_count=93,
        metadata={"quarantined": 7},
    )

    start_call, finish_call = cursor.calls
    assert "ON CONFLICT (run_id, stage_name) DO UPDATE" in start_call.query
    assert "duration_seconds = GREATEST" in finish_call.query
    assert isinstance(start_call.parameters, tuple)
    assert isinstance(finish_call.parameters, tuple)
    assert _json_object(start_call.parameters[3]) == {"contract_version": "1.0.0"}
    assert _json_object(finish_call.parameters[3]) == {"quarantined": 7}


def test_pipeline_stage_rejects_unknown_status_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)

    with pytest.raises(WarehouseError, match="Unsupported pipeline stage status"):
        repository.finish_stage(RUN_ID, "dbt", status="pretend-success")

    assert cursor.calls == []


@pytest.mark.parametrize("as_directory", [True, False])
def test_initialize_rejects_absent_or_empty_bootstrap_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    as_directory: bool,
) -> None:
    path = tmp_path / ("empty" if as_directory else "empty.sql")
    if as_directory:
        path.mkdir()
    else:
        path.write_text(" \n", encoding="utf-8")
    repository, cursor = _repository(monkeypatch)

    with pytest.raises(WarehouseError, match="No warehouse bootstrap SQL"):
        repository.initialize(path)

    assert cursor.calls == []


def test_ping_requires_the_exact_health_row(monkeypatch: pytest.MonkeyPatch) -> None:
    healthy, healthy_cursor = _repository(monkeypatch, [ScriptedResult(one={"healthy": 1})])
    assert healthy.ping() is True
    assert healthy_cursor.calls == [SqlCall("SELECT 1 AS healthy", None)]

    unhealthy, _ = _repository(monkeypatch, [ScriptedResult(one={"healthy": 0})])
    assert unhealthy.ping() is False


def test_connection_translates_psycopg_errors_and_ping_degrades(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_connect(*_args: object, **_kwargs: object) -> Never:
        raise psycopg.OperationalError("database unavailable")

    monkeypatch.setattr(psycopg, "connect", fail_connect)
    repository = PostgresRepository(Settings())

    assert repository.ping() is False
    with pytest.raises(WarehouseError, match="PostgreSQL operation failed"):
        repository.get_latest_run()


def test_start_and_finish_run_preserve_complete_final_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)
    started = _summary()
    finished = _summary(status=RunStatus.FAILED)

    repository.start_run(started)
    repository.finish_run(finished, {"headline": "file drift", "failed_checks": 1})

    start_call, finish_call = cursor.calls
    assert "INSERT INTO observability.pipeline_runs" in start_call.query
    assert start_call.parameters == {
        "run_id": RUN_ID,
        "batch_id": "batch-2025-07-10",
        "scenario": "incident",
        "status": "running",
        "started_at": STARTED_AT,
    }
    assert "WHERE run_id = %(run_id)s" in finish_call.query
    assert isinstance(finish_call.parameters, dict)
    final = finish_call.parameters
    assert final["status"] == "failed"
    assert final["finished_at"] == "2025-07-10T08:01:30Z"
    assert final["duration_seconds"] == 90.0
    assert final["error_message"] == "contract drift"
    assert _json_object(final["model_row_counts"]) == {"mart_factory_performance": 2}
    assert _json_object(final["test_counts"]) == {"passed": 9, "failed": 1}
    assert _json_object(final["schema_changes"]) == [
        {
            "source_name": "factories",
            "change_type": "additive",
            "expected_columns": ["factory_id"],
            "actual_columns": ["factory_id", "new_column"],
            "missing_columns": [],
            "unexpected_columns": ["new_column"],
        }
    ]
    assert _json_object(final["affected_downstream_models"]) == ["mart_factory_performance"]
    assert _json_object(final["summary"]) == {
        "headline": "file drift",
        "failed_checks": 1,
    }


def test_finish_run_rejects_missing_run_instead_of_silently_succeeding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, _ = _repository(monkeypatch, [ScriptedResult(rowcount=0)])

    with pytest.raises(WarehouseError, match=str(RUN_ID)):
        repository.finish_run(_summary(status=RunStatus.FAILED), {})


@pytest.mark.parametrize("terminal_status", ["loaded", "quarantined", "skipped"])
def test_register_source_file_returns_terminal_content_without_inserting(
    monkeypatch: pytest.MonkeyPatch,
    terminal_status: str,
) -> None:
    existing_id = uuid4()
    repository, cursor = _repository(
        monkeypatch,
        [
            ScriptedResult(
                one={
                    "file_id": str(existing_id),
                    "object_key": "old/object.csv",
                    "status": terminal_status,
                }
            )
        ],
    )

    result = repository.register_source_file(
        run_id=RUN_ID,
        batch_id="batch",
        source_name="factories",
        logical_key="factories/2025-07-10.csv",
        object_key="new/object.csv",
        checksum="same-content",
        schema_fingerprint="schema-v1",
        size_bytes=123,
        row_count=2,
    )

    assert result.file_id == existing_id
    assert result.duplicate is True
    assert result.changed_logical_file is False
    assert result.existing_object_key == "old/object.csv"
    assert len(cursor.calls) == 1
    assert cursor.calls[0].parameters == ("factories", "same-content")
    assert "FOR UPDATE" in cursor.calls[0].query


@pytest.mark.parametrize("retryable_status", ["landed", "validating", "failed"])
def test_register_source_file_resumes_retryable_content_instead_of_skipping(
    monkeypatch: pytest.MonkeyPatch,
    retryable_status: str,
) -> None:
    existing_id = uuid4()
    repository, cursor = _repository(
        monkeypatch,
        [
            ScriptedResult(
                one={
                    "file_id": str(existing_id),
                    "object_key": "old/object.csv",
                    "status": retryable_status,
                }
            ),
            ScriptedResult(rowcount=1),
        ],
    )

    result = repository.register_source_file(
        run_id=RUN_ID,
        batch_id="retry-batch",
        source_name="factories",
        logical_key="retry-batch/factories.csv",
        object_key="incoming/factories/retry.csv",
        checksum="same-content",
        schema_fingerprint="schema-v2",
        size_bytes=456,
        row_count=4,
    )

    assert result.file_id == existing_id
    assert result.duplicate is False
    assert result.changed_logical_file is False
    assert result.existing_object_key is None
    assert len(cursor.calls) == 2
    resumed = cursor.calls[1]
    assert "accepted_count = 0" in resumed.query
    assert "quarantined_count = 0" in resumed.query
    assert "status = 'landed'" in resumed.query
    assert "processed_at = NULL" in resumed.query
    assert "status NOT IN ('loaded', 'quarantined', 'skipped')" in resumed.query
    assert resumed.parameters == (
        RUN_ID,
        "retry-batch",
        "retry-batch/factories.csv",
        "incoming/factories/retry.csv",
        "schema-v2",
        456,
        4,
        existing_id,
    )


def test_register_source_file_rejects_a_lost_retry_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing_id = uuid4()
    repository, _ = _repository(
        monkeypatch,
        [
            ScriptedResult(
                one={
                    "file_id": str(existing_id),
                    "object_key": "old/object.csv",
                    "status": "failed",
                }
            ),
            ScriptedResult(rowcount=0),
        ],
    )

    with pytest.raises(WarehouseError, match="prepared for retry"):
        repository.register_source_file(
            run_id=RUN_ID,
            batch_id="retry-batch",
            source_name="factories",
            logical_key="retry-batch/factories.csv",
            object_key="incoming/factories/retry.csv",
            checksum="same-content",
            schema_fingerprint="schema-v2",
            size_bytes=456,
            row_count=4,
        )


def test_register_source_file_detects_changed_logical_content_and_inserts_ledger_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(
        monkeypatch,
        [
            ScriptedResult(one=None),
            ScriptedResult(one={"checksum": "old-content"}),
            ScriptedResult(),
        ],
    )

    result = repository.register_source_file(
        run_id=RUN_ID,
        batch_id="batch",
        source_name="factories",
        logical_key="factories/2025-07-10.csv",
        object_key="incoming/factories/new.csv",
        checksum="new-content",
        schema_fingerprint="schema-v2",
        size_bytes=456,
        row_count=4,
    )

    assert result.duplicate is False
    assert result.changed_logical_file is True
    assert result.existing_object_key is None
    assert cursor.calls[1].parameters == ("factories/2025-07-10.csv",)
    inserted = cursor.calls[2]
    assert "INSERT INTO observability.source_files" in inserted.query
    assert inserted.parameters == (
        result.file_id,
        RUN_ID,
        "batch",
        "factories",
        "factories/2025-07-10.csv",
        "incoming/factories/new.csv",
        "new-content",
        "schema-v2",
        456,
        4,
    )


def test_complete_source_file_persists_validation_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    repository, cursor = _repository(monkeypatch)

    repository.complete_source_file(
        FILE_ID,
        accepted_count=91,
        quarantined_count=9,
        status="quarantined",
    )

    assert len(cursor.calls) == 1
    assert "processed_at = CURRENT_TIMESTAMP" in cursor.calls[0].query
    assert cursor.calls[0].parameters == (91, 9, "quarantined", FILE_ID)


class TimestampScalar:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def to_pydatetime(self) -> datetime:
        return self.value


class ItemScalar:
    def __init__(self, value: object) -> None:
        self.value = value

    def item(self) -> object:
        return self.value


def test_load_records_builds_typed_idempotent_upsert_with_exact_row_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)
    ingested_at = datetime(2025, 7, 10, 9, 30, tzinfo=UTC)
    updated_at = datetime(2025, 7, 9, 17, 0, tzinfo=UTC)
    records: list[dict[str, Any]] = [
        {
            "factory_id": "FAC-001",
            "factory_name": "Budapest Plant",
            "country_code": "HU",
            "timezone": "Europe/Budapest",
            "opened_on": date(2020, 1, 2),
            "status": ItemScalar("active"),
            "updated_at": TimestampScalar(updated_at),
            "_source_row_number": "41",
            "ignored": "not part of the raw record checksum",
        },
        {
            "factory_id": "FAC-002",
            "factory_name": "Gyor Plant",
            "country_code": "HU",
            "timezone": "Europe/Budapest",
            "opened_on": date(2021, 3, 4),
            "status": "active",
            "updated_at": updated_at,
        },
    ]

    count = repository.load_records(
        "factories",
        records,
        batch_id="batch-17",
        file_id=FILE_ID,
        ingested_at=ingested_at,
    )

    assert count == 2
    assert cursor.calls == []
    assert len(cursor.many_calls) == 1
    statement, rows = cursor.many_calls[0]
    compact_statement = " ".join(statement.split())
    assert 'INSERT INTO raw."factories"' in statement
    assert 'ON CONFLICT ("factory_id") DO UPDATE SET' in statement
    assert (
        'WHERE raw."factories"."_record_checksum" IS DISTINCT FROM EXCLUDED."_record_checksum"'
    ) in compact_statement
    assert 'AND EXCLUDED."updated_at" >= raw."factories"."updated_at"' in compact_statement
    assert '"factory_id" = EXCLUDED."factory_id"' not in statement
    assert rows[0][0:7] == (
        "FAC-001",
        "Budapest Plant",
        "HU",
        "Europe/Budapest",
        date(2020, 1, 2),
        "active",
        updated_at,
    )
    assert rows[0][7:11] == ("batch-17", FILE_ID, 41, ingested_at)
    assert rows[0][11] == _record_checksum(records[0], RAW_TABLES["factories"].columns)
    assert rows[1][9] == 3
    assert rows[1][11] == _record_checksum(records[1], RAW_TABLES["factories"].columns)


def test_load_records_rejects_unknown_sources_and_skips_empty_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)

    assert (
        repository.load_records(
            "factories", [], batch_id="batch", file_id=FILE_ID, ingested_at=STARTED_AT
        )
        == 0
    )
    with pytest.raises(WarehouseError, match="unregistered_source"):
        repository.load_records(
            "unregistered_source",
            [{"id": 1}],
            batch_id="batch",
            file_id=FILE_ID,
            ingested_at=STARTED_AT,
        )

    assert cursor.calls == []
    assert cursor.many_calls == []


def test_schema_changes_are_batched_with_structured_column_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)
    repository.record_schema_changes(run_id=RUN_ID, file_id=FILE_ID, changes=[])
    assert cursor.many_calls == []

    change = SchemaChange(
        source_name="machines",
        change_type="breaking",
        expected_columns=["machine_id", "status"],
        actual_columns=["machine_id", "new_status"],
        missing_columns=["status"],
        unexpected_columns=["new_status"],
    )
    repository.record_schema_changes(run_id=RUN_ID, file_id=FILE_ID, changes=[change])

    query, rows = cursor.many_calls[0]
    assert "INSERT INTO observability.schema_changes" in query
    row = rows[0]
    assert isinstance(row[0], UUID)
    assert row[1:5] == (RUN_ID, FILE_ID, "machines", "breaking")
    assert [_json_object(value) for value in row[5:]] == [
        ["machine_id", "status"],
        ["machine_id", "new_status"],
        ["status"],
        ["new_status"],
    ]


def test_quarantine_retains_payload_and_all_structured_reasons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)
    assert repository.quarantine_records(run_id=RUN_ID, file_id=FILE_ID, records=[]) == 0

    rejected = QuarantinedRecord(
        source_name="machines",
        source_row_number=17,
        raw_payload={"machine_id": "M-17", "temperature": 999},
        reasons=[
            QuarantineReason(
                code="range",
                column="temperature",
                check="temperature_bounds",
                message="outside configured bounds",
                value=999,
            ),
            QuarantineReason(
                code="missing",
                column="status",
                check="required",
                message="required value is absent",
            ),
        ],
    )
    assert repository.quarantine_records(run_id=RUN_ID, file_id=FILE_ID, records=[rejected]) == 1

    query, rows = cursor.many_calls[0]
    assert "INSERT INTO quarantine.records" in query
    assert "ON CONFLICT (file_id, source_row_number) DO UPDATE" in query
    assert "run_id = EXCLUDED.run_id" in query
    assert "quarantined_at = CURRENT_TIMESTAMP" in query
    row = rows[0]
    assert row[1:5] == (RUN_ID, FILE_ID, "machines", 17)
    assert _json_object(row[5]) == {"machine_id": "M-17", "temperature": 999}
    assert _json_object(row[6]) == [
        {
            "code": "range",
            "column": "temperature",
            "check": "temperature_bounds",
            "message": "outside configured bounds",
            "value": 999,
        },
        {
            "code": "missing",
            "column": "status",
            "check": "required",
            "message": "required value is absent",
            "value": None,
        },
    ]


def test_quarantine_normalizes_nonfinite_untrusted_json_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)
    rejected = QuarantinedRecord(
        source_name="machine_telemetry",
        source_row_number=2,
        raw_payload={
            "temperature_c": float("nan"),
            "nested": [float("inf"), "unsafe\x00text"],
            "unsafe\x00key": "value",
        },
        reasons=[
            QuarantineReason(
                code="invalid_type",
                column="temperature_c",
                check="dtype",
                message="must be numeric",
                value=float("-inf"),
            )
        ],
    )

    repository.quarantine_records(run_id=RUN_ID, file_id=FILE_ID, records=[rejected])

    row = cursor.many_calls[0][1][0]
    assert _json_object(row[5]) == {
        "temperature_c": "<non-finite numeric value>",
        "nested": ["<non-finite numeric value>", "unsafe<NUL>text"],
        "unsafe<NUL>key": "value",
    }
    reasons = _json_object(row[6])
    assert isinstance(reasons, list)
    assert reasons[0]["value"] == "<non-finite numeric value>"


def test_commit_source_result_persists_all_file_outcomes_in_one_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch, [ScriptedResult(rowcount=1)])
    accepted = {
        "factory_id": "FAC-001",
        "factory_name": "Budapest Plant",
        "country_code": "HU",
        "timezone": "Europe/Budapest",
        "opened_on": date(2020, 1, 2),
        "status": "active",
        "updated_at": STARTED_AT,
        "_source_row_number": 2,
    }
    rejected = QuarantinedRecord(
        source_name="factories",
        source_row_number=3,
        raw_payload={"factory_id": None},
        reasons=[
            QuarantineReason(
                code="missing",
                column="factory_id",
                check="required",
                message="required value is absent",
            )
        ],
    )
    change = SchemaChange(
        source_name="factories",
        change_type="additive",
        expected_columns=["factory_id"],
        actual_columns=["factory_id", "new_column"],
        unexpected_columns=["new_column"],
    )

    counts = repository.commit_source_result(
        "factories",
        [accepted],
        run_id=RUN_ID,
        batch_id="batch-atomic",
        file_id=FILE_ID,
        ingested_at=FINISHED_AT,
        quarantined_records=[rejected],
        schema_changes=[change],
    )

    assert counts == (1, 1)
    assert cursor.transaction_entries == 1
    assert cursor.commits == 1
    assert cursor.rollbacks == 0
    assert len(cursor.many_calls) == 3
    assert "INSERT INTO raw" in cursor.many_calls[0][0]
    assert "INSERT INTO quarantine.records" in cursor.many_calls[1][0]
    assert "INSERT INTO observability.schema_changes" in cursor.many_calls[2][0]
    assert len(cursor.calls) == 1
    completion = cursor.calls[0]
    assert "WHERE file_id = %s AND status = 'landed'" in completion.query
    assert completion.parameters == (1, 1, "loaded", FILE_ID)


def test_commit_source_result_rolls_back_when_file_is_not_landed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch, [ScriptedResult(rowcount=0)])
    accepted = {
        "factory_id": "FAC-001",
        "factory_name": "Budapest Plant",
        "country_code": "HU",
        "timezone": "Europe/Budapest",
        "opened_on": date(2020, 1, 2),
        "status": "active",
        "updated_at": STARTED_AT,
    }

    with pytest.raises(WarehouseError, match="atomic completion"):
        repository.commit_source_result(
            "factories",
            [accepted],
            run_id=RUN_ID,
            batch_id="batch-atomic",
            file_id=FILE_ID,
            ingested_at=FINISHED_AT,
            quarantined_records=[],
            schema_changes=[],
        )

    assert len(cursor.many_calls) == 1
    assert cursor.transaction_entries == 1
    assert cursor.commits == 0
    assert cursor.rollbacks == 1


def test_commit_source_result_marks_a_validated_empty_file_loaded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch, [ScriptedResult(rowcount=1)])

    counts = repository.commit_source_result(
        "factories",
        [],
        run_id=RUN_ID,
        batch_id="batch-empty",
        file_id=FILE_ID,
        ingested_at=FINISHED_AT,
        quarantined_records=[],
        schema_changes=[],
    )

    assert counts == (0, 0)
    assert cursor.many_calls == []
    assert cursor.calls[0].parameters == (0, 0, "loaded", FILE_ID)


def test_commit_source_result_marks_an_empty_breaking_shape_quarantined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch, [ScriptedResult(rowcount=1)])
    change = SchemaChange(
        source_name="factories",
        change_type="breaking",
        expected_columns=["factory_id"],
        actual_columns=[],
        missing_columns=["factory_id"],
    )

    repository.commit_source_result(
        "factories",
        [],
        run_id=RUN_ID,
        batch_id="batch-empty-breaking",
        file_id=FILE_ID,
        ingested_at=FINISHED_AT,
        quarantined_records=[],
        schema_changes=[change],
    )

    assert cursor.calls[0].parameters == (0, 0, "quarantined", FILE_ID)


def test_quality_results_use_run_scoped_composite_upsert(monkeypatch: pytest.MonkeyPatch) -> None:
    repository, cursor = _repository(monkeypatch)
    repository.record_quality_results([])
    results = [
        QualityResult(
            check_id="contract:machines:temperature",
            run_id=RUN_ID,
            check_name="temperature bounds",
            check_type="contract",
            scope="machines.temperature",
            status="failed",
            severity=Severity.ERROR,
            observed_value=999.5,
            expected="between -20 and 200",
            evidence={"source_row_number": 17},
            occurred_at=STARTED_AT,
        ),
        QualityResult(
            check_id="dbt:not_null",
            run_id=RUN_ID,
            check_name="not null",
            check_type="dbt",
            scope="stg_machines.machine_id",
            status="passed",
            severity=Severity.INFO,
            observed_value=None,
            expected="0 failures",
            occurred_at=FINISHED_AT,
        ),
    ]

    repository.record_quality_results(results)

    assert len(cursor.many_calls) == 1
    query, rows = cursor.many_calls[0]
    assert "ON CONFLICT (check_id, run_id) DO UPDATE SET" in query
    assert rows[0][0:9] == (
        "contract:machines:temperature",
        RUN_ID,
        "temperature bounds",
        "contract",
        "machines.temperature",
        "failed",
        "error",
        "999.5",
        "between -20 and 200",
    )
    assert _json_object(rows[0][9]) == {"source_row_number": 17}
    assert rows[1][7] is None


def test_dbt_artifact_is_upserted_and_oversized_payload_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)
    artifact = {"metadata": {"dbt_version": "1.10"}, "nodes": {"model.x": {}}}

    repository.record_dbt_artifact(RUN_ID, "manifest", artifact)

    call = cursor.calls[0]
    assert "ON CONFLICT (run_id, artifact_type) DO UPDATE" in call.query
    assert isinstance(call.parameters, tuple)
    assert call.parameters[:2] == (RUN_ID, "manifest")
    assert _json_object(call.parameters[2]) == artifact

    with pytest.raises(WarehouseError, match="20 MB safety limit"):
        repository.record_dbt_artifact(RUN_ID, "run_results", {"data": "x" * 20_000_000})
    assert len(cursor.calls) == 1


def test_dbt_execution_lock_is_bounded_and_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)

    with repository.dbt_execution_lock():
        assert "pg_advisory_lock" in cursor.calls[1].query

    assert cursor.calls[0].query == "SET LOCAL statement_timeout = '30s'"
    assert "pg_advisory_unlock" in cursor.calls[2].query
    assert cursor.calls[1].parameters == cursor.calls[2].parameters


def test_model_metadata_columns_and_lineage_are_upserted_per_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)
    repository.upsert_model_metadata(
        RUN_ID,
        models=[
            {
                "unique_id": "model.forgeflow.mart_factory_performance",
                "model_name": "mart_factory_performance",
                "resource_type": "model",
                "database_name": "forgeflow",
                "schema_name": "marts",
                "relation_name": "mart_factory_performance",
                "description": "Factory KPIs",
                "materialization": "table",
                "tags": ["reviewer-facing"],
                "meta": {"owner": "data-platform"},
                "depends_on": ["model.forgeflow.int_production"],
            }
        ],
        columns=[
            {
                "model_unique_id": "model.forgeflow.mart_factory_performance",
                "column_name": "factory_id",
                "data_type": "text",
                "description": "Factory key",
                "tests": ["not_null", "unique"],
                "ordinal_position": 1,
            }
        ],
        lineage_edges=[
            {
                "parent_unique_id": "model.forgeflow.int_production",
                "child_unique_id": "model.forgeflow.mart_factory_performance",
                "parent_name": "int_production",
                "child_name": "mart_factory_performance",
            }
        ],
    )

    assert len(cursor.calls) == 3
    model_call, column_call, edge_call = cursor.calls
    assert "ON CONFLICT (run_id, unique_id) DO UPDATE" in model_call.query
    assert "ON CONFLICT (run_id, model_unique_id, column_name)" in column_call.query
    assert "ON CONFLICT (run_id, parent_unique_id, child_unique_id)" in edge_call.query
    for call in cursor.calls:
        assert isinstance(call.parameters, tuple)
        assert call.parameters[0] == RUN_ID
    assert isinstance(model_call.parameters, tuple)
    assert _json_object(model_call.parameters[9]) == ["reviewer-facing"]
    assert _json_object(model_call.parameters[10]) == {"owner": "data-platform"}
    assert _json_object(model_call.parameters[11]) == ["model.forgeflow.int_production"]
    assert isinstance(column_call.parameters, tuple)
    assert _json_object(column_call.parameters[5]) == ["not_null", "unique"]
    assert isinstance(edge_call.parameters, tuple)
    assert edge_call.parameters[-1] == "depends_on"


def test_manifest_model_row_counts_are_bounded_to_declared_warehouse_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(
        monkeypatch,
        [
            ScriptedResult(one={"relation": "marts.factory_performance"}),
            ScriptedResult(one={"row_count": 3}),
        ],
    )

    counts = repository.count_model_rows(
        [
            {
                "resource_type": "model",
                "model_name": "mart_factory_performance",
                "schema_name": "marts",
                "relation_identifier": "factory_performance",
            },
            {
                "resource_type": "model",
                "model_name": "unsafe",
                "schema_name": "public",
                "relation_identifier": "secrets",
            },
        ]
    )

    assert counts == {"mart_factory_performance": 3}
    assert len(cursor.calls) == 2
    assert cursor.calls[0].parameters == ('"marts"."factory_performance"',)
    assert "COUNT(*)" in cursor.calls[1].query
    assert '"marts"."factory_performance"' in cursor.calls[1].query


def test_source_volume_history_uses_only_comparable_healthy_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(
        monkeypatch,
        [ScriptedResult(all=[{"row_count": 100}, {"row_count": 98}, {"row_count": 101}])],
    )

    history = repository.source_volume_history(
        "machine_telemetry",
        before=STARTED_AT,
        batch_kind="incremental",
        limit=500,
    )

    assert history == [100, 98, 101]
    assert cursor.calls[0].parameters == (
        "machine_telemetry",
        STARTED_AT,
        "%-incremental-%",
        30,
    )
    assert "pipeline_run.status = 'healthy'" in cursor.calls[0].query


def test_run_and_evidence_reads_are_bounded_and_run_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed_check = {"check_id": "check-1", "status": "failed"}
    quarantined = {"quarantine_id": "q-1", "source_name": "machines"}
    repository, cursor = _repository(
        monkeypatch,
        [
            ScriptedResult(one={"count": 12}),
            ScriptedResult(all=[{"run_id": RUN_ID}, {"run_id": BASELINE_ID}]),
            ScriptedResult(one={"run_id": RUN_ID, "status": "failed"}),
            ScriptedResult(one={"run_id": RUN_ID, "status": "failed"}),
            ScriptedResult(one={"run_id": BASELINE_ID, "status": "healthy"}),
            ScriptedResult(all=[{"check_type": "dbt", "status": "failed", "count": 1}]),
            ScriptedResult(all=[failed_check]),
            ScriptedResult(one={"count": 1}),
            ScriptedResult(one=failed_check),
            ScriptedResult(all=[{"source_name": "machines", "reason_code": "range"}]),
            ScriptedResult(all=[quarantined]),
            ScriptedResult(one={"count": 1}),
        ],
    )

    assert repository.list_runs(limit=7, offset=14) == (
        [{"run_id": RUN_ID}, {"run_id": BASELINE_ID}],
        12,
    )
    assert repository.get_run(RUN_ID) == {"run_id": RUN_ID, "status": "failed"}
    assert repository.get_latest_run() == {"run_id": RUN_ID, "status": "failed"}
    assert repository.latest_healthy_run_before(STARTED_AT) == {
        "run_id": BASELINE_ID,
        "status": "healthy",
    }
    assert repository.quality_summary(RUN_ID) == [
        {"check_type": "dbt", "status": "failed", "count": 1}
    ]
    assert repository.list_failed_checks(run_id=RUN_ID, limit=5, offset=10) == (
        [failed_check],
        1,
    )
    assert repository.get_failed_check("check-1", RUN_ID) == failed_check
    assert repository.quarantine_summary(RUN_ID) == [
        {"source_name": "machines", "reason_code": "range"}
    ]
    assert repository.list_quarantined_records(run_id=RUN_ID, limit=4, offset=8) == (
        [quarantined],
        1,
    )

    assert cursor.calls[1].parameters == (7, 14)
    assert "ORDER BY started_at DESC, run_id DESC LIMIT %s OFFSET %s" in cursor.calls[1].query
    assert cursor.calls[4].parameters == (STARTED_AT,)
    assert "started_at < %s" in cursor.calls[4].query
    assert cursor.calls[6].parameters == (RUN_ID, 5, 10)
    assert "status IN ('failed', 'warning')" in cursor.calls[6].query
    assert "CASE severity" in cursor.calls[6].query
    assert "WHEN 'error' THEN 0" in cursor.calls[6].query
    assert "WHEN 'warning' THEN 1" in cursor.calls[6].query
    assert cursor.calls[10].parameters == (RUN_ID, 4, 8)
    assert "raw_payload" not in cursor.calls[10].query


def test_latest_run_resolution_returns_safe_empty_results_when_no_runs_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch, [ScriptedResult(one=None) for _ in range(5)])

    assert repository.quality_summary() == []
    assert repository.list_failed_checks(run_id=None, limit=10, offset=0) == ([], 0)
    assert repository.get_failed_check("missing") is None
    assert repository.quarantine_summary() == []
    assert repository.list_quarantined_records(run_id=None, limit=10, offset=0) == ([], 0)

    assert len(cursor.calls) == 5
    assert all(
        "SELECT run_id FROM observability.pipeline_runs" in call.query for call in cursor.calls
    )


def test_catalog_and_lineage_reads_are_bounded_and_use_latest_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = {"model_name": "mart_factory_performance"}
    repository, cursor = _repository(
        monkeypatch,
        [
            ScriptedResult(all=[model]),
            ScriptedResult(one={"count": 1}),
            ScriptedResult(one=model),
            ScriptedResult(all=[{"column_name": "factory_id"}]),
            ScriptedResult(all=[{"model": "int_production"}]),
            ScriptedResult(all=[{"model": "dashboard_export"}]),
            ScriptedResult(all=[{"model_name": "dashboard_export", "depth": 1}]),
        ],
    )

    assert repository.list_models(limit=25, offset=50) == ([model], 1)
    assert repository.get_model("mart_factory_performance") == model
    assert repository.get_columns("mart_factory_performance") == [{"column_name": "factory_id"}]
    assert repository.get_lineage("mart_factory_performance") == {
        "model_name": "mart_factory_performance",
        "parents": ["int_production"],
        "children": ["dashboard_export"],
    }
    assert repository.get_downstream_impact("mart_factory_performance") == [
        {"model_name": "dashboard_export", "depth": 1}
    ]

    assert cursor.calls[0].parameters == (25, 50)
    assert "DISTINCT ON (model_name)" in cursor.calls[0].query
    assert "LIMIT %s OFFSET %s" in cursor.calls[0].query
    assert "ORDER BY captured_at DESC LIMIT 1" in cursor.calls[2].query
    assert "LIMIT 500" in cursor.calls[3].query
    assert all("latest_run" in cursor.calls[index].query for index in (4, 5, 6))
    assert "downstream.depth < 20" in cursor.calls[6].query
    assert "LIMIT 500" in cursor.calls[6].query


def test_dashboard_reads_are_bounded_and_missing_marts_degrade_to_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(
        monkeypatch,
        [
            ScriptedResult(all=[{"freshness_status": "fresh"}]),
            ScriptedResult(all=[{"factory_id": "FAC-001"}]),
            ScriptedResult(all=[{"run_id": RUN_ID, "failed_checks": 0}]),
        ],
    )
    assert repository.freshness() == [{"freshness_status": "fresh"}]
    assert repository.factory_performance(limit=17) == [{"factory_id": "FAC-001"}]
    assert repository.quality_trend(limit=23) == [{"run_id": RUN_ID, "failed_checks": 0}]
    assert "LIMIT 500" in cursor.calls[0].query
    assert "FROM marts.data_freshness" in cursor.calls[0].query
    assert cursor.calls[1].parameters == (17,)
    assert "FROM marts.factory_performance" in cursor.calls[1].query
    assert cursor.calls[2].parameters == (23,)

    fallback = PostgresRepository(Settings())
    monkeypatch.setattr(fallback, "_connection", UnavailableConnection)
    assert fallback.freshness() == []
    assert fallback.factory_performance() == []


def test_incident_mutations_keep_evidence_and_resolution_linkage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(
        monkeypatch,
        [
            ScriptedResult(),
            ScriptedResult(),
            ScriptedResult(),
            ScriptedResult(one={"incident_id": FILE_ID, "status": "resolved"}),
            ScriptedResult(one={"incident_id": FILE_ID, "status": "open"}),
        ],
    )

    incident_id = repository.create_incident(
        incident_id=FILE_ID,
        failed_run_id=RUN_ID,
        baseline_run_id=BASELINE_ID,
        title="Factory freshness incident",
        evidence={"failed_checks": ["freshness"]},
        explanation={"provider": "deterministic"},
    )
    repository.resolve_incident(incident_id, BASELINE_ID)
    repository.update_incident_explanation(
        incident_id,
        {"provider": "openai", "summary": "Evidence-bounded explanation"},
    )
    assert repository.get_incident(incident_id) == {
        "incident_id": FILE_ID,
        "status": "resolved",
    }
    assert repository.latest_open_incident() == {"incident_id": FILE_ID, "status": "open"}

    create = cursor.calls[0]
    assert "VALUES (%s, %s, %s, 'open', %s, %s, %s)" in create.query
    assert isinstance(create.parameters, tuple)
    assert create.parameters[:4] == (
        FILE_ID,
        RUN_ID,
        BASELINE_ID,
        "Factory freshness incident",
    )
    assert _json_object(create.parameters[4]) == {"failed_checks": ["freshness"]}
    assert _json_object(create.parameters[5]) == {"provider": "deterministic"}
    assert cursor.calls[1].parameters == (BASELINE_ID, FILE_ID)
    assert "status = 'resolved'" in cursor.calls[1].query
    assert "AND status = 'open'" in cursor.calls[1].query
    explanation_update = cursor.calls[2]
    assert "SET explanation = %s" in explanation_update.query
    assert isinstance(explanation_update.parameters, tuple)
    assert _json_object(explanation_update.parameters[0]) == {
        "provider": "openai",
        "summary": "Evidence-bounded explanation",
    }
    assert explanation_update.parameters[1] == FILE_ID
    assert cursor.calls[3].parameters == (FILE_ID,)
    assert "WHERE status = 'open'" in cursor.calls[4].query


@pytest.mark.parametrize("updated_rows", [0, 2])
def test_resolve_incident_requires_exactly_one_open_incident(
    monkeypatch: pytest.MonkeyPatch,
    updated_rows: int,
) -> None:
    repository, cursor = _repository(monkeypatch, [ScriptedResult(rowcount=updated_rows)])

    with pytest.raises(WarehouseError, match=f"Open incident {FILE_ID}"):
        repository.resolve_incident(FILE_ID, RUN_ID)

    assert cursor.transaction_entries == 1
    assert cursor.commits == 0
    assert cursor.rollbacks == 1


def test_update_incident_explanation_rejects_a_missing_incident(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, _ = _repository(monkeypatch, [ScriptedResult(rowcount=0)])

    with pytest.raises(WarehouseError, match=str(FILE_ID)):
        repository.update_incident_explanation(FILE_ID, {"provider": "openai"})


def test_create_incident_generates_an_identifier_when_not_supplied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)

    incident_id = repository.create_incident(
        failed_run_id=RUN_ID,
        baseline_run_id=None,
        title="Unbaselined incident",
        evidence={},
        explanation={},
    )

    assert isinstance(incident_id, UUID)
    assert isinstance(cursor.calls[0].parameters, tuple)
    assert cursor.calls[0].parameters[0] == incident_id


def test_clean_demo_state_only_truncates_whitelisted_demo_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)

    repository.clean_demo_state(confirmed=True)

    assert len(cursor.calls) == 3
    raw_cleanup = cursor.calls[0].query
    assert raw_cleanup.startswith("TRUNCATE raw.")
    for table_name in RAW_TABLES:
        assert f'raw."{table_name}"' in raw_cleanup
    assert "RESTART IDENTITY CASCADE" in raw_cleanup
    metadata_cleanup = cursor.calls[1].query
    assert "quarantine.records" in metadata_cleanup
    assert "observability.pipeline_runs" in metadata_cleanup
    assert "source_files" in metadata_cleanup
    schema_cleanup = cursor.calls[2].query
    assert "DROP SCHEMA IF EXISTS staging CASCADE" in schema_cleanup
    assert "CREATE SCHEMA marts" in schema_cleanup
    assert "GRANT USAGE" in schema_cleanup


def test_clean_demo_state_requires_explicit_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, cursor = _repository(monkeypatch)

    with pytest.raises(WarehouseError, match="explicit confirmation"):
        repository.clean_demo_state()

    assert cursor.calls == []


def test_clean_demo_state_refuses_nonlocal_or_non_demo_databases() -> None:
    repository = PostgresRepository(
        Settings(database_url="postgresql://forgeflow:secret@db.example.invalid/production")
    )

    with pytest.raises(WarehouseError, match="restricted to the local"):
        repository.clean_demo_state(confirmed=True)


def test_scalar_normalization_and_checksums_are_canonical() -> None:
    timestamp = datetime(2025, 7, 10, 10, 15, tzinfo=UTC)
    scalar = ItemScalar(42)
    untouched = object()

    assert _normalize_value(None) is None
    assert _normalize_value(timestamp) is timestamp
    assert _normalize_value(TimestampScalar(timestamp)) == timestamp
    assert _normalize_value(scalar) == 42
    assert _normalize_value(untouched) is untouched
    naive_timestamp = timestamp.replace(tzinfo=None)
    assert _json_value(naive_timestamp) == "2025-07-10T10:15:00+00:00"
    assert _json_value(date(2025, 7, 10)) == "2025-07-10"
    assert _json_value(RUN_ID) == str(RUN_ID)

    columns = ("factory_id", "updated_at")
    naive = {"factory_id": RUN_ID, "updated_at": naive_timestamp, "extra": 1}
    aware = {"updated_at": timestamp, "factory_id": RUN_ID, "extra": 999}
    changed = {"factory_id": RUN_ID, "updated_at": FINISHED_AT}
    assert _record_checksum(naive, columns) == _record_checksum(aware, reversed(columns))
    assert _record_checksum(aware, columns) != _record_checksum(changed, columns)
