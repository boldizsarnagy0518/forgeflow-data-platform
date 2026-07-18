"""CLI contract tests with every external boundary replaced by an in-memory fake."""

from __future__ import annotations

import io
import json
from datetime import UTC, date, datetime, tzinfo
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest
import typer
from click import unstyle
from rich.console import Console
from typer.testing import CliRunner

import forgeflow.cli as cli_module
from forgeflow.config import Settings
from forgeflow.contracts import validate_records
from forgeflow.errors import ForgeFlowError
from forgeflow.models import FailureScenario, RunStatus, RunSummary, SchemaChange
from forgeflow.object_store import S3ObjectStore
from forgeflow.pipeline import PipelineOutcome, PipelineRunner, write_dataset
from forgeflow.synthetic import SyntheticDataset, generate_dataset
from forgeflow.warehouse import PostgresRepository

RUN_ID = UUID("10000000-0000-0000-0000-000000000001")
INCIDENT_ID = UUID("20000000-0000-0000-0000-000000000002")
DEFAULT_BATCH_DATE = date(2025, 7, 10)
INCIDENT_SCHEMA_CHANGE = SchemaChange(
    source_name="machine_telemetry",
    change_type="additive",
    expected_columns=["telemetry_id"],
    actual_columns=["telemetry_id", "firmware_revision"],
    unexpected_columns=["firmware_revision"],
)


class _FrozenDateTime:
    @classmethod
    def now(cls, timezone: tzinfo | None = None) -> datetime:
        del cls, timezone
        return datetime(2025, 7, 12, 9, 30, tzinfo=UTC)


class _FakeRunner:
    def __init__(self, outcomes: list[PipelineOutcome] | None = None) -> None:
        self.outcomes = list(outcomes or [_outcome()])
        self.generated_calls: list[tuple[FailureScenario, date | None, bool, bool]] = []
        self.dataset_calls: list[tuple[SyntheticDataset, str, FailureScenario, bool]] = []
        self.generated_options: list[dict[str, object]] = []
        self.dataset_source_bytes: list[dict[str, bytes] | None] = []

    def _next_outcome(self) -> PipelineOutcome:
        if len(self.outcomes) == 1:
            return self.outcomes[0]
        return self.outcomes.pop(0)

    def run_generated(
        self,
        scenario: FailureScenario = FailureScenario.CLEAN,
        *,
        batch_date: date | None = None,
        incremental: bool = False,
        run_dbt: bool = True,
        recovery_incident_id: UUID | None = None,
        dbt_variables: dict[str, str] | None = None,
    ) -> PipelineOutcome:
        self.generated_calls.append((scenario, batch_date, incremental, run_dbt))
        self.generated_options.append(
            {
                "recovery_incident_id": recovery_incident_id,
                "dbt_variables": dbt_variables,
            }
        )
        return self._next_outcome()

    def run_dataset(
        self,
        dataset: SyntheticDataset,
        *,
        batch_id: str,
        scenario: FailureScenario,
        run_dbt: bool,
        source_bytes: dict[str, bytes] | None = None,
    ) -> PipelineOutcome:
        self.dataset_calls.append((dataset, batch_id, scenario, run_dbt))
        self.dataset_source_bytes.append(source_bytes)
        return self._next_outcome()


class _FakeRepository:
    def __init__(self) -> None:
        self.initialize_calls = 0
        self.clean_calls = 0
        self.open_incident: dict[str, object] | None = {
            "incident_id": INCIDENT_ID,
            "status": "open",
        }

    def initialize(self) -> None:
        self.initialize_calls += 1

    def clean_demo_state(self, *, confirmed: bool = False) -> None:
        assert confirmed
        self.clean_calls += 1

    def latest_open_incident(self) -> dict[str, object] | None:
        return self.open_incident


def _outcome(
    *,
    run_id: UUID = RUN_ID,
    batch_id: str = "batch-001",
    scenario: FailureScenario = FailureScenario.CLEAN,
    status: RunStatus = RunStatus.HEALTHY,
    incident_id: UUID | None = INCIDENT_ID,
    source_file_count: int = 10,
    accepted_row_count: int = 4,
    quarantined_row_count: int = 0,
    skipped_file_count: int = 0,
    failed_checks: int = 0,
    schema_changes: list[SchemaChange] | None = None,
    affected_downstream_models: list[str] | None = None,
) -> PipelineOutcome:
    summary = RunSummary(
        run_id=run_id,
        batch_id=batch_id,
        scenario=scenario,
        status=status,
        started_at=datetime(2025, 7, 10, 8, tzinfo=UTC),
        finished_at=datetime(2025, 7, 10, 8, 0, 3, tzinfo=UTC),
        duration_seconds=3,
        source_file_count=source_file_count,
        source_row_count=5,
        accepted_row_count=accepted_row_count,
        quarantined_row_count=quarantined_row_count,
        skipped_file_count=skipped_file_count,
        failed_checks=failed_checks,
        schema_changes=schema_changes or [],
        affected_downstream_models=affected_downstream_models or [],
    )
    return PipelineOutcome(
        summary=summary,
        incident_id=incident_id,
        human_summary={"observed_facts": ["Four rows were accepted."]},
    )


def _no_logging(level: str) -> None:
    del level


def _install_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    runner: _FakeRunner,
    *,
    repository: _FakeRepository | None = None,
    object_store: object | None = None,
) -> _FakeRepository:
    fake_repository = repository or _FakeRepository()
    fake_object_store = object_store or object()

    def dependencies() -> tuple[Settings, PostgresRepository, S3ObjectStore, PipelineRunner]:
        return (
            settings,
            cast(PostgresRepository, fake_repository),
            cast(S3ObjectStore, fake_object_store),
            cast(PipelineRunner, runner),
        )

    monkeypatch.setattr(cli_module, "_dependencies", dependencies)
    return fake_repository


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path / "data", artifact_dir=tmp_path / "artifacts")


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(autouse=True)
def isolate_cli_callback(monkeypatch: pytest.MonkeyPatch, settings: Settings) -> None:
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "configure_logging", _no_logging)
    monkeypatch.setattr(cli_module, "datetime", _FrozenDateTime)
    # Keep assertions about serialized payloads independent of Rich's terminal wrapping.
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(color_system=None, width=1_000),
    )


def test_help_lists_the_supported_operational_commands(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli_module.app, ["--help"])

    assert result.exit_code == 0
    assert "Operate the local synthetic ForgeFlow" in result.output
    for command in (
        "generate",
        "inject-failure",
        "run-batch",
        "pipeline",
        "status",
        "backfill",
        "clean",
        "demo",
        "incident-demo",
        "recover-demo",
    ):
        assert command in result.output


def test_callback_configures_the_validated_log_level(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    levels: list[str] = []
    fake_runner = _FakeRunner([_outcome(incident_id=None)])
    _install_dependencies(monkeypatch, settings, fake_runner)
    monkeypatch.setattr(cli_module, "configure_logging", levels.append)

    result = cli_runner.invoke(cli_module.app, ["pipeline"])

    assert levels == [settings.log_level]
    assert result.exit_code == 0, result.output


def test_generate_passes_options_to_the_generator_and_prints_bounded_json(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "explicit-output"
    dataset: SyntheticDataset = {
        "factories": [{"factory_id": "F-01"}],
        "machines": [{"machine_id": "M-01"}, {"machine_id": "M-02"}],
    }
    generation_calls: list[tuple[FailureScenario, int, int, date, bool]] = []
    write_calls: list[tuple[SyntheticDataset, Path]] = []

    def fake_generate(
        scenario: FailureScenario,
        *,
        seed: int,
        generated_days: int,
        batch_date: date,
        incremental: bool,
    ) -> SyntheticDataset:
        generation_calls.append((scenario, seed, generated_days, batch_date, incremental))
        return dataset

    def fake_write(generated: SyntheticDataset, output: Path) -> Path:
        write_calls.append((generated, output))
        return output

    monkeypatch.setattr(cli_module, "generate_dataset", fake_generate)
    monkeypatch.setattr(cli_module, "write_dataset", fake_write)

    result = cli_runner.invoke(
        cli_module.app,
        [
            "generate",
            "--scenario",
            "recovery",
            "--batch-date",
            "2025-07-10",
            "--incremental",
            "--seed",
            "41",
            "--output",
            str(destination),
        ],
    )

    assert result.exit_code == 0, result.output
    assert generation_calls == [
        (FailureScenario.RECOVERY, 41, settings.generated_days, DEFAULT_BATCH_DATE, True)
    ]
    assert write_calls == [(dataset, destination)]
    payload = cast(dict[str, Any], json.loads(result.output))
    assert payload == {
        "batch_id": "2025-07-10-incremental-recovery-s41",
        "path": str(destination.resolve()),
        "row_counts": {"factories": 1, "machines": 2},
        "scenario": "recovery",
    }


def test_generate_rejects_an_invalid_iso_date_before_generation(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected_generation(*args: object, **kwargs: object) -> SyntheticDataset:
        del args, kwargs
        raise AssertionError("generation must not start for an invalid date")

    monkeypatch.setattr(cli_module, "generate_dataset", unexpected_generation)

    result = cli_runner.invoke(cli_module.app, ["generate", "--batch-date", "10-07-2025"])

    assert result.exit_code == 2
    assert "batch-date must use YYYY-MM-DD" in result.output


def test_inject_failure_always_builds_an_incremental_incident_fixture(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "incident"
    dataset: SyntheticDataset = {"factories": [{"factory_id": "F-01"}]}
    calls: list[tuple[FailureScenario, int, int, date, bool]] = []
    writes: list[tuple[SyntheticDataset, Path]] = []

    def fake_generate(
        scenario: FailureScenario,
        *,
        seed: int,
        generated_days: int,
        batch_date: date,
        incremental: bool,
    ) -> SyntheticDataset:
        calls.append((scenario, seed, generated_days, batch_date, incremental))
        return dataset

    def fake_write(generated: SyntheticDataset, output: Path) -> Path:
        writes.append((generated, output))
        return output

    monkeypatch.setattr(cli_module, "generate_dataset", fake_generate)
    monkeypatch.setattr(cli_module, "write_dataset", fake_write)

    result = cli_runner.invoke(
        cli_module.app,
        ["inject-failure", "--batch-date", "2025-07-10", "--output", str(destination)],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        (
            FailureScenario.INCIDENT,
            settings.seed,
            settings.generated_days,
            DEFAULT_BATCH_DATE,
            True,
        )
    ]
    assert writes == [(dataset, destination)]
    assert f"Incident fixture written to {destination.resolve()}" in result.output


def test_run_batch_reads_the_directory_and_honors_batch_and_dbt_options(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    tmp_path: Path,
) -> None:
    batch_directory = tmp_path / "source-batch"
    batch_directory.mkdir()
    dataset: SyntheticDataset = {"machines": [{"machine_id": "M-01"}]}
    read_calls: list[Path] = []
    fake_runner = _FakeRunner([_outcome(incident_id=None)])
    _install_dependencies(monkeypatch, settings, fake_runner)

    def fake_read(
        path: Path,
        *,
        settings: Settings | None = None,
        source_bytes: dict[str, bytes] | None = None,
    ) -> SyntheticDataset:
        assert settings is not None
        read_calls.append(path)
        assert source_bytes is not None
        source_bytes["machines"] = b"exact-source-bytes"
        return dataset

    monkeypatch.setattr(cli_module, "read_dataset", fake_read)

    result = cli_runner.invoke(
        cli_module.app,
        [
            "run-batch",
            "--path",
            str(batch_directory),
            "--batch-id",
            "manual-batch",
            "--scenario",
            "incident",
            "--skip-dbt",
        ],
    )

    assert result.exit_code == 0, result.output
    assert read_calls == [batch_directory]
    assert fake_runner.dataset_calls == [(dataset, "manual-batch", FailureScenario.INCIDENT, False)]
    assert fake_runner.dataset_source_bytes == [{"machines": b"exact-source-bytes"}]
    payload = cast(dict[str, Any], json.loads(result.output))
    assert payload["incident_id"] is None
    assert cast(dict[str, Any], payload["run"])["status"] == "healthy"


def test_run_batch_rejects_a_missing_directory_before_resolving_dependencies(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def unexpected_dependencies() -> tuple[
        Settings, PostgresRepository, S3ObjectStore, PipelineRunner
    ]:
        raise AssertionError("Typer path validation must run first")

    monkeypatch.setattr(cli_module, "_dependencies", unexpected_dependencies)
    monkeypatch.chdir(tmp_path)

    result = cli_runner.invoke(cli_module.app, ["run-batch", "--path", "missing"])

    assert result.exit_code == 2
    assert "does not exist" in result.output


def test_pipeline_runs_the_safe_incremental_batch_and_prints_outcome_json(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fake_runner = _FakeRunner([_outcome(incident_id=None)])
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["pipeline"])

    assert result.exit_code == 0, result.output
    assert fake_runner.generated_calls == [(FailureScenario.CLEAN, DEFAULT_BATCH_DATE, True, True)]
    payload = cast(dict[str, Any], json.loads(result.output))
    assert payload["summary"] == {"observed_facts": ["Four rows were accepted."]}
    assert payload["incident_id"] is None


def test_status_reuses_the_service_and_serializes_health_with_latest_run(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    monkeypatch.setattr(cli_module, "console", Console(color_system=None, width=10))
    fake_runner = _FakeRunner()
    repository = _install_dependencies(monkeypatch, settings, fake_runner)
    object_store = object()
    _install_dependencies(
        monkeypatch,
        settings,
        fake_runner,
        repository=repository,
        object_store=object_store,
    )
    constructor_calls: list[tuple[Settings, object, object | None]] = []

    class FakeService:
        def __init__(
            self,
            configured_settings: Settings,
            configured_repository: object,
            *,
            object_store: object | None = None,
        ) -> None:
            constructor_calls.append((configured_settings, configured_repository, object_store))

        def health(self) -> dict[str, object]:
            return {"status": "healthy", "writes_enabled": False}

        def get_latest_pipeline_status(self) -> dict[str, object]:
            return {"run_id": str(RUN_ID), "status": "healthy"}

    monkeypatch.setattr(cli_module, "ForgeFlowService", FakeService)

    result = cli_runner.invoke(cli_module.app, ["status"])

    assert result.exit_code == 0, result.output
    assert constructor_calls == [(settings, repository, object_store)]
    assert json.loads(result.output) == {
        "health": {"status": "healthy", "writes_enabled": False},
        "latest_run": {"run_id": str(RUN_ID), "status": "healthy"},
    }


def test_manual_csv_reader_parses_contract_numbers_and_empty_values(
    tmp_path: Path,
) -> None:
    batch = tmp_path / "batch"
    batch.mkdir()
    (batch / "production_orders.csv").write_text(
        "planned_quantity,actual_quantity,actual_start_at,product_code\n42,7,,PRODUCT-01\n",
        encoding="utf-8",
    )
    (batch / "machine_telemetry.csv").write_text(
        "temperature_c,energy_kwh,operating_state\n37.25,not-a-number,\n",
        encoding="utf-8",
    )
    configured = Settings(
        data_dir=tmp_path / "data",
        artifact_dir=tmp_path / "artifacts",
        max_source_file_bytes=1_000,
        max_source_rows_per_file=10,
    )

    raw_sources: dict[str, bytes] = {}
    dataset = cli_module.read_dataset(
        batch,
        settings=configured,
        source_bytes=raw_sources,
    )

    order = dataset["production_orders"][0]
    telemetry = dataset["machine_telemetry"][0]
    assert order["planned_quantity"] == 42
    assert isinstance(order["planned_quantity"], int)
    assert order["actual_quantity"] == 7
    assert order["actual_start_at"] is None
    assert order["product_code"] == "PRODUCT-01"
    assert telemetry["temperature_c"] == 37.25
    assert isinstance(telemetry["temperature_c"], float)
    assert telemetry["energy_kwh"] == "not-a-number"
    assert telemetry["operating_state"] is None
    assert raw_sources["production_orders"] == (batch / "production_orders.csv").read_bytes()
    assert raw_sources["machine_telemetry"] == (batch / "machine_telemetry.csv").read_bytes()


def test_manual_csv_reader_preserves_non_finite_values_for_row_quarantine(
    tmp_path: Path,
) -> None:
    batch = tmp_path / "batch"
    clean = generate_dataset(
        FailureScenario.CLEAN,
        seed=20250710,
        generated_days=3,
        batch_date=DEFAULT_BATCH_DATE,
        incremental=True,
    )
    base_record = dict(clean["machine_telemetry"][0])
    records = [base_record]
    for index, value in enumerate(("NaN", "inf", "-Infinity", "1e999"), start=1):
        invalid_record = dict(base_record)
        invalid_record["telemetry_id"] = f"TEL-NONFINITE-{index:06d}"
        invalid_record["temperature_c"] = value
        records.append(invalid_record)
    write_dataset({"machine_telemetry": records}, batch)

    dataset = cli_module.read_dataset(batch, settings=Settings())
    loaded = dataset["machine_telemetry"]

    assert isinstance(loaded[0]["temperature_c"], float)
    assert [record["temperature_c"] for record in loaded[1:]] == [
        "NaN",
        "inf",
        "-Infinity",
        "1e999",
    ]

    result = validate_records(
        "machine_telemetry",
        loaded,
        now=datetime(2025, 7, 12, 12, tzinfo=UTC),
    )

    assert len(result.accepted_records) == 1
    assert len(result.quarantined_records) == 4
    assert all(
        any(reason.code == "invalid_type" for reason in record.reasons)
        for record in result.quarantined_records
    )
    json.dumps(result.model_dump(mode="json"), allow_nan=False)


def test_manual_csv_reader_enforces_file_size_and_row_bounds(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    batch.mkdir()
    source = batch / "factories.csv"
    source.write_text("factory_id\nFACTORY-01\n", encoding="utf-8")
    small_file_settings = Settings(
        data_dir=tmp_path / "data",
        artifact_dir=tmp_path / "artifacts",
        max_source_file_bytes=8,
    )

    with pytest.raises(ForgeFlowError, match="8-byte limit"):
        cli_module.read_dataset(batch, settings=small_file_settings)

    source.write_text("factory_id\nFACTORY-01\nFACTORY-02\n", encoding="utf-8")
    one_row_settings = Settings(
        data_dir=tmp_path / "data",
        artifact_dir=tmp_path / "artifacts",
        max_source_file_bytes=1_000,
        max_source_rows_per_file=1,
    )

    with pytest.raises(ForgeFlowError, match="1-row limit"):
        cli_module.read_dataset(batch, settings=one_row_settings)


@pytest.mark.parametrize("unexpected_name", ["unexpected.csv", "unexpected.CSV"])
def test_manual_csv_reader_rejects_unregistered_csv_inputs(
    tmp_path: Path,
    unexpected_name: str,
) -> None:
    batch = tmp_path / "batch"
    batch.mkdir()
    (batch / "factories.csv").write_text(
        "factory_id,factory_name,timezone,country_code,updated_at\n"
        "FACTORY-01,Factory 01,UTC,HU,2025-07-10T00:00:00Z\n",
        encoding="utf-8",
    )
    (batch / unexpected_name).write_text("ignored,value\n", encoding="utf-8")
    raw_sources: dict[str, bytes] = {}

    with pytest.raises(ForgeFlowError, match="unregistered source CSV") as error:
        cli_module.read_dataset(batch, settings=Settings(), source_bytes=raw_sources)

    assert unexpected_name in str(error.value)
    assert raw_sources == {}


def test_manual_csv_reader_rejects_source_symlinks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    batch = tmp_path / "batch"
    batch.mkdir()
    source = batch / "factories.csv"
    source.write_text("factory_id\nFACTORY-01\n", encoding="utf-8")
    original_is_symlink = Path.is_symlink

    def is_symlink(path: Path) -> bool:
        return path == source or original_is_symlink(path)

    monkeypatch.setattr(Path, "is_symlink", is_symlink)

    with pytest.raises(ForgeFlowError, match="cannot be a symbolic link"):
        cli_module.read_dataset(batch, settings=Settings())


def test_run_batch_full_execution_exits_nonzero_for_an_unhealthy_outcome(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    tmp_path: Path,
) -> None:
    batch = tmp_path / "batch"
    batch.mkdir()
    fake_runner = _FakeRunner([_outcome(status=RunStatus.DEGRADED, incident_id=None)])
    _install_dependencies(monkeypatch, settings, fake_runner)
    monkeypatch.setattr(
        cli_module,
        "read_dataset",
        lambda path, settings=None, source_bytes=None: {
            "factories": [{"factory_id": "FACTORY-01"}]
        },
    )

    result = cli_runner.invoke(cli_module.app, ["run-batch", "--path", str(batch)])

    assert result.exit_code == 1
    assert json.loads(result.output)["run"]["status"] == "degraded"


@pytest.mark.parametrize("status", [RunStatus.DEGRADED, RunStatus.FAILED])
def test_run_batch_skip_dbt_exits_nonzero_for_any_unhealthy_outcome(
    status: RunStatus,
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    tmp_path: Path,
) -> None:
    batch = tmp_path / "batch"
    batch.mkdir()
    fake_runner = _FakeRunner([_outcome(status=status, incident_id=None)])
    _install_dependencies(monkeypatch, settings, fake_runner)
    monkeypatch.setattr(
        cli_module,
        "read_dataset",
        lambda path, settings=None, source_bytes=None: {
            "factories": [{"factory_id": "FACTORY-01"}]
        },
    )

    result = cli_runner.invoke(
        cli_module.app,
        ["run-batch", "--path", str(batch), "--skip-dbt"],
    )

    assert result.exit_code == 1
    assert json.loads(result.output)["run"]["status"] == status.value


@pytest.mark.parametrize("command", ["pipeline", "recover-demo"])
def test_operational_single_run_commands_exit_nonzero_for_unhealthy_outcomes(
    command: str,
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    fake_runner = _FakeRunner([_outcome(status=RunStatus.FAILED, incident_id=None)])
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, [command])

    assert result.exit_code == 1
    assert json.loads(result.output)["run"]["status"] == "failed"


def test_backfill_stops_and_exits_nonzero_at_the_first_unhealthy_day(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fake_runner = _FakeRunner(
        [
            _outcome(run_id=UUID("40000000-0000-0000-0000-000000000001"), incident_id=None),
            _outcome(
                run_id=UUID("40000000-0000-0000-0000-000000000002"),
                status=RunStatus.DEGRADED,
                incident_id=None,
            ),
            _outcome(run_id=UUID("40000000-0000-0000-0000-000000000003"), incident_id=None),
        ]
    )
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(
        cli_module.app,
        ["backfill", "--start", "2025-07-01", "--end", "2025-07-03"],
    )

    assert result.exit_code == 1
    payload = cast(list[dict[str, str]], json.loads(result.output))
    assert [item["status"] for item in payload] == ["healthy", "degraded"]
    assert len(fake_runner.generated_calls) == 2


def test_demo_stops_before_replay_when_the_baseline_is_unhealthy(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fake_runner = _FakeRunner([_outcome(status=RunStatus.FAILED, incident_id=None)])
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["demo"])

    assert result.exit_code == 1
    assert len(fake_runner.generated_calls) == 1
    assert "Idempotent replay" not in result.output


@pytest.mark.parametrize(
    ("source_file_count", "accepted_row_count", "quarantined_row_count", "skipped_file_count"),
    [(9, 4, 0, 0), (10, 0, 0, 10), (10, 4, 1, 0), (10, 4, 0, 1)],
)
def test_demo_requires_a_new_complete_clean_baseline(
    source_file_count: int,
    accepted_row_count: int,
    quarantined_row_count: int,
    skipped_file_count: int,
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    fake_runner = _FakeRunner(
        [
            _outcome(
                incident_id=None,
                source_file_count=source_file_count,
                accepted_row_count=accepted_row_count,
                quarantined_row_count=quarantined_row_count,
                skipped_file_count=skipped_file_count,
            )
        ]
    )
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["demo"])

    assert result.exit_code == 1
    assert len(fake_runner.generated_calls) == 1
    assert "Idempotent replay" not in result.output


@pytest.mark.parametrize(
    ("skipped_file_count", "accepted_row_count", "quarantined_row_count"),
    [(9, 0, 0), (10, 1, 0), (10, 0, 1)],
)
def test_demo_replay_requires_all_ten_files_skipped_and_no_processed_rows(
    skipped_file_count: int,
    accepted_row_count: int,
    quarantined_row_count: int,
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    fake_runner = _FakeRunner(
        [
            _outcome(incident_id=None),
            _outcome(
                incident_id=None,
                skipped_file_count=skipped_file_count,
                accepted_row_count=accepted_row_count,
                quarantined_row_count=quarantined_row_count,
            ),
        ]
    )
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["demo"])

    assert result.exit_code == 1
    assert len(fake_runner.generated_calls) == 2


def test_incident_demo_requires_a_failed_incident_with_persisted_identity(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fake_runner = _FakeRunner(
        [
            _outcome(incident_id=None),
            _outcome(
                scenario=FailureScenario.INCIDENT,
                status=RunStatus.DEGRADED,
                incident_id=None,
            ),
        ]
    )
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["incident-demo"])

    assert result.exit_code == 1
    assert len(fake_runner.generated_calls) == 2


@pytest.mark.parametrize(
    ("incident_id", "quarantined_row_count", "failed_checks", "schema_changes", "affected"),
    [
        (None, 1, 1, [INCIDENT_SCHEMA_CHANGE], ["mart_factory_performance"]),
        (INCIDENT_ID, 0, 1, [INCIDENT_SCHEMA_CHANGE], ["mart_factory_performance"]),
        (INCIDENT_ID, 1, 0, [INCIDENT_SCHEMA_CHANGE], ["mart_factory_performance"]),
        (INCIDENT_ID, 1, 1, [], ["mart_factory_performance"]),
        (INCIDENT_ID, 1, 1, [INCIDENT_SCHEMA_CHANGE], []),
    ],
)
def test_incident_demo_requires_the_complete_intended_evidence_bundle(
    incident_id: UUID | None,
    quarantined_row_count: int,
    failed_checks: int,
    schema_changes: list[SchemaChange],
    affected: list[str],
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    fake_runner = _FakeRunner(
        [
            _outcome(incident_id=None),
            _outcome(
                scenario=FailureScenario.INCIDENT,
                status=RunStatus.FAILED,
                incident_id=incident_id,
                quarantined_row_count=quarantined_row_count,
                failed_checks=failed_checks,
                schema_changes=schema_changes,
                affected_downstream_models=affected,
            ),
        ]
    )
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["incident-demo"])

    assert result.exit_code == 1
    assert len(fake_runner.generated_calls) == 2


def test_cleanup_rejects_nonlocal_or_non_demo_databases_before_mutation(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(cli_module, "PROJECT_ROOT", workspace)
    settings = Settings(
        database_url="postgresql://forgeflow:secret@db.example.invalid:5432/production",
        data_dir=workspace / ".forgeflow" / "data",
        artifact_dir=workspace / ".forgeflow" / "artifacts",
    )
    fake_runner = _FakeRunner()
    repository = _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["clean", "--force"])

    assert result.exit_code == 1
    assert isinstance(result.exception, ForgeFlowError)
    assert repository.initialize_calls == 0
    assert repository.clean_calls == 0


def test_cleanup_rejects_a_data_directory_outside_the_repository_runtime(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(cli_module, "PROJECT_ROOT", workspace)
    settings = Settings(
        data_dir=tmp_path / "outside" / "data",
        artifact_dir=workspace / ".forgeflow" / "artifacts",
    )
    fake_runner = _FakeRunner()
    repository = _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["clean", "--force"])

    assert result.exit_code == 1
    assert isinstance(result.exception, ForgeFlowError)
    assert repository.initialize_calls == 0
    assert repository.clean_calls == 0


def test_backfill_force_runs_each_inclusive_day_and_bounds_the_json_result(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    outcomes = [
        _outcome(
            run_id=UUID(f"30000000-0000-0000-0000-{offset:012d}"),
            batch_id=f"batch-{offset}",
            incident_id=None,
        )
        for offset in range(1, 9)
    ]
    fake_runner = _FakeRunner(outcomes)
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(
        cli_module.app,
        ["backfill", "--start", "2025-07-01", "--end", "2025-07-08", "--force"],
    )

    assert result.exit_code == 0, result.output
    assert fake_runner.generated_calls == [
        (FailureScenario.CLEAN, date(2025, 7, day), True, True) for day in range(1, 9)
    ]
    assert fake_runner.generated_options == [
        {
            "recovery_incident_id": None,
            "dbt_variables": {
                "backfill_start": date(2025, 7, day).isoformat(),
                "backfill_end": date(2025, 7, day + 1).isoformat(),
            },
        }
        for day in range(1, 9)
    ]
    payload = cast(list[dict[str, str]], json.loads(result.output))
    assert len(payload) == 8
    assert [item["status"] for item in payload] == ["healthy"] * 8
    assert [item["run_id"] for item in payload] == [
        str(outcome.summary.run_id) for outcome in outcomes
    ]


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (
            ["--start", "01-07-2025", "--end", "2025-07-02"],
            "start must use YYYY-MM-DD",
        ),
        (
            ["--start", "2025-07-02", "--end", "2025-07-01"],
            "end must be on or after start",
        ),
        (
            ["--start", "2025-07-01", "--end", "2025-07-08"],
            "use --force to confirm a backfill longer than seven days",
        ),
        (
            ["--start", "2025-01-01", "--end", "2025-02-01", "--force"],
            "backfills are limited to 31 days per invocation",
        ),
    ],
)
def test_backfill_validation_fails_before_any_external_dependency(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    message: str,
) -> None:
    def unexpected_dependencies() -> tuple[
        Settings, PostgresRepository, S3ObjectStore, PipelineRunner
    ]:
        raise AssertionError("validation must complete before dependencies are created")

    monkeypatch.setattr(cli_module, "_dependencies", unexpected_dependencies)

    result = cli_runner.invoke(cli_module.app, ["backfill", *arguments])

    assert result.exit_code == 2
    normalized_output = " ".join(unstyle(result.output).split())
    assert message in normalized_output


def test_clean_aborts_without_touching_dependencies_when_confirmation_is_declined(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime_root = tmp_path / ".forgeflow" / "data"
    safe_settings = Settings(
        data_dir=runtime_root,
        artifact_dir=tmp_path / ".forgeflow" / "artifacts",
    )
    monkeypatch.setattr(cli_module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(cli_module, "get_settings", lambda: safe_settings)

    def unexpected_dependencies() -> tuple[
        Settings, PostgresRepository, S3ObjectStore, PipelineRunner
    ]:
        raise AssertionError("declining confirmation must have no side effects")

    monkeypatch.setattr(cli_module, "_dependencies", unexpected_dependencies)

    result = cli_runner.invoke(cli_module.app, ["clean"], input="n\n")

    assert result.exit_code == 1
    assert "Delete ForgeFlow warehouse demo rows" in result.output
    assert str(tmp_path / ".forgeflow") in result.output
    assert "Aborted" in result.output


def test_clean_confirmation_runs_repository_cleanup(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime_root = tmp_path / ".forgeflow" / "data"
    settings = Settings(
        data_dir=runtime_root,
        artifact_dir=tmp_path / ".forgeflow" / "artifacts",
    )
    monkeypatch.setattr(cli_module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    fake_runner = _FakeRunner()
    repository = _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["clean"], input="y\n")

    assert result.exit_code == 0, result.output
    assert repository.initialize_calls == 1
    assert repository.clean_calls == 1
    assert "warehouse rows and generated files were removed" in result.output


def test_clean_force_skips_prompt_and_deletes_only_nested_runtime_state(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(cli_module, "PROJECT_ROOT", tmp_path)
    runtime_root = tmp_path / ".forgeflow" / "data"
    runtime_root.mkdir(parents=True)
    (runtime_root / "generated.csv").write_text("synthetic", encoding="utf-8")
    settings = Settings(
        data_dir=runtime_root,
        artifact_dir=tmp_path / ".forgeflow" / "artifacts",
    )
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    fake_runner = _FakeRunner()
    repository = _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["clean", "--force"])

    assert result.exit_code == 0, result.output
    assert "Delete ForgeFlow" not in result.output
    assert repository.initialize_calls == 1
    assert repository.clean_calls == 1
    assert not runtime_root.exists()
    assert "Raw MinIO objects and container volumes are retained" in result.output


def test_cleanup_translates_runtime_deletion_failure_after_database_cleanup(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime_root = tmp_path / ".forgeflow" / "data"
    runtime_root.mkdir(parents=True)
    settings = Settings(
        data_dir=runtime_root,
        artifact_dir=tmp_path / ".forgeflow" / "artifacts",
    )
    monkeypatch.setattr(cli_module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)

    def fail_removal(path: Path) -> None:
        del path
        raise PermissionError("locked runtime")

    monkeypatch.setattr("forgeflow.cli.shutil.rmtree", fail_removal)
    fake_runner = _FakeRunner()
    repository = _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["clean", "--force"])

    assert result.exit_code == 1
    assert isinstance(result.exception, ForgeFlowError)
    assert "runtime path could not be deleted" in str(result.exception)
    assert repository.clean_calls == 1


def test_cleanup_never_uses_the_current_directory_as_its_trust_root(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repository_root = tmp_path / "repository"
    repository_root.mkdir()
    unrelated_cwd = tmp_path / "unrelated"
    unrelated_cwd.mkdir()
    dangerous_target = unrelated_cwd / "generated"
    dangerous_target.mkdir()
    (dangerous_target / "keep.txt").write_text("must survive", encoding="utf-8")
    settings = Settings(
        data_dir=dangerous_target,
        artifact_dir=repository_root / ".forgeflow" / "artifacts",
    )
    monkeypatch.chdir(unrelated_cwd)
    monkeypatch.setattr(cli_module, "PROJECT_ROOT", repository_root)
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    fake_runner = _FakeRunner()
    repository = _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["clean", "--force"])

    assert result.exit_code == 1
    assert dangerous_target.exists()
    assert (dangerous_target / "keep.txt").exists()
    assert repository.clean_calls == 0


def test_cleanup_rejects_a_redirected_repository_runtime_root(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repository_root = tmp_path / "repository"
    repository_root.mkdir()
    outside = tmp_path / "outside"
    (outside / "data").mkdir(parents=True)
    (outside / "artifacts").mkdir()
    (outside / "data" / "keep.txt").write_text("must survive", encoding="utf-8")
    try:
        (repository_root / ".forgeflow").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks are unavailable on this Windows host")
    settings = Settings(
        data_dir=repository_root / ".forgeflow" / "data",
        artifact_dir=repository_root / ".forgeflow" / "artifacts",
    )
    monkeypatch.setattr(cli_module, "PROJECT_ROOT", repository_root)
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    fake_runner = _FakeRunner()
    repository = _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["clean", "--force"])

    assert result.exit_code == 1
    assert (outside / "data" / "keep.txt").exists()
    assert repository.clean_calls == 0


def test_demo_proves_baseline_then_idempotent_replay(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    first = _outcome(batch_id="baseline", incident_id=None)
    replay = _outcome(
        batch_id="replay",
        incident_id=None,
        accepted_row_count=0,
        quarantined_row_count=0,
        skipped_file_count=10,
    )
    fake_runner = _FakeRunner([first, replay])
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["demo"])

    assert result.exit_code == 0, result.output
    assert fake_runner.generated_calls == [
        (FailureScenario.CLEAN, DEFAULT_BATCH_DATE, False, True),
        (FailureScenario.CLEAN, DEFAULT_BATCH_DATE, False, True),
    ]
    assert result.output.index("Healthy baseline") < result.output.index("Idempotent replay")
    assert '"batch_id": "baseline"' in result.output
    assert '"batch_id": "replay"' in result.output


def test_incident_demo_orders_baseline_before_incremental_incident(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fake_runner = _FakeRunner(
        [
            _outcome(batch_id="baseline", incident_id=None),
            _outcome(
                batch_id="incident",
                scenario=FailureScenario.INCIDENT,
                status=RunStatus.FAILED,
                quarantined_row_count=1,
                failed_checks=1,
                schema_changes=[INCIDENT_SCHEMA_CHANGE],
                affected_downstream_models=["mart_factory_performance"],
            ),
        ]
    )
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["incident-demo"])

    assert result.exit_code == 0, result.output
    assert fake_runner.generated_calls == [
        (FailureScenario.CLEAN, DEFAULT_BATCH_DATE, False, True),
        (FailureScenario.INCIDENT, DEFAULT_BATCH_DATE, True, True),
    ]
    assert result.output.index("Baseline") < result.output.index("Intentional incident")


@pytest.mark.parametrize(
    ("outcome_incident_id", "quarantined_row_count", "failed_checks"),
    [
        (UUID("20000000-0000-0000-0000-000000000099"), 0, 0),
        (INCIDENT_ID, 1, 0),
        (INCIDENT_ID, 0, 1),
    ],
)
def test_recover_demo_requires_exact_incident_link_and_clean_quality(
    outcome_incident_id: UUID,
    quarantined_row_count: int,
    failed_checks: int,
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    fake_runner = _FakeRunner(
        [
            _outcome(
                scenario=FailureScenario.RECOVERY,
                incident_id=outcome_incident_id,
                quarantined_row_count=quarantined_row_count,
                failed_checks=failed_checks,
            )
        ]
    )
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["recover-demo"])

    assert result.exit_code == 1


def test_recover_demo_runs_the_incremental_recovery_scenario(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fake_runner = _FakeRunner(
        [
            _outcome(
                scenario=FailureScenario.RECOVERY,
                incident_id=INCIDENT_ID,
                quarantined_row_count=0,
                failed_checks=0,
            )
        ]
    )
    _install_dependencies(monkeypatch, settings, fake_runner)

    result = cli_runner.invoke(cli_module.app, ["recover-demo"])

    assert result.exit_code == 0, result.output
    assert fake_runner.generated_calls == [
        (FailureScenario.RECOVERY, DEFAULT_BATCH_DATE, True, True)
    ]
    assert fake_runner.generated_options == [
        {"recovery_incident_id": INCIDENT_ID, "dbt_variables": None}
    ]
    assert cast(dict[str, Any], json.loads(result.output))["incident_id"] == str(INCIDENT_ID)


def test_outcome_json_uses_pydantic_json_values_and_stringifies_incident_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output: list[str] = []
    monkeypatch.setattr(typer, "echo", output.append)

    cli_module._print_outcome(_outcome())

    payload = cast(dict[str, Any], json.loads(output[0]))
    run = cast(dict[str, Any], payload["run"])
    assert run["run_id"] == str(RUN_ID)
    assert run["scenario"] == "clean"
    assert run["status"] == "healthy"
    assert run["started_at"] == "2025-07-10T08:00:00Z"
    assert payload["incident_id"] == str(INCIDENT_ID)


def test_parse_date_accepts_iso_dates_and_exposes_the_option_in_errors() -> None:
    assert cli_module._parse_date("2024-02-29", "start") == date(2024, 2, 29)

    with pytest.raises(typer.BadParameter, match="start must use YYYY-MM-DD") as exc_info:
        cli_module._parse_date("2023-02-29", "start")

    assert exc_info.value.param_hint == "--start"


def test_main_translates_domain_errors_to_a_safe_nonzero_cli_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = io.StringIO()

    def failing_app() -> None:
        raise ForgeFlowError("warehouse unavailable")

    monkeypatch.setattr(cli_module, "app", failing_app)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=output, color_system=None, width=120),
    )

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code == 1
    assert output.getvalue().strip() == "ForgeFlow error: warehouse unavailable"
