"""Cross-platform ForgeFlow CLI for generation, pipelines, incidents, and backfills."""

from __future__ import annotations

import csv
import io
import json
import math
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit
from uuid import UUID

import typer
from rich.console import Console

from forgeflow.config import Settings, get_settings
from forgeflow.contracts import SOURCE_CONTRACTS, ColumnRule
from forgeflow.errors import ForgeFlowError
from forgeflow.logging import configure_logging
from forgeflow.models import FailureScenario, RunStatus
from forgeflow.object_store import S3ObjectStore
from forgeflow.pipeline import (
    PipelineOutcome,
    PipelineRunner,
    build_batch_id,
    write_dataset,
)
from forgeflow.service import ForgeFlowService
from forgeflow.synthetic import SOURCE_NAMES, SyntheticDataset, SyntheticRecord, generate_dataset
from forgeflow.warehouse import PostgresRepository

app = typer.Typer(
    name="forgeflow",
    help="Operate the local synthetic ForgeFlow data reliability platform.",
    no_args_is_help=True,
)
console = Console()
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _dependencies() -> tuple[Settings, PostgresRepository, S3ObjectStore, PipelineRunner]:
    settings = get_settings()
    repository = PostgresRepository(settings)
    object_store = S3ObjectStore(settings)
    runner = PipelineRunner(settings, repository, object_store)
    return settings, repository, object_store, runner


@app.callback()
def callback() -> None:
    """Configure safe structured logs for every CLI invocation."""
    configure_logging(get_settings().log_level)


@app.command()
def generate(
    scenario: Annotated[FailureScenario, typer.Option(help="Named deterministic scenario.")] = (
        FailureScenario.CLEAN
    ),
    batch_date: Annotated[str | None, typer.Option(help="Batch date (YYYY-MM-DD).")] = None,
    incremental: Annotated[bool, typer.Option(help="Generate one day instead of history.")] = False,
    seed: Annotated[
        int | None, typer.Option(help="Override the configured deterministic seed.")
    ] = None,
    output: Annotated[Path | None, typer.Option(help="Explicit output directory.")] = None,
) -> None:
    """Generate source CSVs without loading them."""
    settings = get_settings()
    resolved_date = (
        _parse_date(batch_date, "batch-date")
        if batch_date
        else datetime.now(UTC).date() - timedelta(days=2)
    )
    resolved_seed = seed if seed is not None else settings.seed
    batch_id = build_batch_id(
        scenario=scenario,
        batch_date=resolved_date,
        seed=resolved_seed,
        incremental=incremental,
    )
    dataset = generate_dataset(
        scenario,
        seed=resolved_seed,
        generated_days=settings.generated_days,
        batch_date=resolved_date,
        incremental=incremental,
    )
    destination = output or settings.data_dir / "generated" / batch_id
    write_dataset(dataset, destination)
    typer.echo(
        json.dumps(
            {
                "batch_id": batch_id,
                "scenario": scenario.value,
                "path": str(destination.resolve()),
                "row_counts": {name: len(rows) for name, rows in dataset.items()},
            },
            indent=2,
            sort_keys=True,
        )
    )


@app.command("run-batch")
def run_batch(
    path: Annotated[Path, typer.Option(exists=True, file_okay=False, help="Source directory.")],
    batch_id: Annotated[str | None, typer.Option(help="Override batch identity.")] = None,
    scenario: Annotated[FailureScenario, typer.Option()] = FailureScenario.CLEAN,
    skip_dbt: Annotated[bool, typer.Option(help="Ingestion-only diagnostic run.")] = False,
) -> None:
    """Run a generated directory through landing, contracts, warehouse, and dbt."""
    settings, _, _, runner = _dependencies()
    raw_sources: dict[str, bytes] = {}
    dataset = read_dataset(path, settings=settings, source_bytes=raw_sources)
    outcome = runner.run_dataset(
        dataset,
        batch_id=batch_id or path.resolve().name,
        scenario=scenario,
        run_dbt=not skip_dbt,
        source_bytes=raw_sources,
    )
    _print_outcome(outcome)
    _require_status(outcome, RunStatus.HEALTHY)


@app.command()
def pipeline() -> None:
    """Generate and run today's safe incremental clean batch."""
    _, _, _, runner = _dependencies()
    outcome = runner.run_generated(
        FailureScenario.CLEAN,
        batch_date=datetime.now(UTC).date() - timedelta(days=2),
        incremental=True,
    )
    _print_outcome(outcome)
    _require_status(outcome, RunStatus.HEALTHY)


@app.command("inject-failure")
def inject_failure(
    batch_date: Annotated[str | None, typer.Option(help="Incident batch date.")] = None,
    output: Annotated[Path | None, typer.Option(help="Explicit output directory.")] = None,
) -> None:
    """Generate the complete named incident fixture without running it."""
    settings = get_settings()
    resolved_date = (
        _parse_date(batch_date, "batch-date")
        if batch_date
        else datetime.now(UTC).date() - timedelta(days=2)
    )
    batch_id = build_batch_id(
        scenario=FailureScenario.INCIDENT,
        batch_date=resolved_date,
        seed=settings.seed,
        incremental=True,
    )
    dataset = generate_dataset(
        FailureScenario.INCIDENT,
        seed=settings.seed,
        generated_days=settings.generated_days,
        batch_date=resolved_date,
        incremental=True,
    )
    destination = output or settings.data_dir / "generated" / batch_id
    write_dataset(dataset, destination)
    console.print(f"Incident fixture written to {destination.resolve()}")


@app.command()
def status() -> None:
    """Show dependency health and the latest persisted run."""
    settings, repository, object_store, _ = _dependencies()
    service = ForgeFlowService(settings, repository, object_store=object_store)
    typer.echo(
        json.dumps(
            {
                "health": service.health(),
                "latest_run": service.get_latest_pipeline_status(),
            },
            indent=2,
            sort_keys=True,
        )
    )


@app.command()
def backfill(
    start: Annotated[str, typer.Option(help="Inclusive first batch date (YYYY-MM-DD).")],
    end: Annotated[str, typer.Option(help="Inclusive last batch date (YYYY-MM-DD).")],
    force: Annotated[bool, typer.Option(help="Allow more than seven daily batches.")] = False,
) -> None:
    """Run a controlled daily backfill with a 31-day safety bound."""
    start_date = _parse_date(start, "start")
    end_date = _parse_date(end, "end")
    if end_date < start_date:
        raise typer.BadParameter("end must be on or after start")
    days = (end_date - start_date).days + 1
    if days > 31:
        raise typer.BadParameter("backfills are limited to 31 days per invocation")
    if days > 7 and not force:
        raise typer.BadParameter("use --force to confirm a backfill longer than seven days")
    _, _, _, runner = _dependencies()
    outcomes: list[dict[str, str]] = []
    all_healthy = True
    for offset in range(days):
        current_date = start_date + timedelta(days=offset)
        outcome = runner.run_generated(
            FailureScenario.CLEAN,
            batch_date=current_date,
            incremental=True,
            dbt_variables={
                "backfill_start": current_date.isoformat(),
                "backfill_end": (current_date + timedelta(days=1)).isoformat(),
            },
        )
        outcomes.append(
            {"run_id": str(outcome.summary.run_id), "status": outcome.summary.status.value}
        )
        if outcome.summary.status != RunStatus.HEALTHY:
            all_healthy = False
            break
    typer.echo(json.dumps(outcomes, indent=2))
    if not all_healthy:
        raise typer.Exit(code=1)


@app.command()
def clean(
    force: Annotated[bool, typer.Option(help="Confirm deletion of local synthetic demo state.")] = (
        False
    ),
) -> None:
    """Remove only configured synthetic demo state; require explicit confirmation."""
    settings = get_settings()
    runtime_roots = _validate_cleanup_scope(settings)
    if not force:
        typer.confirm(
            "Delete ForgeFlow warehouse demo rows and generated files under "
            f"{', '.join(str(path) for path in runtime_roots)}?",
            abort=True,
        )
    _, repository, _, _ = _dependencies()
    repository.initialize()
    repository.clean_demo_state(confirmed=True)
    for runtime_root in runtime_roots:
        if not runtime_root.exists():
            continue
        try:
            shutil.rmtree(runtime_root)
        except OSError as error:
            raise ForgeFlowError(
                "Warehouse demo rows were removed, but a repository-owned runtime path "
                f"could not be deleted: {runtime_root}"
            ) from error
    console.print("Synthetic warehouse rows and generated files were removed.")
    console.print("Raw MinIO objects and container volumes are retained for safety and replay.")


@app.command()
def demo() -> None:
    """Run a healthy baseline, then prove identical ingestion is skipped."""
    _, _, _, runner = _dependencies()
    batch_date = datetime.now(UTC).date() - timedelta(days=2)
    first = runner.run_generated(FailureScenario.CLEAN, batch_date=batch_date)
    console.print("Healthy baseline")
    _print_outcome(first)
    _require_demo_baseline(first)
    repeated = runner.run_generated(FailureScenario.CLEAN, batch_date=batch_date)
    console.print("Idempotent replay")
    _print_outcome(repeated)
    _require_demo_replay(repeated)


@app.command("incident-demo")
def incident_demo() -> None:
    """Run a healthy baseline followed by the deterministic incident batch."""
    _, _, _, runner = _dependencies()
    batch_date = datetime.now(UTC).date() - timedelta(days=2)
    baseline = runner.run_generated(FailureScenario.CLEAN, batch_date=batch_date)
    console.print("Baseline")
    _print_outcome(baseline)
    _require_status(baseline, RunStatus.HEALTHY)
    incident = runner.run_generated(
        FailureScenario.INCIDENT,
        batch_date=batch_date,
        incremental=True,
    )
    console.print("Intentional incident")
    _print_outcome(incident)
    _require_demo_incident(incident)


@app.command("recover-demo")
def recover_demo() -> None:
    """Upsert corrected records, rerun dbt, and resolve the retained incident."""
    _, repository, _, runner = _dependencies()
    repository.initialize()
    incident = repository.latest_open_incident()
    if incident is None:
        raise ForgeFlowError("No open incident is available for recovery")
    incident_id = UUID(str(incident["incident_id"]))
    outcome = runner.run_generated(
        FailureScenario.RECOVERY,
        batch_date=datetime.now(UTC).date() - timedelta(days=2),
        incremental=True,
        recovery_incident_id=incident_id,
    )
    _print_outcome(outcome)
    _require_demo_recovery(outcome, incident_id)


def _print_outcome(outcome: PipelineOutcome) -> None:
    typer.echo(
        json.dumps(
            {
                "run": outcome.summary.model_dump(mode="json"),
                "incident_id": str(outcome.incident_id) if outcome.incident_id else None,
                "summary": outcome.human_summary,
            },
            indent=2,
            sort_keys=True,
        )
    )


def _require_status(outcome: PipelineOutcome, expected: RunStatus) -> None:
    """Return a nonzero process status when an operational outcome is unexpected."""
    if outcome.summary.status != expected:
        raise typer.Exit(code=1)


def _require_demo_replay(outcome: PipelineOutcome) -> None:
    """Require the reviewer demo to prove a complete ten-file idempotent replay."""
    _require_status(outcome, RunStatus.HEALTHY)
    if (
        outcome.summary.skipped_file_count != len(SOURCE_NAMES)
        or outcome.summary.accepted_row_count != 0
        or outcome.summary.quarantined_row_count != 0
    ):
        raise typer.Exit(code=1)


def _require_demo_baseline(outcome: PipelineOutcome) -> None:
    """Require a newly accepted complete baseline rather than a pre-existing replay."""
    _require_status(outcome, RunStatus.HEALTHY)
    if (
        outcome.summary.source_file_count != len(SOURCE_NAMES)
        or outcome.summary.accepted_row_count <= 0
        or outcome.summary.quarantined_row_count != 0
        or outcome.summary.skipped_file_count != 0
    ):
        raise typer.Exit(code=1)


def _require_demo_incident(outcome: PipelineOutcome) -> None:
    """Require the intended evidence-rich incident rather than any generic failure."""
    _require_status(outcome, RunStatus.FAILED)
    if (
        outcome.incident_id is None
        or outcome.summary.quarantined_row_count == 0
        or outcome.summary.failed_checks == 0
        or not outcome.summary.schema_changes
        or not outcome.summary.affected_downstream_models
    ):
        raise typer.Exit(code=1)


def _require_demo_recovery(outcome: PipelineOutcome, incident_id: UUID) -> None:
    """Require a clean recovery linked to exactly the selected persisted incident."""
    _require_status(outcome, RunStatus.HEALTHY)
    if (
        outcome.incident_id != incident_id
        or outcome.summary.quarantined_row_count != 0
        or outcome.summary.failed_checks != 0
    ):
        raise typer.Exit(code=1)


def read_dataset(
    directory: Path,
    *,
    settings: Settings | None = None,
    source_bytes: dict[str, bytes] | None = None,
) -> SyntheticDataset:
    """Read bounded registered CSV files without following links outside the batch root."""
    configured = settings or get_settings()
    if directory.is_symlink():
        raise ForgeFlowError("Batch directories cannot be symbolic links")
    try:
        resolved = directory.resolve(strict=True)
    except OSError as error:
        raise ForgeFlowError(f"Batch directory cannot be resolved: {directory}") from error
    if not resolved.is_dir():
        raise ForgeFlowError(f"Batch directory does not exist: {resolved}")

    registered_csv_names = {f"{source_name}.csv" for source_name in SOURCE_NAMES}
    unregistered_csv: str | None = None
    try:
        for entry in resolved.iterdir():
            if (
                entry.name.casefold().endswith(".csv")
                and entry.name not in registered_csv_names
                and (unregistered_csv is None or entry.name < unregistered_csv)
            ):
                unregistered_csv = entry.name
    except OSError as error:
        raise ForgeFlowError(f"Batch directory cannot be inspected: {resolved}") from error
    if unregistered_csv is not None:
        raise ForgeFlowError(
            f"Batch directory contains an unregistered source CSV: {unregistered_csv!r}"
        )

    dataset: SyntheticDataset = {}
    for source_name in SOURCE_NAMES:
        candidate = resolved / f"{source_name}.csv"
        if candidate.is_symlink():
            raise ForgeFlowError(f"Source CSV cannot be a symbolic link: {candidate.name}")
        if not candidate.exists():
            continue
        try:
            source_path = candidate.resolve(strict=True)
            source_path.relative_to(resolved)
        except (OSError, ValueError) as error:
            raise ForgeFlowError(
                f"Source CSV escapes the batch directory: {candidate.name}"
            ) from error
        if not source_path.is_file():
            raise ForgeFlowError(f"Source CSV is not a regular file: {candidate.name}")
        try:
            file_size = source_path.stat().st_size
        except OSError as error:
            raise ForgeFlowError(f"Source CSV cannot be inspected: {candidate.name}") from error
        if file_size > configured.max_source_file_bytes:
            raise ForgeFlowError(
                f"Source CSV exceeds the {configured.max_source_file_bytes}-byte limit: "
                f"{candidate.name}"
            )
        try:
            raw_content = source_path.read_bytes()
        except OSError as error:
            raise ForgeFlowError(f"Source CSV cannot be read: {candidate.name}") from error
        if len(raw_content) > configured.max_source_file_bytes:
            raise ForgeFlowError(
                f"Source CSV exceeds the {configured.max_source_file_bytes}-byte limit: "
                f"{candidate.name}"
            )
        dataset[source_name] = _read_source_csv(
            source_path,
            raw_content=raw_content,
            source_name=source_name,
            max_rows=configured.max_source_rows_per_file,
        )
        if source_bytes is not None:
            source_bytes[source_name] = raw_content
    if not dataset:
        raise ForgeFlowError(f"No registered source CSV files were found in {resolved}")
    return dataset


def _read_source_csv(
    path: Path,
    *,
    raw_content: bytes,
    source_name: str,
    max_rows: int,
) -> list[SyntheticRecord]:
    contract = SOURCE_CONTRACTS[source_name]
    records: list[SyntheticRecord] = []
    try:
        with io.StringIO(raw_content.decode("utf-8"), newline="") as handle:
            reader = csv.DictReader(handle, strict=True)
            if reader.fieldnames is None:
                raise ForgeFlowError(f"Source CSV has no header: {path.name}")
            if any(not name for name in reader.fieldnames) or len(set(reader.fieldnames)) != len(
                reader.fieldnames
            ):
                raise ForgeFlowError(f"Source CSV has empty or duplicate headers: {path.name}")
            for row_number, row in enumerate(reader, start=2):
                if row_number - 1 > max_rows:
                    raise ForgeFlowError(
                        f"Source CSV exceeds the {max_rows}-row limit: {path.name}"
                    )
                if None in row:
                    raise ForgeFlowError(
                        f"Source CSV has too many fields on row {row_number}: {path.name}"
                    )
                parsed: SyntheticRecord = {}
                for column, value in row.items():
                    if column is None:
                        raise ForgeFlowError(
                            f"Source CSV has too many fields on row {row_number}: {path.name}"
                        )
                    parsed[column] = _parse_csv_value(value, contract.columns.get(column))
                records.append(parsed)
    except (OSError, UnicodeError, csv.Error) as error:
        raise ForgeFlowError(f"Source CSV cannot be parsed: {path.name}") from error
    return records


def _parse_csv_value(value: str | None, rule: ColumnRule | None) -> object:
    if value is None or value == "":
        return None
    kind = rule.kind if rule is not None else None
    if kind == "integer":
        try:
            return int(value)
        except ValueError:
            return value
    if kind == "number":
        try:
            parsed = float(value)
        except ValueError:
            return value
        if not math.isfinite(parsed):
            return value
        return parsed
    return value


def _validate_cleanup_scope(settings: Settings) -> tuple[Path, ...]:
    """Restrict cleanup to the local demo database and repository-owned runtime subtree."""
    database = urlsplit(settings.database_url.get_secret_value())
    if (
        database.scheme not in {"postgres", "postgresql"}
        or database.hostname not in {"127.0.0.1", "localhost", "::1"}
        or database.path.removeprefix("/") != "forgeflow"
    ):
        raise ForgeFlowError(
            "Cleanup is restricted to the local PostgreSQL forgeflow demo database"
        )
    repository_root = PROJECT_ROOT.resolve()
    owned_runtime_root = repository_root / ".forgeflow"
    configured_roots = (settings.data_dir.resolve(), settings.artifact_dir.resolve())
    if any(
        path == owned_runtime_root or owned_runtime_root not in path.parents
        for path in configured_roots
    ):
        raise ForgeFlowError(
            "Cleanup data_dir and artifact_dir must be children of the repository-owned "
            ".forgeflow directory"
        )
    return tuple(dict.fromkeys(configured_roots))


def _parse_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise typer.BadParameter(
            f"{option_name} must use YYYY-MM-DD",
            param_hint=f"--{option_name}",
        ) from error


def main() -> None:
    """Console-script entry point."""
    try:
        app()
    except ForgeFlowError as error:
        console.print(f"[red]ForgeFlow error:[/red] {error}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
