"""Dagster assets, job, resources, and schedule for the complete ForgeFlow path."""

from datetime import date
from typing import Any
from uuid import UUID

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Config,
    Definitions,
    Failure,
    MetadataValue,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from forgeflow.config import get_settings
from forgeflow.models import FailureScenario, RunStatus
from forgeflow.object_store import S3ObjectStore
from forgeflow.pipeline import PipelineRunner
from forgeflow.service import ForgeFlowService
from forgeflow.warehouse import PostgresRepository


class PipelineAssetConfig(Config):
    """Run configuration exposed in Dagster Launchpad."""

    scenario: str = FailureScenario.CLEAN.value
    batch_date: str | None = None
    incremental: bool = True


@asset(
    group_name="platform",
    retry_policy=RetryPolicy(max_retries=3, delay=5),
    description="Initialize schemas and verify transient PostgreSQL/MinIO dependencies.",
)
def platform_ready(context: AssetExecutionContext) -> dict[str, str]:
    """Prepare only external dependencies; retries do not mask deterministic data failures."""
    settings = get_settings()
    repository = PostgresRepository(settings)
    object_store = S3ObjectStore(settings)
    repository.initialize()
    object_store.ensure_bucket()
    state = {
        "warehouse": "reachable" if repository.ping() else "unreachable",
        "object_store": "reachable" if object_store.ping() else "unreachable",
    }
    context.add_output_metadata(state)
    if "unreachable" in state.values():
        raise Failure("A required ForgeFlow dependency is unreachable", metadata=state)
    return state


@asset(
    group_name="pipeline",
    deps=[platform_ready],
    description=(
        "Generate/discover, land, validate, load, transform, test, parse artifacts, and finalize "
        "one evidence-rich ForgeFlow run."
    ),
)
def forgeflow_pipeline_run(
    context: AssetExecutionContext, config: PipelineAssetConfig
) -> dict[str, Any]:
    """Execute the canonical pipeline and propagate failure after evidence persistence."""
    settings = get_settings()
    repository = PostgresRepository(settings)
    runner = PipelineRunner(settings, repository, S3ObjectStore(settings))
    scenario = FailureScenario(config.scenario)
    batch_date = date.fromisoformat(config.batch_date) if config.batch_date else None
    outcome = runner.run_generated(
        scenario,
        batch_date=batch_date,
        incremental=config.incremental,
    )
    metadata = {
        "run_id": str(outcome.summary.run_id),
        "batch_id": outcome.summary.batch_id,
        "status": outcome.summary.status.value,
        "accepted_rows": outcome.summary.accepted_row_count,
        "quarantined_rows": outcome.summary.quarantined_row_count,
        "failed_checks": outcome.summary.failed_checks,
        "summary": MetadataValue.json(outcome.human_summary),
    }
    context.add_output_metadata(metadata)
    if outcome.summary.status != RunStatus.HEALTHY:
        raise Failure(
            "ForgeFlow did not produce a healthy run; inspect persisted warning/failure evidence",
            metadata=metadata,
        )
    return {
        "run_id": str(outcome.summary.run_id),
        "status": outcome.summary.status.value,
        "incident_id": str(outcome.incident_id) if outcome.incident_id else None,
    }


@asset(
    group_name="observability",
    description="Publish the same bounded run summary consumed by API, MCP, CLI, and dashboard.",
)
def latest_operational_summary(
    context: AssetExecutionContext, forgeflow_pipeline_run: dict[str, Any]
) -> dict[str, Any]:
    """Materialize a reviewer-readable operational summary from the shared service."""
    settings = get_settings()
    service = ForgeFlowService(settings, PostgresRepository(settings))
    run_id = UUID(str(forgeflow_pipeline_run["run_id"]))
    summary = service.get_pipeline_run(run_id)
    if summary is None:
        raise Failure("The finalized ForgeFlow run could not be read from observability metadata")
    context.add_output_metadata(
        {
            "run_id": str(run_id),
            "status": str(summary["status"]),
            "failed_checks": int(summary.get("failed_checks", 0)),
        }
    )
    return summary


forgeflow_daily_job = define_asset_job(
    "forgeflow_daily_job",
    selection=AssetSelection.assets(
        platform_ready,
        forgeflow_pipeline_run,
        latest_operational_summary,
    ),
    description="Daily healthy incremental ForgeFlow pipeline.",
)

forgeflow_daily_schedule = ScheduleDefinition(
    name="forgeflow_daily_schedule",
    job=forgeflow_daily_job,
    cron_schedule="0 3 * * *",
    execution_timezone="UTC",
    run_config={
        "ops": {
            "forgeflow_pipeline_run": {
                "config": {
                    "scenario": FailureScenario.CLEAN.value,
                    "incremental": True,
                }
            }
        }
    },
)

defs = Definitions(
    assets=[platform_ready, forgeflow_pipeline_run, latest_operational_summary],
    jobs=[forgeflow_daily_job],
    schedules=[forgeflow_daily_schedule],
)
