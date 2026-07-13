"""Lightweight operational Streamlit dashboard over the shared service layer."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from uuid import UUID

import pandas as pd
import streamlit as st
from streamlit.web import cli as stcli

from forgeflow.config import get_settings
from forgeflow.errors import ForgeFlowError
from forgeflow.service import ForgeFlowService, build_service


def render_dashboard(service: ForgeFlowService | None = None) -> None:
    """Render all operational states using actual ForgeFlow metadata."""
    st.set_page_config(
        page_title="ForgeFlow reliability console",
        page_icon="⚙️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title("ForgeFlow")
    st.caption("AI-assisted industrial data reliability — deterministic synthetic data only")
    current = service or build_service(get_settings())

    try:
        with st.spinner("Loading platform evidence…"):
            health = current.health()
            latest = current.get_latest_pipeline_status()
    except ForgeFlowError as error:
        _failed_state(str(error))
        return

    with st.sidebar:
        st.subheader("Platform state")
        st.status(
            f"Warehouse: {health['warehouse']} · Object store: {health['object_store']}",
            state="complete" if health["status"] == "healthy" else "error",
            expanded=False,
        )
        st.caption("Read-only console. Pipeline mutations are intentionally unavailable here.")

    if latest is None:
        _empty_state()
        return

    status = str(latest["status"])
    if status == "healthy":
        st.success("Latest pipeline run is healthy.")
    elif status == "degraded":
        st.warning("Latest pipeline run completed with quarantine or warning evidence.")
    else:
        st.error("Latest pipeline run failed. Evidence and downstream impact are retained below.")

    tabs = st.tabs(
        [
            "Overview",
            "Runs",
            "Freshness",
            "Quality",
            "Factory metrics",
            "Lineage",
            "Incident comparison",
            "Guided demo",
        ]
    )
    with tabs[0]:
        _overview(latest, current)
    with tabs[1]:
        _runs(current)
    with tabs[2]:
        _freshness(current)
    with tabs[3]:
        _quality(current, UUID(str(latest["run_id"])))
    with tabs[4]:
        _factory_metrics(current)
    with tabs[5]:
        _lineage(current)
    with tabs[6]:
        _comparison(current)
    with tabs[7]:
        _guided_demo()


def _overview(latest: dict[str, Any], service: ForgeFlowService) -> None:
    columns = st.columns(5)
    values: list[tuple[str, str | int]] = [
        ("Status", str(latest["status"]).upper()),
        ("Accepted rows", int(latest.get("accepted_row_count", 0))),
        ("Quarantined", int(latest.get("quarantined_row_count", 0))),
        ("Passed checks", int(latest.get("passed_checks", 0))),
        ("Failed checks", int(latest.get("failed_checks", 0))),
    ]
    for column, (label, value) in zip(columns, values, strict=True):
        column.metric(label, value)
    st.subheader("Latest run")
    st.json(
        {
            "run_id": latest["run_id"],
            "batch_id": latest["batch_id"],
            "scenario": latest["scenario"],
            "started_at": latest["started_at"],
            "duration_seconds": latest.get("duration_seconds"),
            "freshness_status": latest.get("freshness_status"),
            "affected_downstream_models": latest.get("affected_downstream_models", []),
        },
        expanded=False,
    )
    trend = service.get_quality_trend(limit=30)
    if trend:
        frame = pd.DataFrame(trend)
        st.subheader("Quality trend")
        st.line_chart(frame, x="started_at", y=["passed_checks", "failed_checks"])


def _runs(service: ForgeFlowService) -> None:
    page = service.list_pipeline_runs(limit=100)
    if not page.items:
        st.info("No run history is available.")
        return
    frame = pd.DataFrame(page.items)
    preferred = [
        "started_at",
        "status",
        "scenario",
        "batch_id",
        "accepted_row_count",
        "quarantined_row_count",
        "failed_checks",
        "run_id",
    ]
    st.dataframe(frame[[column for column in preferred if column in frame]], hide_index=True)


def _freshness(service: ForgeFlowService) -> None:
    rows = service.get_freshness()
    if not rows:
        st.info("Freshness mart is empty. Complete a healthy dbt build first.")
        return
    frame = pd.DataFrame(rows)
    status_column = "freshness_status"
    if status_column in frame:
        st.bar_chart(frame[status_column].value_counts())
    st.dataframe(frame, hide_index=True)


def _quality(service: ForgeFlowService, run_id: UUID) -> None:
    summary = service.get_data_quality_summary(run_id)
    failed = service.list_failed_checks(run_id=run_id, limit=100)
    quarantined = service.list_quarantined_records(run_id=run_id, limit=100)
    left, right = st.columns(2)
    with left:
        st.subheader("Check summary")
        st.dataframe(pd.DataFrame(summary["checks"]), hide_index=True)
    with right:
        st.subheader("Quarantine reasons")
        st.dataframe(pd.DataFrame(summary["quarantine"]), hide_index=True)
    st.subheader("Failed and warning checks")
    if failed.items:
        st.dataframe(pd.DataFrame(failed.items), hide_index=True)
    else:
        st.success("No failed or warning checks were recorded for this run.")
    st.subheader("Quarantined record metadata")
    if quarantined.items:
        st.dataframe(pd.DataFrame(quarantined.items), hide_index=True)
    else:
        st.success("No records were quarantined for this run.")


def _factory_metrics(service: ForgeFlowService) -> None:
    rows = service.get_factory_performance()
    if not rows:
        st.info("Factory performance is empty. Complete the warehouse demo first.")
        return
    frame = pd.DataFrame(rows)
    st.dataframe(frame, hide_index=True)
    numeric = [
        column
        for column in ("output_attainment_rate", "defect_rate", "unplanned_downtime_hours")
        if column in frame
    ]
    if numeric and "factory_id" in frame:
        st.bar_chart(frame, x="factory_id", y=numeric)


def _lineage(service: ForgeFlowService) -> None:
    page = service.list_models(limit=100)
    names = [str(item["model_name"]) for item in page.items]
    if not names:
        st.info("No parsed dbt metadata is available.")
        return
    selected = st.selectbox("Model", names)
    direct = service.get_model_lineage(selected)
    impact = service.get_downstream_impact(selected)
    left, right = st.columns(2)
    left.subheader("Direct lineage")
    left.json(direct)
    right.subheader("Downstream impact")
    right.dataframe(pd.DataFrame(impact), hide_index=True)


def _comparison(service: ForgeFlowService) -> None:
    runs = service.list_pipeline_runs(limit=100).items
    if len(runs) < 2:
        st.info("At least two runs are required for comparison.")
        return
    labels = {
        f"{run['started_at']} · {run['status']} · {run['batch_id']}": run["run_id"] for run in runs
    }
    left_label = st.selectbox("Baseline run", list(labels), index=min(1, len(labels) - 1))
    right_label = st.selectbox("Comparison run", list(labels), index=0)
    if labels[left_label] == labels[right_label]:
        st.warning("Choose two different runs.")
        return
    comparison = service.compare_pipeline_runs(
        UUID(str(labels[left_label])), UUID(str(labels[right_label]))
    )
    st.json(comparison)


def _guided_demo() -> None:
    st.subheader("Healthy → incident → evidence → recovery")
    st.markdown(
        """
1. Start PostgreSQL and MinIO with `uv run poe up`.
2. Run `uv run poe demo` for a healthy, idempotent baseline.
3. Run `uv run poe incident-demo` to inject named contract, duplicate, business-rule, and freshness failures.
4. Inspect failed checks, quarantine reasons, run comparison, and lineage impact in this console or through MCP.
5. Run `uv run poe recover-demo` to load corrected records and verify a healthy state while preserving incident history.

The demo uses only deterministic synthetic records. The incident explanation labels recorded facts separately from unconfirmed hypotheses.
        """
    )


def _empty_state() -> None:
    st.info("ForgeFlow is connected, but no pipeline run exists yet.")
    st.code("uv run poe demo", language="text")


def _failed_state(message: str) -> None:
    st.error("ForgeFlow could not load platform evidence.")
    st.caption(message)
    st.code("uv run poe up\nuv run forgeflow status", language="text")


def main() -> None:
    """Launch this module through Streamlit's supported CLI entry point."""
    settings = get_settings()
    sys.argv = [
        "streamlit",
        "run",
        str(Path(__file__).resolve()),
        "--server.address",
        settings.dashboard_host,
        "--server.port",
        str(settings.dashboard_port),
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
    ]
    raise SystemExit(stcli.main())


if __name__ == "__main__":
    render_dashboard()
