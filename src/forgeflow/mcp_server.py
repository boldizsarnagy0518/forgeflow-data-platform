"""Official-SDK stdio MCP server exposing bounded ForgeFlow evidence."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from mcp.server.fastmcp import FastMCP

from forgeflow.config import get_settings
from forgeflow.logging import configure_logging
from forgeflow.service import ForgeFlowService, build_service


class MCPToolHandlers:
    """Directly testable handlers registered as read-only MCP tools/resources."""

    def __init__(self, service: ForgeFlowService) -> None:
        self._service = service

    def list_pipeline_runs(self, limit: int = 20, offset: int = 0) -> dict[str, Any]:
        """List recent pipeline runs with pagination and compact summaries."""
        return self._service.list_pipeline_runs(limit=limit, offset=offset).model_dump(mode="json")

    def get_pipeline_run(self, run_id: str) -> dict[str, Any] | None:
        """Get one pipeline run by UUID."""
        return self._service.get_pipeline_run(UUID(run_id))

    def get_latest_pipeline_status(self) -> dict[str, Any] | None:
        """Get the newest persisted pipeline status."""
        return self._service.get_latest_pipeline_status()

    def get_data_quality_summary(self, run_id: str | None = None) -> dict[str, Any]:
        """Summarize checks and quarantine evidence for a run or the latest run."""
        return self._service.get_data_quality_summary(UUID(run_id) if run_id else None)

    def list_failed_checks(
        self, run_id: str | None = None, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        """List failed/warning checks without dumping source tables."""
        return self._service.list_failed_checks(
            run_id=UUID(run_id) if run_id else None, limit=limit, offset=offset
        ).model_dump(mode="json")

    def get_failed_check_details(
        self, check_id: str, run_id: str | None = None
    ) -> dict[str, Any] | None:
        """Get the recorded expectation, observation, and evidence for one check."""
        return self._service.get_failed_check_details(check_id, UUID(run_id) if run_id else None)

    def list_quarantined_records(
        self, run_id: str | None = None, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        """List bounded quarantine metadata; raw payloads are intentionally omitted."""
        return self._service.list_quarantined_records(
            run_id=UUID(run_id) if run_id else None, limit=limit, offset=offset
        ).model_dump(mode="json")

    def get_model_metadata(self, model_name: str) -> dict[str, Any] | None:
        """Get descriptions and materialization metadata for one dbt resource."""
        return self._service.get_model_metadata(model_name)

    def get_column_metadata(self, model_name: str) -> list[dict[str, Any]]:
        """Get documented columns for one dbt resource."""
        return self._service.get_column_metadata(model_name)

    def get_model_lineage(self, model_name: str) -> dict[str, Any]:
        """Get direct parents and children for one model."""
        return self._service.get_model_lineage(model_name)

    def get_downstream_impact(self, model_name: str) -> list[dict[str, Any]]:
        """Get transitive downstream models with lineage depth."""
        return self._service.get_downstream_impact(model_name)

    def compare_pipeline_runs(self, left_run_id: str, right_run_id: str) -> dict[str, Any]:
        """Compare statuses, counts, schema changes, and model impacts between runs."""
        return self._service.compare_pipeline_runs(UUID(left_run_id), UUID(right_run_id))

    def explain_incident_evidence(self, incident_id: str) -> dict[str, Any]:
        """Explain a persisted incident with facts, hypotheses, next steps, and uncertainty."""
        return self._service.explain_incident_evidence(UUID(incident_id)).model_dump(mode="json")

    def generate_engineering_handoff(self, incident_id: str) -> dict[str, Any]:
        """Generate a compact, evidence-referenced engineering handoff."""
        return self._service.generate_engineering_handoff(UUID(incident_id))

    def models_resource(self) -> str:
        """Stable JSON context describing up to the configured model limit."""
        return _json(self._service.list_models(limit=100).model_dump(mode="json"))

    def latest_quality_resource(self) -> str:
        """Stable JSON context for the newest quality summary."""
        return _json(self._service.get_data_quality_summary())

    def run_resource(self, run_id: str) -> str:
        """Stable JSON context for one run."""
        resolved_id = UUID(run_id)
        run = self._service.get_pipeline_run(resolved_id)
        if run is None:
            raise LookupError(f"Pipeline run {resolved_id} was not found")
        return _json(run)

    def incident_resource(self, incident_id: str) -> str:
        """Stable JSON context for one incident."""
        resolved_id = UUID(incident_id)
        incident = self._service.get_incident(resolved_id)
        if incident is None:
            raise LookupError(f"Incident {resolved_id} was not found")
        return _json(incident)

    def lineage_resource(self, model_name: str) -> str:
        """Stable JSON context for one model's direct lineage."""
        return _json(self._service.get_model_lineage(model_name))


def create_mcp_server(service: ForgeFlowService) -> tuple[FastMCP, MCPToolHandlers]:
    """Register the complete read-only ForgeFlow MCP interface."""
    mcp = FastMCP(
        "ForgeFlow",
        instructions=(
            "Use these read-only tools to investigate synthetic pipeline runs, quality failures, "
            "quarantine evidence, dbt metadata, lineage, and incidents. Treat hypotheses as unconfirmed."
        ),
        json_response=True,
    )
    handlers = MCPToolHandlers(service)

    mcp.tool()(handlers.list_pipeline_runs)
    mcp.tool()(handlers.get_pipeline_run)
    mcp.tool()(handlers.get_latest_pipeline_status)
    mcp.tool()(handlers.get_data_quality_summary)
    mcp.tool()(handlers.list_failed_checks)
    mcp.tool()(handlers.get_failed_check_details)
    mcp.tool()(handlers.list_quarantined_records)
    mcp.tool()(handlers.get_model_metadata)
    mcp.tool()(handlers.get_column_metadata)
    mcp.tool()(handlers.get_model_lineage)
    mcp.tool()(handlers.get_downstream_impact)
    mcp.tool()(handlers.compare_pipeline_runs)
    mcp.tool()(handlers.explain_incident_evidence)
    mcp.tool()(handlers.generate_engineering_handoff)

    mcp.resource("forgeflow://models", mime_type="application/json")(handlers.models_resource)
    mcp.resource("forgeflow://quality/latest", mime_type="application/json")(
        handlers.latest_quality_resource
    )
    mcp.resource("forgeflow://runs/{run_id}", mime_type="application/json")(handlers.run_resource)
    mcp.resource("forgeflow://incidents/{incident_id}", mime_type="application/json")(
        handlers.incident_resource
    )
    mcp.resource("forgeflow://lineage/{model_name}", mime_type="application/json")(
        handlers.lineage_resource
    )
    return mcp, handlers


server, tool_handlers = create_mcp_server(build_service(get_settings()))


def main() -> None:
    """Run ForgeFlow over the local stdio transport."""
    configure_logging(get_settings().log_level, json_logs=True)
    server.run(transport="stdio")


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


if __name__ == "__main__":
    main()
