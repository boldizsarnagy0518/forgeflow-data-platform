"""MCP registration and compact read-only payload tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, cast
from uuid import UUID

import pytest
from mcp.server.fastmcp import FastMCP

import forgeflow.mcp_server as mcp_module
from forgeflow.config import Settings
from forgeflow.mcp_server import MCPToolHandlers, create_mcp_server
from forgeflow.service import ForgeFlowService
from forgeflow.warehouse import PostgresRepository

RUN_ID = UUID("50000000-0000-0000-0000-000000000001")
INCIDENT_ID = UUID("50000000-0000-0000-0000-000000000002")

EXPECTED_TOOLS = {
    "list_pipeline_runs",
    "get_pipeline_run",
    "get_latest_pipeline_status",
    "get_data_quality_summary",
    "list_failed_checks",
    "get_failed_check_details",
    "list_quarantined_records",
    "get_model_metadata",
    "get_column_metadata",
    "get_model_lineage",
    "get_downstream_impact",
    "compare_pipeline_runs",
    "explain_incident_evidence",
    "generate_engineering_handoff",
}
EXPECTED_RESOURCES = {"forgeflow://models", "forgeflow://quality/latest"}
EXPECTED_RESOURCE_TEMPLATES = {
    "forgeflow://runs/{run_id}",
    "forgeflow://incidents/{incident_id}",
    "forgeflow://lineage/{model_name}",
}


class _FakeRepository:
    def __init__(self) -> None:
        self.list_run_calls: list[tuple[int, int]] = []
        self.failed_check_calls: list[tuple[UUID | None, int, int]] = []
        self.quarantine_calls: list[tuple[UUID | None, int, int]] = []
        self.model_calls: list[tuple[int, int]] = []
        self.runs = [{"run_id": RUN_ID, "status": "healthy"} for _ in range(8)]
        self.models = [{"name": f"model_{index}"} for index in range(8)]

    def list_runs(self, *, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        self.list_run_calls.append((limit, offset))
        return self.runs[offset : offset + limit], len(self.runs)

    def get_run(self, run_id: UUID) -> dict[str, Any] | None:
        return {"run_id": run_id, "status": "healthy"} if run_id == RUN_ID else None

    def get_latest_run(self) -> dict[str, Any]:
        return {"run_id": RUN_ID, "status": "healthy"}

    def quality_summary(self, run_id: UUID) -> list[dict[str, Any]]:
        return [{"status": "passed", "count": 12}]

    def quarantine_summary(self, run_id: UUID) -> list[dict[str, Any]]:
        return [{"reason_code": "out_of_range", "count": 1}]

    def list_failed_checks(
        self, *, run_id: UUID | None, limit: int, offset: int
    ) -> tuple[list[dict[str, Any]], int]:
        self.failed_check_calls.append((run_id, limit, offset))
        rows = [{"check_id": f"check_{index}"} for index in range(8)]
        return rows[offset : offset + limit], len(rows)

    def list_quarantined_records(
        self, *, run_id: UUID | None, limit: int, offset: int
    ) -> tuple[list[dict[str, Any]], int]:
        self.quarantine_calls.append((run_id, limit, offset))
        rows = [
            {
                "quarantine_id": f"q-{index}",
                "source_name": "machine_telemetry",
                "raw_payload": {"temperature_c": 999, "operator": "must-not-leak"},
                "reasons": [{"code": "out_of_range"}],
            }
            for index in range(8)
        ]
        return rows[offset : offset + limit], len(rows)

    def list_models(self, *, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        self.model_calls.append((limit, offset))
        return self.models[offset : offset + limit], len(self.models)

    def get_incident(self, incident_id: UUID) -> dict[str, Any] | None:
        if incident_id != INCIDENT_ID:
            return None
        return {"incident_id": incident_id, "status": "open"}

    def get_model(self, model_name: str) -> dict[str, Any] | None:
        if model_name == "mart_factory":
            return {"model_name": model_name}
        return None

    def get_lineage(self, model_name: str) -> dict[str, Any]:
        return {"model_name": model_name, "parents": ["stg_machine"], "children": []}


@pytest.fixture
def repository() -> _FakeRepository:
    return _FakeRepository()


@pytest.fixture
def mcp_surface(
    repository: _FakeRepository,
) -> tuple[FastMCP, MCPToolHandlers]:
    service = ForgeFlowService(Settings(max_page_size=3), cast(PostgresRepository, repository))
    return create_mcp_server(service)


@pytest.mark.asyncio
async def test_exact_documented_tools_and_resources_are_registered_read_only(
    mcp_surface: tuple[FastMCP, MCPToolHandlers],
) -> None:
    server, _ = mcp_surface

    tools = await server.list_tools()
    resources = await server.list_resources()
    templates = await server.list_resource_templates()

    tool_names = {tool.name for tool in tools}
    assert tool_names == EXPECTED_TOOLS
    assert {str(resource.uri) for resource in resources} == EXPECTED_RESOURCES
    assert {template.uriTemplate for template in templates} == EXPECTED_RESOURCE_TEMPLATES
    assert all(resource.mimeType == "application/json" for resource in resources)
    assert all(template.mimeType == "application/json" for template in templates)
    assert not any(
        word in name
        for name in tool_names
        for word in ("create", "update", "delete", "trigger", "repair", "backfill", "run_pipeline")
    )


def test_tool_handler_caps_large_pages_and_preserves_requested_offset(
    mcp_surface: tuple[FastMCP, MCPToolHandlers], repository: _FakeRepository
) -> None:
    _, handlers = mcp_surface

    result = handlers.list_pipeline_runs(limit=10_000, offset=2)

    assert result["limit"] == 3
    assert result["offset"] == 2
    assert result["total"] == 8
    assert len(result["items"]) == 3
    assert repository.list_run_calls == [(3, 2)]


def test_tool_handler_rejects_invalid_pagination_before_repository_access(
    mcp_surface: tuple[FastMCP, MCPToolHandlers], repository: _FakeRepository
) -> None:
    _, handlers = mcp_surface

    with pytest.raises(ValueError, match="limit must be at least 1"):
        handlers.list_failed_checks(limit=0)
    with pytest.raises(ValueError, match="offset must be zero or greater"):
        handlers.list_quarantined_records(offset=-1)

    assert repository.failed_check_calls == []
    assert repository.quarantine_calls == []


def test_quarantine_tool_returns_compact_metadata_without_raw_payloads(
    mcp_surface: tuple[FastMCP, MCPToolHandlers], repository: _FakeRepository
) -> None:
    _, handlers = mcp_surface

    result = handlers.list_quarantined_records(limit=999)

    assert result["limit"] == 3
    assert len(result["items"]) == 3
    assert all("raw_payload" not in item for item in result["items"])
    assert "must-not-leak" not in json.dumps(result)
    assert repository.quarantine_calls == [(None, 3, 0)]


def test_resources_serialize_stable_compact_json_context(
    mcp_surface: tuple[FastMCP, MCPToolHandlers], repository: _FakeRepository
) -> None:
    _, handlers = mcp_surface

    models = json.loads(handlers.models_resource())
    quality = json.loads(handlers.latest_quality_resource())
    run = json.loads(handlers.run_resource(str(RUN_ID)))
    incident = json.loads(handlers.incident_resource(str(INCIDENT_ID)))
    lineage = json.loads(handlers.lineage_resource("mart_factory"))

    assert models["limit"] == 3
    assert len(models["items"]) == 3
    assert repository.model_calls == [(3, 0)]
    assert quality == {
        "run_id": str(RUN_ID),
        "run_status": "healthy",
        "checks": [{"status": "passed", "count": 12}],
        "quarantine": [{"reason_code": "out_of_range", "count": 1}],
        "state": "available",
    }
    assert run == {"run_id": str(RUN_ID), "status": "healthy"}
    assert incident == {"incident_id": str(INCIDENT_ID), "status": "open"}
    assert lineage["parents"] == ["stg_machine"]
    assert " " not in handlers.models_resource()


@pytest.mark.parametrize(
    "call",
    [
        lambda handlers: handlers.get_pipeline_run("not-a-uuid"),
        lambda handlers: handlers.run_resource("not-a-uuid"),
        lambda handlers: handlers.incident_resource("not-a-uuid"),
    ],
)
def test_uuid_inputs_are_validated_before_service_access(
    mcp_surface: tuple[FastMCP, MCPToolHandlers],
    call: Callable[[MCPToolHandlers], object],
) -> None:
    _, handlers = mcp_surface

    with pytest.raises(ValueError, match="hexadecimal UUID"):
        call(handlers)


def test_missing_resource_ids_raise_explicit_not_found_errors(
    mcp_surface: tuple[FastMCP, MCPToolHandlers],
) -> None:
    _, handlers = mcp_surface
    missing = "50000000-0000-0000-0000-000000000099"

    with pytest.raises(LookupError, match="Pipeline run"):
        handlers.run_resource(missing)
    with pytest.raises(LookupError, match="Incident"):
        handlers.incident_resource(missing)
    with pytest.raises(LookupError, match="Model"):
        handlers.lineage_resource("missing_model")
    with pytest.raises(LookupError, match="Model"):
        handlers.get_model_lineage("missing_model")


def test_main_configures_structured_stderr_logging_before_stdio_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[object, ...]] = []

    def configure(level: str, *, json_logs: bool = False) -> None:
        events.append(("configure", level, json_logs))

    class FakeServer:
        def run(self, *, transport: str) -> None:
            events.append(("run", transport))

    monkeypatch.setattr(mcp_module, "get_settings", lambda: Settings(log_level="WARNING"))
    monkeypatch.setattr(mcp_module, "configure_logging", configure)
    monkeypatch.setattr(mcp_module, "server", FakeServer())

    mcp_module.main()

    assert events == [("configure", "WARNING", True), ("run", "stdio")]
