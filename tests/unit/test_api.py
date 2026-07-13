"""FastAPI boundary tests for validation, read-only routing, and not-found behavior."""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from forgeflow.api import create_app
from forgeflow.service import ForgeFlowService, Page

RUN_ID = UUID("40000000-0000-0000-0000-000000000001")
OTHER_RUN_ID = UUID("40000000-0000-0000-0000-000000000002")
INCIDENT_ID = UUID("40000000-0000-0000-0000-000000000003")


class _FakeService:
    def __init__(self) -> None:
        self.run_list_calls: list[tuple[int, int]] = []
        self.run_detail_calls: list[UUID] = []
        self.comparison_calls: list[tuple[UUID, UUID]] = []
        self.model_calls: list[str] = []
        self.latest_run: dict[str, Any] | None = None
        self.run: dict[str, Any] | None = None
        self.model: dict[str, Any] | None = None
        self.incident: dict[str, Any] | None = None
        self.failed_check: dict[str, Any] | None = None
        self.raise_model_validation = False

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "warehouse": "reachable",
            "object_store": "reachable",
            "writes_enabled": False,
            "ai_provider": "deterministic",
        }

    def list_pipeline_runs(self, *, limit: int, offset: int) -> Page:
        self.run_list_calls.append((limit, offset))
        return Page(items=[], total=0, limit=limit, offset=offset)

    def get_latest_pipeline_status(self) -> dict[str, Any] | None:
        return self.latest_run

    def get_pipeline_run(self, run_id: UUID) -> dict[str, Any] | None:
        self.run_detail_calls.append(run_id)
        return self.run

    def compare_pipeline_runs(self, left_run_id: UUID, right_run_id: UUID) -> dict[str, Any]:
        self.comparison_calls.append((left_run_id, right_run_id))
        return {
            "left_run_id": str(left_run_id),
            "right_run_id": str(right_run_id),
            "interpretation": "Deltas are observations; they do not prove causation.",
        }

    def get_model_metadata(self, model_name: str) -> dict[str, Any] | None:
        self.model_calls.append(model_name)
        if self.raise_model_validation:
            raise ValueError("model_name contains unsupported characters or is too long")
        return self.model

    def get_model_lineage(self, model_name: str) -> dict[str, Any]:
        if model_name == "missing_model":
            raise LookupError("Model 'missing_model' was not found")
        return {"model_name": model_name, "parents": [], "children": []}

    def get_incident(self, incident_id: UUID) -> dict[str, Any] | None:
        return self.incident

    def get_failed_check_details(
        self, check_id: str, run_id: UUID | None = None
    ) -> dict[str, Any] | None:
        return self.failed_check


@pytest.fixture
def fake_service() -> _FakeService:
    return _FakeService()


@pytest.fixture
def client(fake_service: _FakeService) -> TestClient:
    return TestClient(create_app(cast(ForgeFlowService, fake_service)))


def test_openapi_documents_the_expected_read_only_surface(
    client: TestClient, fake_service: _FakeService
) -> None:
    response = client.get("/openapi.json")

    assert response.status_code == 200
    document = response.json()
    assert document["info"]["title"] == "ForgeFlow Operational API"
    paths = document["paths"]
    assert {
        "/health",
        "/runs",
        "/runs/latest",
        "/runs/{run_id}",
        "/runs/compare/pair",
        "/quality/latest",
        "/quality/failed",
        "/quality/failed/{check_id}",
        "/quarantine",
        "/models",
        "/models/{model_name}",
        "/models/{model_name}/columns",
        "/models/{model_name}/lineage",
        "/models/{model_name}/downstream",
        "/freshness",
        "/factory-performance",
        "/incidents/{incident_id}",
        "/incidents/{incident_id}/summary",
    }.issubset(paths)
    assert all(set(path_item) <= {"get"} for path_item in paths.values())
    assert not any(
        word in operation_id.lower()
        for path_item in paths.values()
        for operation in path_item.values()
        for operation_id in [operation["operationId"]]
        for word in ("create", "update", "delete", "trigger", "repair", "backfill")
    )
    assert fake_service.run_list_calls == []


@pytest.mark.parametrize(
    "url",
    [
        "/runs?limit=0",
        "/runs?limit=501",
        "/runs?offset=-1",
        "/runs/not-a-uuid",
        "/quality/failed?run_id=not-a-uuid",
    ],
)
def test_invalid_query_and_path_inputs_return_422_without_calling_service(
    client: TestClient, fake_service: _FakeService, url: str
) -> None:
    response = client.get(url)

    assert response.status_code == 422
    assert response.json()["detail"]
    assert fake_service.run_list_calls == []
    assert fake_service.run_detail_calls == []


def test_valid_pagination_is_forwarded_to_the_shared_service(
    client: TestClient, fake_service: _FakeService
) -> None:
    response = client.get("/runs?limit=17&offset=4")

    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "limit": 17, "offset": 4}
    assert fake_service.run_list_calls == [(17, 4)]


def test_missing_lineage_model_returns_404_instead_of_an_empty_graph(
    client: TestClient,
) -> None:
    response = client.get("/models/missing_model/lineage")

    assert response.status_code == 404
    assert "missing_model" in response.json()["detail"]


def test_literal_compare_route_is_reachable_before_dynamic_run_route(
    client: TestClient, fake_service: _FakeService
) -> None:
    response = client.get(
        "/runs/compare/pair",
        params={"left_run_id": str(RUN_ID), "right_run_id": str(OTHER_RUN_ID)},
    )

    assert response.status_code == 200
    assert response.json()["left_run_id"] == str(RUN_ID)
    assert fake_service.comparison_calls == [(RUN_ID, OTHER_RUN_ID)]
    assert fake_service.run_detail_calls == []


@pytest.mark.parametrize(
    ("path", "detail"),
    [
        ("/runs/latest", "No pipeline runs found"),
        (f"/runs/{RUN_ID}", "Run not found"),
        ("/models/not_found", "Model not found"),
        (f"/incidents/{INCIDENT_ID}", "Incident not found"),
        ("/quality/failed/missing.check", "Check not found"),
    ],
)
def test_missing_resources_return_specific_404_responses(
    client: TestClient, path: str, detail: str
) -> None:
    response = client.get(path)

    assert response.status_code == 404
    assert response.json() == {"detail": detail}


def test_service_identifier_validation_is_translated_to_api_422(
    client: TestClient, fake_service: _FakeService
) -> None:
    fake_service.raise_model_validation = True

    response = client.get("/models/bad%3Bdrop")

    assert response.status_code == 422
    assert response.json() == {
        "detail": "model_name contains unsupported characters or is too long"
    }
    assert fake_service.model_calls == ["bad;drop"]


def test_health_response_is_typed_and_exposes_no_connection_details(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "warehouse": "reachable",
        "object_store": "reachable",
        "writes_enabled": False,
        "ai_provider": "deterministic",
    }
    assert "password" not in response.text.lower()
    assert "postgresql://" not in response.text.lower()
