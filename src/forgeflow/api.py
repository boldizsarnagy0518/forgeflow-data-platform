"""Read-only FastAPI operational and metadata surface."""

from typing import Annotated, Any
from uuid import UUID

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel

from forgeflow.config import Settings, get_settings
from forgeflow.service import ForgeFlowService, Page, build_service


class HealthResponse(BaseModel):
    """Dependency health without sensitive details."""

    status: str
    warehouse: str
    object_store: str
    writes_enabled: bool
    ai_provider: str


class QualitySummaryResponse(BaseModel):
    """Grouped quality and quarantine state."""

    run_id: str | None
    run_status: str | None = None
    checks: list[dict[str, Any]]
    quarantine: list[dict[str, Any]]
    state: str


def create_app(service: ForgeFlowService | None = None) -> FastAPI:
    """Create the API with an injectable service for boundary-focused tests."""
    application = FastAPI(
        title="ForgeFlow Operational API",
        summary="Read-only reliability, quality, lineage, and incident evidence",
        description=(
            "ForgeFlow exposes bounded metadata from a deterministic synthetic industrial data "
            "platform. Operational mutations are intentionally not exposed."
        ),
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )
    resolved_service = service or build_service(get_settings())

    def dependency() -> ForgeFlowService:
        return resolved_service

    ServiceDependency = Annotated[ForgeFlowService, Depends(dependency)]
    Limit = Annotated[int, Query(ge=1, le=500)]
    Offset = Annotated[int, Query(ge=0)]

    @application.get("/health", response_model=HealthResponse, tags=["platform"])
    def health(current: ServiceDependency) -> dict[str, Any]:
        return current.health()

    @application.get("/runs", response_model=Page, tags=["runs"])
    def runs(current: ServiceDependency, limit: Limit = 20, offset: Offset = 0) -> Page:
        return current.list_pipeline_runs(limit=limit, offset=offset)

    @application.get("/runs/latest", response_model=dict[str, Any], tags=["runs"])
    def latest_run(current: ServiceDependency) -> dict[str, Any]:
        result = current.get_latest_pipeline_status()
        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No pipeline runs found"
            )
        return result

    @application.get("/runs/compare/pair", response_model=dict[str, Any], tags=["runs"])
    def compare_runs(
        left_run_id: UUID, right_run_id: UUID, current: ServiceDependency
    ) -> dict[str, Any]:
        try:
            return current.compare_pipeline_runs(left_run_id, right_run_id)
        except LookupError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error

    @application.get("/runs/{run_id}", response_model=dict[str, Any], tags=["runs"])
    def run_details(run_id: UUID, current: ServiceDependency) -> dict[str, Any]:
        result = current.get_pipeline_run(run_id)
        if result is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        return result

    @application.get("/quality/latest", response_model=QualitySummaryResponse, tags=["quality"])
    def latest_quality(current: ServiceDependency) -> dict[str, Any]:
        return current.get_data_quality_summary()

    @application.get("/quality/failed", response_model=Page, tags=["quality"])
    def failed_checks(
        current: ServiceDependency,
        run_id: UUID | None = None,
        limit: Limit = 50,
        offset: Offset = 0,
    ) -> Page:
        return current.list_failed_checks(run_id=run_id, limit=limit, offset=offset)

    @application.get("/quality/failed/{check_id}", response_model=dict[str, Any], tags=["quality"])
    def failed_check_details(
        check_id: str, current: ServiceDependency, run_id: UUID | None = None
    ) -> dict[str, Any]:
        try:
            result = current.get_failed_check_details(check_id, run_id)
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
            ) from error
        if result is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Check not found")
        return result

    @application.get("/quarantine", response_model=Page, tags=["quality"])
    def quarantine(
        current: ServiceDependency,
        run_id: UUID | None = None,
        limit: Limit = 50,
        offset: Offset = 0,
    ) -> Page:
        return current.list_quarantined_records(run_id=run_id, limit=limit, offset=offset)

    @application.get("/models", response_model=Page, tags=["metadata"])
    def models(current: ServiceDependency, limit: Limit = 100, offset: Offset = 0) -> Page:
        return current.list_models(limit=limit, offset=offset)

    @application.get("/models/{model_name}", response_model=dict[str, Any], tags=["metadata"])
    def model(model_name: str, current: ServiceDependency) -> dict[str, Any]:
        try:
            result = current.get_model_metadata(model_name)
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
            ) from error
        if result is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Model not found")
        return result

    @application.get(
        "/models/{model_name}/columns", response_model=list[dict[str, Any]], tags=["metadata"]
    )
    def model_columns(model_name: str, current: ServiceDependency) -> list[dict[str, Any]]:
        try:
            return current.get_column_metadata(model_name)
        except LookupError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
            ) from error

    @application.get(
        "/models/{model_name}/lineage", response_model=dict[str, Any], tags=["lineage"]
    )
    def lineage(model_name: str, current: ServiceDependency) -> dict[str, Any]:
        try:
            return current.get_model_lineage(model_name)
        except LookupError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
            ) from error

    @application.get(
        "/models/{model_name}/downstream", response_model=list[dict[str, Any]], tags=["lineage"]
    )
    def downstream(model_name: str, current: ServiceDependency) -> list[dict[str, Any]]:
        try:
            return current.get_downstream_impact(model_name)
        except LookupError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
            ) from error

    @application.get("/freshness", response_model=list[dict[str, Any]], tags=["quality"])
    def freshness(current: ServiceDependency) -> list[dict[str, Any]]:
        return current.get_freshness()

    @application.get(
        "/factory-performance", response_model=list[dict[str, Any]], tags=["analytics"]
    )
    def factory_performance(current: ServiceDependency) -> list[dict[str, Any]]:
        return current.get_factory_performance()

    @application.get("/incidents/{incident_id}", response_model=dict[str, Any], tags=["incidents"])
    def incident(incident_id: UUID, current: ServiceDependency) -> dict[str, Any]:
        result = current.get_incident(incident_id)
        if result is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Incident not found")
        return result

    @application.get(
        "/incidents/{incident_id}/summary",
        response_model=dict[str, Any],
        tags=["incidents"],
    )
    def incident_summary(incident_id: UUID, current: ServiceDependency) -> dict[str, Any]:
        try:
            return current.generate_engineering_handoff(incident_id)
        except LookupError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error

    return application


app = create_app()


def main(settings: Settings | None = None) -> None:
    """Run the local operational API."""
    resolved = settings or get_settings()
    uvicorn.run(
        app, host=resolved.api_host, port=resolved.api_port, log_level=resolved.log_level.lower()
    )


if __name__ == "__main__":
    main()
