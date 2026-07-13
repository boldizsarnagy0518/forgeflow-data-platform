"""Centralized, environment-driven ForgeFlow configuration."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from forgeflow.errors import ConfigurationError


class Settings(BaseSettings):
    """Validated settings shared by CLI, orchestration, API, MCP, and dashboard."""

    model_config = SettingsConfigDict(
        env_prefix="FORGEFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: SecretStr = SecretStr(
        "postgresql://forgeflow:forgeflow_local_only@127.0.0.1:5432/forgeflow"
    )
    s3_endpoint: str = "http://127.0.0.1:9000"
    s3_access_key: str = "forgeflow"
    s3_secret_key: SecretStr = SecretStr("forgeflow_local_only")
    s3_bucket: str = "forgeflow-raw"
    s3_region: str = "us-east-1"
    data_dir: Path = Path(".forgeflow/data")
    artifact_dir: Path = Path(".forgeflow/artifacts")
    dbt_project_dir: Path = Path("dbt")
    dbt_profiles_dir: Path = Path("dbt")
    dbt_target: str = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    max_page_size: int = Field(default=100, ge=1, le=500)
    max_page_offset: int = Field(default=10_000, ge=0, le=1_000_000)
    max_source_file_bytes: int = Field(
        default=25 * 1024 * 1024,
        ge=1,
        le=100 * 1024 * 1024,
    )
    max_source_rows_per_file: int = Field(default=250_000, ge=1, le=1_000_000)
    enable_writes: bool = False
    ai_provider: Literal["deterministic", "openai"] = "deterministic"
    openai_model: str = "gpt-5.4-mini"
    openai_api_key: SecretStr | None = Field(default=None, validation_alias="OPENAI_API_KEY")
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65535)
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = Field(default=8501, ge=1, le=65535)
    seed: int = 20250710
    generated_days: int = Field(default=14, ge=2, le=90)

    @field_validator("s3_endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        """Require an explicit HTTP(S) endpoint and remove a trailing slash."""
        endpoint = value.rstrip("/")
        if not endpoint.startswith(("http://", "https://")):
            msg = "s3_endpoint must use http:// or https://"
            raise ValueError(msg)
        return endpoint

    @model_validator(mode="after")
    def validate_provider(self) -> Self:
        """Fail early if the opt-in provider is selected without credentials."""
        if self.ai_provider == "openai" and (
            self.openai_api_key is None or not self.openai_api_key.get_secret_value().strip()
        ):
            raise ConfigurationError(
                "FORGEFLOW_AI_PROVIDER=openai requires a non-empty OPENAI_API_KEY"
            )
        return self

    def ensure_runtime_directories(self) -> None:
        """Create only the repository-scoped runtime directories ForgeFlow owns."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)

    @property
    def safe_summary(self) -> dict[str, str | int | bool]:
        """Return configuration safe for logs and diagnostics."""
        return {
            "database": "configured",
            "s3_endpoint": self.s3_endpoint,
            "s3_bucket": self.s3_bucket,
            "data_dir": str(self.data_dir),
            "artifact_dir": str(self.artifact_dir),
            "dbt_target": self.dbt_target,
            "max_page_size": self.max_page_size,
            "max_page_offset": self.max_page_offset,
            "max_source_file_bytes": self.max_source_file_bytes,
            "max_source_rows_per_file": self.max_source_rows_per_file,
            "enable_writes": self.enable_writes,
            "ai_provider": self.ai_provider,
            "api_address": f"{self.api_host}:{self.api_port}",
            "dashboard_address": f"{self.dashboard_host}:{self.dashboard_port}",
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide validated settings instance."""
    return Settings()
