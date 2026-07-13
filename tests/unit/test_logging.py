"""Behavioral tests for credential-safe structured logging configuration."""

from __future__ import annotations

import io
import json
import logging
import sys
from typing import cast

import pytest
import structlog

import forgeflow.logging as logging_module


def test_json_logging_filters_by_level_and_never_dumps_environment_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = io.StringIO()
    secret = "postgresql://operator:do-not-log@warehouse/forgeflow"
    monkeypatch.setattr(sys, "stderr", stream)
    monkeypatch.setenv("FORGEFLOW_DATABASE_URL", secret)
    structlog.reset_defaults()
    try:
        logging_module.configure_logging("INFO", json_logs=True)
        logger = structlog.get_logger("forgeflow.test.safe")
        logger.debug("filtered_event", unsafe="must-not-appear")
        logger.info("configured", component="cli")
    finally:
        structlog.reset_defaults()

    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    payload = cast(dict[str, object], json.loads(lines[0]))
    assert payload["event"] == "configured"
    assert payload["component"] == "cli"
    assert payload["level"] == "info"
    assert isinstance(payload["timestamp"], str)
    assert "filtered_event" not in stream.getvalue()
    assert secret not in stream.getvalue()


def test_logging_configures_stdlib_output_without_credential_bearing_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    basic_config_calls: list[dict[str, object]] = []
    structlog_calls: list[dict[str, object]] = []
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)

    def fake_basic_config(**kwargs: object) -> None:
        basic_config_calls.append(kwargs)

    def fake_structlog_configure(**kwargs: object) -> None:
        structlog_calls.append(kwargs)

    monkeypatch.setattr(logging, "basicConfig", fake_basic_config)
    monkeypatch.setattr(structlog, "configure", fake_structlog_configure)

    logging_module.configure_logging("WARNING")

    assert basic_config_calls == [{"format": "%(message)s", "stream": stream, "level": "WARNING"}]
    assert len(structlog_calls) == 1
    configuration = structlog_calls[0]
    assert configuration["cache_logger_on_first_use"] is True
    assert len(cast(list[object], configuration["processors"])) == 5
    assert "password" not in repr(configuration).lower()
    assert "credential" not in repr(configuration).lower()
