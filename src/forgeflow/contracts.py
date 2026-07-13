"""Pandera-backed source contracts with structured quarantine evidence."""

from __future__ import annotations

import math
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import pandas as pd
import pandera.pandas as pa

from forgeflow.errors import ContractError
from forgeflow.models import (
    ContractResult,
    QuarantinedRecord,
    QuarantineReason,
    SchemaChange,
)
from forgeflow.synthetic import SOURCE_NAMES

ColumnKind = Literal["string", "integer", "number", "date", "timestamp"]
RowChecker = Callable[[Mapping[str, Any]], bool]

IDENTIFIER_PATTERN = r"^[A-Z][A-Z0-9-]{2,63}$"
FUTURE_TOLERANCE = timedelta(minutes=5)
MAX_REASON_VALUE_CHARACTERS = 200
REDACTED_REASON_VALUE = "<redacted: rejected value exceeds evidence limit>"


@dataclass(frozen=True, slots=True)
class ColumnRule:
    """Human-readable metadata from which a Pandera column is built."""

    kind: ColumnKind
    nullable: bool = False
    unique: bool = False
    accepted_values: tuple[str, ...] = ()
    minimum: int | float | None = None
    maximum: int | float | None = None
    pattern: str | None = None
    not_future: bool = False


@dataclass(frozen=True, slots=True)
class ForeignKeyRule:
    """Dataset-level relationship evaluated after row-level validation."""

    column: str
    parent_source: str
    parent_column: str


@dataclass(frozen=True, slots=True)
class RowRule:
    """Cross-column rule with stable machine and human descriptions."""

    code: str
    columns: tuple[str, ...]
    message: str
    checker: RowChecker


@dataclass(frozen=True, slots=True)
class SourceContract:
    """Complete contract for one source file."""

    source_name: str
    columns: Mapping[str, ColumnRule]
    primary_key: str
    version: str = "1.0.0"
    foreign_keys: tuple[ForeignKeyRule, ...] = ()
    row_rules: tuple[RowRule, ...] = ()

    @property
    def expected_columns(self) -> tuple[str, ...]:
        """Return the stable source-file column order."""
        return tuple(self.columns)

    @property
    def required_columns(self) -> tuple[str, ...]:
        """Return columns for which missing or null values are rejected."""
        return tuple(name for name, rule in self.columns.items() if not rule.nullable)

    def build_schema(self, *, now: datetime | None = None) -> pa.DataFrameSchema:
        """Build a Pandera schema using the validation clock for timestamp checks."""
        validation_time = _normalise_now(now)
        pandera_columns = {
            name: _build_column(rule, validation_time) for name, rule in self.columns.items()
        }
        return pa.DataFrameSchema(
            pandera_columns,
            strict=False,
            ordered=False,
            coerce=False,
            name=self.source_name,
        )


def _identifier(*, unique: bool = False) -> ColumnRule:
    return ColumnRule("string", unique=unique, pattern=IDENTIFIER_PATTERN)


def _text() -> ColumnRule:
    return ColumnRule("string")


def _enum(*values: str) -> ColumnRule:
    return ColumnRule("string", accepted_values=tuple(values))


def _integer(minimum: int, maximum: int) -> ColumnRule:
    return ColumnRule("integer", minimum=minimum, maximum=maximum)


def _number(minimum: float, maximum: float) -> ColumnRule:
    return ColumnRule("number", minimum=minimum, maximum=maximum)


def _date(*, not_future: bool = True) -> ColumnRule:
    return ColumnRule("date", not_future=not_future)


def _timestamp(*, nullable: bool = False, not_future: bool = True) -> ColumnRule:
    return ColumnRule("timestamp", nullable=nullable, not_future=not_future)


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        msg = "timestamp values must be ISO-8601 strings"
        raise TypeError(msg)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        msg = "timestamp values must include a UTC offset"
        raise ValueError(msg)
    return parsed.astimezone(UTC)


def _valid_iso_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _valid_iso_timestamp(value: Any) -> bool:
    try:
        _parse_timestamp(value)
    except (TypeError, ValueError):
        return False
    return True


def _date_not_future(value: Any, now: datetime) -> bool:
    if not _valid_iso_date(value):
        return True
    return date.fromisoformat(str(value)) <= now.date()


def _timestamp_not_future(value: Any, now: datetime) -> bool:
    try:
        parsed = _parse_timestamp(value)
    except (TypeError, ValueError):
        return True
    return parsed <= now + FUTURE_TOLERANCE


def _non_blank(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _postgres_text_safe(value: Any) -> bool:
    return isinstance(value, str) and "\x00" not in value


def _build_column(rule: ColumnRule, now: datetime) -> pa.Column:
    checks: list[pa.Check] = []
    data_type: type[Any]
    if rule.kind == "integer":
        data_type = int
    elif rule.kind == "number":
        data_type = float
    else:
        data_type = str
        checks.append(pa.Check(_non_blank, element_wise=True, name="non_blank"))
        checks.append(pa.Check(_postgres_text_safe, element_wise=True, name="postgres_text_safe"))

    if rule.accepted_values:
        checks.append(pa.Check.isin(rule.accepted_values, name="accepted_values"))
    if rule.minimum is not None and rule.maximum is not None:
        checks.append(
            pa.Check.in_range(
                rule.minimum,
                rule.maximum,
                include_min=True,
                include_max=True,
                name="accepted_range",
            )
        )
    if rule.pattern is not None:
        checks.append(pa.Check.str_matches(rule.pattern, name="identifier_format"))
    if rule.kind == "date":
        checks.append(pa.Check(_valid_iso_date, element_wise=True, name="valid_iso_date"))
        if rule.not_future:
            checks.append(
                pa.Check(
                    lambda value: _date_not_future(value, now),
                    element_wise=True,
                    name="not_in_future",
                )
            )
    elif rule.kind == "timestamp":
        checks.append(pa.Check(_valid_iso_timestamp, element_wise=True, name="valid_iso_timestamp"))
        if rule.not_future:
            checks.append(
                pa.Check(
                    lambda value: _timestamp_not_future(value, now),
                    element_wise=True,
                    name="not_in_future",
                )
            )
    return pa.Column(
        data_type,
        checks=checks,
        nullable=rule.nullable,
        unique=rule.unique,
        required=True,
    )


def _ordered_timestamps(
    row: Mapping[str, Any],
    earlier_column: str,
    later_column: str,
    *,
    allow_equal: bool = False,
    allow_missing_later: bool = False,
) -> bool:
    later_value = row.get(later_column)
    if later_value is None and allow_missing_later:
        return True
    try:
        earlier = _parse_timestamp(row.get(earlier_column))
        later = _parse_timestamp(later_value)
    except (TypeError, ValueError):
        return True
    return later >= earlier if allow_equal else later > earlier


def _actual_timestamp_pair(row: Mapping[str, Any]) -> bool:
    return (row.get("actual_start_at") is None) is (row.get("actual_end_at") is None)


def _inspection_totals(row: Mapping[str, Any]) -> bool:
    try:
        return int(row["passed_units"]) + int(row["failed_units"]) == int(row["sample_size"])
    except (KeyError, TypeError, ValueError):
        return True


def _inspection_result_matches(row: Mapping[str, Any]) -> bool:
    try:
        failed_units = int(row["failed_units"])
    except (KeyError, TypeError, ValueError):
        return True
    return row.get("result") == ("pass" if failed_units == 0 else "fail")


SOURCE_CONTRACTS: dict[str, SourceContract] = {
    "factories": SourceContract(
        source_name="factories",
        columns={
            "factory_id": _identifier(unique=True),
            "factory_name": _text(),
            "country_code": _enum("HU", "DE", "CZ"),
            "timezone": _enum("Europe/Budapest", "Europe/Berlin", "Europe/Prague"),
            "opened_on": _date(),
            "status": _enum("active", "inactive"),
            "updated_at": _timestamp(),
        },
        primary_key="factory_id",
    ),
    "production_lines": SourceContract(
        source_name="production_lines",
        columns={
            "production_line_id": _identifier(unique=True),
            "factory_id": _identifier(),
            "line_name": _text(),
            "product_family": _enum("motor", "pump", "gearbox"),
            "nominal_capacity_per_hour": _integer(1, 10_000),
            "status": _enum("active", "inactive"),
            "updated_at": _timestamp(),
        },
        primary_key="production_line_id",
        foreign_keys=(ForeignKeyRule("factory_id", "factories", "factory_id"),),
    ),
    "machines": SourceContract(
        source_name="machines",
        columns={
            "machine_id": _identifier(unique=True),
            "production_line_id": _identifier(),
            "machine_name": _text(),
            "machine_type": _enum("cnc", "press", "robot", "welder", "inspection"),
            "manufacturer": _text(),
            "model": _text(),
            "installed_on": _date(),
            "status": _enum("active", "maintenance", "retired"),
            "updated_at": _timestamp(),
        },
        primary_key="machine_id",
        foreign_keys=(
            ForeignKeyRule("production_line_id", "production_lines", "production_line_id"),
        ),
    ),
    "shifts": SourceContract(
        source_name="shifts",
        columns={
            "shift_id": _identifier(unique=True),
            "factory_id": _identifier(),
            "shift_name": _enum("morning", "afternoon", "night"),
            "started_at": _timestamp(),
            "ended_at": _timestamp(),
            "operator_id": _identifier(),
            "updated_at": _timestamp(),
        },
        primary_key="shift_id",
        foreign_keys=(ForeignKeyRule("factory_id", "factories", "factory_id"),),
        row_rules=(
            RowRule(
                "invalid_timestamp_order",
                ("started_at", "ended_at"),
                "ended_at must be later than started_at",
                lambda row: _ordered_timestamps(row, "started_at", "ended_at"),
            ),
            RowRule(
                "invalid_timestamp_order",
                ("ended_at", "updated_at"),
                "updated_at must not precede ended_at",
                lambda row: _ordered_timestamps(
                    row,
                    "ended_at",
                    "updated_at",
                    allow_equal=True,
                ),
            ),
        ),
    ),
    "production_orders": SourceContract(
        source_name="production_orders",
        columns={
            "production_order_id": _identifier(unique=True),
            "production_line_id": _identifier(),
            "product_code": _identifier(),
            "planned_start_at": _timestamp(not_future=False),
            "planned_end_at": _timestamp(not_future=False),
            "actual_start_at": _timestamp(nullable=True),
            "actual_end_at": _timestamp(nullable=True),
            "planned_quantity": _integer(1, 1_000_000),
            "actual_quantity": _integer(0, 1_000_000),
            "status": _enum("planned", "in_progress", "completed", "cancelled"),
            "updated_at": _timestamp(),
        },
        primary_key="production_order_id",
        foreign_keys=(
            ForeignKeyRule("production_line_id", "production_lines", "production_line_id"),
        ),
        row_rules=(
            RowRule(
                "invalid_timestamp_order",
                ("planned_start_at", "planned_end_at"),
                "planned_end_at must be later than planned_start_at",
                lambda row: _ordered_timestamps(row, "planned_start_at", "planned_end_at"),
            ),
            RowRule(
                "incomplete_timestamp_pair",
                ("actual_start_at", "actual_end_at"),
                "actual_start_at and actual_end_at must either both be set or both be null",
                _actual_timestamp_pair,
            ),
            RowRule(
                "invalid_timestamp_order",
                ("actual_start_at", "actual_end_at"),
                "actual_end_at must not precede actual_start_at",
                lambda row: (
                    True
                    if row.get("actual_start_at") is None or row.get("actual_end_at") is None
                    else _ordered_timestamps(
                        row,
                        "actual_start_at",
                        "actual_end_at",
                        allow_equal=True,
                    )
                ),
            ),
        ),
    ),
    "machine_telemetry": SourceContract(
        source_name="machine_telemetry",
        columns={
            "telemetry_id": _identifier(unique=True),
            "machine_id": _identifier(),
            "event_timestamp": _timestamp(),
            "temperature_c": _number(-40.0, 180.0),
            "vibration_mm_s": _number(0.0, 50.0),
            "pressure_bar": _number(0.0, 500.0),
            "energy_kwh": _number(0.0, 100_000.0),
            "operating_state": _enum("running", "idle", "stopped"),
            "updated_at": _timestamp(),
        },
        primary_key="telemetry_id",
        foreign_keys=(ForeignKeyRule("machine_id", "machines", "machine_id"),),
        row_rules=(
            RowRule(
                "invalid_timestamp_order",
                ("event_timestamp", "updated_at"),
                "updated_at must not precede event_timestamp",
                lambda row: _ordered_timestamps(
                    row,
                    "event_timestamp",
                    "updated_at",
                    allow_equal=True,
                ),
            ),
        ),
    ),
    "downtime_events": SourceContract(
        source_name="downtime_events",
        columns={
            "downtime_event_id": _identifier(unique=True),
            "machine_id": _identifier(),
            "started_at": _timestamp(),
            "ended_at": _timestamp(nullable=True),
            "downtime_type": _enum("planned", "unplanned"),
            "reason_code": _enum(
                "maintenance",
                "breakdown",
                "changeover",
                "material_shortage",
            ),
            "updated_at": _timestamp(),
        },
        primary_key="downtime_event_id",
        foreign_keys=(ForeignKeyRule("machine_id", "machines", "machine_id"),),
        row_rules=(
            RowRule(
                "invalid_timestamp_order",
                ("started_at", "ended_at"),
                "ended_at must be later than started_at when present",
                lambda row: _ordered_timestamps(
                    row,
                    "started_at",
                    "ended_at",
                    allow_missing_later=True,
                ),
            ),
            RowRule(
                "invalid_timestamp_order",
                ("started_at", "updated_at"),
                "updated_at must not precede started_at",
                lambda row: _ordered_timestamps(
                    row,
                    "started_at",
                    "updated_at",
                    allow_equal=True,
                ),
            ),
        ),
    ),
    "maintenance_work_orders": SourceContract(
        source_name="maintenance_work_orders",
        columns={
            "maintenance_work_order_id": _identifier(unique=True),
            "machine_id": _identifier(),
            "created_at": _timestamp(),
            "scheduled_for": _timestamp(not_future=False),
            "completed_at": _timestamp(nullable=True),
            "maintenance_type": _enum("preventive", "corrective", "inspection"),
            "priority": _enum("low", "medium", "high", "critical"),
            "status": _enum("open", "in_progress", "completed", "cancelled"),
            "technician_id": _identifier(),
            "updated_at": _timestamp(),
        },
        primary_key="maintenance_work_order_id",
        foreign_keys=(ForeignKeyRule("machine_id", "machines", "machine_id"),),
        row_rules=(
            RowRule(
                "invalid_timestamp_order",
                ("created_at", "completed_at"),
                "completed_at must not precede created_at",
                lambda row: _ordered_timestamps(
                    row,
                    "created_at",
                    "completed_at",
                    allow_equal=True,
                    allow_missing_later=True,
                ),
            ),
            RowRule(
                "invalid_timestamp_order",
                ("created_at", "updated_at"),
                "updated_at must not precede created_at",
                lambda row: _ordered_timestamps(
                    row,
                    "created_at",
                    "updated_at",
                    allow_equal=True,
                ),
            ),
        ),
    ),
    "quality_inspections": SourceContract(
        source_name="quality_inspections",
        columns={
            "quality_inspection_id": _identifier(unique=True),
            "production_order_id": _identifier(),
            "inspected_at": _timestamp(),
            "sample_size": _integer(1, 1_000_000),
            "passed_units": _integer(0, 1_000_000),
            "failed_units": _integer(0, 1_000_000),
            "result": _enum("pass", "fail"),
            "inspector_id": _identifier(),
            "updated_at": _timestamp(),
        },
        primary_key="quality_inspection_id",
        foreign_keys=(
            ForeignKeyRule("production_order_id", "production_orders", "production_order_id"),
        ),
        row_rules=(
            RowRule(
                "inspection_total_mismatch",
                ("sample_size", "passed_units", "failed_units"),
                "passed_units plus failed_units must equal sample_size",
                _inspection_totals,
            ),
            RowRule(
                "inspection_result_mismatch",
                ("failed_units", "result"),
                "result must be fail when failed_units is positive and pass otherwise",
                _inspection_result_matches,
            ),
            RowRule(
                "invalid_timestamp_order",
                ("inspected_at", "updated_at"),
                "updated_at must not precede inspected_at",
                lambda row: _ordered_timestamps(
                    row,
                    "inspected_at",
                    "updated_at",
                    allow_equal=True,
                ),
            ),
        ),
    ),
    "product_defects": SourceContract(
        source_name="product_defects",
        columns={
            "product_defect_id": _identifier(unique=True),
            "quality_inspection_id": _identifier(),
            "detected_at": _timestamp(),
            "defect_type": _enum(
                "dimensional",
                "surface",
                "assembly",
                "material",
                "functional",
            ),
            "severity": _enum("minor", "major", "critical"),
            "defect_count": _integer(1, 1_000_000),
            "updated_at": _timestamp(),
        },
        primary_key="product_defect_id",
        foreign_keys=(
            ForeignKeyRule(
                "quality_inspection_id",
                "quality_inspections",
                "quality_inspection_id",
            ),
        ),
        row_rules=(
            RowRule(
                "invalid_timestamp_order",
                ("detected_at", "updated_at"),
                "updated_at must not precede detected_at",
                lambda row: _ordered_timestamps(
                    row,
                    "detected_at",
                    "updated_at",
                    allow_equal=True,
                ),
            ),
        ),
    ),
}

# Familiar alias for readers who search for "data contracts" explicitly.
DATA_CONTRACTS = SOURCE_CONTRACTS


def get_contract(source_name: str) -> SourceContract:
    """Return a registered source contract or a contextual domain error."""
    try:
        return SOURCE_CONTRACTS[source_name]
    except KeyError as exc:
        known_sources = ", ".join(SOURCE_NAMES)
        msg = f"unknown source '{source_name}'; expected one of: {known_sources}"
        raise ContractError(msg) from exc


def _normalise_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        msg = "the contract validation clock must include a UTC offset"
        raise ContractError(msg)
    return now.astimezone(UTC)


def _actual_columns(records: Sequence[Mapping[str, Any]]) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for record in records:
        for column in record:
            if column not in seen:
                columns.append(column)
                seen.add(column)
    return columns


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return isinstance(missing, bool) and missing


def _safe_reason_value(value: Any) -> str | int | float | bool | None:
    if _is_missing(value):
        return None
    if isinstance(value, str):
        return value if len(value) <= MAX_REASON_VALUE_CHARACTERS else REDACTED_REASON_VALUE
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if -(10**18) <= value <= 10**18:
            return value
        return "<redacted: integer exceeds evidence limit>"
    if isinstance(value, float):
        return value if math.isfinite(value) else "<non-finite numeric value>"
    item = getattr(value, "item", None)
    if callable(item):
        scalar = item()
        if scalar is not value:
            return _safe_reason_value(scalar)
    rendered = str(value)
    return rendered if len(rendered) <= MAX_REASON_VALUE_CHARACTERS else REDACTED_REASON_VALUE


def _failure_code(check: str) -> str:
    if "not_nullable" in check:
        return "required_value_missing"
    if "accepted_values" in check or "isin(" in check:
        return "invalid_enum"
    if "accepted_range" in check or "in_range(" in check:
        return "out_of_range"
    if "not_in_future" in check:
        return "future_timestamp"
    if "valid_iso_timestamp" in check:
        return "invalid_timestamp"
    if "valid_iso_date" in check:
        return "invalid_date"
    if "identifier_format" in check:
        return "invalid_identifier"
    if "non_blank" in check:
        return "blank_value"
    if "postgres_text_safe" in check:
        return "invalid_text_encoding"
    if "dtype" in check or "coerce" in check:
        return "invalid_type"
    if "unique" in check:
        return "duplicate_identifier"
    return "contract_check_failed"


def _failure_message(
    contract: SourceContract,
    column: str | None,
    code: str,
) -> str:
    column_label = column or "record"
    rule = contract.columns.get(column) if column is not None else None
    if code == "required_value_missing":
        return f"{column_label} is required and cannot be null"
    if code == "invalid_enum" and rule is not None:
        allowed = ", ".join(rule.accepted_values)
        return f"{column_label} must be one of: {allowed}"
    if code == "out_of_range" and rule is not None:
        return f"{column_label} must be between {rule.minimum} and {rule.maximum} inclusive"
    if code == "future_timestamp":
        return f"{column_label} must not be in the future"
    if code == "invalid_timestamp":
        return f"{column_label} must be an ISO-8601 timestamp with a UTC offset"
    if code == "invalid_date":
        return f"{column_label} must be an ISO-8601 date"
    if code == "invalid_identifier":
        return f"{column_label} must use the documented uppercase synthetic identifier format"
    if code == "blank_value":
        return f"{column_label} cannot be blank"
    if code == "invalid_text_encoding":
        return f"{column_label} cannot contain the NUL character"
    if code == "invalid_type" and rule is not None:
        return f"{column_label} must have source type {rule.kind}"
    if code == "duplicate_identifier":
        return f"{column_label} must be unique within the source file"
    return f"{column_label} failed its data contract"


def _pandera_reasons(
    error: pa.errors.SchemaError | pa.errors.SchemaErrors,
    contract: SourceContract,
) -> list[QuarantineReason]:
    failure_cases = getattr(error, "failure_cases", None)
    if not isinstance(failure_cases, pd.DataFrame) or failure_cases.empty:
        return [
            QuarantineReason(
                code="contract_check_failed",
                check="pandera_schema",
                message="record failed its Pandera data contract",
            )
        ]

    return [
        _reason_from_pandera_failure(failure, contract)
        for failure in failure_cases.to_dict(orient="records")
    ]


def _reason_from_pandera_failure(
    failure: Mapping[Hashable, Any],
    contract: SourceContract,
) -> QuarantineReason:
    raw_column = failure.get("column")
    column = raw_column if isinstance(raw_column, str) else None
    check = str(failure.get("check", "pandera_schema"))
    code = _failure_code(check)
    return QuarantineReason(
        code=code,
        column=column,
        check=check,
        message=_failure_message(contract, column, code),
        value=_safe_reason_value(failure.get("failure_case")),
    )


def _indexed_pandera_reasons(
    error: pa.errors.SchemaError | pa.errors.SchemaErrors,
    contract: SourceContract,
    candidate_rows: set[int],
) -> dict[int, list[QuarantineReason]] | None:
    """Map vectorized Pandera failures to source rows, or request precise fallback."""
    failure_cases = getattr(error, "failure_cases", None)
    if not isinstance(failure_cases, pd.DataFrame) or failure_cases.empty:
        return None
    indexed: dict[int, list[QuarantineReason]] = {}
    for failure in failure_cases.to_dict(orient="records"):
        raw_index = failure.get("index")
        if not isinstance(raw_index, int) or raw_index not in candidate_rows:
            return None
        indexed.setdefault(raw_index, []).append(_reason_from_pandera_failure(failure, contract))
    return indexed


def _deduplicate_reasons(reasons: Sequence[QuarantineReason]) -> list[QuarantineReason]:
    unique_reasons: list[QuarantineReason] = []
    seen: set[tuple[str, str | None, str, str]] = set()
    for reason in reasons:
        value = repr(reason.value)
        key = (reason.code, reason.column, reason.check, value)
        if key not in seen:
            unique_reasons.append(reason)
            seen.add(key)
    return unique_reasons


def validate_records(
    source_name: str,
    records: Sequence[Mapping[str, Any]],
    *,
    now: datetime | None = None,
    observed_columns: Sequence[str] | None = None,
) -> ContractResult:
    """Validate one source and retain every rejected row with structured reasons.

    ``observed_columns`` preserves a CSV header when a source contains zero data rows.
    Without that explicit file-level shape, an empty sequence has no columns to infer.
    """
    contract = get_contract(source_name)
    validation_time = _normalise_now(now)
    actual_columns = (
        list(dict.fromkeys(observed_columns))
        if observed_columns is not None
        else _actual_columns(records)
    )
    expected_columns = list(contract.expected_columns)
    missing_columns = [column for column in expected_columns if column not in actual_columns]
    unexpected_columns = [column for column in actual_columns if column not in contract.columns]
    schema_changes: list[SchemaChange] = []
    if missing_columns or unexpected_columns:
        schema_changes.append(
            SchemaChange(
                source_name=source_name,
                change_type="breaking" if missing_columns else "additive",
                expected_columns=expected_columns,
                actual_columns=actual_columns,
                missing_columns=missing_columns,
                unexpected_columns=unexpected_columns,
            )
        )

    schema = contract.build_schema(now=validation_time)
    seen_identifiers: set[Any] = set()
    raw_by_row: dict[int, dict[str, Any]] = {}
    expected_by_row: dict[int, dict[str, Any]] = {}
    reasons_by_row: dict[int, list[QuarantineReason]] = {}
    candidate_rows: list[int] = []

    for source_row_number, raw_record in enumerate(records, start=2):
        expected_record = {column: raw_record.get(column) for column in expected_columns}
        raw_by_row[source_row_number] = dict(raw_record)
        expected_by_row[source_row_number] = expected_record
        reasons: list[QuarantineReason] = []
        for column in contract.required_columns:
            if column not in raw_record or _is_missing(raw_record.get(column)):
                is_file_level_missing = column in missing_columns
                reasons.append(
                    QuarantineReason(
                        code=(
                            "missing_required_column"
                            if is_file_level_missing
                            else "required_value_missing"
                        ),
                        column=column,
                        check="column_presence" if is_file_level_missing else "not_nullable",
                        message=(
                            f"required column '{column}' is absent from the {source_name} file"
                            if is_file_level_missing
                            else f"{column} is required and cannot be null"
                        ),
                        value=None,
                    )
                )

        identifier = expected_record.get(contract.primary_key)
        identifier_is_hashable = isinstance(identifier, Hashable)
        duplicate_identifier = identifier_is_hashable and identifier in seen_identifiers
        if duplicate_identifier:
            reasons.append(
                QuarantineReason(
                    code="duplicate_identifier",
                    column=contract.primary_key,
                    check="unique_identifier",
                    message=(
                        f"{contract.primary_key} must be unique within the {source_name} file"
                    ),
                    value=_safe_reason_value(identifier),
                )
            )
        elif identifier_is_hashable and not _is_missing(identifier):
            seen_identifiers.add(identifier)

        reasons_by_row[source_row_number] = reasons
        if not reasons:
            candidate_rows.append(source_row_number)

    schema_failed_rows: set[int] = set()
    if candidate_rows:
        frame = pd.DataFrame(
            [expected_by_row[row_number] for row_number in candidate_rows],
            index=candidate_rows,
        )
        try:
            schema.validate(frame, lazy=True)
        except (pa.errors.SchemaError, pa.errors.SchemaErrors) as exc:
            indexed_reasons = _indexed_pandera_reasons(
                exc,
                contract,
                set(candidate_rows),
            )
            if indexed_reasons is not None:
                for row_number, reasons in indexed_reasons.items():
                    reasons_by_row[row_number].extend(reasons)
                    schema_failed_rows.add(row_number)
            else:
                # Dtype failures can be dataframe-level in Pandera. Revalidating only
                # the affected candidate file one row at a time preserves precise
                # quarantine attribution without penalizing the healthy fast path.
                for row_number in candidate_rows:
                    row_frame = pd.DataFrame(
                        [expected_by_row[row_number]],
                        index=[row_number],
                    )
                    try:
                        schema.validate(row_frame, lazy=True)
                    except (pa.errors.SchemaError, pa.errors.SchemaErrors) as row_error:
                        reasons_by_row[row_number].extend(_pandera_reasons(row_error, contract))
                        schema_failed_rows.add(row_number)

    for row_number in candidate_rows:
        if row_number in schema_failed_rows:
            continue
        expected_record = expected_by_row[row_number]
        for row_rule in contract.row_rules:
            if not row_rule.checker(expected_record):
                reasons_by_row[row_number].append(
                    QuarantineReason(
                        code=row_rule.code,
                        column=",".join(row_rule.columns),
                        check=row_rule.code,
                        message=row_rule.message,
                    )
                )

    accepted_records: list[dict[str, Any]] = []
    quarantined_records: list[QuarantinedRecord] = []
    for source_row_number in range(2, len(records) + 2):
        reasons = _deduplicate_reasons(reasons_by_row[source_row_number])
        if reasons:
            quarantined_records.append(
                QuarantinedRecord(
                    source_name=source_name,
                    source_row_number=source_row_number,
                    raw_payload=raw_by_row[source_row_number],
                    reasons=reasons,
                )
            )
        else:
            accepted_records.append(expected_by_row[source_row_number])

    return ContractResult(
        source_name=source_name,
        source_rows=len(records),
        accepted_records=accepted_records,
        quarantined_records=quarantined_records,
        schema_changes=schema_changes,
    )


def _raw_record_lookup(
    contract: SourceContract,
    records: Sequence[Mapping[str, Any]],
) -> dict[Any, tuple[int, dict[str, Any]]]:
    lookup: dict[Any, tuple[int, dict[str, Any]]] = {}
    for row_number, record in enumerate(records, start=2):
        identifier = record.get(contract.primary_key)
        try:
            lookup.setdefault(identifier, (row_number, dict(record)))
        except TypeError:
            continue
    return lookup


def validate_dataset(
    dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    now: datetime | None = None,
    observed_columns: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, ContractResult]:
    """Validate sources in dependency order and enforce accepted-row foreign keys."""
    unknown_sources = set(dataset).difference(SOURCE_CONTRACTS)
    if unknown_sources:
        names = ", ".join(sorted(unknown_sources))
        msg = f"dataset contains unknown sources: {names}"
        raise ContractError(msg)

    validation_time = _normalise_now(now)
    results = {
        source_name: validate_records(
            source_name,
            dataset[source_name],
            now=validation_time,
            observed_columns=(observed_columns or {}).get(source_name),
        )
        for source_name in SOURCE_NAMES
        if source_name in dataset
    }

    for source_name in SOURCE_NAMES:
        if source_name not in results:
            continue
        contract = SOURCE_CONTRACTS[source_name]
        if not contract.foreign_keys:
            continue
        result = results[source_name]
        lookup = _raw_record_lookup(contract, dataset[source_name])
        retained: list[dict[str, Any]] = []
        relationship_failures: list[QuarantinedRecord] = []
        for accepted_record in result.accepted_records:
            reasons: list[QuarantineReason] = []
            for foreign_key in contract.foreign_keys:
                parent_result = results.get(foreign_key.parent_source)
                if parent_result is None:
                    reasons.append(
                        QuarantineReason(
                            code="missing_parent_source",
                            column=foreign_key.column,
                            check="foreign_key",
                            message=(
                                f"{foreign_key.column} cannot be verified because required "
                                f"parent source {foreign_key.parent_source} is absent"
                            ),
                            value=_safe_reason_value(accepted_record.get(foreign_key.column)),
                        )
                    )
                    continue
                parent_values = {
                    parent_record.get(foreign_key.parent_column)
                    for parent_record in parent_result.accepted_records
                }
                value = accepted_record.get(foreign_key.column)
                if value not in parent_values:
                    reasons.append(
                        QuarantineReason(
                            code="referential_integrity_violation",
                            column=foreign_key.column,
                            check="foreign_key",
                            message=(
                                f"{foreign_key.column} must reference an accepted "
                                f"{foreign_key.parent_source}.{foreign_key.parent_column}"
                            ),
                            value=_safe_reason_value(value),
                        )
                    )
            if not reasons:
                retained.append(accepted_record)
                continue

            identifier = accepted_record.get(contract.primary_key)
            row_number, raw_payload = lookup.get(identifier, (2, dict(accepted_record)))
            relationship_failures.append(
                QuarantinedRecord(
                    source_name=source_name,
                    source_row_number=row_number,
                    raw_payload=raw_payload,
                    reasons=reasons,
                )
            )

        if relationship_failures:
            quarantined = [*result.quarantined_records, *relationship_failures]
            quarantined.sort(key=lambda record: record.source_row_number)
            results[source_name] = result.model_copy(
                update={
                    "accepted_records": retained,
                    "quarantined_records": quarantined,
                }
            )

    return results


class ContractValidator:
    """Small injectable facade for orchestration and ingestion boundaries."""

    def validate_source(
        self,
        source_name: str,
        records: Sequence[Mapping[str, Any]],
        *,
        now: datetime | None = None,
    ) -> ContractResult:
        """Validate a single source file."""
        return validate_records(source_name, records, now=now)

    def validate_dataset(
        self,
        dataset: Mapping[str, Sequence[Mapping[str, Any]]],
        *,
        now: datetime | None = None,
    ) -> dict[str, ContractResult]:
        """Validate a related set of source files."""
        return validate_dataset(dataset, now=now)


validate_source = validate_records
