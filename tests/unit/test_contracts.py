"""Unit coverage for Pandera source contracts and quarantine evidence."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime, timedelta

import pytest

from forgeflow.contracts import SOURCE_CONTRACTS, validate_dataset, validate_records
from forgeflow.errors import ContractError
from forgeflow.models import ContractResult
from forgeflow.synthetic import SOURCE_NAMES, SyntheticDataGenerator

BATCH_DATE = date(2025, 7, 10)
VALIDATION_TIME = datetime(2025, 7, 12, 12, tzinfo=UTC)


def _dataset(*, incident: bool = False) -> dict[str, list[dict[str, object]]]:
    scenario = "incident" if incident else "clean"
    return SyntheticDataGenerator(seed=73, generated_days=2).generate(
        scenario,
        batch_date=BATCH_DATE,
    )


def _reason_codes(results: dict[str, ContractResult]) -> set[str]:
    return {
        reason.code
        for result in results.values()
        for quarantined in result.quarantined_records
        for reason in quarantined.reasons
    }


def test_every_source_has_an_explicit_pandera_contract() -> None:
    assert tuple(SOURCE_CONTRACTS) == SOURCE_NAMES
    for source_name, contract in SOURCE_CONTRACTS.items():
        assert contract.source_name == source_name
        assert contract.version == "1.0.0"
        assert contract.primary_key in contract.required_columns
        assert contract.columns[contract.primary_key].unique
        assert contract.build_schema(now=VALIDATION_TIME).name == source_name


def test_clean_dataset_is_fully_accepted() -> None:
    dataset = _dataset()
    results = validate_dataset(dataset, now=VALIDATION_TIME)

    assert tuple(results) == SOURCE_NAMES
    for source_name, result in results.items():
        assert result.source_rows == len(dataset[source_name])
        assert len(result.accepted_records) == result.source_rows
        assert result.quarantined_records == []
        assert result.schema_changes == []


def test_incident_yields_structured_quarantine_and_schema_evidence() -> None:
    results = validate_dataset(_dataset(incident=True), now=VALIDATION_TIME)

    assert {
        "missing_required_column",
        "duplicate_identifier",
        "out_of_range",
        "invalid_enum",
        "future_timestamp",
        "referential_integrity_violation",
    }.issubset(_reason_codes(results))

    maintenance_change = results["maintenance_work_orders"].schema_changes[0]
    assert maintenance_change.change_type == "breaking"
    assert maintenance_change.missing_columns == ["priority"]
    telemetry_change = results["machine_telemetry"].schema_changes[0]
    assert telemetry_change.change_type == "additive"
    assert telemetry_change.unexpected_columns == ["firmware_revision"]

    for result in results.values():
        for quarantined in result.quarantined_records:
            assert quarantined.source_row_number >= 2
            assert quarantined.raw_payload
            assert quarantined.reasons
            assert all(
                reason.code and reason.check and reason.message for reason in quarantined.reasons
            )


def test_additive_drift_is_recorded_but_valid_rows_are_projected_and_accepted() -> None:
    records = deepcopy(_dataset()["machine_telemetry"][:2])
    records[0]["new_sensor_label"] = "beta"
    records[1]["new_sensor_label"] = "beta"

    result = validate_records("machine_telemetry", records, now=VALIDATION_TIME)

    assert len(result.accepted_records) == 2
    assert result.quarantined_records == []
    assert result.schema_changes[0].change_type == "additive"
    assert result.schema_changes[0].unexpected_columns == ["new_sensor_label"]
    assert all("new_sensor_label" not in row for row in result.accepted_records)


def test_missing_required_column_quarantines_every_preserved_row() -> None:
    records = deepcopy(_dataset()["factories"])
    for record in records:
        record.pop("country_code")

    result = validate_records("factories", records, now=VALIDATION_TIME)

    assert result.accepted_records == []
    assert len(result.quarantined_records) == len(records)
    assert result.schema_changes[0].change_type == "breaking"
    assert result.schema_changes[0].missing_columns == ["country_code"]
    assert all(
        quarantined.reasons[0].code == "missing_required_column"
        for quarantined in result.quarantined_records
    )


def test_only_later_occurrence_of_duplicate_identifier_is_quarantined() -> None:
    record = deepcopy(_dataset()["machine_telemetry"][0])
    result = validate_records(
        "machine_telemetry",
        [record, deepcopy(record)],
        now=VALIDATION_TIME,
    )

    assert len(result.accepted_records) == 1
    assert len(result.quarantined_records) == 1
    assert result.quarantined_records[0].source_row_number == 3
    assert {reason.code for reason in result.quarantined_records[0].reasons} == {
        "duplicate_identifier"
    }


def test_ranges_enums_and_future_timestamps_have_specific_reasons() -> None:
    record = deepcopy(_dataset()["machine_telemetry"][0])
    record["temperature_c"] = 500.0
    record["operating_state"] = "teleporting"
    record["event_timestamp"] = "2100-01-01T00:00:00Z"
    record["updated_at"] = "2100-01-01T00:05:00Z"

    result = validate_records("machine_telemetry", [record], now=VALIDATION_TIME)
    codes = {reason.code for reason in result.quarantined_records[0].reasons}

    assert {"out_of_range", "invalid_enum", "future_timestamp"}.issubset(codes)
    assert result.accepted_records == []


def test_rejected_values_are_compact_json_safe_while_raw_payload_is_preserved() -> None:
    oversized_value = "sensitive-value-" * 1_000
    record = deepcopy(_dataset()["machine_telemetry"][0])
    record["operating_state"] = oversized_value
    record["temperature_c"] = float("inf")

    result = validate_records("machine_telemetry", [record], now=VALIDATION_TIME)

    rejected = result.quarantined_records[0]
    assert rejected.raw_payload["operating_state"] == oversized_value
    values = {reason.value for reason in rejected.reasons}
    assert "<redacted: rejected value exceeds evidence limit>" in values
    assert "<non-finite numeric value>" in values
    assert all(not isinstance(value, float) or value == value for value in values)


def test_embedded_nul_text_is_quarantined_before_postgresql_loading() -> None:
    record = deepcopy(_dataset()["factories"][0])
    record["factory_name"] = "unsafe\x00factory"

    result = validate_records("factories", [record], now=VALIDATION_TIME)

    assert result.accepted_records == []
    assert {reason.code for reason in result.quarantined_records[0].reasons} == {
        "invalid_text_encoding"
    }


def test_schema_change_caps_column_counts_and_redacts_oversized_headers() -> None:
    record = deepcopy(_dataset()["machine_telemetry"][0])
    for index in range(150):
        record[f"untrusted-{index}-" + "x" * 500] = "retained only in the raw source"

    result = validate_records("machine_telemetry", [record], now=VALIDATION_TIME)

    change = result.schema_changes[0]
    assert len(change.actual_columns) <= 100
    assert len(change.unexpected_columns) <= 100
    assert "<redacted: column name exceeds evidence limit>" in change.unexpected_columns
    assert "<additional columns omitted>" in change.unexpected_columns
    assert all(len(column) <= 128 for column in change.actual_columns)
    assert len(result.accepted_records) == 1


def test_cross_column_timestamp_and_inspection_rules_are_enforced() -> None:
    shift = deepcopy(_dataset()["shifts"][0])
    shift["ended_at"] = shift["started_at"]
    shift_result = validate_records("shifts", [shift], now=VALIDATION_TIME)

    inspection = deepcopy(_dataset()["quality_inspections"][0])
    inspection["passed_units"] = 19
    inspection["failed_units"] = 2
    inspection["result"] = "pass"
    inspection_result = validate_records(
        "quality_inspections",
        [inspection],
        now=VALIDATION_TIME,
    )

    assert {reason.code for reason in shift_result.quarantined_records[0].reasons} == {
        "invalid_timestamp_order"
    }
    assert {
        "inspection_total_mismatch",
        "inspection_result_mismatch",
    } == {reason.code for reason in inspection_result.quarantined_records[0].reasons}


def test_dataset_validation_quarantines_referential_integrity_violation() -> None:
    dataset = _dataset()
    invalid = deepcopy(dataset["product_defects"][0])
    invalid["product_defect_id"] = "DEF-RI-TEST"
    invalid["quality_inspection_id"] = "QIN-NOT-FOUND"
    dataset["product_defects"].append(invalid)

    result = validate_dataset(dataset, now=VALIDATION_TIME)["product_defects"]

    rejected = next(
        row
        for row in result.quarantined_records
        if row.raw_payload["product_defect_id"] == "DEF-RI-TEST"
    )
    assert rejected.reasons[0].code == "referential_integrity_violation"
    assert rejected.reasons[0].column == "quality_inspection_id"


def test_dataset_validation_quarantines_children_when_parent_source_is_absent() -> None:
    child_rows = deepcopy(_dataset()["production_lines"][:2])

    result = validate_dataset(
        {"production_lines": child_rows},
        now=VALIDATION_TIME,
    )["production_lines"]

    assert result.accepted_records == []
    assert len(result.quarantined_records) == len(child_rows)
    assert all(
        {reason.code for reason in row.reasons} == {"missing_parent_source"}
        for row in result.quarantined_records
    )


def test_late_arrival_and_business_overrun_remain_contract_valid() -> None:
    results = validate_dataset(_dataset(incident=True), now=VALIDATION_TIME)

    late = next(
        row
        for row in results["machine_telemetry"].accepted_records
        if row["telemetry_id"] == "TEL-LATE-000001"
    )
    business_order = next(
        row
        for row in results["production_orders"].accepted_records
        if row["production_order_id"] == "ORD-BUSINESS-RULE-001"
    )

    event_time = datetime.fromisoformat(str(late["event_timestamp"]).replace("Z", "+00:00"))
    updated_at = datetime.fromisoformat(str(late["updated_at"]).replace("Z", "+00:00"))
    assert timedelta(hours=24) < updated_at - event_time < timedelta(hours=48)
    assert business_order["actual_quantity"] == 175
    assert int(business_order["actual_quantity"]) > int(business_order["planned_quantity"]) * 1.5


def test_unknown_source_and_naive_validation_clock_raise_domain_errors() -> None:
    with pytest.raises(ContractError, match="unknown source"):
        validate_records("not_a_source", [])

    with pytest.raises(ContractError, match="UTC offset"):
        # An intentionally naive value exercises the public boundary rejection.
        validate_records("factories", [], now=datetime(2025, 7, 12))  # noqa: DTZ001
