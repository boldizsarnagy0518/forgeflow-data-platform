"""Unit coverage for deterministic source generation and incident fixtures."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

from forgeflow.models import FailureScenario
from forgeflow.synthetic import (
    SOURCE_NAMES,
    IncidentInjection,
    SyntheticDataGenerator,
    inject_incidents,
)

BATCH_DATE = date(2025, 7, 10)


def _generator(seed: int = 41, days: int = 3) -> SyntheticDataGenerator:
    return SyntheticDataGenerator(seed=seed, generated_days=days)


def test_clean_generation_covers_all_sources_and_relationships() -> None:
    dataset = _generator().generate_baseline(batch_date=BATCH_DATE)

    assert tuple(dataset) == SOURCE_NAMES
    assert len(dataset["factories"]) == 3
    assert len(dataset["production_lines"]) == 6
    assert len(dataset["machines"]) == 18
    assert len(dataset["shifts"]) == 27
    assert len(dataset["production_orders"]) == 18
    assert len(dataset["machine_telemetry"]) == 216
    assert dataset["downtime_events"]
    assert len(dataset["maintenance_work_orders"]) == 18
    assert len(dataset["quality_inspections"]) == 18
    assert dataset["product_defects"]

    factory_ids = {row["factory_id"] for row in dataset["factories"]}
    line_ids = {row["production_line_id"] for row in dataset["production_lines"]}
    machine_ids = {row["machine_id"] for row in dataset["machines"]}
    order_ids = {row["production_order_id"] for row in dataset["production_orders"]}
    inspection_ids = {row["quality_inspection_id"] for row in dataset["quality_inspections"]}

    assert all(row["factory_id"] in factory_ids for row in dataset["production_lines"])
    assert all(row["production_line_id"] in line_ids for row in dataset["machines"])
    assert all(row["machine_id"] in machine_ids for row in dataset["machine_telemetry"])
    assert all(row["production_order_id"] in order_ids for row in dataset["quality_inspections"])
    assert all(row["quality_inspection_id"] in inspection_ids for row in dataset["product_defects"])
    json.dumps(dataset)


def test_seed_and_batch_date_make_generation_reproducible() -> None:
    first = _generator(seed=2025).generate_baseline(batch_date=BATCH_DATE)
    second = _generator(seed=2025).generate_baseline(batch_date=BATCH_DATE)
    changed_seed = _generator(seed=2026).generate_baseline(batch_date=BATCH_DATE)

    assert first == second
    assert first["factories"] == changed_seed["factories"]
    assert first["machine_telemetry"] != changed_seed["machine_telemetry"]


def test_incremental_batch_contains_one_day_of_new_facts() -> None:
    generator = _generator(days=4)
    baseline = generator.generate_baseline(batch_date=BATCH_DATE)
    incremental = generator.generate_incremental(batch_date=BATCH_DATE)

    assert incremental["factories"] == baseline["factories"]
    assert incremental["production_lines"] == baseline["production_lines"]
    assert incremental["machines"] == baseline["machines"]
    assert len(incremental["shifts"]) == 9
    assert len(incremental["production_orders"]) == 6
    assert len(incremental["machine_telemetry"]) == 72
    assert all(
        row["event_timestamp"].startswith(BATCH_DATE.isoformat())
        for row in incremental["machine_telemetry"]
    )


def test_full_incident_scenario_injects_every_named_failure() -> None:
    generator = _generator(days=2)
    clean = generator.generate_baseline(batch_date=BATCH_DATE)
    incident = generator.generate(FailureScenario.INCIDENT, batch_date=BATCH_DATE)

    assert all("priority" in row for row in clean["maintenance_work_orders"])
    assert all("priority" not in row for row in incident["maintenance_work_orders"])
    assert all("firmware_revision" in row for row in incident["machine_telemetry"])
    assert incident["machine_telemetry"][-1]["telemetry_id"] == "TEL-LATE-000001"
    assert incident["machine_telemetry"][-2] == incident["machine_telemetry"][0]
    assert incident["machine_telemetry"][1]["temperature_c"] == 999.0
    assert incident["quality_inspections"][0]["result"] == "review"
    assert incident["downtime_events"][0]["started_at"].startswith("2100-")
    assert incident["product_defects"][-1]["quality_inspection_id"] == "QIN-UNKNOWN-999999"

    business_order = incident["production_orders"][-1]
    assert business_order["production_order_id"] == "ORD-BUSINESS-RULE-001"
    assert business_order["planned_quantity"] == 100
    assert business_order["actual_quantity"] == 175
    assert int(business_order["actual_quantity"]) > int(business_order["planned_quantity"]) * 1.5


def test_named_injection_is_selective_and_does_not_mutate_input() -> None:
    clean = _generator(days=1).generate_baseline(batch_date=BATCH_DATE)
    injected = inject_incidents(clean, [IncidentInjection.INVALID_ENUM])

    assert clean["quality_inspections"][0]["result"] in {"pass", "fail"}
    assert injected["quality_inspections"][0]["result"] == "review"
    assert injected["machine_telemetry"] == clean["machine_telemetry"]
    assert injected["maintenance_work_orders"] == clean["maintenance_work_orders"]


def test_recovery_scenario_contains_only_the_corrective_upsert() -> None:
    generator = _generator(days=1)

    recovery = generator.generate(FailureScenario.RECOVERY, batch_date=BATCH_DATE)
    clean = generator.generate(FailureScenario.CLEAN, batch_date=BATCH_DATE)

    assert recovery["production_orders"][:-1] == clean["production_orders"]
    correction = recovery["production_orders"][-1]
    assert correction["production_order_id"] == "ORD-BUSINESS-RULE-001"
    assert correction["planned_quantity"] == correction["actual_quantity"] == 100
    assert all(
        recovery[source] == clean[source]
        for source in SOURCE_NAMES
        if source != "production_orders"
    )


def test_late_event_is_old_but_arrives_with_current_batch_update() -> None:
    clean = _generator(days=2).generate_baseline(batch_date=BATCH_DATE)
    incident = inject_incidents(clean, [IncidentInjection.LATE_ARRIVING_EVENT])
    late = incident["machine_telemetry"][-1]

    event_timestamp = datetime.fromisoformat(late["event_timestamp"].replace("Z", "+00:00"))
    updated_at = datetime.fromisoformat(late["updated_at"].replace("Z", "+00:00"))

    assert timedelta(hours=24) < updated_at - event_timestamp < timedelta(hours=48)
