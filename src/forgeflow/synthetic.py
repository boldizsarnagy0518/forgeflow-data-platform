"""Deterministic synthetic industrial source data and named incident injection.

The generator deliberately emits source-shaped, JSON-serializable records. Warehouse
lineage is added by ingestion so replaying a generated object produces the same source
checksum for a given seed, batch date, and scenario.
"""

from __future__ import annotations

import random
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import Any, Self

from forgeflow.config import Settings, get_settings
from forgeflow.models import FailureScenario

SOURCE_NAMES: tuple[str, ...] = (
    "factories",
    "production_lines",
    "machines",
    "shifts",
    "production_orders",
    "machine_telemetry",
    "downtime_events",
    "maintenance_work_orders",
    "quality_inspections",
    "product_defects",
)


class IncidentInjection(StrEnum):
    """Independently selectable deterministic source-data failures."""

    MISSING_REQUIRED_COLUMN = "missing_required_column"
    ADDITIVE_SCHEMA_DRIFT = "additive_schema_drift"
    DUPLICATE_RECORD = "duplicate_record"
    IMPOSSIBLE_MEASUREMENT = "impossible_measurement"
    INVALID_ENUM = "invalid_enum"
    FUTURE_TIMESTAMP = "future_timestamp"
    LATE_ARRIVING_EVENT = "late_arriving_event"
    REFERENTIAL_INTEGRITY_VIOLATION = "referential_integrity_violation"
    PRODUCTION_BUSINESS_RULE_VIOLATION = "production_business_rule_violation"


DEFAULT_INCIDENT_INJECTIONS: tuple[IncidentInjection, ...] = tuple(IncidentInjection)

SyntheticRecord = dict[str, Any]
SyntheticDataset = dict[str, list[SyntheticRecord]]


def _utc_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour=hour, minute=minute), tzinfo=UTC)


def _copy_dataset(dataset: Mapping[str, Sequence[Mapping[str, Any]]]) -> SyntheticDataset:
    return {source: [dict(record) for record in records] for source, records in dataset.items()}


class SyntheticDataGenerator:
    """Generate deterministic historical or one-day industrial source batches.

    Determinism is defined by ``seed`` plus the explicit ``batch_date``. When no batch
    date is supplied, the most recently completed UTC day is used so the healthy data
    does not accidentally contain in-progress, future-dated shifts.
    """

    def __init__(
        self,
        seed: int | None = None,
        generated_days: int | None = None,
        *,
        settings: Settings | None = None,
    ) -> None:
        if seed is None or generated_days is None:
            resolved_settings = settings or get_settings()
            seed = resolved_settings.seed if seed is None else seed
            generated_days = (
                resolved_settings.generated_days if generated_days is None else generated_days
            )
        if generated_days < 1:
            msg = "generated_days must be at least one"
            raise ValueError(msg)
        self.seed = seed
        self.generated_days = generated_days

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        """Build a generator from the centralized application settings."""
        return cls(settings=settings)

    def generate(
        self,
        scenario: FailureScenario | str = FailureScenario.CLEAN,
        *,
        batch_date: date | None = None,
        incremental: bool = False,
        injections: Iterable[IncidentInjection | str] | None = None,
    ) -> SyntheticDataset:
        """Generate all ten sources for a historical or incremental batch."""
        resolved_scenario = FailureScenario(scenario)
        resolved_batch_date = batch_date or (datetime.now(UTC).date() - timedelta(days=2))
        days = 1 if incremental else self.generated_days
        dataset = self._generate_clean(resolved_batch_date, days)

        selected_injections: Iterable[IncidentInjection | str]
        if injections is not None:
            selected_injections = injections
        elif resolved_scenario is FailureScenario.INCIDENT:
            selected_injections = DEFAULT_INCIDENT_INJECTIONS
        else:
            selected_injections = ()
        generated = inject_incidents(dataset, selected_injections)
        if resolved_scenario is FailureScenario.RECOVERY:
            _apply_recovery_corrections(generated)
        return generated

    def generate_baseline(self, *, batch_date: date | None = None) -> SyntheticDataset:
        """Generate a clean multi-day historical baseline."""
        return self.generate(FailureScenario.CLEAN, batch_date=batch_date)

    def generate_incremental(
        self,
        *,
        batch_date: date,
        scenario: FailureScenario | str = FailureScenario.CLEAN,
        injections: Iterable[IncidentInjection | str] | None = None,
    ) -> SyntheticDataset:
        """Generate a deterministic one-day batch, including referenced dimensions."""
        return self.generate(
            scenario,
            batch_date=batch_date,
            incremental=True,
            injections=injections,
        )

    def _generate_clean(self, batch_date: date, days: int) -> SyntheticDataset:
        # This PRNG drives reproducible demo fixtures, never secrets or authorization.
        random_source = random.Random(self.seed)  # noqa: S311  # nosec B311
        first_day = batch_date - timedelta(days=days - 1)
        generated_at = _at(batch_date + timedelta(days=1), 8)

        factories = self._factories(generated_at)
        production_lines = self._production_lines(factories, generated_at)
        machines = self._machines(production_lines, generated_at)
        shifts = self._shifts(factories, first_day, days)
        production_orders = self._production_orders(
            production_lines,
            first_day,
            days,
            random_source,
        )
        machine_telemetry = self._machine_telemetry(
            machines,
            first_day,
            days,
            random_source,
        )
        downtime_events = self._downtime_events(machines, first_day, days)
        maintenance_work_orders = self._maintenance_work_orders(
            machines,
            first_day,
            days,
        )
        quality_inspections = self._quality_inspections(production_orders)
        product_defects = self._product_defects(quality_inspections)

        return {
            "factories": factories,
            "production_lines": production_lines,
            "machines": machines,
            "shifts": shifts,
            "production_orders": production_orders,
            "machine_telemetry": machine_telemetry,
            "downtime_events": downtime_events,
            "maintenance_work_orders": maintenance_work_orders,
            "quality_inspections": quality_inspections,
            "product_defects": product_defects,
        }

    @staticmethod
    def _factories(generated_at: datetime) -> list[SyntheticRecord]:
        definitions = (
            ("HU", "Europe/Budapest", "Factory Alpha", date(2012, 5, 14)),
            ("DE", "Europe/Berlin", "Factory Beta", date(2015, 9, 1)),
            ("CZ", "Europe/Prague", "Factory Gamma", date(2018, 3, 19)),
        )
        return [
            {
                "factory_id": f"FAC-{index:03d}",
                "factory_name": name,
                "country_code": country,
                "timezone": timezone,
                "opened_on": opened_on.isoformat(),
                "status": "active",
                "updated_at": _utc_timestamp(generated_at),
            }
            for index, (country, timezone, name, opened_on) in enumerate(definitions, start=1)
        ]

    @staticmethod
    def _production_lines(
        factories: Sequence[SyntheticRecord],
        generated_at: datetime,
    ) -> list[SyntheticRecord]:
        families = ("motor", "pump", "gearbox")
        records: list[SyntheticRecord] = []
        line_number = 1
        for factory_index, factory in enumerate(factories):
            for local_line in range(1, 3):
                records.append(
                    {
                        "production_line_id": f"LINE-{line_number:03d}",
                        "factory_id": factory["factory_id"],
                        "line_name": f"Line {factory_index + 1}-{local_line}",
                        "product_family": families[(line_number - 1) % len(families)],
                        "nominal_capacity_per_hour": 90 + (line_number * 10),
                        "status": "active",
                        "updated_at": _utc_timestamp(generated_at),
                    }
                )
                line_number += 1
        return records

    @staticmethod
    def _machines(
        production_lines: Sequence[SyntheticRecord],
        generated_at: datetime,
    ) -> list[SyntheticRecord]:
        machine_types = ("cnc", "press", "robot", "welder", "inspection")
        manufacturers = ("Synthetic Dynamics", "Example Automation", "Demo Robotics")
        records: list[SyntheticRecord] = []
        machine_number = 1
        for production_line in production_lines:
            for local_machine in range(1, 4):
                machine_type = machine_types[(machine_number - 1) % len(machine_types)]
                records.append(
                    {
                        "machine_id": f"MCH-{machine_number:03d}",
                        "production_line_id": production_line["production_line_id"],
                        "machine_name": f"Machine {machine_number:03d}",
                        "machine_type": machine_type,
                        "manufacturer": manufacturers[(machine_number - 1) % len(manufacturers)],
                        "model": f"SYN-{machine_type.upper()}-{local_machine}00",
                        "installed_on": date(
                            2019 + (machine_number % 5),
                            1 + local_machine,
                            10,
                        ).isoformat(),
                        "status": "active",
                        "updated_at": _utc_timestamp(generated_at),
                    }
                )
                machine_number += 1
        return records

    @staticmethod
    def _shifts(
        factories: Sequence[SyntheticRecord],
        first_day: date,
        days: int,
    ) -> list[SyntheticRecord]:
        shift_definitions = (("morning", 6), ("afternoon", 14), ("night", 22))
        records: list[SyntheticRecord] = []
        for day_offset in range(days):
            shift_day = first_day + timedelta(days=day_offset)
            for factory_index, factory in enumerate(factories, start=1):
                for shift_index, (shift_name, start_hour) in enumerate(
                    shift_definitions,
                    start=1,
                ):
                    started_at = _at(shift_day, start_hour)
                    ended_at = started_at + timedelta(hours=8)
                    records.append(
                        {
                            "shift_id": (
                                f"SHF-{shift_day:%Y%m%d}-{factory_index:02d}-{shift_index:02d}"
                            ),
                            "factory_id": factory["factory_id"],
                            "shift_name": shift_name,
                            "started_at": _utc_timestamp(started_at),
                            "ended_at": _utc_timestamp(ended_at),
                            "operator_id": f"OP-{
                                ((day_offset * 9 + factory_index * 3 + shift_index) % 36) + 1:03d}",
                            "updated_at": _utc_timestamp(ended_at + timedelta(minutes=5)),
                        }
                    )
        return records

    @staticmethod
    def _production_orders(
        production_lines: Sequence[SyntheticRecord],
        first_day: date,
        days: int,
        random_source: random.Random,
    ) -> list[SyntheticRecord]:
        records: list[SyntheticRecord] = []
        for day_offset in range(days):
            order_day = first_day + timedelta(days=day_offset)
            for line_index, production_line in enumerate(production_lines, start=1):
                planned_start = _at(order_day, 6)
                planned_end = planned_start + timedelta(hours=8)
                actual_start = planned_start + timedelta(minutes=random_source.randint(0, 12))
                actual_end = planned_end - timedelta(minutes=random_source.randint(0, 25))
                planned_quantity = random_source.randint(720, 960)
                actual_quantity = planned_quantity - random_source.randint(0, 45)
                family = str(production_line["product_family"])
                records.append(
                    {
                        "production_order_id": f"ORD-{order_day:%Y%m%d}-{line_index:03d}",
                        "production_line_id": production_line["production_line_id"],
                        "product_code": f"PRD-{family.upper()}-{(line_index % 4) + 1:03d}",
                        "planned_start_at": _utc_timestamp(planned_start),
                        "planned_end_at": _utc_timestamp(planned_end),
                        "actual_start_at": _utc_timestamp(actual_start),
                        "actual_end_at": _utc_timestamp(actual_end),
                        "planned_quantity": planned_quantity,
                        "actual_quantity": actual_quantity,
                        "status": "completed",
                        "updated_at": _utc_timestamp(actual_end + timedelta(minutes=10)),
                    }
                )
        return records

    @staticmethod
    def _machine_telemetry(
        machines: Sequence[SyntheticRecord],
        first_day: date,
        days: int,
        random_source: random.Random,
    ) -> list[SyntheticRecord]:
        records: list[SyntheticRecord] = []
        sample_hours = (2, 8, 14, 20)
        for day_offset in range(days):
            event_day = first_day + timedelta(days=day_offset)
            for machine_index, machine in enumerate(machines, start=1):
                for sample_index, sample_hour in enumerate(sample_hours, start=1):
                    event_timestamp = _at(event_day, sample_hour)
                    operating_state = "idle" if sample_index == 1 else "running"
                    state_factor = 0.65 if operating_state == "idle" else 1.0
                    records.append(
                        {
                            "telemetry_id": (
                                f"TEL-{event_day:%Y%m%d}-{machine_index:03d}-{sample_index:02d}"
                            ),
                            "machine_id": machine["machine_id"],
                            "event_timestamp": _utc_timestamp(event_timestamp),
                            "temperature_c": round(
                                (48 + random_source.uniform(0, 28)) * state_factor,
                                2,
                            ),
                            "vibration_mm_s": round(
                                (0.8 + random_source.uniform(0, 4.2)) * state_factor,
                                3,
                            ),
                            "pressure_bar": round(
                                (2.5 + random_source.uniform(0, 7.5)) * state_factor,
                                2,
                            ),
                            "energy_kwh": round(
                                (8 + random_source.uniform(0, 42)) * state_factor,
                                3,
                            ),
                            "operating_state": operating_state,
                            "updated_at": _utc_timestamp(event_timestamp + timedelta(minutes=15)),
                        }
                    )
        return records

    @staticmethod
    def _downtime_events(
        machines: Sequence[SyntheticRecord],
        first_day: date,
        days: int,
    ) -> list[SyntheticRecord]:
        reasons = ("maintenance", "breakdown", "changeover", "material_shortage")
        records: list[SyntheticRecord] = []
        event_number = 1
        for day_offset in range(days):
            event_day = first_day + timedelta(days=day_offset)
            for machine_index, machine in enumerate(machines, start=1):
                if (day_offset + machine_index) % 7 != 0:
                    continue
                started_at = _at(event_day, 9 + (machine_index % 4))
                duration = 25 + ((machine_index + day_offset) % 6) * 15
                ended_at = started_at + timedelta(minutes=duration)
                reason_code = reasons[(machine_index + day_offset) % len(reasons)]
                downtime_type = (
                    "planned" if reason_code in {"maintenance", "changeover"} else "unplanned"
                )
                records.append(
                    {
                        "downtime_event_id": f"DWN-{event_number:06d}",
                        "machine_id": machine["machine_id"],
                        "started_at": _utc_timestamp(started_at),
                        "ended_at": _utc_timestamp(ended_at),
                        "downtime_type": downtime_type,
                        "reason_code": reason_code,
                        "updated_at": _utc_timestamp(ended_at + timedelta(minutes=10)),
                    }
                )
                event_number += 1
        return records

    @staticmethod
    def _maintenance_work_orders(
        machines: Sequence[SyntheticRecord],
        first_day: date,
        days: int,
    ) -> list[SyntheticRecord]:
        maintenance_types = ("preventive", "corrective", "inspection")
        priorities = ("low", "medium", "high", "critical")
        records: list[SyntheticRecord] = []
        for machine_index, machine in enumerate(machines, start=1):
            work_day = first_day + timedelta(days=(machine_index - 1) % days)
            created_at = _at(work_day, 7)
            scheduled_for = created_at + timedelta(hours=3)
            is_open = machine_index % 5 == 0
            completed_at = None if is_open else scheduled_for + timedelta(hours=2)
            updated_at = completed_at or (created_at + timedelta(hours=1))
            records.append(
                {
                    "maintenance_work_order_id": f"MWO-{machine_index:05d}",
                    "machine_id": machine["machine_id"],
                    "created_at": _utc_timestamp(created_at),
                    "scheduled_for": _utc_timestamp(scheduled_for),
                    "completed_at": (
                        _utc_timestamp(completed_at) if completed_at is not None else None
                    ),
                    "maintenance_type": maintenance_types[
                        (machine_index - 1) % len(maintenance_types)
                    ],
                    "priority": priorities[(machine_index - 1) % len(priorities)],
                    "status": "open" if is_open else "completed",
                    "technician_id": f"TECH-{((machine_index - 1) % 8) + 1:03d}",
                    "updated_at": _utc_timestamp(updated_at + timedelta(minutes=5)),
                }
            )
        return records

    @staticmethod
    def _quality_inspections(
        production_orders: Sequence[SyntheticRecord],
    ) -> list[SyntheticRecord]:
        records: list[SyntheticRecord] = []
        for inspection_index, production_order in enumerate(production_orders, start=1):
            actual_end = datetime.fromisoformat(
                str(production_order["actual_end_at"]).replace("Z", "+00:00")
            )
            inspected_at = actual_end + timedelta(minutes=30)
            sample_size = 20
            failed_units = 1 + (inspection_index % 2) if inspection_index % 5 == 0 else 0
            records.append(
                {
                    "quality_inspection_id": f"QIN-{inspection_index:06d}",
                    "production_order_id": production_order["production_order_id"],
                    "inspected_at": _utc_timestamp(inspected_at),
                    "sample_size": sample_size,
                    "passed_units": sample_size - failed_units,
                    "failed_units": failed_units,
                    "result": "fail" if failed_units else "pass",
                    "inspector_id": f"INSP-{((inspection_index - 1) % 12) + 1:03d}",
                    "updated_at": _utc_timestamp(inspected_at + timedelta(minutes=5)),
                }
            )
        return records

    @staticmethod
    def _product_defects(
        quality_inspections: Sequence[SyntheticRecord],
    ) -> list[SyntheticRecord]:
        defect_types = ("dimensional", "surface", "assembly", "material", "functional")
        severities = ("minor", "major", "critical")
        records: list[SyntheticRecord] = []
        defect_number = 1
        for inspection_index, inspection in enumerate(quality_inspections, start=1):
            defect_count = int(inspection["failed_units"])
            if defect_count == 0:
                continue
            detected_at = str(inspection["inspected_at"])
            records.append(
                {
                    "product_defect_id": f"DEF-{defect_number:06d}",
                    "quality_inspection_id": inspection["quality_inspection_id"],
                    "detected_at": detected_at,
                    "defect_type": defect_types[(inspection_index - 1) % len(defect_types)],
                    "severity": severities[(inspection_index - 1) % len(severities)],
                    "defect_count": defect_count,
                    "updated_at": _utc_timestamp(
                        datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
                        + timedelta(minutes=5)
                    ),
                }
            )
            defect_number += 1
        return records


def inject_incidents(
    dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    injections: Iterable[IncidentInjection | str] = DEFAULT_INCIDENT_INJECTIONS,
) -> SyntheticDataset:
    """Return a copy with the selected named failures injected in a stable order."""
    injected = _copy_dataset(dataset)
    missing_sources = set(SOURCE_NAMES).difference(injected)
    if missing_sources:
        names = ", ".join(sorted(missing_sources))
        msg = f"cannot inject incidents because sources are missing: {names}"
        raise ValueError(msg)

    for requested_injection in injections:
        injection = IncidentInjection(requested_injection)
        _apply_injection(injected, injection)
    return injected


def _apply_injection(dataset: SyntheticDataset, injection: IncidentInjection) -> None:
    if injection is IncidentInjection.MISSING_REQUIRED_COLUMN:
        for record in dataset["maintenance_work_orders"]:
            record.pop("priority", None)
        return

    if injection is IncidentInjection.ADDITIVE_SCHEMA_DRIFT:
        for record in dataset["machine_telemetry"]:
            record["firmware_revision"] = "unsupported-demo-revision"
        return

    if injection is IncidentInjection.DUPLICATE_RECORD:
        dataset["machine_telemetry"].append(dict(dataset["machine_telemetry"][0]))
        return

    if injection is IncidentInjection.IMPOSSIBLE_MEASUREMENT:
        dataset["machine_telemetry"][1]["temperature_c"] = 999.0
        return

    if injection is IncidentInjection.INVALID_ENUM:
        dataset["quality_inspections"][0]["result"] = "review"
        return

    if injection is IncidentInjection.FUTURE_TIMESTAMP:
        future_start = datetime(2100, 1, 1, 8, tzinfo=UTC)
        future_record = dataset["downtime_events"][0]
        future_record["started_at"] = _utc_timestamp(future_start)
        future_record["ended_at"] = _utc_timestamp(future_start + timedelta(hours=1))
        future_record["updated_at"] = _utc_timestamp(future_start + timedelta(hours=1, minutes=5))
        return

    if injection is IncidentInjection.LATE_ARRIVING_EVENT:
        reference = dataset["machine_telemetry"][0]
        latest_event = max(
            datetime.fromisoformat(str(row["event_timestamp"]).replace("Z", "+00:00"))
            for row in dataset["machine_telemetry"]
        )
        latest_update = max(
            datetime.fromisoformat(str(row["updated_at"]).replace("Z", "+00:00"))
            for row in dataset["machine_telemetry"]
        )
        late_record: SyntheticRecord = {
            "telemetry_id": "TEL-LATE-000001",
            "machine_id": reference["machine_id"],
            # Inside dbt's 48-hour incremental lookback but delayed beyond the 24-hour SLA.
            "event_timestamp": _utc_timestamp(latest_event - timedelta(hours=36)),
            "temperature_c": 61.25,
            "vibration_mm_s": 2.125,
            "pressure_bar": 5.75,
            "energy_kwh": 24.5,
            "operating_state": "running",
            "updated_at": _utc_timestamp(latest_update + timedelta(minutes=1)),
        }
        if "firmware_revision" in reference:
            late_record["firmware_revision"] = reference["firmware_revision"]
        dataset["machine_telemetry"].append(late_record)
        return

    if injection is IncidentInjection.REFERENTIAL_INTEGRITY_VIOLATION:
        reference_defect = dataset["product_defects"][0]
        dataset["product_defects"].append(
            {
                **reference_defect,
                "product_defect_id": "DEF-RI-000001",
                "quality_inspection_id": "QIN-UNKNOWN-999999",
            }
        )
        return

    if injection is IncidentInjection.PRODUCTION_BUSINESS_RULE_VIOLATION:
        reference_order = dataset["production_orders"][-1]
        dataset["production_orders"].append(
            {
                **reference_order,
                "production_order_id": "ORD-BUSINESS-RULE-001",
                "planned_quantity": 100,
                # Contract-valid but intentionally above dbt's 150% overrun rule.
                "actual_quantity": 175,
            }
        )


def _apply_recovery_corrections(dataset: SyntheticDataset) -> None:
    """Emit upserts that correct accepted incident rows without deleting history."""
    reference_order = dataset["production_orders"][-1]
    dataset["production_orders"].append(
        {
            **reference_order,
            "production_order_id": "ORD-BUSINESS-RULE-001",
            "planned_quantity": 100,
            "actual_quantity": 100,
        }
    )


def generate_dataset(
    scenario: FailureScenario | str = FailureScenario.CLEAN,
    *,
    seed: int | None = None,
    generated_days: int | None = None,
    batch_date: date | None = None,
    incremental: bool = False,
    injections: Iterable[IncidentInjection | str] | None = None,
    settings: Settings | None = None,
) -> SyntheticDataset:
    """Convenience API used by CLI and orchestration boundaries."""
    return SyntheticDataGenerator(
        seed=seed,
        generated_days=generated_days,
        settings=settings,
    ).generate(
        scenario,
        batch_date=batch_date,
        incremental=incremental,
        injections=injections,
    )


# A concise compatibility name for callers that prefer a generator noun.
SyntheticGenerator = SyntheticDataGenerator
