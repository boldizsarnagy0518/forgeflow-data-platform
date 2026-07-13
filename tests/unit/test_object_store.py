"""Unit coverage for replayable filesystem landing and object-key safety."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TypedDict

import pytest

import forgeflow.object_store as object_store_module
from forgeflow.errors import ObjectStoreError
from forgeflow.object_store import (
    FilesystemObjectStore,
    schema_fingerprint,
    sha256_bytes,
)


class PutParameters(TypedDict):
    """Typed keyword payload for repeated object-store writes."""

    content: bytes
    source_name: str
    batch_id: str
    filename: str


def test_filesystem_landing_round_trips_content_with_stable_checksum(tmp_path: Path) -> None:
    root = tmp_path / "object-store"
    store = FilesystemObjectStore(root, bucket="raw")
    content = b"factory_id,factory_name\nFAC-001,Synthetic Factory\n"

    landed = store.put_bytes(
        content=content,
        source_name="factories",
        batch_id="2025-07-10-clean",
        filename="factories.csv",
    )

    expected_checksum = hashlib.sha256(content).hexdigest()
    assert landed.bucket == "raw"
    assert landed.checksum == expected_checksum == sha256_bytes(content)
    assert landed.size_bytes == len(content)
    assert landed.object_key == (
        f"incoming/factories/2025-07-10-clean/{expected_checksum}-factories.csv"
    )
    assert store.get_bytes(landed.object_key) == content
    assert store.ping()


def test_identical_content_has_the_same_replay_identity(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects")
    parameters: PutParameters = {
        "content": b"id,value\n1,alpha\n",
        "source_name": "factories",
        "batch_id": "batch-001",
        "filename": "factories.csv",
    }

    first = store.put_bytes(**parameters)
    second = store.put_bytes(**parameters)

    assert first == second
    assert store.get_bytes(first.object_key) == parameters["content"]


def test_content_change_changes_checksum_and_content_address(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects")
    first = store.put_bytes(
        content=b"id,value\n1,alpha\n",
        source_name="factories",
        batch_id="batch-001",
        filename="factories.csv",
    )
    changed = store.put_bytes(
        content=b"id,value\n1,beta\n",
        source_name="factories",
        batch_id="batch-001",
        filename="factories.csv",
    )

    assert first.checksum != changed.checksum
    assert first.object_key != changed.object_key
    assert store.get_bytes(first.object_key) != store.get_bytes(changed.object_key)


def test_full_checksum_prevents_short_prefix_object_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = FilesystemObjectStore(tmp_path / "objects")
    shared_prefix = "a" * 12
    digests = iter((shared_prefix + "1" * 52, shared_prefix + "2" * 52))
    monkeypatch.setattr(object_store_module, "sha256_bytes", lambda content: next(digests))

    first = store.put_bytes(
        content=b"first",
        source_name="factories",
        batch_id="collision-batch",
        filename="factories.csv",
    )
    second = store.put_bytes(
        content=b"second",
        source_name="factories",
        batch_id="collision-batch",
        filename="factories.csv",
    )

    assert first.object_key != second.object_key
    assert first.object_key.endswith(f"{first.checksum}-factories.csv")
    assert second.object_key.endswith(f"{second.checksum}-factories.csv")
    assert store.get_bytes(first.object_key) == b"first"
    assert store.get_bytes(second.object_key) == b"second"


@pytest.mark.parametrize(
    ("field", "unsafe_value"),
    [
        ("source_name", ".."),
        ("source_name", "///"),
        ("batch_id", "."),
        ("batch_id", "\\\\"),
        ("filename", ".."),
    ],
)
def test_landing_rejects_empty_or_dot_only_key_segments(
    tmp_path: Path, field: str, unsafe_value: str
) -> None:
    segments = {
        "source_name": "factories",
        "batch_id": "batch-001",
        "filename": "factories.csv",
    }
    segments[field] = unsafe_value

    with pytest.raises(ObjectStoreError, match="Unsafe or empty object-key segment"):
        FilesystemObjectStore(tmp_path / "objects").put_bytes(
            content=b"data",
            source_name=segments["source_name"],
            batch_id=segments["batch_id"],
            filename=segments["filename"],
        )


def test_read_rejects_parent_traversal_even_when_target_stays_under_storage_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "objects"
    store = FilesystemObjectStore(root, bucket="raw")
    store.ensure_bucket()
    sibling = root / "outside-bucket" / "secret.csv"
    sibling.parent.mkdir()
    sibling.write_bytes(b"must not be readable through the raw bucket")

    with pytest.raises(ObjectStoreError, match="escaped the configured storage root"):
        store.get_bytes("../outside-bucket/secret.csv")


def test_read_rejects_traversal_outside_the_storage_root(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    store = FilesystemObjectStore(root, bucket="raw")
    store.ensure_bucket()
    outside = tmp_path / "outside.csv"
    outside.write_bytes(b"outside")

    with pytest.raises(ObjectStoreError, match="escaped the configured storage root"):
        store.get_bytes("../../outside.csv")


def test_missing_object_is_translated_to_domain_error(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects", bucket="raw")
    store.ensure_bucket()

    with pytest.raises(ObjectStoreError, match="Unable to read raw object"):
        store.get_bytes("incoming/factories/missing.csv")


def test_schema_fingerprint_is_stable_and_order_sensitive() -> None:
    columns = ["factory_id", "factory_name", "country_code"]

    assert schema_fingerprint(columns) == schema_fingerprint(columns.copy())
    assert schema_fingerprint(columns) == sha256_bytes(
        b"factory_id\x1ffactory_name\x1fcountry_code"
    )
    assert schema_fingerprint(columns) != schema_fingerprint(list(reversed(columns)))
