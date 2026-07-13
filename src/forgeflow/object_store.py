"""Raw landing adapters for MinIO/S3 and deterministic test storage."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from forgeflow.config import Settings
from forgeflow.errors import ObjectStoreError

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client


@dataclass(frozen=True, slots=True)
class LandedObject:
    """Immutable identity and location of one replayable source object."""

    bucket: str
    object_key: str
    checksum: str
    size_bytes: int


class ObjectStore(Protocol):
    """Minimal raw landing boundary used by the pipeline."""

    def ensure_bucket(self) -> None:
        """Create the configured bucket when it does not exist."""

    def put_bytes(
        self,
        *,
        content: bytes,
        source_name: str,
        batch_id: str,
        filename: str,
    ) -> LandedObject:
        """Land bytes under a content-addressed, replayable object key."""

    def get_bytes(self, object_key: str) -> bytes:
        """Read an object for controlled replay."""

    def ping(self) -> bool:
        """Return whether the storage boundary is reachable."""


def sha256_bytes(content: bytes) -> str:
    """Return a lowercase SHA-256 digest for stable ingestion identity."""
    return hashlib.sha256(content).hexdigest()


def schema_fingerprint(columns: list[str]) -> str:
    """Return an order-sensitive fingerprint so source column movement is visible."""
    return sha256_bytes("\x1f".join(columns).encode("utf-8"))


def _safe_segment(value: str) -> str:
    allowed = {"-", "_", "."}
    segment = "".join(
        character for character in value if character.isalnum() or character in allowed
    )
    if not segment or segment in {".", ".."}:
        raise ObjectStoreError(f"Unsafe or empty object-key segment: {value!r}")
    return segment


class S3ObjectStore:
    """S3-compatible implementation configured for the local MinIO service."""

    def __init__(self, settings: Settings, client: S3Client | None = None) -> None:
        self._bucket = settings.s3_bucket
        self._client = client or boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
            region_name=settings.s3_region,
        )

    def ensure_bucket(self) -> None:
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in {403, 404}:
                raise ObjectStoreError("Unable to inspect the raw landing bucket") from error
            try:
                self._client.create_bucket(Bucket=self._bucket)
            except (BotoCoreError, ClientError) as create_error:
                raise ObjectStoreError("Unable to create the raw landing bucket") from create_error
        except BotoCoreError as error:
            raise ObjectStoreError("Unable to reach the raw landing service") from error

    def put_bytes(
        self,
        *,
        content: bytes,
        source_name: str,
        batch_id: str,
        filename: str,
    ) -> LandedObject:
        checksum = sha256_bytes(content)
        object_key = "/".join(
            (
                "incoming",
                _safe_segment(source_name),
                _safe_segment(batch_id),
                f"{checksum}-{_safe_segment(filename)}",
            )
        )
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=object_key,
                Body=content,
                ContentType="text/csv",
                Metadata={"sha256": checksum, "source": source_name, "batch-id": batch_id},
            )
        except (BotoCoreError, ClientError) as error:
            raise ObjectStoreError(f"Unable to land source object for {source_name}") from error
        return LandedObject(self._bucket, object_key, checksum, len(content))

    def get_bytes(self, object_key: str) -> bytes:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=object_key)
            body = response["Body"].read()
        except (BotoCoreError, ClientError, KeyError) as error:
            raise ObjectStoreError(f"Unable to read raw object {object_key!r}") from error
        if not isinstance(body, bytes):
            raise ObjectStoreError(f"Raw object {object_key!r} returned non-byte content")
        return body

    def ping(self) -> bool:
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except (BotoCoreError, ClientError):
            return False
        return True


class FilesystemObjectStore:
    """Repository-scoped adapter for unit tests and explicit offline development."""

    def __init__(self, root: Path, bucket: str = "forgeflow-raw") -> None:
        self._bucket = bucket
        self._root = root.resolve()

    def ensure_bucket(self) -> None:
        (self._root / self._bucket).mkdir(parents=True, exist_ok=True)

    def put_bytes(
        self,
        *,
        content: bytes,
        source_name: str,
        batch_id: str,
        filename: str,
    ) -> LandedObject:
        self.ensure_bucket()
        checksum = sha256_bytes(content)
        object_key = "/".join(
            (
                "incoming",
                _safe_segment(source_name),
                _safe_segment(batch_id),
                f"{checksum}-{_safe_segment(filename)}",
            )
        )
        bucket_root = (self._root / self._bucket).resolve()
        destination = (bucket_root / Path(object_key)).resolve()
        if bucket_root not in destination.parents:
            raise ObjectStoreError("Resolved object path escaped the configured storage root")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        return LandedObject(self._bucket, object_key, checksum, len(content))

    def get_bytes(self, object_key: str) -> bytes:
        bucket_root = (self._root / self._bucket).resolve()
        path = (bucket_root / Path(object_key)).resolve()
        if bucket_root not in path.parents:
            raise ObjectStoreError("Resolved object path escaped the configured storage root")
        try:
            return path.read_bytes()
        except OSError as error:
            raise ObjectStoreError(f"Unable to read raw object {object_key!r}") from error

    def ping(self) -> bool:
        return (self._root / self._bucket).is_dir()
