"""Object storage abstraction: real S3 (or moto) and a local filesystem store.

The pipeline never calls boto3 directly for object I/O - it goes through :class:`Storage`. That is
what makes the whole thing runnable with zero AWS cost:

* ``S3Storage``  - ``s3://`` URIs, real pre-signed URLs, conditional writes (``IfNoneMatch``)
* ``LocalStorage`` - ``file://`` URIs / plain directories, atomic exclusive create, no pre-signing

Every method takes a **store-relative key** (the strings produced by
:mod:`agent_reports.common.keys`); each store maps that onto its own address space, so callers never
concatenate buckets or filesystem roots.
"""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from botocore.exceptions import ClientError, ParamValidationError

from .errors import ConfigError, PermanentError
from .settings import Settings

__all__ = [
    "LocalStorage",
    "S3Storage",
    "Storage",
    "Zones",
    "open_store",
    "open_zones",
    "parse_uri",
]


class Storage(Protocol):
    """Minimal object-store surface used by this pipeline."""

    def key_at(self, relative_key: str) -> str: ...

    def uri_for(self, relative_key: str) -> str: ...

    def put_bytes(
        self,
        relative_key: str,
        data: bytes,
        *,
        content_type: str = "text/csv",
        if_none_match: bool = False,
    ) -> str: ...

    def get_bytes(self, relative_key: str) -> bytes: ...

    def iter_lines(self, relative_key: str) -> Iterator[str]: ...

    def put_json(self, relative_key: str, payload: Any) -> str: ...

    def get_json(self, relative_key: str) -> Any: ...

    def exists(self, relative_key: str) -> bool: ...

    def size(self, relative_key: str) -> int: ...

    def last_modified(self, relative_key: str) -> datetime | None: ...

    def list_keys(self, prefix: str = "") -> list[str]: ...

    def delete(self, relative_key: str) -> None: ...

    def presign_get(
        self, relative_key: str, *, expires_in: int, filename: str | None = None
    ) -> str: ...


# --------------------------------------------------------------------------- helpers
def parse_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/prefix`` / ``file:///path`` / plain path into ``(scheme, target)``."""
    if isinstance(uri, Path):
        return "file", str(uri)
    if not isinstance(uri, str) or not uri:
        raise ConfigError("storage URI must be a non-empty string", context={"uri": repr(uri)})
    if uri.startswith("s3://"):
        parsed = urlparse(uri)
        if not parsed.netloc:
            raise ConfigError("s3 URI is missing a bucket", context={"uri": uri})
        return "s3", f"{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
    if uri.startswith("file://"):
        raw = urlparse(uri).path
        # file:///C:/x on Windows and file:///home/x on POSIX both come back with a leading slash.
        if len(raw) > 2 and raw[0] == "/" and raw[2] == ":":
            raw = raw[1:]
        return "file", raw
    if "://" in uri:
        raise ConfigError("unsupported storage scheme", context={"uri": uri})
    return "file", uri


def open_store(uri: str, settings: Settings, client: Any = None) -> Storage:
    """Build the right :class:`Storage` for a URI.

    ``s3://bucket/prefix``  -> :class:`S3Storage` rooted at ``prefix``
    ``file:///path`` / path -> :class:`LocalStorage` rooted at ``path``
    """
    scheme, target = parse_uri(uri)
    if scheme == "s3":
        bucket, _, prefix = target.partition("/")
        if client is None:
            from .aws import s3_client

            client = s3_client(settings)
        return S3Storage(client, bucket=bucket, prefix=prefix)
    return LocalStorage(Path(target))


@dataclass(frozen=True)
class Zones:
    """The three storage zones the pipeline reads and writes."""

    raw: Storage
    processed: Storage
    reports: Storage


def open_zones(settings: Settings, client: Any = None) -> Zones:
    """Open raw/processed/reports stores for the configured environment (S3 or local mode)."""
    uris = settings.zone_uris()
    return Zones(
        raw=open_store(uris["raw"], settings, client),
        processed=open_store(uris["processed"], settings, client),
        reports=open_store(uris["reports"], settings, client),
    )


# --------------------------------------------------------------------------- S3
@dataclass
class S3Storage:
    """Real S3 (or moto) storage."""

    client: Any
    bucket: str
    prefix: str = ""

    def key_at(self, relative_key: str) -> str:
        cleaned = relative_key.lstrip("/")
        return f"{self.prefix.strip('/')}/{cleaned}" if self.prefix else cleaned

    def uri_for(self, relative_key: str) -> str:
        return f"s3://{self.bucket}/{self.key_at(relative_key)}"

    def put_bytes(
        self,
        relative_key: str,
        data: bytes,
        *,
        content_type: str = "text/csv",
        if_none_match: bool = False,
    ) -> str:
        request: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": self.key_at(relative_key),
            "Body": data,
            "ContentType": content_type,
        }
        if if_none_match:
            request["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**request)
        except ParamValidationError as exc:  # pragma: no cover - very old botocore only
            raise PermanentError(
                "this botocore build does not support conditional S3 writes",
                context={"error": str(exc)},
            ) from exc
        return self.uri_for(relative_key)

    def get_bytes(self, relative_key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self.key_at(relative_key))
        body: bytes = response["Body"].read()
        return body

    def iter_lines(self, relative_key: str) -> Iterator[str]:
        """Stream an object line by line (the S3 body is never fully materialised)."""
        response = self.client.get_object(Bucket=self.bucket, Key=self.key_at(relative_key))
        for raw in response["Body"].iter_lines():
            yield raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    def put_json(self, relative_key: str, payload: Any) -> str:
        return self.put_bytes(
            relative_key,
            json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8"),
            content_type="application/json",
        )

    def get_json(self, relative_key: str) -> Any:
        return json.loads(self.get_bytes(relative_key).decode("utf-8"))

    def exists(self, relative_key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self.key_at(relative_key))
        except ClientError as exc:
            if _error_code(exc) in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
        return True

    def size(self, relative_key: str) -> int:
        head = self.client.head_object(Bucket=self.bucket, Key=self.key_at(relative_key))
        return int(head["ContentLength"])

    def last_modified(self, relative_key: str) -> datetime | None:
        head = self.client.head_object(Bucket=self.bucket, Key=self.key_at(relative_key))
        stamp = head.get("LastModified")
        return stamp if isinstance(stamp, datetime) else None

    def list_keys(self, prefix: str = "") -> list[str]:
        start = self.key_at(prefix) if prefix else self.prefix
        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=start):
            for item in page.get("Contents", []):
                key = str(item["Key"])
                keys.append(key[len(self.prefix) :].lstrip("/") if self.prefix else key)
        return sorted(keys)

    def delete(self, relative_key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self.key_at(relative_key))

    def presign_get(
        self, relative_key: str, *, expires_in: int, filename: str | None = None
    ) -> str:
        params: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": self.key_at(relative_key),
        }
        if filename:
            params["ResponseContentDisposition"] = f'attachment; filename="{filename}"'
        url: str = self.client.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=expires_in
        )
        return url


# --------------------------------------------------------------------------- local FS
@dataclass
class LocalStorage:
    """Filesystem store used for local development, the demo path and the Spark job's output.

    Pre-signing is an S3 capability, so :meth:`presign_get` raises instead of pretending: the
    dispatcher, which always talks to S3 (moto offline, real AWS in production), is the only caller.
    """

    root: Path
    presign_base_url: str | None = None

    def __post_init__(self) -> None:
        self.root = Path(self.root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def key_at(self, relative_key: str) -> str:
        return str(self.path_for(relative_key))

    def path_for(self, relative_key: str) -> Path:
        candidate = (self.root / relative_key.lstrip("/")).resolve()
        if candidate != self.root and self.root not in candidate.parents:
            raise PermanentError(
                "refusing to write outside the storage root",
                context={"key": relative_key, "root": str(self.root)},
            )
        return candidate

    def uri_for(self, relative_key: str) -> str:
        return self.path_for(relative_key).as_uri()

    def put_bytes(
        self,
        relative_key: str,
        data: bytes,
        *,
        content_type: str = "text/csv",
        if_none_match: bool = False,
    ) -> str:
        path = self.path_for(relative_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        if if_none_match:
            self._create_if_absent(path, data)
        else:
            path.write_bytes(data)
        return path.as_uri()

    def _create_if_absent(self, path: Path, data: bytes) -> None:
        """Atomically create *path* with the full content, or raise ``FileExistsError``.

        ``O_EXCL`` alone is not enough: it publishes the name before the bytes are written, so a
        racing reader can observe an empty file (which is exactly the race the dispatch ledger is
        trying to make impossible). So the data is written to a private temp file first and then
        hard-linked into place - ``link`` is atomic and fails if the target already exists.
        """
        if path.exists():
            raise FileExistsError(str(path))
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        temporary.write_bytes(data)
        try:
            os.link(temporary, path)
        except (AttributeError, NotImplementedError, OSError) as exc:  # pragma: no cover
            if isinstance(exc, FileExistsError):
                raise
            # Filesystem without hard links (rare, e.g. some network shares): fall back to O_EXCL,
            # which keeps the create-if-absent guarantee even though the write is not atomic.
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
        finally:
            temporary.unlink(missing_ok=True)

    def get_bytes(self, relative_key: str) -> bytes:
        path = self.path_for(relative_key)
        if not path.exists():
            raise FileNotFoundError(str(path))
        return path.read_bytes()

    def iter_lines(self, relative_key: str) -> Iterator[str]:
        path = self.path_for(relative_key)
        if not path.exists():
            raise FileNotFoundError(str(path))
        with path.open("r", encoding="utf-8", newline="") as handle:
            for line in handle:
                yield line.rstrip("\r\n")

    def put_json(self, relative_key: str, payload: Any) -> str:
        return self.put_bytes(
            relative_key,
            json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8"),
            content_type="application/json",
        )

    def get_json(self, relative_key: str) -> Any:
        return json.loads(self.get_bytes(relative_key).decode("utf-8"))

    def exists(self, relative_key: str) -> bool:
        return self.path_for(relative_key).exists()

    def size(self, relative_key: str) -> int:
        return self.path_for(relative_key).stat().st_size

    def last_modified(self, relative_key: str) -> datetime | None:
        path = self.path_for(relative_key)
        if not path.exists():
            return None
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)

    def list_keys(self, prefix: str = "") -> list[str]:
        base = self.path_for(prefix) if prefix else self.root
        if base.is_file():
            return [str(base.relative_to(self.root)).replace(os.sep, "/")]
        if not base.exists():
            return []
        keys = []
        for path in sorted(base.rglob("*")):
            if path.is_file():
                keys.append(str(path.relative_to(self.root)).replace(os.sep, "/"))
        return keys

    def delete(self, relative_key: str) -> None:
        self.path_for(relative_key).unlink(missing_ok=True)

    def presign_get(
        self, relative_key: str, *, expires_in: int, filename: str | None = None
    ) -> str:
        raise ConfigError(
            "pre-signed URLs are an S3 feature; run the dispatcher against S3 (moto offline, "
            "real AWS in production) instead of the local filesystem store",
            context={"key": relative_key},
        )


def _error_code(exc: ClientError) -> str:
    response = exc.response if isinstance(exc.response, dict) else {}
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    code = error.get("Code") if isinstance(error, dict) else None
    if code:
        return str(code)
    meta = response.get("ResponseMetadata", {}) if isinstance(response, dict) else {}
    status = meta.get("HTTPStatusCode") if isinstance(meta, dict) else None
    return str(status) if status is not None else "Unknown"
