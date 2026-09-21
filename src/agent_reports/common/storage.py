"""Object storage abstraction: real S3 (or moto) and a local filesystem store.

The pipeline never calls boto3 directly for object I/O - it goes through :class:`Storage`. That is
what makes the whole thing runnable with zero AWS cost:

* ``S3Storage``  - ``s3://`` URIs, real pre-signed URLs, conditional writes (``IfNoneMatch`` /
  ``If-Match``) that are also exclusive *inside one process* (see :func:`_conditional_lock`)
* ``LocalStorage`` - ``file://`` URIs / plain directories, atomic exclusive create, no pre-signing

Every method takes a **store-relative key** (the strings produced by
:mod:`agent_reports.common.keys`); each store maps that onto its own address space, so callers never
concatenate buckets or filesystem roots.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import weakref
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from botocore.exceptions import ClientError, ParamValidationError

from .errors import ConfigError, DependencyError, PermanentError
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

    def get_bytes_with_version(self, relative_key: str) -> tuple[bytes, str]: ...

    def put_bytes_if_version(
        self,
        relative_key: str,
        data: bytes,
        *,
        version: str,
        content_type: str = "text/csv",
    ) -> bool: ...

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
    """Real S3 (or moto) storage.

    Conditional writes are guarded twice: ``If-None-Match``/``If-Match`` on the request (real S3
    evaluates those server-side, which is what separates two *processes*) and an in-process lock plus
    a version re-read (which is what separates two *threads* of one process - see
    :func:`_conditional_lock`).
    """

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
        if not if_none_match:
            self._put_object(request)
            return self.uri_for(relative_key)
        request["IfNoneMatch"] = "*"
        # Create-if-absent, made exclusive inside this process as well as on the wire. A backend whose
        # ``If-None-Match`` check is a compare *followed by* a write (moto does exactly that, with no
        # lock in between) would otherwise let two racing workers create the same marker - and two
        # markers created for one agent is two emails sent. ``DispatchLedger._create_only`` maps the
        # ``FileExistsError`` this raises onto "lost the race".
        with _conditional_lock(self.bucket, self.key_at(relative_key)):
            if self._current_version(relative_key) is not None:
                raise FileExistsError(self.uri_for(relative_key))
            self._put_object(request)
        return self.uri_for(relative_key)

    def _put_object(self, request: dict[str, Any]) -> None:
        try:
            self.client.put_object(**request)
        except ParamValidationError as exc:  # pragma: no cover - very old botocore only
            raise PermanentError(
                "this botocore build does not support conditional S3 writes",
                context={"error": str(exc)},
            ) from exc

    def _current_version(self, relative_key: str) -> str | None:
        """The ETag the object has *right now*, or ``None`` when it does not exist.

        The conditional writes call this inside the in-process lock, so a caller whose version went
        stale is rejected by this process rather than by the backend - see
        :meth:`put_bytes_if_version` for why that has to hold whatever the backend does.
        """
        try:
            head = self.client.head_object(Bucket=self.bucket, Key=self.key_at(relative_key))
        except ClientError as exc:
            if _error_code(exc) in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        version = str(head.get("ETag") or "")
        if not version:
            raise PermanentError(
                "S3 returned no ETag, so a conditional write is impossible",
                context={"key": relative_key},
            )
        return version

    def get_bytes(self, relative_key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self.key_at(relative_key))
        body: bytes = response["Body"].read()
        return body

    def get_bytes_with_version(self, relative_key: str) -> tuple[bytes, str]:
        """Read the object together with its ETag - the token a conditional write needs."""
        response = self.client.get_object(Bucket=self.bucket, Key=self.key_at(relative_key))
        body: bytes = response["Body"].read()
        version = str(response.get("ETag") or "")
        if not version:
            raise PermanentError(
                "S3 returned no ETag, so a conditional write is impossible",
                context={"key": relative_key},
            )
        return body, version

    def put_bytes_if_version(
        self,
        relative_key: str,
        data: bytes,
        *,
        version: str,
        content_type: str = "text/csv",
    ) -> bool:
        """Compare-and-set: write only if the object still has *version*. ``False`` when it moved.

        Two guards, because there are two races to close:

        * **Inside one process** the write is made exclusive by the per-key lock plus a re-read of the
          object's current version within it. This is the half that does not depend on the backend:
          moto evaluates ``If-Match`` as a compare *followed by* a write with no lock in between, so
          two racing threads can both pass the comparison and both win. That is what CI saw - two
          winners for one stale lease (run 35640872774) - while this machine happened to serialise the
          threads. Lambda never runs two of our workers in one process, but a threaded caller, and
          every offline test, does.
        * **Across processes** the ``If-Match`` header is the guard: real S3 evaluates it server-side,
          so two Lambda processes that read the same version cannot both write. That enforcement
          cannot be verified offline - see
          ``test_s3_put_object_requests_carry_the_conditional_headers`` for the half that can be
          (the header is really sent).
        """
        key = self.key_at(relative_key)
        with _conditional_lock(self.bucket, key):
            current = self._current_version(relative_key)
            if current is None or current != version:
                return False
            try:
                self._put_object(
                    {
                        "Bucket": self.bucket,
                        "Key": key,
                        "Body": data,
                        "ContentType": content_type,
                        "IfMatch": version,
                    }
                )
            except ClientError as exc:
                if _error_code(exc) in ("412", "PreconditionFailed", "ConditionalRequestConflict"):
                    return False
                raise
        return True

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

    def get_bytes_with_version(self, relative_key: str) -> tuple[bytes, str]:
        """Read the file together with a content-derived version token.

        S3's ETag is a hash of the object, so using a hash here keeps one semantics for both stores
        (the ledger's conditional writes do not care which store they are talking to).
        """
        payload = self.get_bytes(relative_key)
        return payload, _content_version(payload)

    def put_bytes_if_version(
        self,
        relative_key: str,
        data: bytes,
        *,
        version: str,
        content_type: str = "text/csv",
    ) -> bool:
        """Compare-and-set under an exclusive per-key lock.

        A read-compare-write is not atomic on a filesystem, so the lock is what makes two racing
        writers decide the same way the S3 ``If-Match`` decides: exactly one of them wins. The
        replacement itself goes through a temp file + ``os.replace`` so a concurrent *reader* never
        observes a half-written marker.
        """
        path = self.path_for(relative_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = path.with_name(f".{path.name}.lock")
        _acquire_lock(lock)
        try:
            if not path.exists():
                return False
            if _content_version(path.read_bytes()) != version:
                return False
            _atomic_replace(path, data)
            return True
        finally:
            lock.unlink(missing_ok=True)

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
            # A leading dot marks a transient file (the create-if-absent temp file, the
            # compare-and-set lock). Those are never data, so they are not listed as objects.
            if path.is_file() and not path.name.startswith("."):
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


# --------------------------------------------------------------------------- conditional-write helpers
#: In-process locks for the conditional writes, keyed by ``(bucket, object key)``.
#:
#: ``If-Match``/``If-None-Match`` are evaluated by the *server*, which is what separates two Lambda
#: processes - but it is not what separates two threads, and not every backend that speaks the S3 API
#: evaluates those headers atomically: moto compares the ETag and then writes, with no lock in
#: between, so two racing threads can both pass the comparison and both win (CI run 35640872774: two
#: winners for one stale lease). So every conditional write also takes the lock for its key and
#: re-reads the object's version inside it: exactly one writer per key gets through, whatever the
#: backend does.
#:
#: The registry holds weak references, so a lock lives only as long as somebody is using it and a
#: warm process cannot accumulate one lock per marker it has ever touched. Two threads always get the
#: *same* lock object for a key: whoever retrieves it holds a strong reference for the whole critical
#: section, so it cannot be collected while it is held. The critical section is one HEAD and one PUT
#: and never re-enters the storage layer, so a plain (non-reentrant) lock cannot deadlock.
_CONDITIONAL_LOCKS: weakref.WeakValueDictionary[tuple[str, str], threading.Lock] = (
    weakref.WeakValueDictionary()
)
_CONDITIONAL_LOCKS_GUARD = threading.Lock()


def _conditional_lock(bucket: str, key: str) -> threading.Lock:
    """The in-process lock that makes conditional writes to one object exclusive."""
    with _CONDITIONAL_LOCKS_GUARD:
        lock = _CONDITIONAL_LOCKS.get((bucket, key))
        if lock is None:
            lock = threading.Lock()
            _CONDITIONAL_LOCKS[(bucket, key)] = lock
        return lock


def _content_version(payload: bytes) -> str:
    """Version token for the filesystem store: a hash of the bytes, like an S3 ETag."""
    return hashlib.sha256(payload).hexdigest()


def _atomic_replace(path: Path, data: bytes) -> None:
    """Replace *path* with *data* atomically, so readers see old or new, never half."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    temporary.write_bytes(data)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _acquire_lock(lock: Path, *, timeout: float = 5.0, stale_after: float = 30.0) -> None:
    """Take an exclusive per-key lock, or raise a retryable error.

    ``O_CREAT|O_EXCL`` is atomic on every filesystem this runs on, so exactly one contender wins.
    A lock left behind by a crashed holder is stolen once it is older than *stale_after* - otherwise
    one dead process would wedge every later conditional write for that key.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            handle = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except OSError as exc:
            # Windows returns ERROR_ACCESS_DENIED (PermissionError) rather than ERROR_FILE_EXISTS when
            # the lock file is being created and removed concurrently, so "somebody else holds it" has
            # two possible shapes. Anything else is a real I/O problem and must not be swallowed.
            if not isinstance(exc, (FileExistsError, PermissionError)):
                raise
            try:
                age = time.time() - lock.stat().st_mtime
            except OSError:  # the holder released it between the two calls
                continue
            if age > stale_after:
                lock.unlink(missing_ok=True)
                continue
            if time.monotonic() > deadline:
                raise DependencyError(
                    "timed out waiting for the local storage lock",
                    context={"lock": str(lock)},
                ) from exc
            time.sleep(0.001)
            continue
        os.close(handle)
        return
