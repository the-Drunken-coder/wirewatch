from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import unquote_plus, urlsplit, urlunsplit
import json
import os
import re
import stat
import sys
import threading
import time
import traceback
import uuid

EVENT_BYTES = 16_384
LOG_BYTES = 5_000_000
LOG_BACKUPS = 4
TRACE_BYTES = 4_000_000
TRACE_COUNT = 50
TRACE_TOTAL_BYTES = 128_000_000
STREAM_BYTES = 65_536
RAW_BYTES = 262_144
_EVENT_LOCK = threading.RLock()
_TRACE_LOCK = threading.RLock()
_ACTIVE: set[Path] = set()
_SECRET = re.compile(
    r"authorization|cookie|password|passwd|secret|credential|^key$|"
    r"(?:^|[_-])(?:token|(?:api|access|private|signing|encryption)[_-]?key|auth|signature|sig|session[_-]?id)(?:$|[_-])|"
    r"^(?:access|refresh|id)Token$", re.I)
_URL = re.compile(r"https?://[^\s<>\"']+", re.I)
_BEARER = re.compile(r"\bBearer\s+[^\s,;\"']+", re.I)
_QUOTED = (r'\\"(?:\\\\(?:\\.|[^"\\])|[^"\\\r\n])*\\"'
           r'|"(?:\\.|[^"\\\r\n])*"'
           r"|'(?:\\.|[^'\\\r\n])*'")
_ASSIGNMENT = re.compile(
    rf'''(?<![\w.-])(?:(?P<prefix>(?P<name>{_QUOTED}|[\w.-]+)\s*[:=]\s*)'''
    rf'''(?P<value>{_QUOTED}|[^\s,;{{}}\[\]&]+)|(?P<quoted>{_QUOTED}))''')
_HEADER = re.compile(r"(?<![\"'])\b(authorization|(?:set-)?cookie)\s*[:=]\s*[^\r\n]*", re.I)
_CREDENTIAL = re.compile(r"\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,}|github_pat_[A-Za-z0-9_]+|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)\b")


def _sensitive_name(name: str) -> bool:
    return _SECRET.search(name) is not None


def _unquote(value: str) -> str:
    if value.startswith(r'\"'):
        value = json.loads('"' + value + '"')
    if value[0] == "'":
        value = '"' + re.sub(r'\\.|"', lambda match: {"\\'": "'", '"': '\\"'}.get(match[0], match[0]), value[1:-1]) + '"'
    return json.loads(value)


def _redact_assignment(match: re.Match[str]) -> str:
    name = match["name"]
    prefix = match["prefix"] or ""
    value = match["value"] or match["quoted"]
    quote = value[0] if len(value) >= 2 and value[0] in ('"', "'") and value[-1] == value[0] else ""
    if value.startswith(r'\"') and value.endswith(r'\"'):
        quote = r'\"'
    if name is not None:
        if name.startswith(('"', "'", r'\"')):
            try:
                name = _unquote(name)
            except json.JSONDecodeError:
                pass
        if _sensitive_name(name):
            return prefix + quote + "[REDACTED]" + quote
    decoded = value
    if quote:
        try:
            decoded = _unquote(value)
        except json.JSONDecodeError:
            return match[0]
    clean = str(redact(decoded))
    if clean == decoded:
        return match[0]
    if quote:
        clean = json.dumps(clean, ensure_ascii=False)
        if quote == r'\"':
            clean = json.dumps(clean, ensure_ascii=False)[1:-1]
    return prefix + clean


def _url(match: re.Match[str]) -> str:
    try:
        parts = urlsplit(match.group())
        host = parts.netloc.rsplit("@", 1)[-1]
        query = "&".join(part.partition("=")[0] + "=%5BREDACTED%5D" if _sensitive_name(unquote_plus(part.partition("=")[0])) else part
                         for part in parts.query.split("&"))
        return urlunsplit((parts.scheme, host, parts.path, query, parts.fragment))
    except ValueError:
        return "[REDACTED URL]"


def redact(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): "[REDACTED]" if _sensitive_name(str(key)) else redact(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        value = _URL.sub(_url, value)
        value = _BEARER.sub("Bearer [REDACTED]", value)
        value = _HEADER.sub(lambda match: match[1] + "=[REDACTED]", value)
        value = _ASSIGNMENT.sub(_redact_assignment, value)
        return _CREDENTIAL.sub("[REDACTED]", value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return redact(str(value))


def text(value: str, limit: int) -> str:
    return "".join(char for char in str(redact(value)) if ord(char) >= 32 or char in "\n\t")[:limit]


def encoded(value: object) -> bytes:
    return json.dumps(redact(value), ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8", errors="backslashreplace")


def _record(event: str, level: str, fields: dict[str, object]) -> bytes:
    record = {"at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"), "level": level,
              "event": event, "thread": threading.current_thread().name, **fields}
    data = encoded(record)
    if len(data) + 1 > EVENT_BYTES:
        record = {key: text(value, 500) if isinstance(value, str) else value for key, value in record.items()
                  if value is None or isinstance(value, (str, int, float, bool))}
        record.update(truncated=True, original_bytes=len(data))
        data = encoded(record)
        if len(data) + 1 > EVENT_BYTES:
            data = encoded({"at": record["at"], "level": level, "event": event, "truncated": True,
                            "original_bytes": record["original_bytes"]})
    return data + b"\n"


def emergency(event: str, **fields: object) -> None:
    try:
        sys.stderr.write(_record(event, "ERROR", fields).decode("utf-8"))
        sys.stderr.flush()
    except Exception:
        pass


def _directory(path: Path) -> None:
    path.mkdir(mode=0o700, exist_ok=True)
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise OSError(f"Unsafe diagnostic directory: {path.name}")
    path.chmod(0o700)


def _safe_target(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if not stat.S_ISREG(mode):
        raise OSError(f"Unsafe diagnostic file: {path.name}")


def _open(path: Path, flags: int) -> int:
    fd = os.open(path, flags | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("Diagnostic target is not a regular file")
        os.fchmod(fd, 0o600)
    except BaseException:
        os.close(fd)
        raise
    return fd


class Events:
    def __init__(self, archive_parent: Path) -> None:
        self.root = archive_parent / "diagnostics"
        self.level = os.environ.get("WIREWATCH_LOG_LEVEL", "INFO").upper()

    def emit(self, event: str, level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO", **fields: object) -> None:
        if level == "DEBUG" and self.level != "DEBUG":
            return
        try:
            data = _record(event, level, fields)
            with _EVENT_LOCK:
                try:
                    sys.stderr.write(data.decode("utf-8"))
                    sys.stderr.flush()
                except Exception:
                    pass
                _directory(self.root)
                path = self.root / "events.jsonl"
                _safe_target(path)
                if path.exists() and path.stat().st_size + len(data) > LOG_BYTES:
                    targets = [path, *[self.root / f"events.jsonl.{i}" for i in range(1, LOG_BACKUPS + 1)]]
                    for target in targets:
                        _safe_target(target)
                    for i in range(LOG_BACKUPS, 0, -1):
                        source = targets[i - 1]
                        if source.exists():
                            source.chmod(0o600)
                            source.replace(targets[i])
                with os.fdopen(_open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND), "ab") as stream:
                    stream.write(data)
        except Exception as exc:
            emergency("diagnostics_write_failed", failed_event=event, error=text(str(exc), 1000),
                      **{key: value for key, value in fields.items() if key in ("job_id", "attempt_id", "request_id")})


class Rejected(ValueError):
    def __init__(self, message: str, *, path: str, expected: object = None, received: object = None,
                 **details: object) -> None:
        super().__init__(message)
        self.details = {"path": path, "expected": expected, "received": received, **details}


@dataclass(frozen=True)
class AttemptKey:
    job_id: str
    kind: str
    scan_id: int | None = None
    run_id: str | None = None
    round: int | None = None
    page: int | None = None
    audit_id: int | None = None


class ModelTraces:
    def __init__(self, archive_parent: Path, events: Events) -> None:
        self.root = archive_parent / "diagnostics" / "traces"
        self.events = events

    def open(self, key: AttemptKey, payload: dict[str, object], schema: dict[str, object], instructions: str) -> ModelAttempt:
        attempt = ModelAttempt(self, key)
        try:
            attempt.save(input=payload, schema=schema, instructions=instructions, stage="preflight")
        except Exception as exc:
            with _TRACE_LOCK:
                _ACTIVE.discard(attempt.path)
            self.events.emit("model_trace_preflight_failed", "ERROR", attempt_id=attempt.id, **asdict(key), error=str(exc))
            raise RuntimeError("Cannot retain required model input. No model call was made") from exc
        self.events.emit("model_attempt_opened", attempt_id=attempt.id, **asdict(key))
        return attempt

    def _prune(self) -> None:
        paths = [*self.root.glob("*.json"), *self.root.glob("*.tmp")]
        for path in paths:
            _safe_target(path)
        retained = sorted((path for path in paths if path.with_suffix(".json") not in _ACTIVE), key=lambda path: path.stat().st_mtime_ns)
        total = sum(path.stat().st_size for path in paths)
        while retained and (len(retained) > TRACE_COUNT or total > TRACE_TOTAL_BYTES):
            path = retained.pop(0)
            size = path.stat().st_size
            path.unlink()
            total -= size
            self.events.emit("model_trace_evicted", attempt_id=path.stem, bytes=size, temporary=path.suffix == ".tmp")
        if total > TRACE_TOTAL_BYTES:
            raise OSError("Active model traces exceed the diagnostic retention byte limit")


class ModelAttempt:
    def __init__(self, store: ModelTraces, key: AttemptKey) -> None:
        self.store = store
        self.id = uuid.uuid4().hex
        self.path = store.root / f"{self.id}.json"
        self.record: dict[str, object] = {"attempt_id": self.id, "correlation": asdict(key),
                                         "started_at": time.time(), "outcome": "pending"}
        self.committed = False
        self.started = time.monotonic()
        with _TRACE_LOCK:
            _ACTIVE.add(self.path)

    def __enter__(self) -> ModelAttempt:
        return self

    def save(self, **fields: object) -> None:
        record = {**self.record, **fields}
        data = encoded(record)
        if len(data) > TRACE_BYTES:
            raise ValueError(f"Complete model trace exceeds {TRACE_BYTES} bytes; evidence was not truncated")
        temporary = self.path.with_suffix(".tmp")
        try:
            with _TRACE_LOCK:
                _directory(self.store.root.parent)
                _directory(self.store.root)
                _safe_target(self.path)
                with os.fdopen(_open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                temporary.replace(self.path)
                self.record = record
                self.store._prune()
        except Exception as exc:
            emergency("model_trace_write_failed", attempt_id=self.id, **self.record["correlation"], error=str(exc))
            raise
        finally:
            try:
                with _TRACE_LOCK:
                    temporary.unlink(missing_ok=True)
            except OSError as exc:
                emergency("model_trace_cleanup_failed", attempt_id=self.id, error=str(exc))

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> bool:
        outcome = "applied" if self.committed else self.record.get("outcome", "pending")
        fields: dict[str, object] = {"finished_at": time.time(), "duration_ms": round((time.monotonic() - self.started) * 1000)}
        if exc is not None:
            if isinstance(exc, Rejected) and "stage" in exc.details:
                fields["stage"] = exc.details["stage"]
            outcome = "rejected" if isinstance(exc, ValueError) else "failed"
            fields["error"] = {"type": type(exc).__name__, "message": str(exc),
                               "stack": "".join(traceback.format_exception(exc, limit=12)),
                               "details": exc.details if isinstance(exc, Rejected) else None}
        elif outcome == "pending":
            outcome = "validated"
        confirmed = True
        try:
            self.save(outcome=outcome, **fields)
        except Exception:
            confirmed = False
            emergency("model_trace_outcome_unconfirmed", attempt_id=self.id, **self.record["correlation"],
                      application_committed=self.committed, intended_outcome=outcome,
                      original_error=text(str(exc), 1000) if exc else None)
            if exc is None and not self.committed:
                raise
        finally:
            with _TRACE_LOCK:
                _ACTIVE.discard(self.path)
                try:
                    self.store._prune()
                except Exception as failure:
                    emergency("model_trace_retention_failed", attempt_id=self.id, error=str(failure))
            self.store.events.emit("model_attempt_finished", "ERROR" if exc is not None else "INFO",
                                   attempt_id=self.id, **self.record["correlation"], outcome=outcome,
                                   application_committed=self.committed, diagnostic_confirmed=confirmed, duration_ms=fields["duration_ms"])
        return False
