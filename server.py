#!/usr/bin/env python3
"""Run Wirewatch on loopback with Python's standard library."""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
import argparse
import json
import re
import threading
import time
import traceback
import uuid
from collections import deque
import mimetypes

from diagnostics import text
from engine import Engine
from model import ScanWindow, discovery, fetch_feed, required_text


REPORT_LOCK = threading.Lock()
REPORT_TIMES: deque[float] = deque()
REQUEST_ID = re.compile(r"[0-9a-f]{16}")


def client_error(data: dict[str, object]) -> dict[str, object]:
    fields = {"kind", "message", "request_id", "path", "status", "issue_path", "stack", "count"}
    if set(data) != fields:
        raise ValueError("Client error requires exactly kind, message, request_id, path, status, issue_path, stack, count")
    if data["kind"] not in ("api", "schema", "runtime", "rejection", "render"):
        raise ValueError("Invalid client error kind")
    if not isinstance(data["message"], str) or not data["message"].strip():
        raise ValueError("Client error message must be nonempty text")
    request_id = data["request_id"]
    if request_id is not None and (not isinstance(request_id, str) or not REQUEST_ID.fullmatch(request_id)):
        raise ValueError("Invalid client error request_id")
    path = data["path"]
    if path is not None and (not isinstance(path, str) or not path.startswith("/") or len(path) > 256 or "?" in path or "#" in path or any(ord(char) < 32 for char in path)):
        raise ValueError("Client error path must be an application route without query or fragment, at most 256 characters")
    status = data["status"]
    if status is not None and (type(status) is not int or not 100 <= status <= 599):
        raise ValueError("Client error status must be an integer from 100 to 599 or null")
    if type(data["count"]) is not int or not 1 <= data["count"] <= 1000:
        raise ValueError("Client error count must be an integer from 1 to 1000")
    for key in ("issue_path", "stack"):
        if data[key] is not None and not isinstance(data[key], str):
            raise ValueError(f"Client error {key} must be text or null")
    return {**data, "message": text(data["message"], 2000),
            "issue_path": text(data["issue_path"], 300) if data["issue_path"] is not None else None,
            "stack": text(data["stack"], 4000) if data["stack"] is not None else None}


def accept_report() -> bool:
    with REPORT_LOCK:
        current = time.monotonic()
        while REPORT_TIMES and REPORT_TIMES[0] <= current - 60:
            REPORT_TIMES.popleft()
        if len(REPORT_TIMES) >= 30:
            return False
        REPORT_TIMES.append(current)
        return True


STATIC = Path(__file__).resolve().parent / "static"


def handler_for(engine: Engine, port: int) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def handle_one_request(self) -> None:
            self.request_id = uuid.uuid4().hex[:16]
            self.started = time.monotonic()
            super().handle_one_request()

        def parse_request(self) -> bool:
            accepted = super().parse_request()
            if accepted:
                supplied = self.headers.get("X-Wirewatch-Request", "")
                if REQUEST_ID.fullmatch(supplied):
                    self.request_id = supplied
                engine.events.emit("http_started", "DEBUG", request_id=self.request_id,
                                   method=self.command, path=self.route())
            return accepted

        def route(self) -> str:
            return getattr(self, "path", "/").partition("?")[0].partition("#")[0][:256]

        def send_error(self, code: int, message: str | None = None, explain: str | None = None) -> None:
            self.send(code, {"error": message or self.responses.get(code, ("Request failed",))[0]})

        def send(self, status: int, payload: object, content_type: str = "application/json; charset=utf-8", download: bool = False) -> None:
            body = payload if isinstance(payload, bytes) else json.dumps(payload, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Wirewatch-Request", self.request_id)
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
            if download:
                self.send_header("Content-Disposition", 'attachment; filename="wirewatch-records.json"')
            try:
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, OSError) as exc:
                engine.events.emit("http_disconnected", "WARNING", request_id=self.request_id, path=self.route(),
                                   status=status, error=str(exc))
            quiet = status < 400 and self.route() in ("/api/state", "/api/health")
            engine.events.emit("http_completed", "DEBUG" if quiet else "WARNING" if status >= 400 else "INFO",
                               request_id=self.request_id, method=getattr(self, "command", None), path=self.route(), status=status,
                               duration_ms=round((time.monotonic() - self.started) * 1000),
                               job_id=payload.get("job_id") if isinstance(payload, dict) else None)

        def local_request(self, mutation: bool = False) -> bool:
            hosts = {f"127.0.0.1:{port}", f"localhost:{port}"}
            if port == 80:
                hosts.update({"127.0.0.1", "localhost"})
            host = self.headers.get("Host", "")
            origin = self.headers.get("Origin")
            if host not in hosts or (origin is not None and origin not in {f"http://{h}" for h in hosts}):
                self.send(403, {"error": "Only same-origin loopback requests are accepted"})
                return False
            if mutation and (self.headers.get_content_type() != "application/json" or self.headers.get("X-Wirewatch") != "1"):
                self.send(403, {"error": "Mutations require JSON and X-Wirewatch: 1"})
                return False
            return True

        def do_GET(self) -> None:
            if not self.local_request():
                return
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            try:
                if parsed.path == "/api/state":
                    self.send(200, engine.state())
                elif parsed.path == "/api/scans/review":
                    try:
                        scan_id = int(query.get("scan_id", [""])[0])
                    except ValueError as exc:
                        raise ValueError("Expected a positive integer scan_id") from exc
                    self.send(200, {"setup_review": engine.setup_history(scan_id)})
                elif parsed.path == "/api/health":
                    self.send(200, {"ok": True})
                elif parsed.path == "/api/export":
                    records = engine.state()["records"]
                    scan = query.get("scan", [""])[0]
                    kept = query.get("decision", ["all"])[0]
                    search = query.get("q", [""])[0].casefold()
                    filtered = [r for r in records if (not scan or str(r["scan_id"]) == scan)
                        and (kept == "all" or r["kept"] == (kept == "kept"))
                        and (not search or search in f"{r['title']} {r['summary']} {r['source']}".casefold())]
                    self.send(200, filtered, download=True)
                elif parsed.path == "/api/discover":
                    topic = required_text(query.get("topic", [""])[0], "Topic", 300)
                    window = ScanWindow.parse({key: query[key][0] for key in ("mode", "start_date", "end_date") if key in query})
                    feeds = discovery(topic, window)
                    try:
                        headlines = [{"title": a.title, "url": a.url, "source": a.source, "published_at": a.published_at} for a in fetch_feed(feeds[0]["url"])]
                        self.send(200, {"feeds": feeds, "headlines": headlines, "error": None, **window.to_dict()})
                    except Exception as exc:
                        engine.events.emit("discovery_failed", "WARNING", request_id=self.request_id, error=str(exc))
                        self.send(200, {"feeds": feeds, "headlines": [], "error": str(exc), **window.to_dict()})
                else:
                    name = {"/": "index.html", "/app.js": "app.js", "/style.css": "style.css"}.get(parsed.path)
                    if parsed.path.startswith("/assets/"):
                        name = unquote(parsed.path).lstrip("/")
                    target = (STATIC / name).resolve() if name else None
                    if target is None or not target.is_relative_to(STATIC.resolve()) or not target.is_file():
                        self.send(404, {"error": "Not found"})
                    else:
                        mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
                        self.send(200, target.read_bytes(), mime)
            except ValueError as exc:
                engine.events.emit("http_validation_failed", "WARNING", request_id=self.request_id, path=self.route(), error=str(exc))
                self.send(400, {"error": str(exc)})
            except Exception as exc:
                engine.events.emit("http_failed", "ERROR", request_id=self.request_id, path=self.route(), error_type=type(exc).__name__, error=str(exc), stack=traceback.format_exc(limit=8))
                self.send(500, {"error": "Request failed. See the local terminal for details"})

        def do_POST(self) -> None:
            if not self.local_request(mutation=True):
                return
            try:
                path = urlsplit(self.path).path
                length = int(self.headers.get("Content-Length", "0"))
                if path == "/api/client-errors" and length > 8192:
                    self.send(413, {"error": "Client error report exceeds 8192 bytes"})
                    return
                if not 1 <= length <= (8192 if path == "/api/client-errors" else 1_000_000):
                    raise ValueError("JSON body must contain 1 to 1000000 bytes")
                data = json.loads(self.rfile.read(length))
                if not isinstance(data, dict):
                    raise ValueError("Expected a JSON object")
                path = urlsplit(self.path).path
                if path == "/api/client-errors":
                    report = client_error(data)
                    if not accept_report():
                        self.send(429, {"error": "Client error reporting limit reached"})
                        return
                    engine.events.emit("client_error", "WARNING", reporting_request_id=self.request_id, **report)
                    result = {"ok": True}
                elif path == "/api/scans":
                    result = {"scan_id": engine.create_scan(data)}
                elif path == "/api/scans/update":
                    engine.update_scan(data)
                    result = {"ok": True}
                elif path == "/api/scans/review":
                    engine.control_setup_review(data)
                    result = {"ok": True}
                elif path == "/api/scans/delete":
                    engine.delete_scan(data)
                    result = {"ok": True}
                elif path == "/api/settings":
                    engine.save_settings(data)
                    result = {"ok": True}
                elif path in ("/api/poll", "/api/plan", "/api/audit", "/api/decision-audit", "/api/replay"):
                    kind = {"/api/poll": "poll", "/api/plan": "setup", "/api/audit": "audit", "/api/decision-audit": "decision_audit", "/api/replay": "replay"}[path]
                    result = {"job_id": engine.enqueue(kind, data)}
                else:
                    self.send(404, {"error": "Unknown endpoint"})
                    return
                self.send(200, result)
            except ValueError as exc:
                engine.events.emit("http_validation_failed", "WARNING", request_id=self.request_id, path=self.route(), error=str(exc))
                self.send(400, {"error": str(exc)})
            except Exception as exc:
                engine.events.emit("http_failed", "ERROR", request_id=self.request_id, path=self.route(), error_type=type(exc).__name__, error=str(exc), stack=traceback.format_exc(limit=8))
                self.send(500, {"error": "Request failed. See the local terminal for details"})

        def log_message(self, format: str, *args: object) -> None:
            pass

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description="Wirewatch local news scan console")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="127.0.0.1", choices=("127.0.0.1", "0.0.0.0"))
    parser.add_argument("--db", type=Path, default=Path(__file__).resolve().parent / "data" / "wirewatch.sqlite3")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    engine = Engine(args.db)
    server = ThreadingHTTPServer((args.host, args.port), handler_for(engine, args.port))
    engine.events.emit("http_listening", host=args.host, port=args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        engine.events.emit("app_stopping")
        engine.stop_event.set()
        server.server_close()
        engine.events.emit("app_stopped")


if __name__ == "__main__":
    main()
