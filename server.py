#!/usr/bin/env python3
"""Run Wirewatch on loopback with Python's standard library."""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
import argparse
import json
import logging
import mimetypes

from engine import Engine
from model import ScanWindow, discovery, fetch_feed, required_text


STATIC = Path(__file__).resolve().parent / "static"


def handler_for(engine: Engine, port: int) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def send(self, status: int, payload: object, content_type: str = "application/json; charset=utf-8", download: bool = False) -> None:
            body = payload if isinstance(payload, bytes) else json.dumps(payload, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
            if download:
                self.send_header("Content-Disposition", 'attachment; filename="wirewatch-records.json"')
            self.end_headers()
            self.wfile.write(body)

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
                self.send(400, {"error": str(exc)})
            except Exception:
                logging.exception("GET failed")
                self.send(500, {"error": "Request failed. See the local terminal for details"})

        def do_POST(self) -> None:
            if not self.local_request(mutation=True):
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= length <= 1_000_000:
                    raise ValueError("JSON body must contain 1 to 1000000 bytes")
                data = json.loads(self.rfile.read(length))
                if not isinstance(data, dict):
                    raise ValueError("Expected a JSON object")
                path = urlsplit(self.path).path
                if path == "/api/scans":
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
                self.send(400, {"error": str(exc)})
            except Exception:
                logging.exception("POST failed")
                self.send(500, {"error": "Request failed. See the local terminal for details"})

        def log_message(self, format: str, *args: object) -> None:
            logging.debug(format, *args)

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description="Wirewatch local news scan console")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="127.0.0.1", choices=("127.0.0.1", "0.0.0.0"))
    parser.add_argument("--db", type=Path, default=Path(__file__).resolve().parent / "data" / "wirewatch.sqlite3")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    engine = Engine(args.db)
    server = ThreadingHTTPServer((args.host, args.port), handler_for(engine, args.port))
    logging.info("Wirewatch http://127.0.0.1:%s | database %s", args.port, args.db)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        engine.stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()
