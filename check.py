"""Behavior checks using local HTTP feeds, temporary SQLite, and a stub model boundary."""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen
import json
import logging
import sqlite3
import subprocess
import sys
import time

import model
import server
import setup_review as review
from engine import Engine, SCHEMA
from model import Rules, ScanWindow, Validators, discovery, evaluate, fetch_conditional, parse_feed, publication_time, query_url


RULES = {"any_terms": [], "all_terms": [], "exclude_terms": []}
WINDOW = {"mode": "historical", "start_date": "2026-01-01", "end_date": "2026-01-02"}
RSS = b'''<rss version="2.0"><channel><title>Fixture source</title>
<item><title>New chip factory</title><link>https://example.com/chip#fragment</link><description>&lt;p&gt;Capacity expands&lt;/p&gt;</description><guid>a</guid><pubDate>2026-01-01T00:00:00Z</pubDate></item>
<item><title>Football result</title><link>https://example.com/sport</link><description>League final</description><pubDate>2026-01-02T23:59:59Z</pubDate></item>
<item><title>Unsafe</title><link>javascript:alert(1)</link></item></channel></rss>'''
ATOM = b'''<feed xmlns="http://www.w3.org/2005/Atom"><title>Atom source</title><entry><id>urn:item:1</id><title>Battery research</title><link href="https://example.com/battery"/><summary type="html">&lt;b&gt;New&lt;/b&gt; cell</summary><updated>2026-01-02</updated></entry></feed>'''


@dataclass
class Response:
    body: bytes = RSS
    status: int = 200
    etag: str | None = '"fixture-1"'
    modified: str | None = "Thu, 01 Jan 2026 00:00:00 GMT"
    retry: str | None = None
    entered: Event | None = None
    release: Event | None = None


@contextmanager
def feeds():
    routes = {"/rss": Response(), "/atom": Response(ATOM, etag='"atom-1"')}
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            route = routes[self.path]
            requests.append((self.path, dict(self.headers)))
            if route.entered:
                route.entered.set()
            if route.release:
                assert route.release.wait(8), "Feed fixture was never released"
            status = route.status
            if status == 200 and route.etag and self.headers.get("If-None-Match") == route.etag:
                status = 304
            self.send_response(status)
            for name, value in (("ETag", route.etag), ("Last-Modified", route.modified), ("Retry-After", route.retry)):
                if value is not None:
                    self.send_header(name, value)
            body = route.body if status == 200 else b""
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}", routes, requests
    finally:
        for route in routes.values():
            if route.release:
                route.release.set()
        httpd.shutdown()
        httpd.server_close()
        thread.join(3)


def rejected(action, message="Invalid operation accepted", error=ValueError):
    try:
        action()
    except error:
        return
    raise AssertionError(message)


def wait_for(condition, message, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.02)
    raise AssertionError(message)


def create(engine, topic, url, **options):
    return engine.create_scan({"topic": topic, "objective": "Find relevant news", "rules": RULES,
        "feed_urls": [url], **options})


def drain(engine):
    for _ in range(100):
        if not engine.run_next("collector"):
            return
    raise AssertionError("Deterministic jobs did not drain")


def force_due(engine, url):
    with engine.transaction() as db:
        db.execute("UPDATE feed_state SET next_check_at=0 WHERE url=?", (url,))


def stop(engine):
    engine.stop_event.set()
    for worker in (engine.worker, engine.model_worker):
        if worker.is_alive():
            worker.join(6)
            assert not worker.is_alive(), "Engine worker did not stop"


def setup_plan(topic, url):
    return {"rationale": "Fixture setup", "actions": [
        {"tool": "create_scan", "scan_ref": "new", "topic": topic, "objective": "Model objective", "rules": RULES, "rationale": "Create fixture"},
        {"tool": "subscribe_feed", "scan_ref": "new", "feed_url": url, "rationale": "Requested source"}]}


def no_op(payload, progress):
    return {"rationale": "Reviewed all supplied records", "actions": []}


def check_parsing():
    rss, atom = parse_feed(RSS, "https://example.com/rss"), parse_feed(ATOM, "https://example.com/atom")
    assert len(rss) == 2 and rss[0].summary == "Capacity expands" and rss[0].url == "https://example.com/chip"
    assert atom[0].summary == "New cell" and atom[0].published_at == "2026-01-02T00:00:00+00:00"
    assert evaluate(Rules(["chip"], ["factory"], ["rumor"]), rss[0].title, rss[0].summary).kept
    assert not evaluate(Rules([], [], ["chip"]), rss[0].title, "").kept
    assert publication_time("Wed, 31 Dec 2025 19:00:00 -0500").isoformat() == "2026-01-01T00:00:00+00:00"
    assert publication_time("not a date") is None and publication_time("2026-01-02T10:00:00") is None
    for invalid in ({"mode": "other"}, {"mode": "live", "start_date": "2026-01-01"},
            {**WINDOW, "start_date": "2026-02-30"}, {**WINDOW, "end_date": "9999-01-01"},
            {**WINDOW, "start_date": "2026-01-03"}, {**WINDOW, "start_date": "20260101"}, {"mode": "historical"}):
        rejected(lambda: ScanWindow.parse(invalid))
    window = ScanWindow.parse(WINDOW)
    assert parse_qs(urlsplit(query_url("chips", window)).query)["q"][0] == "(chips) after:2026-01-01 before:2026-01-03"
    assert len(discovery("chips", window)) == 1
    rejected(lambda: query_url("chips AFTER:2020-01-01", window))
    assert model.next_check_delay(1) == 30 and model.next_check_delay(30) == 1800


def check_collection(directory, base, routes, requests):
    engine = Engine(directory / "collection.sqlite3", start_worker=False)
    url = base + "/rss"
    first = create(engine, "Chips", url, rules={**RULES, "any_terms": ["chip"]}, poll_interval_minutes=10080)
    second = create(engine, "Shared", url)
    count = len(requests)
    drain(engine)
    assert len(requests) == count + 1, "Shared URL fetched more than once in one due cycle"
    state = engine.state()
    assert state["article_count"] == 2 and state["scans"][0]["kept"] == 1 and state["scans"][0]["discarded"] == 1
    assert state["collection_interval_seconds"] == 15
    assert state["scans"][0]["feeds"][0]["next_check_at"] and state["scans"][0]["feeds"][0]["failures"] == 0
    events, jobs = len(state["activity"]), len(state["jobs"])
    force_due(engine, url)
    assert engine.collect_due() == 1 and requests[-1][1]["If-None-Match"] == '"fixture-1"'
    assert requests[-1][1]["If-Modified-Since"] == routes["/rss"].modified
    assert engine.state()["article_count"] == 2 and len(engine.state()["activity"]) == events
    engine._schedule()
    assert len(engine.state()["jobs"]) == jobs, "Empty collection tick created jobs"
    engine.update_scan({"scan_id": first, "expected_revision": 1, "rules": {**RULES, "any_terms": ["football"]}})
    drain(engine)
    state = engine.state()
    assert state["scans"][0]["revision"] == 2
    assert next(r for r in state["records"] if r["scan_id"] == first and r["title"] == "Football result")["kept"]
    assert len([a for a in state["activity"] if a["type"] == "decision_changed"]) == 2
    rejected(lambda: engine.update_scan({"scan_id": first, "expected_revision": 1, "rules": RULES}))
    engine.update_scan({"scan_id": first, "objective": "Changed objective"})
    assert engine.state()["scans"][0]["objective"] == "Changed objective"
    engine.update_scan({"scan_id": second, "status": "paused"})
    routes["/rss"] = Response(RSS.replace(b"/sport", b"/late"), etag='"fixture-2"')
    force_due(engine, url)
    engine.collect_due()
    assert not any(r["url"].endswith("/late") and r["scan_id"] == second for r in engine.state()["records"])
    engine.update_scan({"scan_id": second, "status": "active"})
    drain(engine)
    assert any(r["url"].endswith("/late") and r["scan_id"] == second for r in engine.state()["records"]), "Resume did not catch up paused archive"
    third = create(engine, "New subscriber", url)
    drain(engine)
    assert next(s for s in engine.state()["scans"] if s["id"] == third)["kept"] == 3
    feed = next(s for s in engine.state()["scans"] if s["id"] == third)["feeds"][0]
    engine.update_scan({"scan_id": third, "feed_url": url})
    assert len(next(s for s in engine.state()["scans"] if s["id"] == third)["feeds"]) == 1
    engine.update_scan({"scan_id": third, "remove_feed_id": feed["id"]})
    assert next(s for s in engine.state()["scans"] if s["id"] == third)["feeds"] == []
    routes["/rss"] = Response()
    return engine



def check_cache_races(directory, base, routes, requests):
    for action in ("new", "resume"):
        for status in (200, 304):
            path = f"/cache-{action}-{status}"
            url = base + path
            routes[path] = Response()
            engine = Engine(directory / f"cache-{action}-{status}.sqlite3", start_worker=False)
            create(engine, "Existing subscriber", url)
            if action == "resume":
                target = create(engine, "Catch up", url)
            drain(engine)
            if action == "resume":
                engine.update_scan({"scan_id": target, "status": "paused"})
            entered, release = Event(), Event()
            routes[path] = Response(RSS.replace(b"/sport", b"/cache-late"), status=status, etag='"cache-2"', entered=entered, release=release)
            force_due(engine, url)
            thread = Thread(target=engine.collect_due, daemon=True)
            thread.start()
            try:
                assert entered.wait(3)
                if action == "new":
                    target = create(engine, "Catch up", url)
                else:
                    engine.update_scan({"scan_id": target, "status": "active"})
                assert engine.run_next("collector"), "Catch-up replay was not persisted"
                assert len([r for r in engine.state()["records"] if r["scan_id"] == target]) == 2
                release.set()
                thread.join(5)
                assert not thread.is_alive()
                routes[path].status = 200
                force_due(engine, url)
                engine.collect_due()
                assert requests[-1][1]["If-None-Match"] == '"cache-2"'
                records = [r for r in engine.state()["records"] if r["scan_id"] == target]
                assert len(records) == (3 if status == 200 else 2), f"Cached {action} missed an in-flight response"
            finally:
                release.set()
                thread.join(5)


def check_shared_arrivals(directory, base):
    engine = Engine(directory / "shared-arrivals.sqlite3", start_worker=False)
    paused = create(engine, "Paused shared archive", base + "/rss")
    historical = create(engine, "Finite shared archive", base + "/rss", **WINDOW)
    drain(engine)
    engine.update_scan({"scan_id": paused, "status": "paused"})
    first = create(engine, "RSS live", base + "/rss")
    second = create(engine, "Atom live", base + "/atom")
    drain(engine)
    state = engine.state()
    assert state["article_count"] == 3
    for scan_id in (first, second):
        records = [r for r in state["records"] if r["scan_id"] == scan_id]
        assert len(records) == 3 and all(r["kept"] for r in records), "Distinct live feeds missed shared arrivals"
    for scan_id in (paused, historical):
        assert len([r for r in state["records"] if r["scan_id"] == scan_id]) == 2, "Paused or historical scan followed live arrivals"
    with patch("engine.run_codex", side_effect=no_op):
        engine.enqueue("decision_audit")
        engine.run_next("model")
    audit = engine.state()["audits"][0]
    assert audit["status"] == "completed" and audit["report"]["missing_count"] == audit["report"]["inconsistent_count"] == 0


def check_shared_arrival_races(directory, base, routes):
    for subscribed in (True, False):
        for action in ("delete", "pause", "resume", "remove", "readd", "empty-resume", "rules", "subscribe"):
            if subscribed and action == "empty-resume":
                continue
            path = f"/shared-{subscribed}-{action}"
            url = base + path
            routes[path] = Response()
            engine = Engine(directory / f"shared-{subscribed}-{action}.sqlite3", start_worker=False)
            source = create(engine, "Surviving source", url)
            target = create(engine, "Changing target", url if subscribed else base + "/atom")
            drain(engine)
            feed = next(s for s in engine.state()["scans"] if s["id"] == target)["feeds"][0]
            if action == "empty-resume":
                engine.update_scan({"scan_id": target, "remove_feed_id": feed["id"]})
            entered, release = Event(), Event()
            routes[path] = Response(RSS.replace(b"/sport", b"/shared-late"), etag='"shared-late"', entered=entered, release=release)
            force_due(engine, url)
            thread = Thread(target=engine.collect_due, daemon=True)
            thread.start()
            try:
                assert entered.wait(3)
                if action == "delete":
                    engine.delete_scan({"scan_id": target})
                elif action in ("pause", "resume", "empty-resume"):
                    engine.update_scan({"scan_id": target, "status": "paused"})
                    if action != "pause":
                        engine.update_scan({"scan_id": target, "status": "active"})
                elif action == "rules":
                    engine.update_scan({"scan_id": target, "rules": {**RULES, "exclude_terms": ["football"]}})
                elif action == "subscribe":
                    engine.update_scan({"scan_id": target, "feed_url": base + ("/atom" if subscribed else "/rss")})
                    assert engine.run_next("collector"), "Subscription replay was not persisted"
                else:
                    engine.update_scan({"scan_id": target, "remove_feed_id": feed["id"]})
                    if action == "readd":
                        engine.update_scan({"scan_id": target, "feed_url": feed["url"]})
                release.set()
                thread.join(5)
                assert not thread.is_alive()
                state = engine.state()
                arrivals = [r for r in state["records"] if r["url"].endswith("/shared-late")]
                assert any(r["scan_id"] == source and r["kept"] for r in arrivals), "Valid source failed to ingest shared arrival"
                if action == "rules":
                    record = next(r for r in arrivals if r["scan_id"] == target)
                    assert record["rule_revision"] == 2 and not record["kept"], "Shared arrival used stale rules"
                elif action == "subscribe":
                    assert any(r["scan_id"] == target and r["kept"] for r in arrivals), "Added subscription hid a shared arrival"
                else:
                    assert not any(r["scan_id"] == target for r in arrivals), f"Stale shared {subscribed} {action} result reached target"
            finally:
                release.set()
                thread.join(5)


def check_stale_source_start(directory, base, routes):
    for window in ({}, WINDOW):
        path = f"/stale-start-{window.get('mode', 'live')}"
        url = base + path
        routes[path] = Response()
        engine = Engine(directory / f"stale-start-{window.get('mode', 'live')}.sqlite3", start_worker=False)
        source = create(engine, "Valid source", url)
        target = create(engine, "Replaced source", url, **window)
        drain(engine)
        feed = engine.state()["scans"][1]["feeds"][0]
        engine.update_scan({"scan_id": target, "remove_feed_id": feed["id"]})
        engine.update_scan({"scan_id": target, "feed_url": url})
        routes[path] = Response(RSS.replace(b"/sport", b"/prestart-late"), etag='"prestart"')
        engine._check_url(url, for_scan=target, token=feed["token"])
        arrivals = [r["scan_id"] for r in engine.state()["records"] if r["url"].endswith("/prestart-late")]
        assert arrivals == [source], "Replaced source token became a shared arrival target"


def check_replay_completion(directory, base):
    for action in ("rules", "replay", "restart", "queued-restart", "delete"):
        engine = Engine(directory / f"replay-completion-{action}.sqlite3", start_worker=False)
        scan_id = create(engine, "Replay target", base + "/rss")
        create(engine, "Other source", base + "/atom")
        drain(engine)
        job_id = engine.enqueue("replay", {"scan_id": scan_id})
        ready, release = Event(), Event()
        original = engine._replay_job
        def completed_body(*args):
            result = original(*args)
            ready.set()
            assert release.wait(8), "Replay completion fixture was never released"
            if action == "restart":
                raise SystemExit
            return result
        with patch.object(engine, "_replay_job", side_effect=completed_body):
            thread = Thread(target=engine.run_next, args=("collector",), daemon=True)
            thread.start()
            try:
                assert ready.wait(3), "Replay never reached completion boundary"
                rejected(lambda: engine.enqueue("replay", {"scan_id": scan_id}))
                if action == "replay":
                    with engine.transaction() as db:
                        db.execute("DELETE FROM decisions WHERE scan_id=? AND article_id=1", (scan_id,))
                    engine.apply_plan({"rationale": "Repeat accepted replay", "actions": [
                        {"tool": "replay_history", "scan_ref": str(scan_id), "rationale": "Repair missing decision"}]}, [], "audit", "Objective")
                else:
                    engine.update_scan({"scan_id": scan_id, "rules": {**RULES, "any_terms": ["football"]}})
                    engine.update_scan({"scan_id": scan_id, "rules": {**RULES, "any_terms": ["battery"]}})
                active = [j for j in engine.state()["jobs"] if j["kind"] == "replay" and j["scan_id"] == scan_id and j["status"] in ("queued", "running")]
                assert len(active) == 1 and active[0]["id"] == job_id, "Replay coalescing lost unique active job"
                if action == "delete":
                    engine.delete_scan({"scan_id": scan_id})
                release.set()
                thread.join(5)
                assert not thread.is_alive()
            finally:
                release.set()
                thread.join(5)
        state = engine.state()
        job = next(j for j in state["jobs"] if j["id"] == job_id)
        assert job["status"] == ("cancelled" if action == "delete" else "running" if action == "restart" else "queued"), "Accepted replay disappeared at completion"
        if action in ("restart", "queued-restart", "delete"):
            engine = Engine(engine.database, start_worker=False)
        drain(engine)
        state = engine.state()
        if action == "delete":
            assert not any(r["scan_id"] == scan_id for r in state["records"])
            assert next(j for j in state["jobs"] if j["id"] == job_id)["status"] == "cancelled", "Deleted replay was requeued"
            continue
        records = [r for r in state["records"] if r["scan_id"] == scan_id]
        revision = 1 if action == "replay" else 3
        assert len(records) == 3 and all(r["rule_revision"] == revision for r in records), "Final replay revision was lost"
        assert {r["title"] for r in records if r["kept"]} == ({"New chip factory", "Football result", "Battery research"} if action == "replay" else {"Battery research"})
        assert next(j for j in state["jobs"] if j["id"] == job_id)["status"] == "completed"


def check_backoff(directory, base, routes, requests):
    url = base + "/failure"
    routes["/failure"] = Response(status=503, retry="3600")
    engine = Engine(directory / "backoff.sqlite3", start_worker=False)
    scan_id = create(engine, "Failures", url)
    drain(engine)
    state = engine.state()
    feed = state["scans"][0]["feeds"][0]
    assert feed["failures"] == 1 and "503" in feed["error"]
    assert publication_time(feed["next_check_at"]).timestamp() >= time.time() + 3590
    count = len(requests)
    create(engine, "New backed-off subscriber", url)
    drain(engine)
    assert publication_time(engine.state()["scans"][1]["feeds"][0]["next_check_at"]).timestamp() >= time.time() + 3590
    engine.enqueue("poll", {"scan_id": scan_id})
    drain(engine)
    assert len(requests) == count, "Manual rerun or new subscription ignored Retry-After floor"
    assert "retry floor" in engine.state()["jobs"][-1]["message"]
    assert engine.collect_due(at=time.time() + 100) == 0
    restarted = Engine(engine.database, start_worker=False)
    assert restarted.state()["scans"][0]["feeds"][0]["next_check_at"] == feed["next_check_at"]
    date_floor = datetime.now(timezone.utc) + timedelta(hours=2)
    routes["/failure"].retry = format_datetime(date_floor, usegmt=True)
    with engine.transaction() as db:
        db.execute("UPDATE feed_state SET retry_at=0,checked_at=0,next_check_at=0 WHERE url=?", (url,))
    engine.enqueue("poll", {"scan_id": scan_id})
    drain(engine)
    assert publication_time(engine.state()["scans"][0]["feeds"][0]["next_check_at"]).timestamp() >= date_floor.timestamp() - 1
    routes["/failure"] = Response(status=500)
    with engine.transaction() as db:
        db.execute("UPDATE feed_state SET retry_at=0,checked_at=0,next_check_at=0 WHERE url=?", (url,))
    engine.enqueue("poll", {"scan_id": scan_id})
    drain(engine)
    assert engine.state()["scans"][0]["feeds"][0]["failures"] == 3
    assert 115 <= publication_time(engine.state()["scans"][0]["feeds"][0]["next_check_at"]).timestamp() - time.time() <= 121
    routes["/failure"] = Response(ATOM, etag=None, modified=None)
    engine.enqueue("poll", {"scan_id": scan_id})
    drain(engine)
    assert engine.state()["scans"][0]["feeds"][0]["failures"] == 0
    assert engine.state()["scans"][0]["kept"] == 1
    assert any(a["type"] == "feed_polled" and "recovered" in a["message"] for a in engine.state()["activity"])


def check_windows(directory, base, routes):
    stamps = ["2026-01-01T00:00:00Z", "2026-01-02T23:59:59Z", "Wed, 31 Dec 2025 19:00:00 -0500",
        "2025-12-31T23:59:59Z", "2026-01-03T00:00:00Z", "", "unparseable"]
    body = '<rss><channel><title>Window fixture</title>' + ''.join(
        f'<item><title>Article {i}</title><link>https://example.com/date-{i}</link><pubDate>{stamp}</pubDate></item>'
        for i, stamp in enumerate(stamps)) + '</channel></rss>'
    routes["/history"] = Response(body.encode())
    url = base + "/history"
    engine = Engine(directory / "windows.sqlite3", start_worker=False)
    scan_id = create(engine, "History", url, **WINDOW)
    drain(engine)
    scan = engine.state()["scans"][0]
    assert scan["completed_at"] and scan["last_error"] is None and scan["kept"] == 3 and scan["discarded"] == 4
    assert engine.state()["stats"]["active"] == 0
    assert all("publication date" in r["reasons"][0].casefold() for r in engine.state()["records"] if not r["kept"])
    rejected(lambda: engine.update_scan({"scan_id": scan_id, "mode": "live"}))
    engine.update_scan({"scan_id": scan_id, "feed_url": base + "/atom"})
    drain(engine)
    assert len(engine.state()["scans"][0]["feeds"]) == 2
    jobs = len(engine.state()["jobs"])
    engine._schedule()
    assert len(engine.state()["jobs"]) == jobs and engine.collect_due() == 0
    live_id = create(engine, "Live source", base + "/atom")
    drain(engine)
    assert not any(r["url"].endswith("/battery") and r["scan_id"] == scan_id for r in engine.state()["records"])
    with engine.transaction() as db:
        history = [dict(row) for row in db.execute("SELECT * FROM articles ORDER BY id")]
    payload, _ = engine._decision_context(engine.state(), history)
    assert not any(gap["scan_id"] == scan_id for gap in payload["report"]["missing_decisions"]), "Same-second later arrival became a historical gap"
    engine.enqueue("replay", {"scan_id": scan_id})
    drain(engine)
    assert any(r["url"].endswith("/battery") and r["scan_id"] == scan_id for r in engine.state()["records"])
    engine.update_scan({"scan_id": scan_id, "rules": {**RULES, "exclude_terms": ["article"]}})
    drain(engine)
    assert next(s for s in engine.state()["scans"] if s["id"] == scan_id)["kept"] == 1
    routes["/history"].status = 500
    engine.enqueue("poll", {"scan_id": scan_id})
    drain(engine)
    assert "500" in engine.state()["scans"][0]["last_error"] and engine.state()["scans"][0]["completed_at"]
    jobs = len(engine.state()["jobs"])
    engine._schedule()
    assert len(engine.state()["jobs"]) == jobs
    routes["/history"].status = 200
    engine.enqueue("poll", {"scan_id": scan_id})
    drain(engine)
    assert engine.state()["scans"][0]["last_error"] is None
    for window in ({}, WINDOW):
        defaults = Engine(directory / f"default-{window.get('mode', 'live')}.sqlite3", start_worker=False)
        default_id = defaults.create_scan({"topic": "Default", "objective": "Objective", "rules": RULES, **window})
        assert defaults.state()["scans"][0]["feeds"][0]["url"] == query_url("Default", ScanWindow.parse(window))
        defaults.delete_scan({"scan_id": default_id})


def check_races(directory, base, routes):
    for action in ("delete", "pause", "remove", "readd", "resume", "shared"):
        path = "/race-" + action
        entered, release = Event(), Event()
        routes[path] = Response(RSS, entered=entered, release=release)
        engine = Engine(directory / f"race-{action}.sqlite3", start_worker=False)
        url = base + path
        scan_id = create(engine, "Fetching", url)
        engine.run_next("collector")
        if action == "shared":
            survivor = create(engine, "Survivor", url)
        feed = engine.state()["scans"][0]["feeds"][0]
        thread = Thread(target=engine.run_next, args=("collector",), daemon=True)
        thread.start()
        try:
            assert entered.wait(3), "Poll never entered real HTTP fixture"
            if action in ("delete", "shared"):
                engine.delete_scan({"scan_id": scan_id})
            elif action in ("pause", "resume"):
                engine.update_scan({"scan_id": scan_id, "status": "paused"})
                if action == "resume":
                    engine.update_scan({"scan_id": scan_id, "status": "active"})
            else:
                engine.update_scan({"scan_id": scan_id, "remove_feed_id": feed["id"]})
                if action == "readd":
                    engine.update_scan({"scan_id": scan_id, "feed_url": url})
                    assert engine.state()["scans"][0]["feeds"][0]["id"] == feed["id"], "Fixture did not exercise ID reuse"
            release.set()
            thread.join(5)
            assert not thread.is_alive()
            state = engine.state()
            if action == "shared":
                assert state["article_count"] == 2 and {r["scan_id"] for r in state["records"]} == {survivor}
            else:
                assert state["article_count"] == 0 and state["records"] == [], f"Stale {action} result was applied"
            if action in ("readd", "resume"):
                assert state["scans"][0]["feeds"][0]["last_checked"] is None
            if action == "delete":
                new_id = create(engine, "Fetching", url)
                assert new_id > scan_id
                engine.delete_scan({"scan_id": new_id})
                assert create(Engine(engine.database, start_worker=False), "After restart", url) > new_id
        finally:
            release.set()
            thread.join(5)



def check_queued_deletion(directory, base):
    engine = Engine(directory / "queued-deletion.sqlite3", start_worker=False)
    survivor = create(engine, "Keep", base + "/rss")
    drain(engine)
    engine.update_scan({"scan_id": survivor, "status": "paused"})
    dead = create(engine, "Delete queued", base + "/rss")
    before = engine.state()
    engine.delete_scan({"scan_id": dead})
    drain(engine)
    after = engine.state()
    assert after["article_count"] == before["article_count"] and after["records"] == before["records"] and after["audits"] == before["audits"]
    assert all(j["status"] == "cancelled" for j in after["jobs"] if j["scan_id"] == dead)
    assert any(a["type"] == "scan_deleted" and a["details"]["topic"] == "Delete queued" for a in after["activity"])
    for value in (None, True, "1", 0, dead):
        rejected(lambda: engine.delete_scan({"scan_id": value}))

def check_recovery(directory, base):
    url = base + "/rss"
    engine = Engine(directory / "recovery.sqlite3", start_worker=False)
    scan_id = create(engine, "Interrupted history", url, **WINDOW)
    with engine.transaction() as db:
        db.execute("UPDATE jobs SET status='running' WHERE kind IN ('poll','replay')")
    restarted = Engine(engine.database, start_worker=False)
    assert all(j["status"] == "queued" for j in restarted.state()["jobs"])
    drain(restarted)
    before = restarted.state()
    assert before["scans"][0]["completed_at"] and len(before["records"]) == 2
    restarted.enqueue("replay", {"scan_id": scan_id})
    drain(restarted)
    assert restarted.state()["records"] == before["records"], "Replay changed unchanged decisions"
    for kind in ("setup", "audit", "decision_audit"):
        engine = Engine(directory / f"interrupted-{kind}.sqlite3", start_worker=False)
        create(engine, "Existing", url)
        drain(engine)
        data = {"topic": "Queued", "objective": "Objective", "feed_urls": [url]}
        job_id = engine.enqueue(kind, data if kind == "setup" else {})
        with engine.transaction() as db:
            db.execute("UPDATE jobs SET status='running' WHERE id=?", (job_id,))
            if kind != "setup":
                db.execute("INSERT INTO audits(started_at,status,kind) VALUES (?,'running',?)", (model.now(), "decision" if kind == "decision_audit" else "coverage"))
        recovered = Engine(engine.database, start_worker=False)
        with patch("engine.run_codex", side_effect=AssertionError("Interrupted model retried")):
            assert not recovered.run_next("model")
        assert next(j for j in recovered.state()["jobs"] if j["id"] == job_id)["status"] == "failed"
        assert all(a["status"] == "failed" for a in recovered.state()["audits"])
    queued = Engine(directory / "queued-model.sqlite3", start_worker=False)
    job_id = queued.enqueue("setup", {"topic": "Never started", "objective": "Objective", "feed_urls": [url]})
    queued = Engine(queued.database, start_worker=False)
    with patch("engine.run_codex", return_value=setup_plan("Never started", url)) as boundary:
        assert queued.run_next("model") and not queued.run_next("model") and boundary.call_count == 1
    state = Engine(queued.database, start_worker=False).state()
    assert next(j for j in state["jobs"] if j["id"] == job_id)["status"] == "completed"
    assert {j["kind"] for j in state["jobs"] if j["status"] == "queued"} == {"poll", "replay", "setup_review"}
    assert state["scans"][0]["setup_review"]["calls_used"] == 1
    assert len(state["scans"]) == 1


def check_plans(directory, base):
    url = base + "/rss"
    engine = Engine(directory / "plans.sqlite3", start_worker=False)
    historical = create(engine, "Source history", url, **WINDOW)
    drain(engine)
    prior = engine.state()
    invalid = setup_plan("Must roll back", url)
    invalid["actions"].append({"tool": "subscribe_feed", "scan_ref": "9999", "feed_url": url, "rationale": "Invalid target"})
    rejected(lambda: engine.apply_plan(invalid, [{"url": url}], "audit", "Objective"))
    assert engine.state() == prior, "Invalid plan left partial effects or downstream jobs"
    related = setup_plan("Related history", url)
    related["actions"][0].update(source_scan_ref=str(historical))
    engine.apply_plan(related, [{"url": url}], "audit", "Inherited objective")
    engine.apply_plan(related, [{"url": url}], "audit", "Inherited objective")
    assert len(engine.state()["scans"]) == 2
    related_scan = engine.state()["scans"][1]
    assert related_scan["objective"] == "Inherited objective" and {k: related_scan[k] for k in WINDOW} == WINDOW
    rejected(lambda: engine.apply_plan({"rationale": "Reject", "actions": [{**related["actions"][0], "topic": "Bad dates", "end_date": "2026-01-03"}]}, [], "audit", "Objective"))
    rejected(lambda: engine.apply_plan(setup_plan("Unknown URL", "https://unlisted.example/rss"), [], "audit", "Objective"))
    for index, window in enumerate(({}, WINDOW)):
        name = f"Custom model {index}"
        engine.enqueue("setup", {"topic": name, "objective": "Objective", "feed_urls": [base + "/atom"], **window})
        def plan(payload, progress):
            assert payload["user_feed_urls"] == [base + "/atom"]
            assert base + "/atom" in {f["url"] for f in payload["discovery"]}
            assert payload["preview"][0]["title"] == "Battery research"
            return setup_plan(name, base + "/atom")
        with patch("engine.run_codex", side_effect=plan):
            engine.run_next("model")
        scan = next(s for s in engine.state()["scans"] if s["topic"] == name)
        assert scan["mode"] == window.get("mode", "live")
    rejected(lambda: engine.enqueue("setup", {"topic": "Invalid", "objective": "Objective", "mode": "historical"}))
    engine.enqueue("setup", {"topic": "Stale", "objective": "Objective", "feed_urls": [url]})
    def stale(payload, progress):
        temporary = create(engine, "Transient", url)
        engine.delete_scan({"scan_id": temporary})
        return setup_plan("Stale", url)
    with patch("engine.run_codex", side_effect=stale):
        engine.run_next("model")
    assert not any(s["topic"] == "Stale" for s in engine.state()["scans"])
    assert "deleted" in engine.state()["jobs"][-1]["message"] or any("deleted" in j["message"] for j in engine.state()["jobs"] if j["kind"] == "setup")
    snapshot = engine._snapshot(engine.state()["scans"])
    subscription = engine.state()["scans"][0]["feeds"][0]
    engine.update_scan({"scan_id": historical, "remove_feed_id": subscription["id"]})
    engine.update_scan({"scan_id": historical, "feed_url": url})
    rejected(lambda: engine.apply_plan({"rationale": "Stale subscription", "actions": []}, [], "audit", "Objective", expected=snapshot))
    atomic = Engine(directory / "atomic-create.sqlite3", start_worker=False)
    with atomic.transaction() as db:
        db.execute("CREATE TRIGGER reject_poll BEFORE INSERT ON jobs WHEN NEW.kind='poll' BEGIN SELECT RAISE(ABORT,'Fixture rejected poll'); END")
    rejected(lambda: create(atomic, "Rollback creation", url), error=sqlite3.IntegrityError)
    assert atomic.state()["scans"] == [] and atomic.state()["jobs"] == []
    atomic_model = Engine(directory / "atomic-model.sqlite3", start_worker=False)
    job_id = atomic_model.enqueue("setup", {"topic": "Atomic model", "objective": "Objective", "feed_urls": [url]})
    with atomic_model.transaction() as db:
        db.execute("CREATE TRIGGER reject_completion BEFORE UPDATE ON jobs WHEN NEW.kind='setup' AND NEW.status='completed' BEGIN SELECT RAISE(ABORT,'Fixture rejected completion'); END")
    with patch("engine.run_codex", return_value=setup_plan("Atomic model", url)):
        atomic_model.run_next("model")
    state = Engine(atomic_model.database, start_worker=False).state()
    assert state["scans"] == [] and len(state["jobs"]) == 1 and state["jobs"][0]["id"] == job_id and state["jobs"][0]["status"] == "failed"
    assert not any(event["type"] in ("model_tool", "plan_applied", "model_plan") for event in state["activity"])


def check_lanes(directory, base, routes):
    engine = Engine(directory / "lanes.sqlite3", start_worker=False)
    url = base + "/rss"
    first = create(engine, "Live during model", url)
    paused = create(engine, "Paused replay", url)
    drain(engine)
    engine.update_scan({"scan_id": paused, "status": "paused"})
    entered, release = Event(), Event()
    engine.enqueue("setup", {"topic": "Blocked setup", "objective": "Objective", "feed_urls": [url]})
    def blocked(payload, progress):
        entered.set()
        assert release.wait(8), "Model fixture was never released"
        return setup_plan("Blocked setup", url)
    with patch("engine.run_codex", side_effect=blocked) as boundary:
        engine.model_worker.start()
        try:
            assert entered.wait(3)
            engine.save_settings({"audit_enabled": True, "decision_audit_enabled": True})
            engine._schedule()
            engine.save_settings({"audit_enabled": False, "decision_audit_enabled": False})
            assert not engine.run_next("model"), "A second model job claimed during running setup"
            engine.worker.start()
            routes["/rss"] = Response(RSS.replace(b"/sport", b"/while-model"), etag='"while-model"')
            force_due(engine, url)
            wait_for(lambda: any(r["url"].endswith("/while-model") and r["scan_id"] == first for r in engine.state()["records"]), "Collection waited for model")
            replay = engine.enqueue("replay", {"scan_id": paused})
            wait_for(lambda: any(j["id"] == replay and j["status"] == "completed" for j in engine.state()["jobs"]), "Replay waited for model")
            assert any(r["url"].endswith("/while-model") and r["scan_id"] == paused for r in engine.state()["records"])
            assert boundary.call_count == 1 and not release.is_set()
            release.set()
            wait_for(lambda: any(s["topic"] == "Blocked setup" for s in engine.state()["scans"]), "Collector fields invalidated model snapshot")
        finally:
            release.set()
            stop(engine)
    routes["/rss"] = Response()
    entered, release = Event(), Event()
    routes["/slow"] = Response(entered=entered, release=release)
    engine = Engine(directory / "fair.sqlite3", start_worker=False)
    create(engine, "Slow history", base + "/slow", **WINDOW)
    fast = create(engine, "Fast live", base + "/atom")
    engine.worker.start()
    try:
        assert entered.wait(3)
        wait_for(lambda: any(r["scan_id"] == fast for r in engine.state()["records"]), "Historical fetch blocked all live collection")
        assert not release.is_set()
    finally:
        release.set()
        stop(engine)


def check_audits(directory, base, requests):
    engine = Engine(directory / "audits.sqlite3", start_worker=False)
    engine._schedule()
    assert engine.state()["jobs"] == []
    scan_id = create(engine, "Decision review", base + "/rss", rules={**RULES, "any_terms": ["chip"]})
    drain(engine)
    settings = engine.state()["settings"]
    assert settings["audit_enabled"] is False and settings["decision_audit_enabled"] is False
    engine._schedule()
    assert not any(j["kind"] in ("audit", "decision_audit") for j in engine.state()["jobs"])
    engine.save_settings({"audit_enabled": True, "audit_interval_minutes": 17})
    engine.save_settings({"decision_audit_enabled": True, "decision_audit_interval_minutes": 19})
    engine._schedule()
    pending = {j["kind"]: j for j in engine.state()["jobs"] if j["status"] == "queued"}
    assert set(pending) == {"audit", "decision_audit"}, "Independent audit schedules starved each other"
    engine.save_settings({"audit_enabled": False, "decision_audit_enabled": False})
    with patch("engine.run_codex", side_effect=AssertionError("Disabled scheduled audit invoked model")):
        assert engine.run_next("model") and engine.run_next("model")
    assert all(j["status"] == "cancelled" for j in engine.state()["jobs"] if j["id"] in {p["id"] for p in pending.values()})
    calls, network = [], len(requests)
    def review(payload, progress):
        calls.append(payload)
        assert payload["mode"] == "decision_audit" and len(payload["history"]) == 2 and len(payload["decisions"]) == 2
        assert {d["kept"] for d in payload["decisions"]} == {True, False}
        assert payload["scans"][0]["objective"] == "Find relevant news"
        return no_op(payload, progress)
    with patch("engine.run_codex", side_effect=review):
        engine.enqueue("decision_audit")
        engine.run_next("model")
        assert len(calls) == 1 and len(requests) == network
        audit = engine.state()["audits"][0]
        assert audit["kind"] == "decision" and audit["coverage_count"] == audit["decision_count"] == 2 and audit["fresh_count"] == 0
        assert audit["report"]["missing_count"] == audit["report"]["inconsistent_count"] == 0
        assert audit["input_hash"]
        engine.save_settings({"decision_audit_enabled": True})
        with engine.transaction() as db:
            db.execute("UPDATE settings SET value='0' WHERE key='last_decision_audit_at'")
        engine._schedule()
        engine.run_next("model")
        assert len(calls) == 1 and engine.state()["jobs"][-1]["status"] == "cancelled"
        engine.enqueue("decision_audit")
        engine.run_next("model")
        assert len(calls) == 2, "Manual unchanged review was skipped"
        with engine.transaction() as db:
            db.execute("UPDATE articles SET summary=summary || ' metadata change' WHERE id=1")
            db.execute("UPDATE settings SET value='0' WHERE key='last_decision_audit_at'")
        engine._schedule()
        engine.run_next("model")
        assert len(calls) == 3, "Changed article metadata was skipped"
    with engine.transaction() as db:
        db.execute("DELETE FROM decisions WHERE article_id=1")
        db.execute("UPDATE decisions SET reasons='[\"Wrong reason\"]' WHERE article_id=2")
    def repair(payload, progress):
        assert payload["report"]["missing_count"] == payload["report"]["inconsistent_count"] == 1
        return {"rationale": "Repair deterministic gaps", "actions": [{"tool": "replay_history", "scan_ref": str(scan_id), "rationale": "Re-evaluate all history"}]}
    with patch("engine.run_codex", side_effect=repair):
        engine.enqueue("decision_audit")
        engine.run_next("model")
    drain(engine)
    assert engine.state()["scans"][0]["kept"] == 1 and engine.state()["scans"][0]["discarded"] == 1
    for action in (setup_plan("Forbidden", base + "/rss")["actions"][0],
            {"tool": "subscribe_feed", "scan_ref": str(scan_id), "feed_url": base + "/atom", "rationale": "Forbidden"}):
        with patch("engine.run_codex", return_value={"rationale": "Invalid decision tool", "actions": [action]}):
            engine.enqueue("decision_audit")
            engine.run_next("model")
        assert engine.state()["audits"][0]["status"] == "failed" and engine.state()["audits"][0]["input_hash"] is None
    engine.update_scan({"scan_id": scan_id, "status": "paused"})
    with engine.transaction() as db:
        db.execute("DELETE FROM decisions")
        history = [dict(row) for row in db.execute("SELECT * FROM articles ORDER BY id")]
    payload, _ = engine._decision_context(engine.state(), history)
    assert payload["report"]["missing_count"] == 0, "Paused gaps were reported as missing"
    with patch("model.codex_path", return_value="unused"), patch("model.subprocess.run", side_effect=AssertionError("Oversized review invoked Codex")):
        rejected(lambda: model.run_codex({"history": "x" * model.MAX_MODEL_BYTES}, lambda _: None))
    assert engine.state()["settings"]["audit_interval_minutes"] == 17
    with engine.transaction() as db:
        db.execute("UPDATE articles SET summary=? WHERE id=1", ("x" * model.MAX_MODEL_BYTES,))
    with patch("model.codex_path", return_value="unused"), patch("model.subprocess.run", side_effect=AssertionError("Oversized audit invoked Codex")):
        engine.enqueue("decision_audit")
        engine.run_next("model")
    audit = engine.state()["audits"][0]
    assert audit["status"] == "failed" and audit["coverage_count"] == 2 and "600000" in audit["error"]
    assert audit["input_hash"] is None


def check_migration(directory, base):
    path = directory / "legacy.sqlite3"
    legacy_schema = SCHEMA[:SCHEMA.index("CREATE TABLE IF NOT EXISTS jobs")]
    with sqlite3.connect(path) as db:
        db.executescript(legacy_schema)
        db.execute("INSERT INTO scans(id,topic,topic_key,objective,rules,created_at) VALUES (7,'Old','old','Original objective',?,'2026-01-01')", (json.dumps(RULES),))
        db.execute("INSERT INTO feeds(scan_id,url) VALUES (7,?)", (base + "/rss",))
        article = parse_feed(RSS, base + "/rss")[0]
        db.execute("INSERT INTO articles(stable_key,title,url,source,published_at,summary,feed_url,fetched_at) VALUES (?,?,?,?,?,?,?,?)", (*asdict(article).values(), "2026-01-01"))
        db.execute("INSERT INTO decisions VALUES (7,1,1,'[\"Original reason\"]','[]',1,'2026-01-01')")
        db.execute("INSERT INTO activity(at,type,scan_id,message,details) VALUES ('2026-01-01','scan_deleted',18,'Old deleted scan','null')")
        db.execute("INSERT INTO audits(started_at,status) VALUES ('2026-01-01','completed')")
        db.execute("INSERT INTO settings VALUES ('audit_enabled','false')")
        db.execute("INSERT INTO settings VALUES ('audit_interval_minutes','23')")
    engine = Engine(path, start_worker=False)
    state = engine.state()
    scan = state["scans"][0]
    assert scan["id"] == 7 and scan["mode"] == "live" and scan["start_date"] is None and scan["last_error"] is None
    assert state["audits"][0]["kind"] == "coverage" and state["audits"][0]["windows"] == [] and state["audits"][0]["report"] == {}
    assert state["article_count"] == 1 and state["records"][0]["reasons"] == ["Original reason"]
    assert state["settings"]["audit_enabled"] is False and state["settings"]["audit_interval_minutes"] == 23
    assert state["settings"]["decision_audit_enabled"] is False
    assert create(engine, "After migration", base + "/rss") > 18


def check_http(directory, base):
    engine = Engine(directory / "http.sqlite3", start_worker=False)
    assets = directory / "static" / "assets"
    assets.mkdir(parents=True)
    (assets / "fixture.js").write_text("console.log('fixture')")
    (directory / "secret.txt").write_text("outside static")
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.handler_for(engine, 0))
    httpd.RequestHandlerClass = server.handler_for(engine, httpd.server_port)
    worker = Thread(target=httpd.serve_forever, daemon=True)
    worker.start()
    origin = f"http://127.0.0.1:{httpd.server_port}"
    def request(path, data=None, headers=None):
        return urlopen(Request(origin + path, data=json.dumps(data).encode() if data is not None else None,
            headers=headers or {"Content-Type": "application/json", "X-Wirewatch": "1"}), timeout=3)
    def status(path, data=None, headers=None):
        try:
            with request(path, data, headers) as response:
                return response.status
        except HTTPError as exc:
            return exc.code
    try:
        with patch("server.STATIC", directory / "static"):
            assert status("/api/health") == 200 and status("/assets/fixture.js") == 200
            assert status("/assets/%2e%2e/%2e%2e/secret.txt") == 404
            assert status("/api/state", headers={"Host": "evil.example"}) == 403
            assert status("/api/scans", {"topic": "No header"}, {"Content-Type": "application/json"}) == 403
            assert status("/api/scans", {}, {"Content-Type": "application/json", "X-Wirewatch": "1", "Origin": "http://evil.example"}) == 403
            with request("/api/scans", {"topic": "HTTP history", "objective": "Objective", "rules": RULES, "feed_urls": [base + "/rss"], **WINDOW}) as response:
                scan_id = json.load(response)["scan_id"]
            drain(engine)
            with request(f"/api/export?scan={scan_id}&decision=kept") as response:
                records = json.load(response)
                assert len(records) == 2 and all({key: r[key] for key in WINDOW} == WINDOW for r in records)
                assert "attachment" in response.headers["Content-Disposition"]
            with request("/api/state") as response:
                state = json.load(response)
                assert state["collection_interval_seconds"] == 15 and "failures" in state["scans"][0]["feeds"][0]
                assert state["scans"][0]["setup_review"] is None
            with patch("engine.run_codex", side_effect=AssertionError("Viewing or manual controls invoked a model")):
                assert status("/api/scans/review", {"scan_id": scan_id, "action": "start"}) == 200
                first_run = engine.setup_history(scan_id)
                assert status("/api/scans/review", {"scan_id": scan_id, "action": "start"}) == 200
                assert engine.setup_history(scan_id) == first_run
                with request(f"/api/scans/review?scan_id={scan_id}") as response:
                    assert json.load(response)["setup_review"] == first_run
                assert not engine.run_next("model")
                assert status("/api/scans/review", {"scan_id": scan_id, "action": "stop"}) == 200
                assert engine.setup_history(scan_id)["phase"] == "stopped"
                assert engine.state()["scans"][0]["status"] == "active"
            for invalid in (None, True, "1", 0, 999):
                assert status("/api/scans/review", {"scan_id": invalid, "action": "start"}) == 400
            assert status("/api/scans/review", {"scan_id": scan_id, "action": "other"}) == 400
            assert status("/api/scans/review?scan_id=bad") == 400
            assert status("/api/scans/update", {"scan_id": scan_id, "end_date": "2026-01-03"}) == 400
            assert status("/api/plan", {"topic": "bad", "objective": "bad", "mode": "historical"}) == 400
            assert status("/api/plan", {"topic": "HTTP model", "objective": "Objective", "feed_urls": [base + "/rss"], **WINDOW}) == 200
            assert status("/api/decision-audit", {}) == 200 and status("/api/audit", {}) == 200
            assert status("/api/settings", {"decision_audit_enabled": True}) == 200
            assert engine.state()["settings"]["audit_enabled"] is False
            assert status("/api/scans/delete", {"scan_id": scan_id}) == 200
            assert status("/api/scans/delete", {"scan_id": scan_id}) == 400 and status("/api/scans/delete", {}) == 400
    finally:
        httpd.shutdown()
        httpd.server_close()
        worker.join(3)


def review_action(tool, *, url=None, query=None, rules=None):
    return {"tool": tool, "rules": rules, "feed_url": url, "query": query, "rationale": f"Fixture {tool} based on inspected evidence"}


def review_result(payload, actions=None, *, stable=False, findings=None):
    return {"page_number": payload["page_number"], "rationale": "Inspected every supplied full title, excerpt, decision and actual probe result",
        "stable": stable, "findings": payload["findings"] if findings is None else findings, "actions": actions or []}


def review_evidence(payload):
    return [dict(zip(["type", *payload["evidence_columns"][row[0]]], row, strict=True)) for row in payload["evidence"]]


def review_fixture(directory, base, name, **options):
    engine = Engine(directory / f"setup-{name}.sqlite3", start_worker=False)
    scan_id = create(engine, name, base + "/rss", **options)
    drain(engine)
    assert engine.setup_history(scan_id) is None, "Manual creation started a model review"
    engine.control_setup_review({"scan_id": scan_id, "action": "start"})
    return engine, scan_id


def review_turn(engine, scan_id, boundary, before_model=None):
    drain(engine)
    state = engine.setup_history(scan_id)
    at = publication_time(state["next_review_at"]).timestamp() + 1
    with patch("engine.time.time", return_value=at), patch("engine.run_codex", side_effect=boundary) as calls:
        assert not engine.run_next("model"), "Review skipped its replay through the frozen highwater"
        drain(engine)
        if before_model:
            before_model()
        assert engine.run_next("model"), "Due setup review did not run"
    return calls.call_count


def seed_review_history(engine, scan_id, count, size=150):
    drain(engine)
    with engine.transaction() as db:
        first = db.execute("SELECT coalesce(max(id),0)+1 FROM articles").fetchone()[0]
        for ident in range(first, count + 1):
            db.execute("""INSERT INTO articles(id,stable_key,title,url,source,published_at,summary,feed_url,fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?)""", (ident, f"fixture-{ident}", "Battery recall missed" if ident == count else f"Chip factory {ident}",
                    f"https://example.com/fixture/{ident}", "Full fixture source", "2026-01-01T12:00:00+00:00", "é" * size,
                    "https://example.com/original-feed", model.now()))
    engine.enqueue("replay", {"scan_id": scan_id})
    drain(engine)


def check_setup_pages(directory, base, routes):
    urls = []
    for index in range(20):
        query = query_url(f"recall source {index} " + "consumer product safety " * 35)
        path = "/rss/search?" + urlsplit(query).query
        routes[path] = Response(b"<rss><channel><title>Query fixture</title></channel></rss>", etag=None)
        urls.append(base + path)
    probe_urls = [base + f"/long-url-probe-{index}" for index in range(6)]
    for probe_url in probe_urls:
        routes[urlsplit(probe_url).path] = Response(('''<feed xmlns="http://www.w3.org/2005/Atom"><title>Independent 安全 source</title>'''
            + "".join(f'''<entry><id>urn:probe:{index}</id><title>Battery research 安全 {index}</title>
            <link href="https://news.google.com/rss/articles/{'B' * 1100}{index}"/>
            <summary>{'安全é' * 60}</summary><updated>2026-01-02</updated></entry>''' for index in range(100))
            + "</feed>").encode(), etag=None)
    engine = Engine(directory / "setup-1800-records.sqlite3", start_worker=False)
    scan_id = create(engine, "1800-records", urls[0], feed_urls=urls, rules={**RULES, "any_terms": ["chip"]})
    seed_review_history(engine, scan_id, 1800)
    with engine.transaction() as db:
        for ident in range(1, 1801):
            db.execute("UPDATE articles SET title=title||?,summary=?,url=?,feed_url=? WHERE id=?",
                (" 完整标题é", "é" * 2000 if ident == 1799 else "安全é" * 60,
                    f"https://news.google.com/rss/articles/{'A' * 1100}{ident}?oc=5", urls[ident % 20], ident))
    engine.enqueue("replay", {"scan_id": scan_id})
    drain(engine)
    engine.control_setup_review({"scan_id": scan_id, "action": "start"})
    originals = {}
    seen = {1: [], 2: []}
    probe_items = []
    pages_per_round = {}
    def snapshot():
        with engine.transaction() as db:
            originals.update({row["id"]: dict(row) for row in db.execute("""SELECT a.*,d.kept,d.reasons,d.matched_terms,d.rule_revision,d.decided_at
                FROM articles a JOIN decisions d ON d.article_id=a.id WHERE d.scan_id=?""", (scan_id,))})
    def inspect(payload, progress, **kwargs):
        number, round_number = payload["page_number"], payload["round"]
        assert kwargs["schema"] == review.SCHEMA and kwargs["system_prompt"] == review.PROMPT
        assert len(review.encoded(payload)) + len(review.PROMPT.encode()) <= 600_000
        assert len(review.encoded(payload["evidence"])) <= 450_000
        serialized = review.encoded(payload).decode()
        assert "A" * 1100 not in serialized and "B" * 1100 not in serialized
        assert all(serialized.count(url) == 1 for url in urls), "Repeated feed URLs still consume model context"
        assert payload["manifest"]["article_ids"] == list(range(1, 1801))
        assert payload["report"]["missing_count"] == payload["report"]["inconsistent_count"] == 0
        catalog = {source["source_ref"]: source for source in payload["source_catalog"]}
        assert len(catalog) == len({source["url"] for source in catalog.values()})
        assert {catalog[feed["source_ref"]]["url"] for feed in payload["feeds"]} == set(urls)
        assert all(feed["last_checked"] and not feed["error"] for feed in payload["feeds"])
        assert all(catalog[feed["source_ref"]]["authorized"] for feed in payload["feeds"])
        assert payload["scan"]["rules"] == {**RULES, "any_terms": ["chip"] if round_number == 1 else ["chip", "battery"]}
        assert engine.setup_history(scan_id)["calls_used"] == sum(len(items) for items in pages_per_round.values()) + 1
        pages_per_round.setdefault(round_number, []).append(number)
        findings = payload["findings"]
        for item in review_evidence(payload):
            if item["type"] == "decision":
                ident = item["article_id"]
                seen[round_number].append(ident)
                for key in ("title", "summary", "source", "published_at", "fetched_at", "rule_revision", "decided_at"):
                    assert item[key] == originals[ident][key], f"Lost article field {key}"
                assert catalog[item["source_ref"]]["url"] == originals[ident]["feed_url"]
                assert "url" not in item and "feed_url" not in item
                assert item["kept"] == bool(originals[ident]["kept"])
                assert item["reasons"] == json.loads(originals[ident]["reasons"])
                assert item["matched_terms"] == json.loads(originals[ident]["matched_terms"])
                assert item["rule_revision"] == round_number
                if ident == 1800:
                    assert item["kept"] == (round_number == 2), "Late discarded miss was not replayed"
                    findings = [{"id": "late-battery-miss", "article_ids": [1800], "probe_ids": [],
                        "reason": "The final discarded record is relevant; the second round confirms the new rules keep it" if round_number == 2 else "Battery recall in the final page was incorrectly discarded",
                        "resolved": round_number == 2}]
            elif item["type"] == "probe_article":
                probe_items.append(item)
                assert "url" not in item and "stable_key" not in item and "feed_url" not in item
                assert catalog[item["source_ref"]]["url"] == probe_urls[item["probe_id"] - 1] and catalog[item["source_ref"]]["authorized"]
            elif item["type"] == "probe":
                assert catalog[item["source_ref"]]["url"] == probe_urls[item["id"] - 1]
                assert item["independent"] and item["executed_at"] and item["error"] is None
        actions = []
        final = number == payload["page_count"]
        if final:
            assert seen[round_number][-1] == 1800 and findings, "Last decision or cumulative finding was lost"
            if round_number == 1:
                actions = [review_action("update_rules", rules={**RULES, "any_terms": ["chip", "battery"]}),
                    *[review_action("probe_feed", url=url) for url in probe_urls]]
        if round_number == 2:
            assert payload["findings"] and payload["previous_rounds"]
            for probe, url in zip(payload["manifest"]["probes"], probe_urls, strict=True):
                assert probe["item_count"] == probe["in_window_count"] == 100 and probe["independent"]
                assert catalog[probe["source_ref"]]["url"] == url
        return review_result(payload, actions, stable=final and round_number == 2, findings=findings)
    for _ in range(2):
        assert 2 <= review_turn(engine, scan_id, inspect, snapshot) <= 4
    state = engine.setup_history(scan_id)
    assert state["phase"] == "ready" and state["reviewed_count"] == state["total_count"] == 1800
    assert seen[1] == seen[2] == list(range(1, 1801)), "Full history was sampled, repeated, or skipped"
    assert state["calls_used"] == sum(len(items) for items in pages_per_round.values()) <= 8
    assert engine.state()["article_count"] == 1800, "Unsubscribed probe was ingested into the archive"
    assert [(item["probe_id"], item["item_index"]) for item in probe_items] == [(probe, index) for probe in range(1, 7) for index in range(100)]
    expected_probes = [asdict(article) for url in probe_urls for article in parse_feed(routes[urlsplit(url).path].body, url)]
    for item, expected in zip(probe_items, expected_probes):
        assert item["window_reason"] is None
        for key in ("title", "summary", "source", "published_at"):
            assert item[key] == expected[key], f"Lost probe semantics {key}"
    with engine.transaction() as db:
        assert {row["id"]: row["url"] for row in db.execute("SELECT id,url FROM articles")} == {
            ident: article["url"] for ident, article in originals.items()}, "Model formatting changed clickable archive URLs"
    assert state["history"][0]["status"] == "changed" and state["history"][1]["status"] == "stable"
    finished = state["history"]
    engine.update_scan({"scan_id": scan_id, "objective": "New scope requires a fresh review"})
    assert engine.setup_history(scan_id)["phase"] == "needs_attention"
    assert engine.setup_history(scan_id)["history"] == finished, "Finished round history changed on invalidation"
    engine.control_setup_review({"scan_id": scan_id, "action": "start"})
    restarted = engine.setup_history(scan_id)
    assert restarted["run_id"] != state["run_id"] and restarted["calls_used"] == 0 and restarted["lifetime_calls_used"] == state["calls_used"]


def check_setup_sources(directory, base, routes):
    for mode in ({}, WINDOW):
        name = "historical" if mode else "live"
        engine, scan_id = review_fixture(directory, base, f"sources-{name}", **mode)
        def inspect(payload, progress, **kwargs):
            actions = []
            if payload["round"] == 1:
                actions = [review_action("probe_feed", url=base + "/atom")]
            elif payload["round"] == 2:
                probe_items = [item for item in review_evidence(payload) if item["type"] == "probe_article"]
                assert probe_items[0]["title"] == "Battery research" and probe_items[0]["window_reason"] is None
                assert any(source["url"] == base + "/atom" and source["authorized"] for source in payload["source_catalog"])
                actions = [review_action("add_feed", url=base + "/atom"), review_action("remove_feed", url=base + "/rss")]
            return review_result(payload, actions, stable=payload["round"] == 3)
        for _ in range(3):
            assert review_turn(engine, scan_id, inspect) == 1
        state = engine.state()
        scan = state["scans"][0]
        assert scan["setup_review"]["phase"] == "ready" and scan["setup_review"]["calls_used"] == 3
        assert [feed["url"] for feed in scan["feeds"]] == [base + "/atom"]
        assert scan["kept"] == 3 and state["article_count"] == 3
        assert state["settings"]["audit_enabled"] is state["settings"]["decision_audit_enabled"] is False
        if mode:
            assert scan["completed_at"] and scan["archive_through_id"] == 3
            assert not engine.collect_due(time.time() + 3600), "Historical setup started recurring collection"
    routes["/probe-window"] = Response(RSS.replace(b"2026-01-02T23:59:59Z", b"2026-01-03T00:00:00Z"), etag=None)
    engine, scan_id = review_fixture(directory, base, "probe-window", **WINDOW)
    review_turn(engine, scan_id, lambda payload, progress, **kwargs: review_result(payload, [review_action("probe_feed", url=base + "/probe-window")]))
    def dates(payload, progress, **kwargs):
        items = [item for item in review_evidence(payload) if item["type"] == "probe_article"]
        assert items[0]["window_reason"] is None and "outside historical window" in items[1]["window_reason"]
        assert payload["manifest"]["probes"][0]["in_window_count"] == 1
        return review_result(payload, stable=True)
    review_turn(engine, scan_id, dates)
    assert engine.setup_history(scan_id)["phase"] == "ready"
    engine, scan_id = review_fixture(directory, base, "dated-query", **WINDOW)
    review_turn(engine, scan_id, lambda payload, progress, **kwargs: review_result(payload, [review_action("probe_query", query="site:local.example battery burn")]))
    with engine.transaction() as db:
        pending = json.loads(db.execute("SELECT pending_probes FROM setup_runs").fetchone()[0])
    q = parse_qs(urlsplit(pending[0]["url"]).query)["q"][0]
    assert q == "(site:local.example battery burn) after:2026-01-01 before:2026-01-03"
    engine.control_setup_review({"scan_id": scan_id, "action": "stop"})
    for action in (review_action("probe_query", query="news after:1999-01-01"),
            review_action("probe_feed", url="http://user:secret@example.com/feed"),
            review_action("add_feed", url="https://unexecuted.example/feed")):
        rejected(lambda: review.parse_actions([action], Rules.parse(RULES), {base + "/rss"}, {base + "/rss"}, ScanWindow.parse(WINDOW)))
    own_probe = review.parse_actions([review_action("add_feed", url=base + "/atom"), review_action("probe_feed", url=base + "/atom")],
        Rules.parse(RULES), {base + "/rss"}, {base + "/rss", base + "/atom"}, ScanWindow())
    assert not own_probe.probes[0]["independent"], "A source added by the same plan falsely counted as independent"
    routes["/review-error"] = Response(status=500)
    engine, scan_id = review_fixture(directory, base, "removable-error")
    engine.update_scan({"scan_id": scan_id, "feed_url": base + "/review-error"})
    engine.control_setup_review({"scan_id": scan_id, "action": "start"})
    def remove_failed(payload, progress, **kwargs):
        if payload["round"] == 1:
            assert any(feed["error"] for feed in payload["feeds"]), "Feed failure was hidden from review"
            return review_result(payload, [review_action("remove_feed", url=base + "/review-error"), review_action("probe_feed", url=base + "/atom")])
        return review_result(payload, stable=True)
    review_turn(engine, scan_id, remove_failed)
    review_turn(engine, scan_id, remove_failed)
    assert engine.setup_history(scan_id)["phase"] == "ready", "One failed source prevented review from removing it"

    engine, scan_id = review_fixture(directory, base, "historical-window-arrival", **WINDOW)
    live = create(engine, "Separate live arrivals", base + "/rss")
    drain(engine)
    routes["/rss"] = Response(RSS.replace(b"/sport", b"/during-historical-window"), etag='"historical-window-arrival"')
    force_due(engine, base + "/rss")
    engine.collect_due()
    assert any(record["url"].endswith("/during-historical-window") and record["scan_id"] == live for record in engine.state()["records"])
    assert not any(record["url"].endswith("/during-historical-window") and record["scan_id"] == scan_id for record in engine.state()["records"])
    def replayed_window(payload, progress, **kwargs):
        with engine.transaction() as db:
            arrival = db.execute("SELECT id FROM articles WHERE url LIKE '%/during-historical-window'").fetchone()[0]
        assert arrival in {item["article_id"] for item in review_evidence(payload) if item["type"] == "decision"}
        assert payload["report"]["article_count"] == 3 and payload["report"]["missing_count"] == 0
        return review_result(payload, [review_action("probe_feed", url=base + "/atom")])
    review_turn(engine, scan_id, replayed_window)
    routes["/rss"] = Response()


def check_setup_gates(directory, base, routes):
    routes["/review-empty"] = Response(b"<rss><channel><title>Empty</title></channel></rss>", etag=None)
    routes["/review-failed-probe"] = Response(status=503)
    for gate in ("no-probe", "own-probe", "empty-probe", "failed-probe", "empty-archive", "zero-kept", "feed-error", "last-round-change", "gaps", "inconsistent", "changed-during-review"):
        engine, scan_id = review_fixture(directory, base, f"gate-{gate}", rules={**RULES, "any_terms": ["not present"]} if gate == "zero-kept" else RULES)
        if gate == "empty-archive":
            with engine.transaction() as db:
                db.execute("DELETE FROM decisions")
                db.execute("DELETE FROM articles")
            feed = engine.state()["scans"][0]["feeds"][0]
            engine.update_scan({"scan_id": scan_id, "remove_feed_id": feed["id"], "feed_url": base + "/review-empty"})
            engine.control_setup_review({"scan_id": scan_id, "action": "start"})
        if gate == "feed-error":
            engine.update_scan({"scan_id": scan_id, "feed_url": base + "/review-failed-probe"})
            engine.control_setup_review({"scan_id": scan_id, "action": "start"})
        def disrupt_decisions():
            with engine.transaction() as db:
                if gate == "gaps":
                    db.execute("DELETE FROM decisions WHERE article_id=1")
                else:
                    db.execute("UPDATE decisions SET matched_terms='[\"wrong\"]' WHERE article_id=1")
        def inspect(payload, progress, **kwargs):
            if gate in ("gaps", "inconsistent"):
                raise AssertionError("Inconsistent deterministic evidence spent a model call")
            if gate == "no-probe":
                return review_result(payload, stable=True)
            if payload["round"] == 1:
                url = base + {"own-probe": "/rss", "empty-probe": "/review-empty", "failed-probe": "/review-failed-probe"}.get(gate, "/atom")
                return review_result(payload, [review_action("probe_feed", url=url)])
            if gate == "last-round-change":
                return review_result(payload, [review_action("update_rules", rules={**RULES, "exclude_terms": [f"excluded-{payload['round']}"]})])
            if gate == "changed-during-review":
                with engine.transaction() as db:
                    db.execute("DELETE FROM decisions WHERE article_id=1")
            return review_result(payload, stable=True)
        calls = review_turn(engine, scan_id, inspect, disrupt_decisions if gate in ("gaps", "inconsistent") else None)
        if gate not in ("no-probe", "own-probe", "gaps", "inconsistent"):
            calls += review_turn(engine, scan_id, inspect)
        if gate == "last-round-change":
            calls += review_turn(engine, scan_id, inspect)
        result = engine.setup_history(scan_id)
        assert result["phase"] == "needs_attention" and result["reason"], f"Ready gate failed for {gate}"
        assert result["calls_used"] == calls
        if gate in ("gaps", "inconsistent"):
            assert calls == 0
        with patch("engine.run_codex", side_effect=AssertionError("Terminal run spent an extra model call")):
            assert not engine.run_next("model")


def check_setup_limits(directory, base):
    for limit in ("oversized-record", "fifth-page", "insufficient-calls", "failure", "atomic"):
        engine, scan_id = review_fixture(directory, base, f"limit-{limit}")
        if limit in ("fifth-page", "insufficient-calls"):
            seed_review_history(engine, scan_id, 2400 if limit == "fifth-page" else 800, 400)
        if limit == "oversized-record":
            with engine.transaction() as db:
                db.execute("UPDATE articles SET summary=? WHERE id=1", ("é" * 250_000,))
        if limit == "insufficient-calls":
            with engine.transaction() as db:
                db.execute("UPDATE setup_runs SET calls_used=11")
        if limit == "atomic":
            with engine.transaction() as db:
                db.execute("""CREATE TRIGGER reject_setup_completion BEFORE UPDATE ON jobs
                    WHEN NEW.kind='setup_review' AND NEW.status='completed'
                    BEGIN SELECT RAISE(ABORT,'Fixture rejected setup completion'); END""")
        def fail(payload, progress, **kwargs):
            assert limit in ("failure", "atomic"), "Bound preflight spent a model call"
            state = engine.setup_history(scan_id)
            assert state["calls_used"] == state["history"][-1]["calls_used"] == 1, "Call was not reserved before invocation"
            if limit == "failure":
                raise RuntimeError("Fixture CLI failure after durable reservation")
            return review_result(payload, [review_action("update_rules", rules={**RULES, "exclude_terms": ["factory"]}), review_action("probe_feed", url=base + "/atom")])
        calls = review_turn(engine, scan_id, fail)
        state = engine.setup_history(scan_id)
        assert state["phase"] == "needs_attention" and state["history"][-1]["status"] == "failed"
        assert calls == (1 if limit in ("failure", "atomic") else 0)
        assert state["calls_used"] == (11 if limit == "insufficient-calls" else calls)
        if limit == "atomic":
            assert engine.state()["scans"][0]["revision"] == 1 and state["history"][-1]["actions"] == []
            assert len(state["history"]) == 1 and state["round"] == 1, "Plan completion rollback left next-round effects"
        assert not Engine(engine.database, start_worker=False).run_next("model"), "Failed model work retried on restart"
    for violation in ("missing-probe", "unknown-id", "nonfinal-action", "lost-finding", "too-many-actions", "too-many-probes", "remove-all", "too-many-feeds", "other-scan", "output-bytes"):
        engine, scan_id = review_fixture(directory, base, f"schema-{violation}")
        if violation in ("nonfinal-action", "lost-finding"):
            seed_review_history(engine, scan_id, 800, 400)
        def invalid(payload, progress, **kwargs):
            actions = [review_action("probe_feed", url=base + "/atom")]
            findings = payload["findings"]
            if violation == "missing-probe":
                actions = []
            elif violation == "unknown-id":
                findings = [{"id": "fabricated", "article_ids": [99999], "probe_ids": [], "reason": "Invented ID", "resolved": False}]
            elif violation == "lost-finding":
                findings = [{"id": "persist", "article_ids": [1], "probe_ids": [], "reason": "Keep this finding", "resolved": False}] if payload["page_number"] == 1 else []
                actions = []
            elif violation == "too-many-actions":
                actions *= 25
            elif violation == "too-many-probes":
                actions = [review_action("probe_feed", url=base + f"/new-{index}") for index in range(7)]
            elif violation == "remove-all":
                actions.append(review_action("remove_feed", url=base + "/rss"))
            elif violation == "too-many-feeds":
                actions += [review_action("add_feed", url=f"https://existing.example/{index}") for index in range(20)]
            elif violation == "other-scan":
                actions[0]["scan_id"] = scan_id + 1
            elif violation == "output-bytes":
                return {**review_result(payload), "rationale": "é" * 80_001}
            return review_result(payload, actions, findings=findings)
        if violation == "too-many-feeds":
            with engine.transaction() as db:
                other = engine._create(db, "Catalog fixture", "Objective", Rules.parse(RULES), 5)
                for index in range(20):
                    engine._subscribe(db, other, f"https://existing.example/{index}")
            with engine.transaction() as db:
                db.execute("UPDATE jobs SET status='completed' WHERE scan_id=?", (other,))
        assert review_turn(engine, scan_id, invalid) == (2 if violation == "lost-finding" else 1)
        assert engine.setup_history(scan_id)["phase"] == "needs_attention", f"Schema violation accepted for {violation}"
        assert engine.state()["scans"][0]["revision"] == 1 and len(engine.state()["scans"][0]["feeds"]) == 1

    for initial in (False, True):
        if initial:
            engine = Engine(directory / "setup-13-calls.sqlite3", start_worker=False)
            job_id = engine.enqueue("setup", {"topic": "Automatic bounded setup", "objective": "Objective", "feed_urls": [base + "/rss"]})
            def initial_plan(payload, progress):
                with engine.transaction() as db:
                    assert json.loads(db.execute("SELECT payload FROM jobs WHERE id=?", (job_id,)).fetchone()[0])["cli_calls_used"] == 1
                return setup_plan("Automatic bounded setup", base + "/rss")
            with patch("engine.run_codex", side_effect=initial_plan) as calls:
                assert engine.run_next("model") and calls.call_count == 1
            scan_id = engine.state()["scans"][0]["id"]
            assert engine.setup_history(scan_id)["call_limit"] == 13
        else:
            engine, scan_id = review_fixture(directory, base, "12-calls")
        seed_review_history(engine, scan_id, 1800, 350)
        def maximal(payload, progress, **kwargs):
            assert payload["page_count"] == 4
            if payload["page_number"] != 4:
                return review_result(payload)
            if payload["round"] == 1:
                return review_result(payload, [review_action("probe_feed", url=base + "/atom")])
            if payload["round"] == 2:
                return review_result(payload, [review_action("update_rules", rules={**RULES, "exclude_terms": ["unseen exclusion"]})])
            return review_result(payload, stable=True)
        for _ in range(3):
            assert review_turn(engine, scan_id, maximal) == 4
        result = engine.setup_history(scan_id)
        assert result["phase"] == "ready" and result["calls_used"] == result["call_limit"] == 12 + int(initial)
        assert sum(item["calls_used"] for item in result["history"]) == 12
        with patch("engine.run_codex", side_effect=AssertionError("Review exceeded its CLI limit")):
            assert not engine.run_next("model")


def check_setup_cancellation(directory, base):
    for change in ("stop", "edit", "pause", "delete", "new-run", "restart"):
        engine, scan_id = review_fixture(directory, base, f"cancel-{change}")
        seed_review_history(engine, scan_id, 800, 400)
        original = engine.setup_history(scan_id)
        def cancelled(payload, progress, **kwargs):
            assert payload["page_count"] >= 2 and payload["page_number"] == 1
            if change in ("stop", "new-run"):
                engine.control_setup_review({"scan_id": scan_id, "action": "stop"})
                if change == "new-run":
                    engine.control_setup_review({"scan_id": scan_id, "action": "start"})
            elif change == "edit":
                engine.update_scan({"scan_id": scan_id, "objective": "Manual edit wins"})
            elif change == "pause":
                engine.update_scan({"scan_id": scan_id, "status": "paused"})
            elif change == "delete":
                engine.delete_scan({"scan_id": scan_id})
            else:
                Engine(engine.database, start_worker=False)
            return review_result(payload)
        assert review_turn(engine, scan_id, cancelled) == 1, f"Paid another page after {change}"
        state = engine.setup_history(scan_id)
        assert state["lifetime_calls_used"] == 1 and state["history"][-1]["finished_at"]
        assert state["history"][-1]["run_id"] == original["run_id"]
        if change == "new-run":
            assert state["run_id"] != original["run_id"] and state["calls_used"] == 0 and state["phase"] == "collecting"
        else:
            assert state["phase"] in ("stopped", "needs_attention")
            with patch("engine.run_codex", side_effect=AssertionError("Cancelled review retried")):
                assert not engine.run_next("model")
        if change == "delete":
            assert engine.state()["scans"] == [] and engine.state()["article_count"] == 800
    engine, scan_id = review_fixture(directory, base, "queued-wallclock")
    initial = engine.setup_history(scan_id)
    restarted = Engine(engine.database, start_worker=False)
    restarted.control_setup_review({"scan_id": scan_id, "action": "start"})
    assert restarted.setup_history(scan_id) == initial, "Queued restart or repeated Start reset wallclock or spend"
    with patch("engine.run_codex", side_effect=AssertionError("Review ignored its collection window")):
        assert not restarted.run_next("model")
    review_turn(restarted, scan_id, lambda payload, progress, **kwargs: review_result(payload, [review_action("probe_feed", url=base + "/atom")]))
    assert restarted.setup_history(scan_id)["round"] == 2 and restarted.setup_history(scan_id)["calls_used"] == 1
    engine, scan_id = review_fixture(directory, base, "settle-timeout")
    state = engine.setup_history(scan_id)
    at = publication_time(state["next_review_at"]).timestamp() + 302
    with patch("engine.time.time", return_value=at), patch("engine.run_codex", side_effect=AssertionError("Unsettled work invoked model")):
        assert not engine.run_next("model")
    assert engine.setup_history(scan_id)["phase"] == "needs_attention" and engine.setup_history(scan_id)["calls_used"] == 0
    engine, scan_id = review_fixture(directory, base, "cancel-final-effects")
    def final_cancel(payload, progress, **kwargs):
        assert payload["page_count"] == 1
        engine.control_setup_review({"scan_id": scan_id, "action": "stop"})
        return review_result(payload, [review_action("update_rules", rules={**RULES, "exclude_terms": ["chip"]}), review_action("probe_feed", url=base + "/atom")])
    assert review_turn(engine, scan_id, final_cancel) == 1
    assert engine.state()["scans"][0]["revision"] == 1 and engine.setup_history(scan_id)["phase"] == "stopped"
    assert engine.setup_history(scan_id)["history"][-1]["actions"] == []


def check_setup_isolation(directory, base, routes):
    engine, scan_id = review_fixture(directory, base, "isolated-model")
    seed_review_history(engine, scan_id, 800, 400)
    unrelated = create(engine, "Unrelated scan", base + "/rss")
    drain(engine)
    engine.update_scan({"scan_id": unrelated, "status": "paused"})
    entered, release = Event(), Event()
    received = []
    def blocked(payload, progress, **kwargs):
        received.extend(item["article_id"] for item in review_evidence(payload) if item["type"] == "decision")
        if payload["page_number"] == 1:
            entered.set()
            assert release.wait(8), "Setup model fixture was never released"
        final = payload["page_number"] == payload["page_count"]
        return review_result(payload, [review_action("probe_feed", url=base + "/atom")] if final else [])
    state = engine.setup_history(scan_id)
    at = publication_time(state["next_review_at"]).timestamp() + 1
    with patch("engine.time.time", return_value=at), patch("engine.run_codex", side_effect=blocked) as calls:
        assert not engine.run_next("model")
        drain(engine)
        engine.model_worker.start()
        try:
            assert entered.wait(3)
            engine.update_scan({"scan_id": unrelated, "objective": "Independent manual edit"})
            routes["/rss"] = Response(RSS.replace(b"/sport", b"/setup-live-arrival"), etag='"setup-live-arrival"')
            force_due(engine, base + "/rss")
            assert engine.collect_due() == 1
            assert any(record["url"].endswith("/setup-live-arrival") and record["scan_id"] == scan_id for record in engine.state()["records"])
            engine.enqueue("replay", {"scan_id": unrelated})
            drain(engine)
            assert any(record["url"].endswith("/setup-live-arrival") and record["scan_id"] == unrelated for record in engine.state()["records"])
            assert engine.setup_history(scan_id)["phase"] == "reviewing" and not release.is_set()
            release.set()
            wait_for(lambda: engine.setup_history(scan_id)["round"] == 2, "Ordinary arrivals or unrelated edits invalidated setup review")
            assert received == list(range(1, 801)), "Frozen pages chased live arrivals"
            assert calls.call_count in (2, 3) and engine.setup_history(scan_id)["calls_used"] == calls.call_count
        finally:
            release.set()
            stop(engine)
    routes["/rss"] = Response()


def check_setup_job_migration(directory, base):
    path = directory / "old-jobs-check.sqlite3"
    with sqlite3.connect(path) as db:
        db.executescript(SCHEMA.replace(",'setup_review'", ""))
        db.execute("CREATE INDEX fixture_jobs_origin ON jobs(origin)")
        db.execute("INSERT INTO jobs VALUES ('durable','audit','audit',NULL,'{}','user','queued','Retain job','2026-01-01',NULL)")
        db.execute("INSERT INTO jobs VALUES ('finished','setup','setup',NULL,'{}','user','completed','Retain finished','2026-01-01','2026-01-02')")
    engine = Engine(path, start_worker=False)
    with engine.transaction() as db:
        assert [tuple(row) for row in db.execute("SELECT id,status,message FROM jobs ORDER BY rowid")] == [
            ("durable", "queued", "Retain job"), ("finished", "completed", "Retain finished")]
        assert {row["name"] for row in db.execute("PRAGMA index_list(jobs)")} >= {"jobs_active", "fixture_jobs_origin"}
        rejected(lambda: db.execute("INSERT INTO jobs SELECT 'duplicate',key,kind,scan_id,payload,origin,status,message,created_at,finished_at FROM jobs WHERE id='durable'"), error=sqlite3.IntegrityError)
        db.execute("UPDATE jobs SET status='cancelled' WHERE id='durable'")
    scan_id = create(engine, "After jobs migration", base + "/rss")
    engine.control_setup_review({"scan_id": scan_id, "action": "start"})
    assert any(job["kind"] == "setup_review" for job in engine.state()["jobs"])


def check_setup_migration_rollback(directory):
    interrupt = '''
import os, sqlite3, sys
from pathlib import Path
from unittest.mock import patch
from engine import Engine
connect = sqlite3.connect
class Interrupted(sqlite3.Connection):
    def execute(self, sql, *args):
        result = super().execute(sql, *args)
        if sql == 'ALTER TABLE jobs RENAME TO jobs_before_setup_review':
            assert self.in_transaction, 'Migration rename autocommitted'
            if sys.argv[2] == 'crash':
                os._exit(73)
            raise sqlite3.OperationalError('Injected migration failure after rename')
        return result
with patch('engine.sqlite3.connect', side_effect=lambda *a, **k: connect(*a, **k, factory=Interrupted)):
    Engine(Path(sys.argv[1]), start_worker=False)
'''
    for failure in ("exception", "crash"):
        path = directory / f"jobs-migration-{failure}.sqlite3"
        with sqlite3.connect(path) as db:
            db.executescript(SCHEMA.replace(",'setup_review'", ""))
            db.execute("CREATE INDEX fixture_jobs_origin ON jobs(origin)")
            db.execute("INSERT INTO jobs VALUES ('accepted','audit','audit',NULL,'{}','user','queued','Accepted before restart','2026-01-01',NULL)")
        result = subprocess.run([sys.executable, "-c", interrupt, str(path), failure],
            cwd=Path(__file__).resolve().parent, capture_output=True, text=True, timeout=10)
        assert result.returncode == (73 if failure == "crash" else 1), result.stderr
        if failure == "exception":
            assert "Injected migration failure after rename" in result.stderr, result.stderr
        with sqlite3.connect(path) as db:
            assert db.execute("SELECT id,message FROM jobs").fetchall() == [("accepted", "Accepted before restart")]
            assert db.execute("SELECT name FROM sqlite_master WHERE name='jobs_before_setup_review'").fetchone() is None
            assert {row[1] for row in db.execute("PRAGMA index_list(jobs)")} >= {"jobs_active", "fixture_jobs_origin"}
        engine = Engine(path, start_worker=False)
        with engine.transaction() as db:
            assert db.execute("SELECT count(*) FROM jobs WHERE id='accepted'").fetchone()[0] == 1
            rejected(lambda: db.execute("INSERT INTO jobs SELECT 'duplicate',key,kind,scan_id,payload,origin,status,message,created_at,finished_at FROM jobs WHERE id='accepted'"), error=sqlite3.IntegrityError)


def check_setup_settle_resume(directory, base):
    for case in ("lane", "drained", "initial-queued", "initial-running", "initial-legacy", "frozen-queued", "frozen-running"):
        engine, scan_id = review_fixture(directory, base, f"settle-{case}")
        initial = engine.setup_history(scan_id)
        due = publication_time(initial["next_review_at"]).timestamp()
        if not case.startswith("initial"):
            drain(engine)
        if case.startswith("frozen"):
            with patch("engine.time.time", return_value=due + 1):
                assert not engine.run_next("model")
        if case.endswith("running"):
            with engine.transaction() as db:
                db.execute("UPDATE jobs SET status='running' WHERE id=(SELECT id FROM jobs WHERE status='queued' AND kind IN ('poll','replay') ORDER BY rowid LIMIT 1)")
        if case == "initial-legacy":
            with engine.transaction() as db:
                db.execute("ALTER TABLE setup_runs DROP COLUMN settle_deadline")
        current = due + 600
        with patch("engine.time.time", return_value=current), patch("engine.run_codex", side_effect=AssertionError("Unsettled review spent a model call")):
            if case == "lane":
                blocked = engine.enqueue("audit", {})
                with engine.transaction() as db:
                    db.execute("UPDATE jobs SET status='running' WHERE id=?", (blocked,))
                assert not engine.run_next("model")
                with engine.transaction() as db:
                    db.execute("UPDATE jobs SET status='completed' WHERE id=?", (blocked,))
            else:
                engine = Engine(engine.database, start_worker=False)
            assert engine.setup_history(scan_id)["next_review_at"] == initial["next_review_at"]
            assert not engine.run_next("model")
            assert not engine.run_next("model")
            assert engine.setup_history(scan_id)["phase"] == "collecting"
            with engine.transaction() as db:
                deadline = db.execute("SELECT settle_deadline FROM setup_runs").fetchone()[0]
            assert deadline == current + 300, "Resumed or newly frozen work did not get its own settle allowance"
            drain(engine)
            if case.startswith("initial"):
                assert not engine.run_next("model")
                drain(engine)
        with patch("engine.time.time", return_value=current + 299), patch("engine.run_codex", side_effect=lambda payload, progress, **kwargs:
                review_result(payload, [review_action("probe_feed", url=base + "/atom")])) as calls:
            assert engine.run_next("model") and calls.call_count == 1
            assert engine.setup_history(scan_id)["round"] == 2
    engine, scan_id = review_fixture(directory, base, "frozen-work-timeout")
    drain(engine)
    due = publication_time(engine.setup_history(scan_id)["next_review_at"]).timestamp()
    with patch("engine.time.time", return_value=due + 600):
        assert not engine.run_next("model")
    with patch("engine.time.time", return_value=due + 1200):
        engine = Engine(engine.database, start_worker=False)
    with engine.transaction() as db:
        deadline = db.execute("SELECT settle_deadline FROM setup_runs").fetchone()[0]
    assert deadline == due + 1500
    with patch("engine.time.time", return_value=deadline - 1), patch("engine.run_codex", side_effect=AssertionError("Hung replay invoked model")):
        assert not engine.run_next("model") and engine.setup_history(scan_id)["phase"] == "collecting"
    with patch("engine.time.time", return_value=deadline):
        assert not engine.run_next("model")
    assert engine.setup_history(scan_id)["phase"] == "needs_attention" and engine.setup_history(scan_id)["calls_used"] == 0


def check_setup_google_probes(directory, base):
    window = ScanWindow.parse(WINDOW)
    old_window = ScanWindow.parse({"mode": "historical", "start_date": "2019-01-01", "end_date": "2019-01-31"})
    own = query_url("battery recall", window)
    other = query_url("toy recall", window)
    rules = Rules.parse(RULES)
    for original in (other, query_url("toy recall", old_window),
            query_url("toy recall", old_window).replace("news.google.com/", "NEWS.GOOGLE.COM./"),
            query_url("(toy recall) after:2019-01-01 before:2019-02-01")):
        assert review.probe_url(original, window) == other
        assert review.probe_url(review.probe_url(original, window), window) == other
        plan = review.parse_actions([review_action("probe_feed", url=original)], rules, {own}, {own, original}, window)
        assert plan.probes[0]["url"] == other and plan.probes[0]["independent"]
    for subscription in (own, query_url("battery recall", old_window), own.replace("news.google.com/", "news.google.com./")):
        plan = review.parse_actions([review_action("probe_feed", url=own)], rules, {subscription}, {subscription}, window)
        assert not plan.probes[0]["independent"], "Equivalent dated subscription counted as an independent source"
    for variant in (own.replace("%20", "+"), own.replace("hl=en-US&gl=US&ceid=US:en", "gl=CA&ceid=CA:en&hl=en-CA"),
            query_url("((battery recall) after:2019-01-01 before:2019-02-01) after:2020-01-01 before:2020-02-01")):
        assert review.probe_url(variant, window) == own
    longest = query_url("a" * 1000, window)
    assert review.probe_url(longest, window) == longest, "Bounded URL dates consumed the permitted query length"
    for mode in (window, ScanWindow()):
        existing = query_url("a" * 1028, mode)
        assert review.probe_url(existing, mode) == existing
        plan = review.parse_actions([review_action("probe_feed", url=existing)], rules, {existing}, {existing}, mode)
        assert not plan.probes[0]["independent"], "An existing long query changed its source identity"
        rejected(lambda: review.parse_actions([review_action("probe_query", query="a" * 1028)], rules, {own}, {own}, mode))
    for mode in (window, ScanWindow()):
        assert review.probe_url(base + "/rss?after=2019-01-01", mode) == base + "/rss?after=2019-01-01"
        for query in ("recall after:2019-01-01", "recall BEFORE : 2019-02-01"):
            rejected(lambda: review.parse_actions([review_action("probe_query", query=query)], rules, {own}, {own}, mode))
    live = query_url("toy recall after:2019-01-01")
    assert review.probe_url(live, ScanWindow()) == live
    for subscription in (query_url("toy recall"), other, other.replace("news.google.com/", "news.google.com./")):
        plan = review.parse_actions([review_action("probe_feed", url=live)], rules, {subscription}, {subscription}, ScanWindow())
        assert plan.probes[0]["url"] == live and not plan.probes[0]["independent"], "A live dated variant falsely counted as independent"
    engine, scan_id = review_fixture(directory, base, "authorized-dated-probe", **WINDOW)
    with engine.transaction() as db:
        other_scan = engine._create(db, "Authorized historical source", "Inspect other source", rules, 5, window)
        engine._subscribe(db, other_scan, other)
    def inspect(payload, progress, **kwargs):
        assert any(source["url"] == other and source["authorized"] for source in payload["source_catalog"])
        return review_result(payload, [review_action("probe_feed", url=other)])
    assert review_turn(engine, scan_id, inspect) == 1
    with engine.transaction() as db:
        pending = json.loads(db.execute("SELECT pending_probes FROM setup_runs").fetchone()[0])
    assert pending[0]["url"] == other and pending[0]["independent"]


def check_setup_dense_bounds():
    record = {"type": "decision", "article_id": 1, "title": "完整标题é", "summary": "", "source": "安全 source",
        "published_at": "2026-01-01", "feed_url": "https://example.com/rss", "url": "https://example.com/" + "A" * 1100,
        "fetched_at": "2026-01-02", "rule_revision": 3, "decided_at": "2026-01-03", "kept": False,
        "reasons": ["Excluded 安全"], "matched_terms": ["é"]}
    findings = [{"id": f"finding-{index}", "article_ids": [1], "probe_ids": [], "reason": "é" * 2000, "resolved": False} for index in range(10)]
    metadata = {"authorized_feed_urls": [record["feed_url"]], "feeds": [], "manifest": {"probes": []}, "previous_rounds": [{"findings": findings}]}
    evidence, metadata = review.model_input([record], metadata)
    payload = review.context(metadata, evidence, 1, 1, findings)
    available = 450_000 - len(review.encoded(payload["evidence"]))
    evidence[0]["summary"] = "é" * (available // 2) + "x" * (available % 2)
    pages = review.pages(evidence, metadata)
    payload = review.context(metadata, pages[0], 1, 1, findings)
    assert len(review.encoded(payload["evidence"])) == 450_000
    assert len(review.PROMPT.encode()) + len(review.encoded(payload)) <= 600_000
    assert review_evidence(payload) == evidence and payload["findings"] == findings
    assert payload["previous_rounds"][0]["findings"] == findings
    evidence[0]["summary"] += "é"
    rejected(lambda: review.pages(evidence, metadata))
    rejected(lambda: review.context(metadata, evidence, 1, 1, findings))


def check_setup_poll_race(directory, base, routes):
    engine, scan_id = review_fixture(directory, base, "source-during-poll")
    drain(engine)
    entered, release = Event(), Event()
    routes["/rss"] = Response(entered=entered, release=release, etag=None)
    job_id = engine.enqueue("poll", {"scan_id": scan_id})
    worker = Thread(target=lambda: engine.run_next("collector"), daemon=True)
    worker.start()
    try:
        assert entered.wait(3)
        engine.update_scan({"scan_id": scan_id, "feed_url": base + "/atom"})
        engine.control_setup_review({"scan_id": scan_id, "action": "start"})
        release.set()
        worker.join(5)
        assert not worker.is_alive()
        assert next(job for job in engine.state()["jobs"] if job["id"] == job_id)["status"] == "queued", "A cancelled old poll lost the new setup poll request"
        drain(engine)
        scan = engine.state()["scans"][0]
        assert all(feed["last_checked"] for feed in scan["feeds"])
        assert scan["kept"] == 3, "New source was not fetched before setup review"
    finally:
        release.set()
        worker.join(5)
        routes["/rss"] = Response()


def main():
    logging.disable(logging.CRITICAL)
    check_parsing()
    with TemporaryDirectory() as directory, feeds() as (base, routes, requests):
        path = Path(directory)
        check_collection(path, base, routes, requests)
        check_cache_races(path, base, routes, requests)
        check_shared_arrivals(path, base)
        check_shared_arrival_races(path, base, routes)
        check_stale_source_start(path, base, routes)
        check_replay_completion(path, base)
        check_backoff(path, base, routes, requests)
        check_windows(path, base, routes)
        check_races(path, base, routes)
        check_queued_deletion(path, base)
        check_recovery(path, base)
        check_plans(path, base)
        check_lanes(path, base, routes)
        check_audits(path, base, requests)
        check_migration(path, base)
        check_http(path, base)
        check_setup_pages(path, base, routes)
        check_setup_sources(path, base, routes)
        check_setup_gates(path, base, routes)
        check_setup_limits(path, base)
        check_setup_cancellation(path, base)
        check_setup_isolation(path, base, routes)
        check_setup_job_migration(path, base)
        check_setup_migration_rollback(path)
        check_setup_settle_resume(path, base)
        check_setup_google_probes(path, base)
        check_setup_dense_bounds()
        check_setup_poll_race(path, base, routes)
    print(json.dumps({"result": "PASS", "checks": ["RSS and Atom", "unsafe URL rejection", "deterministic decisions", "rule replay", "immutable decision activity", "subscription idempotence", "plan rollback", "audit scan idempotence and inheritance", "stale editor rejection", "restart persistence", "UTC date windows and unknown dates", "finite historical scheduling and manual rerun", "historical setup enforcement", "historical audit inheritance", "queued and inflight deletion", "durable IDs", "stale plan rejection after deletion", "old database migration", "HTTP API and asset safety", "200 and conditional 304", "shared URL deduplication", "Retry-After seconds and HTTP dates", "durable backoff and recovery", "resume and new subscriber cache", "pause removal and ID reuse races", "collection and replay during blocked model", "bounded fetch fairness", "interrupted deterministic recovery", "zero interrupted model retries", "queued model runs once", "atomic accepted creation and downstream requests", "custom feeds in both setup modes", "independent audits off and scheduling", "complete decision audit and diagnostics", "unchanged scheduled decision skips", "decision tools restricted", "partial settings", "historical JSON export", "finite historical diagnostic eligibility", "bounded model context", "inflight 200 and 304 cache races", "atomic model bookkeeping rollback", "cross-source live archive and zero audit gaps", "paused and historical arrival exclusion", "shared arrival status and subscription races", "current arrival rules and subscription additions", "post-body replay requests and final revisions", "running and requeued replay restart", "coalesced replay deletion", "stale source token before fetch",
        "full 1800-record UTF8 review with long URLs, 20 queries and late discarded miss", "live and historical source convergence and probe windows",
        "mandatory independent probes and every ready gate", "page output action finding and context limits",
        "durable reservation and exact 12 or 13 CLI caps", "atomic setup effects history phase and completion rollback",
        "stop edit pause delete replacement and restart between pages", "queued collection wallclock and settling deadline",
        "collector replay and unrelated edits during blocked setup review", "jobs CHECK migration preserves rows and indexes",
        "historical arrivals during collection reach frozen replay", "new source poll survives cancellation of old poll",
        "jobs migration exception and process crash rollback", "late lane and resumed deterministic work get bounded settling",
        "dated Google probe canonicalization and conservative independence", "lossless dense rows six 100-item probes and exact UTF8 boundaries"]}))


if __name__ == "__main__":
    main()
