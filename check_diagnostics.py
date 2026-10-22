from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, redirect_stderr
from http.server import ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from threading import Event, Thread, current_thread
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote_plus, unquote, urlsplit
from urllib.request import Request, urlopen
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
import time

import diagnostics
from diagnostics import AttemptKey, Events, ModelTraces, Rejected
from engine import Engine
import model
import server
import setup_review as review

RULES = {"any_terms": [], "all_terms": [], "exclude_terms": []}


def fails(action, error=ValueError):
    try:
        action()
    except error as exc:
        return exc
    raise AssertionError("Invalid boundary operation succeeded")


def drain(engine):
    for _ in range(100):
        if not engine.run_next("collector"):
            return
    raise AssertionError("Collector did not settle")


def fixture(path, base):
    path.mkdir()
    engine = Engine(path / "archive.sqlite3", start_worker=False)
    scan_id = engine.create_scan({"topic": path.name, "objective": "Retain the complete test evidence", "rules": RULES,
                                  "feed_urls": [base + "/rss"]})
    drain(engine)
    return engine, scan_id


def review_turn(engine, scan_id, boundary):
    engine.control_setup_review({"scan_id": scan_id, "action": "start"})
    drain(engine)
    at = model.publication_time(engine.setup_history(scan_id)["next_review_at"]).timestamp() + 1
    with patch("engine.time.time", return_value=at), patch("engine.run_codex", side_effect=boundary) as calls:
        assert not engine.run_next("model")
        drain(engine)
        assert engine.run_next("model")
    return calls


def trace_records(engine):
    return [json.loads(path.read_text()) for path in engine.traces.root.glob("*.json")]


def break_traces(root):
    root.rename(root.with_name(root.name + "-retained"))
    root.write_text("Storage became unavailable")


def check_existing_sources(directory, base):
    malformed = ["https://news.google.com/rss/search", "https://news.google.com/rss/search?q=",
                 "https://news.google.com/rss/search?q=one&q=two", "https://news.google.com/rss/search?q=one&q=",
                 model.query_url("after:2026-01-01 before:2026-01-02")]
    for url in malformed:
        assert review.source_identity(url) == model.safe_url(url)
        assert review.parse_actions([], model.Rules.parse(RULES), {url}, {url}, model.ScanWindow()).feeds == {url}
        for window in (model.ScanWindow(), model.ScanWindow.parse({"mode": "historical", "start_date": "2026-01-01", "end_date": "2026-01-02"})):
            for tool in ("probe_feed", "add_feed"):
                action = {"tool": tool, "rules": None, "feed_url": url, "query": None, "rationale": "Test strict new sources"}
                fails(lambda: review.parse_actions([action], model.Rules.parse(RULES), {base + "/rss"}, {url}, window))
        engine, scan_id = fixture(directory / f"malformed-{len(list(directory.iterdir()))}", base)
        with engine.transaction() as db:
            db.execute("UPDATE feeds SET url=? WHERE scan_id=?", (url, scan_id))
            db.execute("INSERT OR IGNORE INTO feed_state(url,next_check_at) VALUES (?,0)", (url,))
            original_feed = dict(db.execute("SELECT id,scan_id,url,token FROM feeds WHERE scan_id=?", (scan_id,)).fetchone())
        engine.apply_plan({"rationale": "An empty plan remains valid", "actions": []}, [], "audit", "Fixture")
        action = {"tool": "subscribe_feed", "scan_ref": str(scan_id), "feed_url": url, "rationale": "Keep the existing source"}
        audit_job = engine.enqueue("audit")
        with patch("engine.fetch_feed", return_value=[]), patch("engine.run_codex", return_value={
                "rationale": "Retain existing subscription", "actions": [action]}) as calls:
            assert engine.run_next("model") and calls.call_count == 1
        assert next(job for job in engine.state()["jobs"] if job["id"] == audit_job)["status"] == "completed"
        assert engine.state()["audits"][0]["status"] == "completed"
        assert not engine.run_next("model"), "Existing subscription caused a paid retry"
        with engine.transaction() as db:
            assert dict(db.execute("SELECT id,scan_id,url,token FROM feeds WHERE scan_id=?", (scan_id,)).fetchone()) == original_feed
        other_id = engine.create_scan({"topic": "Other target", "objective": "Keep ownership strict", "rules": RULES,
                                       "feed_urls": [base + "/rss"]})
        before = engine.state()
        fails(lambda: engine.apply_plan({"rationale": "Cross-target is a new subscription", "actions": [
            {**action, "scan_ref": str(other_id)}]}, [{"url": url}], "audit", "Fixture"), Rejected)
        assert engine.state() == before
        for actions in ([], [{"tool": "add_feed", "rules": None, "feed_url": url, "query": None,
                              "rationale": "Already subscribed to this target"}]):
            def retain(payload, progress, **kwargs):
                return {"page_number": payload["page_number"], "rationale": "Keep the source", "stable": False,
                        "findings": [], "actions": actions}
            engine.control_setup_review({"scan_id": scan_id, "action": "start"})
            probe = {"id": 1, "url": base + "/atom", "query": None, "independent": True,
                     "articles": [{**article.__dict__, "window_reason": None} for article in model.fetch_feed(base + "/atom")],
                     "item_count": 1, "in_window_count": 1, "error": None, "executed_at": model.now()}
            with engine.transaction() as db:
                db.execute("UPDATE setup_runs SET probes=? WHERE scan_id=?", (json.dumps([probe]), scan_id))
            with patch("engine.fetch_conditional", return_value=model.FeedFetch([], model.Validators())):
                calls = review_turn(engine, scan_id, retain)
            assert calls.call_count == 1
            traces = trace_records(engine)
            assert all(item["outcome"] == "applied" and "error" not in item for item in traces)
            trace = next(item for item in traces if item["correlation"]["kind"] == "setup_review"
                         and item["response"]["actions"] == actions)
            with engine.transaction() as db:
                assert dict(db.execute("SELECT id,scan_id,url,token FROM feeds WHERE scan_id=?", (scan_id,)).fetchone()) == original_feed
            assert next(job for job in engine.state()["jobs"] if job["id"] == trace["correlation"]["job_id"])["status"] == "completed"
            assert engine.setup_history(scan_id)["calls_used"] == 1
    valid = model.query_url("(" + "term " * 350 + ") after:2026-01-01 before:2026-01-02")
    assert review.source_identity(valid) == review.probe_source(valid)
    fails(lambda: review.source_identity("https://user:secret@example.com/"))


def check_storage(directory):
    path = directory / "storage"
    path.mkdir()
    events = Events(path)
    secret = "secret-fixture-token"
    events.emit("redaction", nested={"authorization": secret, "api_key": secret, "accessToken": secret},
                message=f"Bearer {secret} https://user:{secret}@example.com/rss?api_key={secret}&q=visible")
    store = ModelTraces(path, events)
    with store.open(AttemptKey("job", "audit"), {"objective": "Private success input", "nested": {"cookie": secret}}, {}, "Instructions") as attempt:
        attempt.save(raw={"text": f"Authorization: Bearer {secret}"})
    literal = "https://example.com/rss?q=hello%20world&q=two&hl=en-US"
    assert diagnostics.redact(literal) == literal
    assert secret not in str(diagnostics.redact("Cookie: session=" + secret + "; other=" + secret))
    assert secret not in str(diagnostics.redact("https://example.com/?key=" + secret + "&accessToken=" + secret))
    data = attempt.path.read_text()
    assert "Private success input" in data and secret not in data
    assert secret not in (events.root / "events.jsonl").read_text()
    unicode_message = "café 😀 " + chr(0xD800) + "|" + chr(0xDFFF)
    with store.open(AttemptKey("unicode", "audit"), {"message": unicode_message}, {}, "") as unicode_attempt:
        pass
    unicode_bytes = unicode_attempt.path.read_bytes()
    assert json.loads(unicode_bytes)["input"]["message"] == unicode_message
    assert "café 😀".encode("utf-8") in unicode_bytes and b"\\ud800|\\udfff" in unicode_bytes
    for root in (events.root, store.root):
        assert stat.S_IMODE(root.stat().st_mode) == 0o700
    for target in (attempt.path, events.root / "events.jsonl"):
        assert stat.S_IMODE(target.stat().st_mode) == 0o600
    oversized = fails(lambda: store.open(AttemptKey("large", "audit"), {"input": "x" * 4_000_000}, {}, ""), RuntimeError)
    assert "No model call" in str(oversized)
    oversized = fails(lambda: store.open(AttemptKey("large-surrogates", "audit"),
                      {"input": chr(0xD800) * (4_000_000 // 6 + 1)}, {}, ""), RuntimeError)
    assert "Complete model trace exceeds 4000000 bytes" in str(oversized.__cause__)
    active = store.open(AttemptKey("active", "audit"), {"important": "active"}, {}, "")
    def retain(index):
        with store.open(AttemptKey(str(index), "audit"), {"index": index}, {}, ""):
            pass
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(retain, range(56)))
    assert active.path.exists() and len(list(store.root.glob("*.json"))) == 51
    active.__exit__(None, None, None)
    assert len(list(store.root.glob("*.json"))) == 50
    assert all(json.loads(target.read_text())["outcome"] == "validated" for target in store.root.glob("*.json"))
    orphan = store.root / "interrupted-write.tmp"
    orphan.write_text("x" * 2000)
    orphan.chmod(0o600)
    with patch("diagnostics.TRACE_TOTAL_BYTES", 1600):
        with store.open(AttemptKey("byte-bound", "audit"), {"input": "x" * 600}, {}, "") as bounded:
            pass
        assert sum(target.stat().st_size for target in store.root.glob("*.json")) <= 1600
        assert bounded.path.exists() and not orphan.exists()
    with patch("diagnostics.LOG_BYTES", 1200):
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda i: events.emit("parallel", index=i, message="é" * 100), range(100)))
    logs = list(events.root.glob("events.jsonl*"))
    assert len(logs) == 5
    for target in logs:
        assert target.stat().st_size <= 1200 and stat.S_IMODE(target.stat().st_mode) == 0o600
        for line in target.read_bytes().splitlines():
            assert json.loads(line)["event"] == "parallel"
    events.emit("bounded", message="é" * 20_000, nested=["x" * 20_000] * 3)
    line = (events.root / "events.jsonl").read_bytes().splitlines()[-1]
    assert len(line) + 1 <= 16_384 and json.loads(line)["truncated"]
    events.emit("bounded_surrogates", message=chr(0xD800) * 20_000)
    line = (events.root / "events.jsonl").read_bytes().splitlines()[-1]
    assert len(line) + 1 <= 16_384 and json.loads(line)["truncated"]
    assert json.loads(line)["original_bytes"] > 16_384
    victim = path / "victim"
    victim.write_text("unchanged")
    (events.root / "events.jsonl").unlink()
    (events.root / "events.jsonl").symlink_to(victim)
    events.emit("unsafe_event_target")
    assert victim.read_text() == "unchanged"
    traces = path / "unsafe"
    traces.mkdir()
    (traces / "diagnostics").symlink_to(events.root, target_is_directory=True)
    unsafe = ModelTraces(traces, Events(traces))
    fails(lambda: unsafe.open(AttemptKey("unsafe", "audit"), {}, {}, ""), RuntimeError)
    with store.open(AttemptKey("preserve-original", "audit"), {}, {}, "") as original:
        original.save(stage="page_validation")
    attempt = store.open(AttemptKey("failed-finalization", "audit"), {}, {}, "")
    break_traces(store.root)
    exception = Rejected("Original rejection", path="findings[2].article_ids", expected=[1], received=[9])
    def reject():
        with attempt:
            raise exception
    assert fails(reject) is exception


def check_failed_write_cleanup(directory):
    root = directory / "failed-write-cleanup"
    root.mkdir()
    store = ModelTraces(root, Events(root))
    failed = Event()
    resume = Event()
    finished = Event()
    overlapping = Event()
    results = {}
    original_fsync, original_emergency, original_stat = os.fsync, diagnostics.emergency, Path.stat
    failure = OSError("Controlled failed fsync")
    attempt = diagnostics.ModelAttempt(store, AttemptKey("writer-a", "audit"))
    temporary = attempt.path.with_suffix(".tmp")

    def fsync(fd):
        if current_thread().name == "writer-a":
            raise failure
        original_fsync(fd)

    def emergency(event, **fields):
        if current_thread().name == "writer-a" and event == "model_trace_write_failed":
            failed.set()
            assert resume.wait(5), "Concurrent prune did not start"
        original_emergency(event, **fields)

    def stat(path, *args, **kwargs):
        if current_thread().name == "writer-b" and path == temporary and not overlapping.is_set():
            overlapping.set()
            resume.set()
            # Cleanup may wait for pruning's lock, so allow pruning to finish either way.
            finished.wait(0.2)
        return original_stat(path, *args, **kwargs)

    def writer_a():
        try:
            attempt.save(input={"objective": "Failed writer input"})
        except Exception as exc:
            results["a"] = exc
        finally:
            finished.set()

    def writer_b():
        try:
            with store.open(AttemptKey("writer-b", "audit"), {"objective": "Concurrent writer input"}, {}, "") as other:
                results["b_path"] = other.path
        except Exception as exc:
            results["b"] = exc

    a = Thread(target=writer_a, name="writer-a")
    b = Thread(target=writer_b, name="writer-b")
    captured = StringIO()
    try:
        with redirect_stderr(captured), patch("diagnostics.os.fsync", fsync), patch("diagnostics.emergency", emergency), patch.object(Path, "stat", stat):
            a.start()
            assert failed.wait(5), "Failed fsync was not observed"
            b.start()
            a.join(5)
            b.join(5)
            assert not a.is_alive() and not b.is_alive(), "Concurrent writers did not finish"
    finally:
        resume.set()
        for thread in (a, b):
            if thread.ident is not None:
                thread.join(5)
        with diagnostics._TRACE_LOCK:
            diagnostics._ACTIVE.discard(attempt.path)
    assert overlapping.is_set() and results.get("a") is failure
    assert "b" not in results, (results.get("b"), getattr(results.get("b"), "__cause__", None))
    record = json.loads(results["b_path"].read_bytes())
    assert record["outcome"] == "validated" and record["input"]["objective"] == "Concurrent writer input"
    assert not list(store.root.glob("*.tmp"))
    events = [json.loads(line) for line in captured.getvalue().splitlines()]
    assert [event["attempt_id"] for event in events if event["event"] == "model_trace_write_failed"] == [attempt.id]
    assert not any(event["event"] in ("model_trace_cleanup_failed", "model_trace_retention_failed") for event in events)
    with store.open(AttemptKey("active-byte-limit", "audit"), {"objective": "x" * 600}, {}, "") as active:
        retained = active.path.read_bytes()
        with patch("diagnostics.TRACE_TOTAL_BYTES", len(retained) + 1):
            rejected = fails(lambda: store.open(AttemptKey("over-limit", "audit"), {}, {}, ""), RuntimeError)
        assert "Active model traces exceed" in str(rejected.__cause__)
        assert active.path.read_bytes() == retained
        assert not list(store.root.glob("*.tmp"))


@contextmanager
def fake_cli(directory, mode):
    path = directory / f"fake-codex-{mode}"
    path.write_text("#!" + sys.executable + "\n" + '''import json, os, pathlib, stat, sys, time
args = sys.argv
output = pathlib.Path(args[args.index("--output-last-message") + 1])
assert stat.S_ISFIFO(output.stat().st_mode), "CLI output was not bounded by a pipe"
prompt = sys.stdin.buffer.read()
mode = ''' + repr(mode) + '''
if mode == "redaction":
    objective = json.loads(prompt.decode().split("CONTEXT JSON:\\n", 1)[1])["objective"]
    sys.stderr.write(objective + "\\n")
    sys.stderr.flush()
sys.stdout.buffer.write(("stdout-prefix pid=" + str(os.getpid()) + "\\n").encode() + b"o" * 90000)
sys.stdout.flush()
sys.stderr.buffer.write(b"Authorization: Bearer secret-cli-token\\n" + b"e" * 90000)
sys.stderr.flush()
if mode == "nonzero":
    output.write_text('{"rejected":"nonzero raw response"}')
    sys.exit(7)
if mode == "timeout":
    output.write_text('partial timeout response')
    print("partial timeout stdout", flush=True)
    time.sleep(60)
if mode == "invalid":
    output.write_text('{"rationale":')
elif mode == "array":
    output.write_text('[]')
elif mode == "oversize":
    output.write_text("x" * 300000)
else:
    output.write_text(json.dumps({"rationale": objective if mode == "redaction" else "Safe fake CLI result", "actions":[]}))
''')
    path.chmod(0o700)
    with patch("model.codex_path", return_value=str(path)):
        yield


def check_assignment_redaction(directory):
    root = directory / "assignment-redaction"
    root.mkdir()
    names = ("OPENAI_API_KEY", "AWS_SECRET_ACCESS_KEY", "client_secret", "tenant-token", "tenant_auth",
             "tenant_signature", "client_session_id", "accessToken", "db.password", "db.passwd", "db.token", "db.secret",
             "db.password.value", "db.passwd.value", "db.token.value", "db.secret.value")
    fields = {name: f"opaque-assignment-{index}" for index, name in enumerate(names)}
    benign = {"stable_key": "article-visible-42", "topic_key": "scan-visible-7", "db.monkey": "visible-monkey",
              "db.tokenizer": "visible-tokenizer", "db.secretary": "visible-secretary", "db.passwordless": "visible-passwordless"}
    assignments = []
    for name, secret in fields.items():
        quoted = json.dumps({name: secret + ' "private" \\ tail', "safe": "visible-neighbor"})
        escaped_name = f"\\u{ord(name[0]):04x}" + name[1:]
        escaped = quoted.replace(name, escaped_name)
        assignments.extend((f"{name}={secret}", f"'{name}'='{secret}'", f"'{escaped_name}'='{secret}'",
                            quoted, escaped, json.dumps(escaped), json.dumps(escaped)[1:-1], json.dumps({"detail": escaped}),
                            json.dumps({"detail": f"Cookie: first={secret}; second={secret}"})))
    neighbors = "safe_neighbor=visible-neighbor monkey=visible-neighbor tokenizer=visible-neighbor"
    neighbors += " " + " ".join(f"{name}={value}" for name, value in benign.items())
    neighbors += " Press Secretary: Taylor said the Secretary announced a new policy."
    message = "\n".join(assignments) + "\n" + neighbors
    url = "https://example.com/?" + "&".join(f"%{ord(name[0]):02x}{name[1:]}={secret}" for name, secret in fields.items())
    url += "&" + "&".join(f"{name}={value}" for name, value in benign.items()) + "&q=visible-neighbor"
    payload = {"objective": message, "fields": fields, "source": url, "safe": "visible-neighbor", "benign": benign}
    events = Events(root)
    store = ModelTraces(root, events)
    captured = StringIO()
    with redirect_stderr(captured), fake_cli(root, "redaction"):
        events.emit("assignment_redaction", message=message, fields=fields, source=url, safe="visible-neighbor", benign=benign)
        with store.open(AttemptKey("assignment-redaction", "audit"), payload, model.PLAN_SCHEMA, model.MODEL_PROMPT) as attempt:
            result = model.run_codex(payload, lambda _: None, attempt=attempt)
    assert result == {"rationale": message, "actions": []}
    record = json.loads(attempt.path.read_bytes())
    event = json.loads((events.root / "events.jsonl").read_bytes().splitlines()[0])
    artifacts = (captured.getvalue(), (events.root / "events.jsonl").read_text(), attempt.path.read_text())
    assert all(secret not in artifact for secret in fields.values() for artifact in artifacts)
    for stored in (record["input"], event):
        assert stored["fields"] == dict.fromkeys(names, "[REDACTED]") and stored["safe"] == "visible-neighbor"
        assert stored["benign"] == benign
        assert parse_qs(urlsplit(stored["source"]).query) == {
            **dict.fromkeys(names, ["[REDACTED]"]), **{name: [value] for name, value in benign.items()}, "q": ["visible-neighbor"]}
        assert stored["source"].endswith("&q=visible-neighbor")
    for stored in (record["stdin"], record["raw"]["text"], record["stderr"]["text"], event["message"]):
        assert all(name in stored for name in names) and neighbors in stored
    assert record["response"]["actions"] == [] and record["outcome"] == "validated"


def check_identifier_redaction(directory):
    root = directory / "identifier-redaction"
    root.mkdir()
    identifiers = {"stable_key": "article-visible-42", "topic_key": "scan-visible-7"}
    names = ("key", "token", "auth", "sig", "api_key", "access-key", "privateKey", "signing_key", "encryption_key",
             "secret_key", "tenant_api_key", "tenant-access-key", "tenant_private_key", "tenant-signing-key",
             "tenant_encryption_key", "tenant_secret_key", "app_key", "client_key", "license_key", "subscription_key",
             "app-key", "client-key", "license-key", "subscription-key", "tenantServiceKey", "clientSecret",
             "tenantPassword", "tenantCookie", "tenantCredential", "tenantAuth", "secrets", "credentials", "cookies", "passwords")
    credentials = {name: f"opaque-credential-{index}" for index, name in enumerate(names)}
    detail = {"identifiers": identifiers, "fields": credentials}
    message = json.dumps({**detail, "nested": json.dumps(detail)})
    url = "https://example.com/?stable_key=article-visible-42&topic_key=scan-visible-7&" + "&".join(
        f"{name}={value}" for name, value in credentials.items())
    payload = {"objective": message, **detail, "source": url,
               "feeds": [{"stable_key": identifiers["stable_key"], "token": "opaque-feed-nonce"}]}
    events = Events(root)
    store = ModelTraces(root, events)
    captured = StringIO()
    with redirect_stderr(captured), fake_cli(root, "redaction"):
        events.emit("identifier_redaction", message=message, **detail, source=url)
        with store.open(AttemptKey("identifier-redaction", "audit"), payload, model.PLAN_SCHEMA, model.MODEL_PROMPT) as attempt:
            result = model.run_codex(payload, lambda _: None, attempt=attempt)
    assert result == {"rationale": message, "actions": []}
    record = json.loads(attempt.path.read_bytes())
    event = json.loads((events.root / "events.jsonl").read_bytes().splitlines()[0])
    artifacts = (captured.getvalue(), (events.root / "events.jsonl").read_text(), attempt.path.read_text())
    assert all(secret not in artifact for secret in (*credentials.values(), "opaque-feed-nonce") for artifact in artifacts)
    stdin = json.loads(record["stdin"].split("CONTEXT JSON:\n", 1)[1])
    assert stdin == record["input"]
    assert stdin["feeds"] == [{"stable_key": identifiers["stable_key"], "token": "[REDACTED]"}]
    for stored in (record["input"], event):
        assert stored["identifiers"] == identifiers
        assert stored["fields"] == dict.fromkeys(names, "[REDACTED]")
        assert stored["source"].startswith("https://example.com/?stable_key=article-visible-42&topic_key=scan-visible-7&")
    messages = (record["input"]["objective"], stdin["objective"], json.loads(record["raw"]["text"])["rationale"],
                record["stderr"]["text"].splitlines()[0], record["response"]["rationale"], event["message"])
    for stored in messages:
        decoded = json.loads(stored)
        for item in (decoded, json.loads(decoded["nested"])):
            assert item["identifiers"] == identifiers
            assert item["fields"] == dict.fromkeys(names, "[REDACTED]")
    assert record["outcome"] == "validated"


def check_compact_credentials(directory):
    root = directory / "compact-credentials"
    root.mkdir()
    names = ("clientsecret", "appsecret", "apisecret", "secretkey", "SECRETKEY", "dbpassword", "PGPASSWORD",
             "adminpassword", "password1", "password2", "user_password1", "secret2", "sessioncookie", "authcookie",
             "accesstoken", "Client Secret", "Password Hash")
    benign = {"stable_key": "article-visible", "topic_key": "scan-visible", "secretary": "visible-secretary"}
    prose = "Press Secretary: Taylor said the secretary announced a new policy."
    events = Events(root)
    store = ModelTraces(root, events)
    samples = []
    with redirect_stderr(StringIO()):
        for index, name in enumerate(names):
            secret = f"opaque-compact-{index}-private"
            quoted = json.dumps({name: secret, "safe": "visible-neighbor"})
            escaped_name = "".join(f"\\u{ord(char):04x}" for char in name)
            escaped = quoted.replace(name, escaped_name)
            double = json.dumps(escaped)
            message = "\n".join((f"'{name}'='{secret}'", quoted, escaped, double, json.dumps(double),
                                 repr({name: secret}), r"\n" + quoted, prose))
            if " " not in name:
                message += f"\n{name}={secret}\n\\{name}={secret}"
            sample = {"fields": {name: secret}, "message": message, "benign": benign,
                      "source": f"https://example.com/rss?{quote_plus(name)}={secret}&q=visible"}
            samples.append(sample)
            events.emit("compact_credentials", **sample)
        with store.open(AttemptKey("compact-credentials", "audit"), {"samples": samples}, {}, "") as attempt:
            attempt.save(response={"samples": samples})
    event_text = (events.root / "events.jsonl").read_text()
    trace_text = attempt.path.read_text()
    records = [json.loads(line) for line in event_text.splitlines() if json.loads(line)["event"] == "compact_credentials"]
    trace = json.loads(trace_text)
    assert len(records) == len(names) and trace["outcome"] == "validated"
    for index, name in enumerate(names):
        assert f"opaque-compact-{index}-private" not in event_text + trace_text, name
        for sample in (records[index], trace["input"]["samples"][index], trace["response"]["samples"][index]):
            assert sample["fields"] == {name: "[REDACTED]"}
            assert sample["benign"] == benign and prose in sample["message"]
            assert parse_qs(urlsplit(sample["source"]).query) == {name: ["[REDACTED]"], "q": ["visible"]}
            assert "visible-neighbor" in sample["message"]


def store_escape_boundaries(directory, limit):
    root = directory / f"escape-boundaries-{limit}"
    root.mkdir()
    secret = "opaque-escape-private"
    suffix = f'"{secret}"'
    opening = "token=" + "\\" * (limit - len("token=") - len(suffix)) + suffix
    closing = 'token="' + secret + "\\" * (limit - len('token="') - len(secret) - 1) + '"'
    name_suffix = "token=" + secret
    unicode_name = (r"\u0061" * ((limit - len(name_suffix)) // 6)
                    + "a" * ((limit - len(name_suffix)) % 6) + name_suffix)
    unicode_run = r"\u0061" * (limit // 6) + "x" * (limit % 6)
    repeated = "token=a " * (limit // 8) + "x" * (limit % 8)
    cases = {"slashes": "\\" * limit, "opening": opening, "closing": closing,
             "unicode_name": unicode_name, "unicode_run": unicode_run, "repeated": repeated}
    assert all(len(value.encode()) == limit for value in cases.values())
    events = Events(root)
    store = ModelTraces(root, events)
    with redirect_stderr(StringIO()):
        with store.open(AttemptKey(f"escape-{limit}", "audit"), {}, {}, "") as attempt:
            attempt.save(**{name: {"text": value, "retained_bytes": limit} for name, value in cases.items()})
        events.emit("escape_boundaries", **cases)
    event_bytes = (events.root / "events.jsonl").read_bytes()
    trace_bytes = attempt.path.read_bytes()
    assert secret.encode() not in event_bytes + trace_bytes
    record = json.loads(trace_bytes)
    assert record["outcome"] == "validated"
    for name in ("slashes", "unicode_run"):
        assert record[name]["text"] == cases[name]
    assert "token=a" not in record["repeated"]["text"] and "[REDACTED]" in record["repeated"]["text"]
    assert all(record[name]["retained_bytes"] == limit for name in cases)
    event_lines = event_bytes.splitlines()
    assert all(len(line) + 1 <= diagnostics.EVENT_BYTES for line in event_lines)
    event = next(json.loads(line) for line in event_lines if json.loads(line)["event"] == "escape_boundaries")
    assert event["truncated"]


def check_escape_boundaries(directory):
    script = ("import sys; from pathlib import Path; sys.path.insert(0, sys.argv[1]); "
              "from check_diagnostics import store_escape_boundaries; "
              "store_escape_boundaries(Path(sys.argv[2]), int(sys.argv[3]))")
    for limit in (diagnostics.STREAM_BYTES, diagnostics.RAW_BYTES):
        result = subprocess.run([sys.executable, "-I", "-B", "-c", script, str(Path(__file__).resolve().parent),
                                 str(directory), str(limit)], capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, (limit, result.stdout, result.stderr)


def check_long_evidence(directory, base):
    root = directory / "long-evidence"
    root.mkdir()
    evidence = {"colon": "Fingerprint " + "ab:" * 600, "equals": "Comparison " + "ab=" * 600,
                "prose": "Press Secretary: Taylor said the Secretary announced a new policy.",
                "stable_key": "visible-article", "topic_key": "visible-scan"}
    secret = r"opaque-long-evidence\value"
    embedded = evidence["colon"] + f" client_key={secret} " + evidence["equals"] + f"\ntoken={secret}"
    message = json.dumps({**evidence, "embedded": embedded})
    payload = {"objective": message, **evidence}
    events = Events(root)
    store = ModelTraces(root, events)
    captured = StringIO()
    with redirect_stderr(captured), fake_cli(root, "redaction"):
        events.emit("long_evidence", **payload)
        with store.open(AttemptKey("long-evidence", "audit"), payload, model.PLAN_SCHEMA, model.MODEL_PROMPT) as attempt:
            result = model.run_codex(payload, lambda _: None, attempt=attempt)
    assert result == {"rationale": message, "actions": []}
    record = json.loads(attempt.path.read_bytes())
    event = json.loads((events.root / "events.jsonl").read_bytes().splitlines()[0])
    stdin = json.loads(record["stdin"].split("CONTEXT JSON:\n", 1)[1])
    assert stdin == record["input"] and record["outcome"] == "validated"
    messages = (record["input"]["objective"], stdin["objective"], json.loads(record["raw"]["text"])["rationale"],
                record["stderr"]["text"].splitlines()[0], record["response"]["rationale"], event["objective"])
    for stored in messages:
        assert json.loads(stored) == {**evidence, "embedded": embedded.replace(secret, "[REDACTED]")}
    for stored in (record["input"], event):
        assert all(stored[name] == value for name, value in evidence.items())
    assert all("opaque-long-evidence" not in artifact for artifact in (captured.getvalue(), attempt.path.read_text(),
                                                                      (events.root / "events.jsonl").read_text()))
    engine, scan_id = fixture(directory / "long-evidence-jobs", base)
    with engine.transaction() as db:
        db.execute("UPDATE articles SET summary=?", (embedded + " " + evidence["prose"],))
    audit_id = engine.enqueue("decision_audit")
    with patch("engine.run_codex", return_value={"rationale": "Review complete", "actions": []}) as calls:
        assert engine.run_next("model") and calls.call_count == 1
    assert next(job for job in engine.state()["jobs"] if job["id"] == audit_id)["status"] == "completed"
    expected_summary = embedded.replace(secret, "[REDACTED]") + " " + evidence["prose"]
    assert all(article["summary"] == expected_summary for article in trace_records(engine)[0]["input"]["history"])
    engine.control_setup_review({"scan_id": scan_id, "action": "start"})
    probe = {"id": 1, "url": base + "/atom", "query": None, "independent": True,
             "articles": [{**article.__dict__, "window_reason": None} for article in model.fetch_feed(base + "/atom")],
             "item_count": 1, "in_window_count": 1, "error": None, "executed_at": model.now()}
    with engine.transaction() as db:
        db.execute("UPDATE setup_runs SET probes=? WHERE scan_id=?", (json.dumps([probe]), scan_id))
    def page(payload, progress, **kwargs):
        assert evidence["colon"] in json.dumps(payload) and evidence["equals"] in json.dumps(payload)
        return {"page_number": payload["page_number"], "rationale": "Page complete", "stable": False,
                "findings": [], "actions": []}
    calls = review_turn(engine, scan_id, page)
    assert calls.call_count == 1 and engine.setup_history(scan_id)["calls_used"] == 1
    assert all(job["status"] == "completed" for job in engine.state()["jobs"] if job["kind"] == "setup_review")
    assert not engine.run_next("model"), "Long evidence caused an automatic model retry"


def check_nested_url_redaction(directory):
    root = directory / "nested-url-redaction"
    root.mkdir()
    secret = "nested-fixture-secret"
    urls = [
        f"https://example.com/rss?next=https://user:{secret}@evil.test/x&q=visible&q=&blank=&flag",
        f"https://example.com/rss?next=https%3A%2F%2Fuser%3A{secret}%40evil.test%2Fx&q=hello%20world&q=&blank=&flag#keep",
        f"https://example.com/rss?next=https%3a%2f%2f{secret}%40evil.test%2fx&q=hello+world",
        f"https://example.com/rss?next=visit+https%3A%2F%2Fuser%3A{secret}%40evil.test%2Fx+now&q=visible",
        f"https://example.com/rss?next=https%3A%2F%2Fevil.test%2Fx%3Fclient_secret%3D{secret}%26q%3Dvisible&q=outer",
        f"https://example.com/rss?client%5Fsecret={secret}&next=https%3a%2f%2fevil.test%2fx&q=visible",
    ]
    harmless = [
        "https://example.com/rss?next=https%3a%2f%2fevil.test%2fx%3fq%3dhello%2520world&q=one%20two&q=&blank=&flag&&plus=%2b#keep",
        "https://example.com/rss?next=https://evil.test/x&q=hello+world&q=&blank=&flag",
        "https://news.google.com/rss/search?q=chip%20after%3A2026-01-01&q=&hl=en-US&gl=US&ceid=US%3Aen",
        "HTTPS://Example.COM/rss?", "https://example.com/rss?#", "https://example.com/rss#",
    ]
    message = json.dumps({"urls": urls, "harmless": harmless})
    payload = {"objective": message, "feed_urls": urls, "harmless": harmless}
    events = Events(root)
    store = ModelTraces(root, events)
    captured = StringIO()
    with redirect_stderr(captured), fake_cli(root, "redaction"):
        events.emit("feeds_saved", **payload)
        with store.open(AttemptKey("nested-url-redaction", "audit"), payload, model.PLAN_SCHEMA, model.MODEL_PROMPT) as attempt:
            result = model.run_codex(payload, lambda _: None, attempt=attempt)
    assert result == {"rationale": message, "actions": []}
    artifacts = (captured.getvalue(), (events.root / "events.jsonl").read_text(), attempt.path.read_text())
    assert all(secret not in unquote(artifact) for artifact in artifacts), "Nested URL userinfo leaked into stored diagnostics"
    record = json.loads(attempt.path.read_bytes())
    event = json.loads((events.root / "events.jsonl").read_bytes().splitlines()[0])
    stdin = json.loads(record["stdin"].split("CONTEXT JSON:\n", 1)[1])
    assert stdin == record["input"] and record["outcome"] == "validated"
    for stored in (record["input"], event):
        assert stored["harmless"] == harmless
        assert stored["feed_urls"][0].endswith("&q=visible&q=&blank=&flag")
        assert stored["feed_urls"][1].endswith("&q=hello%20world&q=&blank=&flag#keep")
        assert stored["feed_urls"][2].endswith("&q=hello+world")
        for url in stored["feed_urls"][:3]:
            assert parse_qs(urlsplit(url).query)["next"] == ["https://evil.test/x"]
        assert parse_qs(urlsplit(stored["feed_urls"][3]).query)["next"] == ["visit https://evil.test/x now"]
        inner = parse_qs(urlsplit(stored["feed_urls"][4]).query)["next"][0]
        assert parse_qs(urlsplit(inner).query) == {"client_secret": ["[REDACTED]"], "q": ["visible"]}
        assert stored["feed_urls"][5].endswith("&next=https%3a%2f%2fevil.test%2fx&q=visible")
    messages = (record["input"]["objective"], stdin["objective"], json.loads(record["raw"]["text"])["rationale"],
                record["stderr"]["text"].splitlines()[0], record["response"]["rationale"], event["objective"])
    for stored in messages:
        assert json.loads(stored) == {"urls": event["feed_urls"], "harmless": harmless}


def check_cli(directory):
    root = directory / "cli"
    root.mkdir()
    store = ModelTraces(root, Events(root))
    for mode in ("success", "invalid", "array", "oversize", "nonzero", "timeout"):
        attempt = store.open(AttemptKey(mode, "audit"), {"objective": "Keep this exact successful input", "token": "secret-input-token"}, model.PLAN_SCHEMA, model.MODEL_PROMPT)
        error = None
        with fake_cli(root, mode), patch("model.CODEX_TIMEOUT", 0.5 if mode == "timeout" else 8):
            try:
                with attempt:
                    result = model.run_codex({"objective": "Keep this exact successful input", "token": "secret-input-token"}, lambda _: None, attempt=attempt)
                    assert result["actions"] == []
            except (RuntimeError, ValueError) as exc:
                error = exc
        assert (error is None) == (mode == "success"), (mode, error)
        record = json.loads(attempt.path.read_text())
        assert record["input"]["objective"] == "Keep this exact successful input"
        assert "secret-cli-token" not in attempt.path.read_text() and "secret-input-token" not in attempt.path.read_text()
        assert record["stdout"]["total_bytes"] >= 90000 and record["stdout"]["retained_bytes"] == 65536 and record["stdout"]["truncated"]
        assert record["stderr"]["retained_bytes"] == 65536 and record["stderr"]["truncated"]
        pid = int(re.search(r"pid=(\d+)", record["stdout"]["text"])[1])
        fails(lambda: os.kill(pid, 0), ProcessLookupError)
        temporary = Path(record["command"][record["command"].index("--cd") + 1])
        assert not temporary.exists(), "CLI temporary directory survived"
        assert record["stdin"].endswith('"token":"[REDACTED]"}')
        if mode == "oversize":
            assert record["raw"]["total_bytes"] == 300000 and record["raw"]["retained_bytes"] == 262144 and record["raw"]["truncated"]
        if mode == "invalid":
            assert record["raw"]["text"] == '{"rationale":' and record["error"]["details"]["path"] == "$"
            assert record["error"]["details"]["offset"] == 13
        if mode == "timeout":
            assert record["cli"]["timed_out"] and record["raw"]["text"] == "partial timeout response"
        if mode == "nonzero":
            assert record["cli"]["returncode"] == 7 and "nonzero raw response" in record["raw"]["text"]

    attempt = store.open(AttemptKey("missing-cli", "audit"), {}, model.PLAN_SCHEMA, model.MODEL_PROMPT)
    with patch("model.codex_path", return_value=str(root / "nonexistent-cli")):
        def missing():
            with attempt:
                model.run_codex({}, lambda _: None, attempt=attempt)
        fails(missing, FileNotFoundError)
    record = json.loads(attempt.path.read_text())
    assert record["cli"]["error_type"] == "FileNotFoundError" and record["raw"]["total_bytes"] == 0
    assert not Path(record["command"][record["command"].index("--cd") + 1]).exists()
    attempt = store.open(AttemptKey("missing-cli-storage-failed", "audit"), {}, model.PLAN_SCHEMA, model.MODEL_PROMPT)
    original_popen = model.subprocess.Popen
    original_failures = []
    def break_storage_on_launch_error(*args, **kwargs):
        try:
            return original_popen(*args, **kwargs)
        except FileNotFoundError as exc:
            original_failures.append(exc)
            break_traces(store.root)
            raise
    with patch("model.codex_path", return_value=str(root / "nonexistent-cli")), patch("model.subprocess.Popen", side_effect=break_storage_on_launch_error):
        assert fails(missing, FileNotFoundError) is original_failures[0]
    assert not Path(attempt.record["command"][attempt.record["command"].index("--cd") + 1]).exists()


def check_engine_failures(directory, base):
    for mode in ("initial", "required", "committed"):
        engine, scan_id = fixture(directory / f"io-{mode}", base)
        job_id = engine.enqueue("setup", {"topic": "New scan", "objective": "Fixture", "feed_urls": [base + "/rss"]})
        def boundary(payload, progress, *, attempt):
            if mode == "required":
                break_traces(engine.traces.root)
            return {"rationale": "An applied scan", "actions": [
                {"tool": "create_scan", "scan_ref": "new", "topic": "New scan", "objective": "Fixture", "rules": RULES,
                 "poll_interval_minutes": 5, "rationale": "Create"},
                {"tool": "subscribe_feed", "scan_ref": "new", "feed_url": base + "/rss", "rationale": "Subscribe"}]}
        if mode == "initial":
            engine.traces.root.write_text("Unavailable trace storage")
        original_connect = sqlite3.connect
        triggered = []
        class CommitFailure(sqlite3.Connection):
            def __exit__(self, *args):
                result = super().__exit__(*args)
                if mode == "committed" and args[0] is None and not triggered:
                    row = self.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
                    if row is not None and row[0] == "completed":
                        triggered.append(True)
                        break_traces(engine.traces.root)
                return result
        def connect(*args, **kwargs):
            return original_connect(*args, factory=CommitFailure, **kwargs)
        captured = StringIO()
        with redirect_stderr(captured), patch("engine.run_codex", side_effect=boundary) as calls, patch("engine.sqlite3.connect", side_effect=connect):
            assert engine.run_next("model")
            assert calls.call_count == (0 if mode == "initial" else 1)
        state = engine.state()
        job = next(item for item in state["jobs"] if item["id"] == job_id)
        assert job["status"] == ("completed" if mode == "committed" else "failed")
        assert len(state["scans"]) == (2 if mode == "committed" else 1)
        with engine.transaction() as db:
            payload = json.loads(db.execute("SELECT payload FROM jobs WHERE id=?", (job_id,)).fetchone()[0])
        assert payload.get("cli_calls_used", 0) == (0 if mode == "initial" else 1)
        if mode == "committed":
            assert triggered and '"event":"model_trace_outcome_unconfirmed"' in captured.getvalue()
            assert '"application_committed":true' in captured.getvalue()
        assert not engine.run_next("model"), "Failed or completed model work retried"
        drain(engine)
        assert len(engine.state()["records"]) >= 2
    engine, scan_id = fixture(directory / "review-preflight", base)
    engine.traces.root.write_text("Unavailable trace storage")
    calls = review_turn(engine, scan_id, lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Spent without required trace")))
    assert not calls.called and engine.setup_history(scan_id)["calls_used"] == 0
    engine, scan_id = fixture(directory / "collector-independent", base)
    (engine.events.root / "events.jsonl").unlink()
    engine.events.root.rmdir()
    engine.events.root.write_text("No diagnostic directory available")
    engine.enqueue("replay", {"scan_id": scan_id})
    drain(engine)
    assert len(engine.state()["records"]) == 2


def check_rejections(directory, base):
    engine, scan_id = fixture(directory / "rejected-page", base)
    def bad(payload, progress, **kwargs):
        return {"page_number": payload["page_number"], "rationale": "Rejected raw finding", "stable": False,
                "findings": [{"id": "f", "article_ids": [999, 999], "probe_ids": [], "reason": "Bad references", "resolved": False}], "actions": []}
    review_turn(engine, scan_id, bad)
    record = trace_records(engine)[0]
    assert record["stage"] == "page_validation" and record["outcome"] == "rejected"
    assert record["error"]["details"]["path"] == "findings[0].article_ids"
    assert record["error"]["details"]["bad_refs"] == [999, 999]
    assert record["error"]["details"]["duplicate_refs"] == [999]
    assert record["error"]["details"]["expected"] == [1, 2]
    assert record["response"]["rationale"] == "Rejected raw finding" and record["input"]["source_catalog"]
    assert record["validation"]["supplied_article_ids"] == [1, 2]
    assert engine.setup_history(scan_id)["calls_used"] == 1
    engine, scan_id = fixture(directory / "rejected-application", base)
    job_id = engine.enqueue("decision_audit")
    def stale(payload, progress, **kwargs):
        engine.update_scan({"scan_id": scan_id, "objective": "Changed after model snapshot"})
        return {"rationale": "A stale plan", "actions": []}
    with patch("engine.run_codex", side_effect=stale):
        assert engine.run_next("model")
    record = trace_records(engine)[0]
    assert record["error"]["details"]["path"] == "snapshot.scans"
    assert record["error"]["details"]["stage"] == "stale_snapshot" and record["stage"] == "stale_snapshot"
    assert engine.state()["audits"][0]["status"] == "failed"
    engine, scan_id = fixture(directory / "invalid-action", base)
    engine.enqueue("decision_audit")
    with patch("engine.run_codex", return_value={"rationale": "Bad plan", "actions": [{"tool": "subscribe_feed"}]}):
        engine.run_next("model")
    record = trace_records(engine)[0]
    assert record["error"]["details"]["path"] == "actions[0]" and record["response"]["actions"] == [{"tool": "subscribe_feed"}]
    assert record["stage"] == "plan_validation"
    engine, scan_id = fixture(directory / "rejected-scope", base)
    def scoped(payload, progress, **kwargs):
        return {"page_number": payload["page_number"], "rationale": "Invalid scoped change", "stable": False, "findings": [],
                "actions": [{"tool": "remove_feed", "rules": None, "feed_url": base + "/unsubscribed", "query": None, "rationale": "Outside scope"}]}
    review_turn(engine, scan_id, scoped)
    record = trace_records(engine)[0]
    assert record["stage"] == "scoped_validation" and record["error"]["details"]["path"] == "actions[0].feed_url"
    assert len(engine.state()["scans"][0]["feeds"]) == 1


def check_http(directory, base):
    engine, scan_id = fixture(directory / "http", base)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.handler_for(engine, 0))
    httpd.RequestHandlerClass = server.handler_for(engine, httpd.server_port)
    thread = Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{httpd.server_port}"
    def request(path, data=None, headers=None):
        headers = {"Content-Type": "application/json", "X-Wirewatch": "1", **(headers or {})}
        body = json.dumps(data).encode() if data is not None and not isinstance(data, bytes) else data
        try:
            response = urlopen(Request(origin + path, data=body, headers=headers), timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            return response.status, response.headers["X-Wirewatch-Request"], json.loads(response.read())
    report = {"kind": "schema", "message": "Expected a valid object", "request_id": "0123456789abcdef", "path": "/api/state",
              "status": 200, "issue_path": "scans.0.rules", "stack": None, "count": 1}
    try:
        status, request_id, _ = request("/api/health", headers={"X-Wirewatch-Request": "0123456789abcdef"})
        assert status == 200 and request_id == "0123456789abcdef"
        cases = [("/api/state", None, {"Origin": "http://evil.invalid"}, 403),
                 ("/api/no-such-route", None, {}, 404), ("/api/scans", b"{", {}, 400),
                 ("/api/settings", {}, {"X-Wirewatch": "0"}, 403),
                 ("/api/client-errors", b" " * 8193, {}, 413)]
        for path, data, headers, expected in cases:
            status, request_id, _ = request(path, data, {"X-Wirewatch-Request": "INVALID", **headers})
            assert status == expected and re.fullmatch(r"[0-9a-f]{16}", request_id)
        old = engine.database
        engine.database = old.parent
        assert request("/api/state")[0] == 500
        engine.database = old
        invalid = [{**report, "extra": "no"}, {key: value for key, value in report.items() if key != "stack"}]
        for key, values in {"kind": ["unknown", None, []], "message": [None, "", "   ", 7], "request_id": ["bad", 1],
                            "path": ["relative", "/with?query", "/fragment#x", "/" + "x" * 256, 7],
                            "status": [True, 99, 600, "200"], "issue_path": [1], "stack": [{}], "count": [True, 0, 1001, "1"]}.items():
            invalid.extend({**report, key: value} for value in values)
        for value in invalid:
            status, request_id, _ = request("/api/client-errors", value)
            assert status == 400 and re.fullmatch(r"[0-9a-f]{16}", request_id), value
        with server.REPORT_LOCK:
            server.REPORT_TIMES.clear()
        safe = {**report, "message": "Bearer secret-browser-token " + "m" * 2300, "issue_path": "x" * 400, "stack": "s" * 4200}
        assert request("/api/client-errors", safe)[0] == 200
        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(lambda _: request("/api/client-errors", report), range(30)))
        assert sum(status == 200 for status, _, _ in responses) == 29
        assert sum(status == 429 for status, _, _ in responses) == 1
        with patch("server.time.monotonic", return_value=time.monotonic() + 61):
            assert request("/api/client-errors", report)[0] == 200
        records = [json.loads(line) for line in (engine.events.root / "events.jsonl").read_text().splitlines()]
        reports = [record for record in records if record["event"] == "client_error"]
        assert len(reports) == 31 and reports[0]["request_id"] == report["request_id"]
        assert len(reports[0]["message"]) == 2000 and len(reports[0]["issue_path"]) == 300 and len(reports[0]["stack"]) == 4000
        assert "secret-browser-token" not in json.dumps(records)
        assert re.fullmatch(r"[0-9a-f]{16}", reports[0]["reporting_request_id"])
        with server.REPORT_LOCK:
            server.REPORT_TIMES.clear()
        unicode_report = {**report, "message": "café 😀 boom " + chr(0xD800), "stack": "frame " + chr(0xDFFF)}
        status, reporting_id, result = request("/api/client-errors", unicode_report,
                                               {"X-Wirewatch-Request": "fedcba9876543210"})
        assert status == 200 and result == {"ok": True} and reporting_id == "fedcba9876543210"
        lines = (engine.events.root / "events.jsonl").read_bytes().splitlines()
        unicode_line = next(line for line in lines if json.loads(line).get("message") == unicode_report["message"])
        stored = json.loads(unicode_line)
        assert stored["event"] == "client_error" and stored["request_id"] == report["request_id"]
        assert stored["reporting_request_id"] == reporting_id and stored["stack"] == unicode_report["stack"]
        assert "café 😀".encode("utf-8") in unicode_line and b"\\ud800" in unicode_line and b"\\udfff" in unicode_line
        long_message = "Browser fingerprint " + "ab:" * 600
        long_stack = "Browser comparison " + "ab=" * 600 + " client_key=opaque-browser-key"
        status, reporting_id, result = request("/api/client-errors", {**report, "message": long_message, "stack": long_stack})
        assert status == 200 and result == {"ok": True}
        records = [json.loads(line) for line in (engine.events.root / "events.jsonl").read_bytes().splitlines()]
        stored = next(item for item in records if item.get("reporting_request_id") == reporting_id)
        assert stored["event"] == "client_error" and stored["message"] == long_message
        assert stored["stack"] == long_stack.replace("opaque-browser-key", "[REDACTED]")
        assert "opaque-browser-key" not in (engine.events.root / "events.jsonl").read_text()
        status, request_id, result = request("/api/replay", {"scan_id": scan_id})
        deadline = time.monotonic() + 2
        while True:
            records = [json.loads(line) for line in (engine.events.root / "events.jsonl").read_text().splitlines()]
            if any(record.get("request_id") == request_id and record.get("job_id") == result["job_id"] for record in records):
                break
            assert time.monotonic() < deadline, "HTTP completion did not retain its returned job ID"
            time.sleep(0.01)
        assert request("/diagnostics/events.jsonl")[0] == 404
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
        with server.REPORT_LOCK:
            server.REPORT_TIMES.clear()


def run(directory, base):
    root = directory / "diagnostic-checks"
    root.mkdir()
    check_existing_sources(root, base)
    check_storage(root)
    check_failed_write_cleanup(root)
    check_assignment_redaction(root)
    check_identifier_redaction(root)
    check_compact_credentials(root)
    check_escape_boundaries(root)
    check_long_evidence(root, base)
    check_nested_url_redaction(root)
    check_cli(root)
    check_engine_failures(root, base)
    check_rejections(root, base)
    check_http(root, base)
    return ["malformed accepted feeds and strict new probes", "private successful and rejected trace retention",
            "concurrent bounded trace retention and JSONL rotation", "redaction permissions and symlink refusal",
            "fake CLI bounded stdout stderr and raw output", "fake CLI JSON nonzero timeout and temporary cleanup",
            "trace preflight before initial and page reservations", "required diagnostic failure prevents model effects",
            "postcommit diagnostic failure preserves completed effects", "original rejection survives diagnostic failure",
            "rejected evidence paths duplicate refs and stale snapshot details", "collector independence from unavailable diagnostics",
            "HTTP IDs guards errors strict client reports and locked rate limit",
            "long colon and equals evidence survives traces model jobs and HTTP reports",
            "compact spaced and serialized credentials stay private in stored events and traces",
            "stream and raw escape boundaries save under subprocess deadlines"]
