"""SQLite-owned jobs, continuous feed collection, and a separate model lane."""
from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Literal
import json
import hashlib
import sqlite3
import threading
import time
import traceback
import uuid

from diagnostics import AttemptKey, Events, ModelTraces, Rejected
from model import (MODEL_PROMPT, PLAN_SCHEMA, COLLECTION_INTERVAL_SECONDS, FeedHTTPError, Rules, ScanWindow, Validators,
    codex_path, discovery, evaluate, feed_urls, fetch_conditional, fetch_feed, minutes, next_check_delay,
    now, publication_time, query_url, required_text, run_codex, safe_url)
import setup_review as review


SCHEMA = """
CREATE TABLE IF NOT EXISTS scans (
 id INTEGER PRIMARY KEY, topic TEXT NOT NULL, topic_key TEXT NOT NULL UNIQUE,
 objective TEXT NOT NULL, rules TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active'
 CHECK(status IN ('active','paused')), revision INTEGER NOT NULL DEFAULT 1,
 poll_interval_minutes INTEGER NOT NULL DEFAULT 5, last_polled TEXT, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS feeds (
 id INTEGER PRIMARY KEY, scan_id INTEGER NOT NULL REFERENCES scans(id), url TEXT NOT NULL,
 last_checked TEXT, error TEXT, item_count INTEGER NOT NULL DEFAULT 0, UNIQUE(scan_id,url)
);
CREATE TABLE IF NOT EXISTS articles (
 id INTEGER PRIMARY KEY, stable_key TEXT NOT NULL UNIQUE, title TEXT NOT NULL, url TEXT NOT NULL,
 source TEXT NOT NULL, published_at TEXT NOT NULL, summary TEXT NOT NULL, feed_url TEXT NOT NULL,
 fetched_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decisions (
 scan_id INTEGER NOT NULL REFERENCES scans(id), article_id INTEGER NOT NULL REFERENCES articles(id),
 kept INTEGER NOT NULL, reasons TEXT NOT NULL, matched_terms TEXT NOT NULL,
 rule_revision INTEGER NOT NULL, decided_at TEXT NOT NULL, PRIMARY KEY(scan_id,article_id)
);
CREATE TABLE IF NOT EXISTS activity (
 id INTEGER PRIMARY KEY, at TEXT NOT NULL, type TEXT NOT NULL, scan_id INTEGER, message TEXT NOT NULL,
 details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audits (
 id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
 coverage_count INTEGER NOT NULL DEFAULT 0, decision_count INTEGER NOT NULL DEFAULT 0,
 fresh_count INTEGER NOT NULL DEFAULT 0, rationale TEXT NOT NULL DEFAULT '', actions TEXT NOT NULL DEFAULT '[]',
 error TEXT
);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS scan_sequence (singleton INTEGER PRIMARY KEY CHECK(singleton=1), next_id INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (
 id TEXT PRIMARY KEY, key TEXT NOT NULL, kind TEXT NOT NULL
 CHECK(kind IN ('poll','replay','setup','audit','decision_audit','setup_review')), scan_id INTEGER,
 payload TEXT NOT NULL, origin TEXT NOT NULL CHECK(origin IN ('user','schedule','model')),
 status TEXT NOT NULL CHECK(status IN ('queued','running','completed','failed','cancelled')),
 message TEXT NOT NULL, created_at TEXT NOT NULL, finished_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_active ON jobs(key) WHERE status IN ('queued','running');
CREATE TABLE IF NOT EXISTS feed_state (
 url TEXT PRIMARY KEY, etag TEXT, last_modified TEXT, next_check_at REAL NOT NULL,
 failures INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0,
 checked_at REAL NOT NULL DEFAULT 0, claimed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS setup_runs (
 run_id TEXT PRIMARY KEY, scan_id INTEGER NOT NULL,
 phase TEXT NOT NULL CHECK(phase IN ('collecting','reviewing','ready','needs_attention','stopped')),
 round INTEGER NOT NULL, calls_used INTEGER NOT NULL, call_limit INTEGER NOT NULL,
 started_at TEXT NOT NULL, collection_started REAL NOT NULL, next_review_at REAL, settle_deadline REAL,
 config_hash TEXT NOT NULL, work TEXT NOT NULL DEFAULT '{}',
 reviewed_count INTEGER NOT NULL DEFAULT 0, total_count INTEGER NOT NULL DEFAULT 0,
 archive_through_id INTEGER, reason TEXT NOT NULL,
 pending_probes TEXT NOT NULL DEFAULT '[]', probes TEXT NOT NULL DEFAULT '[]',
 findings TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS setup_runs_scan ON setup_runs(scan_id);
CREATE TABLE IF NOT EXISTS setup_rounds (
 id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES setup_runs(run_id), scan_id INTEGER NOT NULL,
 round INTEGER NOT NULL, status TEXT NOT NULL
 CHECK(status IN ('running','changed','stable','needs_attention','failed','stale','cancelled')),
 started_at TEXT NOT NULL, finished_at TEXT, reviewed_count INTEGER NOT NULL DEFAULT 0,
 total_count INTEGER NOT NULL, calls_used INTEGER NOT NULL DEFAULT 0,
 rationale TEXT NOT NULL DEFAULT '', error TEXT, actions TEXT NOT NULL DEFAULT '[]',
 probes TEXT NOT NULL DEFAULT '[]', findings TEXT NOT NULL DEFAULT '[]', UNIQUE(run_id,round)
);
"""
DEFAULTS: dict[str, object] = {"audit_enabled": False, "audit_interval_minutes": 15,
    "decision_audit_enabled": False, "decision_audit_interval_minutes": 15, "last_decision_audit_at": 0,
    "audit_objective": "Find missed relevant news across my scans, improve deterministic coverage, and discover related topics.", "last_audit_at": 0}


JobKind = Literal["poll", "replay", "setup", "audit", "decision_audit", "setup_review"]


@dataclass(frozen=True)
class DurableJob:
    id: str
    kind: JobKind
    payload: dict[str, object]
    origin: str
    created_at: str
    key: str
    scan_id: int | None
    status: Literal["queued", "running", "completed", "failed", "cancelled"]
    message: str
    finished_at: str | None


@dataclass(frozen=True)
class FeedState:
    url: str
    validators: Validators
    next_check_at: float
    failures: int
    retry_at: float


def timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds")


class Engine:
    def __init__(self, database: Path, start_worker: bool = True) -> None:
        self.database = database
        self.events = Events(database.parent)
        self.traces = ModelTraces(database.parent, self.events)
        self.events.emit("app_starting")
        database.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        with self.transaction() as db:
            db.executescript(SCHEMA)
            jobs_sql = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone()[0]
            if "'setup_review'" not in jobs_sql:
                db.execute("BEGIN IMMEDIATE")
                indexes = [row[0] for row in db.execute("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='jobs' AND sql IS NOT NULL")]
                db.execute("ALTER TABLE jobs RENAME TO jobs_before_setup_review")
                db.execute(jobs_sql.replace("'decision_audit'", "'decision_audit','setup_review'"))
                db.execute("INSERT INTO jobs SELECT * FROM jobs_before_setup_review ORDER BY rowid")
                db.execute("DROP TABLE jobs_before_setup_review")
                for index in indexes:
                    db.execute(index)
            if "settle_deadline" not in {row["name"] for row in db.execute("PRAGMA table_info(setup_runs)")}:
                db.execute("ALTER TABLE setup_runs ADD COLUMN settle_deadline REAL")
            columns = {row["name"] for row in db.execute("PRAGMA table_info(scans)")}
            for name, definition in {"mode": "TEXT NOT NULL DEFAULT 'live'", "start_date": "TEXT", "end_date": "TEXT",
                    "completed_at": "TEXT", "last_error": "TEXT", "archive_through_id": "INTEGER",
                    "status_token": "TEXT NOT NULL DEFAULT ''"}.items():
                if name not in columns:
                    db.execute(f"ALTER TABLE scans ADD COLUMN {name} {definition}")
            columns = {row["name"] for row in db.execute("PRAGMA table_info(audits)")}
            for name, definition in {"windows": "TEXT NOT NULL DEFAULT '[]'", "kind": "TEXT NOT NULL DEFAULT 'coverage'",
                    "input_hash": "TEXT", "report": "TEXT NOT NULL DEFAULT '{}'"}.items():
                if name not in columns:
                    db.execute(f"ALTER TABLE audits ADD COLUMN {name} {definition}")
            if "token" not in {row["name"] for row in db.execute("PRAGMA table_info(feeds)")}:
                db.execute("ALTER TABLE feeds ADD COLUMN token TEXT NOT NULL DEFAULT ''")
            for row in db.execute("SELECT id FROM feeds WHERE token=''").fetchall():
                db.execute("UPDATE feeds SET token=? WHERE id=?", (uuid.uuid4().hex, row["id"]))
            db.execute("INSERT OR IGNORE INTO scan_sequence VALUES (1, (SELECT coalesce(max(id),0)+1 FROM (SELECT id FROM scans UNION ALL SELECT scan_id AS id FROM activity)))")
            for key, value in DEFAULTS.items():
                db.execute("INSERT OR IGNORE INTO settings VALUES (?,?)", (key, json.dumps(value)))
            db.execute("INSERT OR IGNORE INTO feed_state(url,next_check_at) SELECT DISTINCT url,0 FROM feeds")
            db.execute("UPDATE feed_state SET claimed=0")
            interrupted = db.execute("SELECT scan_id FROM jobs WHERE status='running' AND kind='setup_review'").fetchall()
            for row in interrupted:
                self._invalidate_setup(db, row["scan_id"], "Server stopped during setup review; no automatic retry", round_status="failed")
            for row in db.execute("SELECT scan_id FROM setup_runs WHERE phase='reviewing'").fetchall():
                self._invalidate_setup(db, row["scan_id"], "Server stopped during setup review; no automatic retry", round_status="failed")
            db.execute("UPDATE jobs SET status='queued',message='Resuming interrupted deterministic work' WHERE status='running' AND kind IN ('poll','replay')")
            for run in db.execute("SELECT run_id,work FROM setup_runs WHERE phase='collecting'").fetchall():
                work = json.loads(run["work"])
                if any(db.execute("SELECT status FROM jobs WHERE id=?", (ident,)).fetchone()[0] == "queued" for ident in work.values()):
                    db.execute("UPDATE setup_runs SET settle_deadline=? WHERE run_id=?", (time.time() + review.SETTLE_SECONDS, run["run_id"]))
            db.execute("UPDATE jobs SET status='failed',finished_at=?,message='Server stopped during model work; not retried' WHERE status='running' AND kind IN ('setup','audit','decision_audit')", (now(),))
            db.execute("UPDATE audits SET status='failed', finished_at=?, error='Server stopped during model work; not retried' WHERE status='running'", (now(),))
            for scan in db.execute("SELECT id FROM scans WHERE mode='historical' AND completed_at IS NULL AND status='active'").fetchall():
                self._enqueue(db, "poll", {"scan_id": scan["id"]}, "schedule")
        self.events.emit("archive_recovered", interrupted_reviews=len(interrupted), model_retry=False)
        self.events.emit("archive_migrated", outcome="ready")
        self.worker = threading.Thread(target=self._loop, name="wirewatch-collector", daemon=True)
        self.model_worker = threading.Thread(target=self._model_loop, name="wirewatch-model", daemon=True)
        if start_worker:
            self.worker.start()
            self.model_worker.start()
        self.events.emit("app_started", workers=start_worker)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.lock:
            db = sqlite3.connect(self.database)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            try:
                with db:
                    yield db
            finally:
                db.close()

    def activity(self, db: sqlite3.Connection, kind: str, message: str, scan_id: int | None = None, details: object = None) -> None:
        db.execute("INSERT INTO activity(at,type,scan_id,message,details) VALUES (?,?,?,?,?)", (now(), kind, scan_id, message, json.dumps(details)))

    def settings(self, db: sqlite3.Connection) -> dict[str, object]:
        return {row["key"]: json.loads(row["value"]) for row in db.execute("SELECT * FROM settings")}

    def state(self) -> dict[str, object]:
        with self.transaction() as db:
            scans = [dict(row) for row in db.execute("SELECT * FROM scans ORDER BY id")]
            for scan in scans:
                scan["rules"] = json.loads(scan["rules"])
                scan["feeds"] = [dict(row) for row in db.execute("""SELECT f.*,fs.next_check_at,fs.failures FROM feeds f
                    JOIN feed_state fs ON fs.url=f.url WHERE scan_id=? ORDER BY f.id""", (scan["id"],))]
                for feed in scan["feeds"]:
                    feed["next_check_at"] = timestamp(feed["next_check_at"]) if feed["next_check_at"] else None
                scan["kept"] = db.execute("SELECT count(*) FROM decisions WHERE scan_id=? AND kept=1", (scan["id"],)).fetchone()[0]
                scan["discarded"] = db.execute("SELECT count(*) FROM decisions WHERE scan_id=? AND kept=0", (scan["id"],)).fetchone()[0]
                scan["setup_review"] = self._setup_state(db, scan["id"])
            records = [dict(row) for row in db.execute("""SELECT a.*,d.scan_id,s.topic AS scan_topic,s.mode,s.start_date,s.end_date,d.kept,d.reasons,d.matched_terms,
                d.rule_revision,d.decided_at FROM decisions d JOIN articles a ON a.id=d.article_id
                JOIN scans s ON s.id=d.scan_id ORDER BY a.id DESC,d.scan_id""")]
            for record in records:
                record["kept"] = bool(record["kept"])
                record["reasons"] = json.loads(record["reasons"])
                record["matched_terms"] = json.loads(record["matched_terms"])
            activity = [dict(row) for row in db.execute("SELECT * FROM activity ORDER BY id DESC")]
            for event in activity:
                event["details"] = json.loads(event["details"])
            audits = [dict(row) for row in db.execute("SELECT * FROM audits ORDER BY id DESC")]
            for audit in audits:
                audit["actions"] = json.loads(audit["actions"])
                audit["windows"] = json.loads(audit["windows"])
                audit["report"] = json.loads(audit["report"])
            settings = self.settings(db)
            article_count = db.execute("SELECT count(*) FROM articles").fetchone()[0]
            jobs = [dict(row) for row in db.execute("SELECT id,kind,scan_id,status,message,created_at,finished_at FROM jobs ORDER BY rowid DESC LIMIT 40")]
            jobs.reverse()
        return {"scans": scans, "records": records, "activity": activity, "audits": audits,
            "settings": settings, "jobs": jobs, "collection_interval_seconds": COLLECTION_INTERVAL_SECONDS, "codex_available": bool(codex_path()), "article_count": article_count,
            "stats": {"active": sum(s["status"] == "active" and (s["mode"] == "live" or s["completed_at"] is None) for s in scans), "kept": sum(r["kept"] for r in records),
                "discarded": sum(not r["kept"] for r in records), "feeds": sum(len(s["feeds"]) for s in scans)}}

    def scan(self, db: sqlite3.Connection, scan_id: int) -> sqlite3.Row:
        row = db.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
        if row is None:
            raise ValueError("Scan does not exist")
        return row

    def _create(self, db: sqlite3.Connection, topic: str, objective: str, rules: Rules, interval: int, window: ScanWindow = ScanWindow()) -> int:
        key = " ".join(topic.casefold().split())
        if window.mode == "historical":
            key = json.dumps([key, *window.to_dict().values()])
            query_url(topic, window)
        existing = db.execute("SELECT * FROM scans WHERE topic_key=?", (key,)).fetchone()
        if existing:
            if ScanWindow.parse(dict(existing)) != window:
                raise ValueError("Topic conflicts with an existing scan's mode or date window")
            return existing["id"]
        scan_id = db.execute("SELECT next_id FROM scan_sequence WHERE singleton=1").fetchone()[0]
        db.execute("UPDATE scan_sequence SET next_id=next_id+1 WHERE singleton=1")
        db.execute("INSERT INTO scans(id,topic,topic_key,objective,rules,poll_interval_minutes,created_at,mode,start_date,end_date) VALUES (?,?,?,?,?,?,?,?,?,?)", (scan_id, topic, key, objective, json.dumps(rules.to_dict()), interval, now(), *window.to_dict().values()))
        self.activity(db, "scan_created", f"Created {topic}", scan_id, window.to_dict())
        self._enqueue(db, "replay", {"scan_id": scan_id}, "model")
        return scan_id

    def _subscribe(self, db: sqlite3.Connection, scan_id: int, url: str) -> None:
        self.scan(db, scan_id)
        cursor = db.execute("INSERT OR IGNORE INTO feeds(scan_id,url,token) VALUES (?,?,?)", (scan_id, url, uuid.uuid4().hex))
        if cursor.rowcount:
            db.execute("""INSERT INTO feed_state(url,next_check_at) VALUES (?,0) ON CONFLICT(url) DO UPDATE SET
                next_check_at=max(retry_at,min(next_check_at,?))""", (url, time.time()))
            self._enqueue(db, "replay", {"scan_id": scan_id}, "model")
            self.activity(db, "feed_subscribed", f"Subscribed to {url}", scan_id)

    def create_scan(self, data: dict[str, object]) -> int:
        topic = required_text(data.get("topic"), "Topic", 300)
        objective = required_text(data.get("objective"), "Objective")
        rules = Rules.parse(data.get("rules"))
        window = ScanWindow.parse(data)
        interval = minutes(data.get("poll_interval_minutes", 5))
        feeds = feed_urls(data.get("feed_urls", [])) or [query_url(topic, window)]
        with self.transaction() as db:
            scan_id = self._create(db, topic, objective, rules, interval, window)
            before = self._setup_hash(db, scan_id)
            for url in feeds:
                self._subscribe(db, scan_id, url)
            if before != self._setup_hash(db, scan_id):
                self._invalidate_setup(db, scan_id, "Scan subscriptions changed; restart setup review")
            self._enqueue(db, "poll", {"scan_id": scan_id}, "user")
        self.events.emit("scan_created", scan_id=scan_id, feed_count=len(feeds), mode=window.mode)
        self.events.emit("feeds_saved", scan_id=scan_id, feed_urls=feeds)
        return scan_id

    def _decide(self, db: sqlite3.Connection, scan: sqlite3.Row, article: sqlite3.Row, rules: Rules) -> None:
        window = ScanWindow.parse(dict(scan))
        result = evaluate(rules, article["title"], article["summary"], window, article["published_at"])
        old = db.execute("SELECT * FROM decisions WHERE scan_id=? AND article_id=?", (scan["id"], article["id"])).fetchone()
        reasons = json.dumps(result.reasons)
        terms = json.dumps(result.matched_terms)
        if old and old["kept"] == result.kept and old["rule_revision"] == scan["revision"] and old["reasons"] == reasons and old["matched_terms"] == terms:
            return
        db.execute("""INSERT INTO decisions VALUES (?,?,?,?,?,?,?) ON CONFLICT(scan_id,article_id) DO UPDATE SET
            kept=excluded.kept,reasons=excluded.reasons,matched_terms=excluded.matched_terms,
            rule_revision=excluded.rule_revision,decided_at=excluded.decided_at""",
            (scan["id"], article["id"], result.kept, reasons, terms, scan["revision"], now()))
        self.activity(db, "decision_changed" if old else "decision", f"{'Kept' if result.kept else 'Discarded'}: {article['title']}", scan["id"],
            {"article_id": article["id"], "kept": result.kept, "previous_kept": bool(old["kept"]) if old else None, "reasons": result.reasons, "rule_revision": scan["revision"], **window.to_dict()})

    def _update_rules(self, db: sqlite3.Connection, scan_id: int, rules: Rules, interval: int | None = None, objective: str | None = None) -> None:
        scan = self.scan(db, scan_id)
        changed = json.loads(scan["rules"]) != rules.to_dict()
        db.execute("UPDATE scans SET rules=?,revision=revision+?,poll_interval_minutes=?,objective=? WHERE id=?", (json.dumps(rules.to_dict()), int(changed), interval or scan["poll_interval_minutes"], objective or scan["objective"], scan_id))
        if changed:
            self.activity(db, "rules_updated", f"Rules updated to revision {scan['revision'] + 1}", scan_id, rules.to_dict())
            self._enqueue(db, "replay", {"scan_id": scan_id}, "model")

    def update_scan(self, data: dict[str, object]) -> None:
        scan_id = self._id(data.get("scan_id"))
        with self.transaction() as db:
            scan = self.scan(db, scan_id)
            before = self._setup_hash(db, scan_id)
            if any(key in data and data[key] != scan[key] for key in ("mode", "start_date", "end_date")):
                raise ValueError("Scan mode and date window are immutable. Create a new scan for another window")
            if "rules" in data or "objective" in data:
                if "expected_revision" in data and data["expected_revision"] != scan["revision"]:
                    raise ValueError("Rules changed since this editor loaded. Reload saved rules before saving")
                self._update_rules(db, scan_id, Rules.parse(data.get("rules", json.loads(scan["rules"]))), minutes(data.get("poll_interval_minutes", scan["poll_interval_minutes"])), required_text(data.get("objective", scan["objective"]), "Objective"))
            if "status" in data:
                if data["status"] not in ("active", "paused"):
                    raise ValueError("Status must be active or paused")
                db.execute("UPDATE scans SET status=? WHERE id=?", (data["status"], scan_id))
                if data["status"] != scan["status"]:
                    db.execute("UPDATE scans SET status_token=? WHERE id=?", (uuid.uuid4().hex, scan_id))
                    db.execute("UPDATE feeds SET token=lower(hex(randomblob(16))),last_checked=NULL WHERE scan_id=?", (scan_id,))
                    if data["status"] == "active":
                        db.execute("UPDATE feed_state SET next_check_at=max(retry_at,min(next_check_at,?)) WHERE url IN (SELECT url FROM feeds WHERE scan_id=?)", (time.time(), scan_id))
                        self._enqueue(db, "replay", {"scan_id": scan_id}, "user")
                    self.activity(db, "scan_status", f"Scan {data['status']}", scan_id)
            if "feed_url" in data:
                self._subscribe(db, scan_id, safe_url(data["feed_url"]))
            if "remove_feed_id" in data:
                removed = db.execute("DELETE FROM feeds WHERE id=? AND scan_id=?", (self._id(data["remove_feed_id"]), scan_id))
                if removed.rowcount:
                    self.activity(db, "feed_removed", "Removed feed subscription; article history retained", scan_id)
            if before != self._setup_hash(db, scan_id):
                self._invalidate_setup(db, scan_id, "Scan configuration changed or paused; restart setup review")

        self.events.emit("scan_updated", scan_id=scan_id, fields=sorted(data), status=data.get("status"))
        if "rules" in data:
            self.events.emit("rules_saved", scan_id=scan_id, term_counts={key: len(value) for key, value in data["rules"].items()})
        if "feed_url" in data or "remove_feed_id" in data:
            self.events.emit("feeds_saved", scan_id=scan_id, feed_url=data.get("feed_url"), removed_feed_id=data.get("remove_feed_id"))

    def delete_scan(self, data: dict[str, object]) -> None:
        scan_id = self._id(data.get("scan_id"))
        with self.transaction() as db:
            scan = self.scan(db, scan_id)
            self._invalidate_setup(db, scan_id, "Scan deleted; review stopped and history retained", phase="stopped", round_status="cancelled")
            self.activity(db, "scan_deleted", f"Deleted {scan['topic']} (scan {scan_id})", scan_id,
                {"topic": scan["topic"], "scan_id": scan_id, **ScanWindow.parse(dict(scan)).to_dict()})
            db.execute("DELETE FROM decisions WHERE scan_id=?", (scan_id,))
            db.execute("DELETE FROM feeds WHERE scan_id=?", (scan_id,))
            db.execute("DELETE FROM scans WHERE id=?", (scan_id,))
            db.execute("UPDATE jobs SET status='cancelled',message='Scan deleted',finished_at=? WHERE scan_id=? AND status IN ('queued','running')", (now(), scan_id))

        self.events.emit("scan_deleted", scan_id=scan_id, outcome="cancelled_pending_jobs")

    @staticmethod
    def _id(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("Expected a positive integer id")
        return value

    def save_settings(self, data: dict[str, object]) -> None:
        with self.transaction() as db:
            current = self.settings(db)
            settings: dict[str, object] = {}
            for prefix in ("audit", "decision_audit"):
                enabled = data.get(f"{prefix}_enabled", current[f"{prefix}_enabled"])
                if not isinstance(enabled, bool):
                    raise ValueError(f"{prefix}_enabled must be true or false")
                settings[f"{prefix}_enabled"] = enabled
                settings[f"{prefix}_interval_minutes"] = minutes(data.get(f"{prefix}_interval_minutes", current[f"{prefix}_interval_minutes"]))
            settings["audit_objective"] = required_text(data.get("audit_objective", current["audit_objective"]), "Audit objective")
            for key, value in settings.items():
                db.execute("UPDATE settings SET value=? WHERE key=?", (json.dumps(value), key))
            self.activity(db, "settings", "Audit settings saved", details=settings)

        self.events.emit("settings_saved", **{key: value for key, value in settings.items() if key != "audit_objective"})

    def _enqueue(self, db: sqlite3.Connection, kind: JobKind, data: dict[str, object], origin: str, strict: bool = False) -> str:
        key = kind if kind in ("setup", "audit", "decision_audit") else f"{kind}:{data.get('scan_id')}"
        if kind == "setup_review":
            key = f"setup_review:{data['scan_id']}:{data['run_id']}:{data['round']}"
        existing = db.execute("SELECT id,status,payload FROM jobs WHERE key=? AND status IN ('queued','running')", (key,)).fetchone()
        if existing:
            if strict:
                raise ValueError("This job is already queued or running")
            if kind in ("poll", "replay") and existing["status"] == "running":
                payload = json.loads(existing["payload"])
                payload["rerun_requested"] = True
                db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(payload), existing["id"]))
            self.events.emit("job_coalesced", "DEBUG", job_id=existing["id"], kind=kind, origin=origin, scan_id=data.get("scan_id"), outcome="pending_commit")
            return existing["id"]
        job_id = uuid.uuid4().hex[:12]
        db.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,'queued','Waiting for the worker',?,NULL)",
            (job_id, key, kind, data.get("scan_id"), json.dumps(data), origin, datetime.now(timezone.utc).isoformat()))
        self.events.emit("job_enqueued", job_id=job_id, kind=kind, origin=origin, scan_id=data.get("scan_id"), run_id=data.get("run_id"), round=data.get("round"), outcome="pending_commit")
        return job_id

    def enqueue(self, kind: JobKind, data: dict[str, object] | None = None) -> str:
        data = dict(data or {})
        if kind not in ("poll", "setup", "audit", "decision_audit", "replay"):
            raise ValueError("Unknown job")
        with self.transaction() as db:
            if kind in ("poll", "replay"):
                scan = self.scan(db, self._id(data.get("scan_id")))
                if kind == "poll" and scan["status"] == "paused":
                    raise ValueError("Resume this scan before polling")
            if kind == "setup":
                topic = required_text(data.get("topic"), "Topic", 300)
                required_text(data.get("objective"), "Objective")
                query_url(topic, ScanWindow.parse(data))
                data["feed_urls"] = feed_urls(data.get("feed_urls", []))
            return self._enqueue(db, kind, data, "user", strict=True)

    def progress(self, job_id: str, message: str, **values: object) -> None:
        with self.transaction() as db:
            self._progress(db, job_id, message, **values)

        self.events.emit("job_progress", job_id=job_id, message=message, **values)

    def _progress(self, db: sqlite3.Connection, job_id: str, message: str, **values: object) -> None:
        status = values.get("status")
        db.execute("""UPDATE jobs SET message=?,status=coalesce(?,status),finished_at=coalesce(?,finished_at)
            WHERE id=? AND status IN ('queued','running')""", (message, status, values.get("finished_at"), job_id))

    def _finish_deterministic(self, db: sqlite3.Connection, job: DurableJob, error: str | None = None) -> None:
        current = db.execute("SELECT status,payload FROM jobs WHERE id=?", (job.id,)).fetchone()
        payload = json.loads(current["payload"])
        scan = db.execute("SELECT status FROM scans WHERE id=?", (job.scan_id,)).fetchone()
        rerun = payload.pop("rerun_requested", False)
        eligible = scan is not None and (job.kind == "replay" or scan["status"] == "active")
        if rerun and eligible and current["status"] in ("running", "cancelled", "failed"):
            db.execute("""UPDATE jobs SET status='queued',payload=?,message='Another deterministic pass requested while running',
                finished_at=NULL,created_at=? WHERE id=?""", (json.dumps(payload), datetime.now(timezone.utc).isoformat(), job.id))
        else:
            self._progress(db, job.id, error or "Completed", status="failed" if error else "completed", finished_at=now())

    def run_next(self, lane: Literal["collector", "model"]) -> bool:
        kinds = ("poll", "replay") if lane == "collector" else ("setup", "audit", "decision_audit", "setup_review")
        with self.transaction() as db:
            if lane == "model" and db.execute("SELECT 1 FROM jobs WHERE status='running' AND kind IN ('setup','audit','decision_audit','setup_review')").fetchone():
                return False
            row = next((row for row in db.execute(f"SELECT * FROM jobs WHERE status='queued' AND kind IN ({','.join('?' for _ in kinds)}) ORDER BY rowid", kinds).fetchall()
                if row["kind"] != "setup_review" or self._setup_due(db, row)), None)
            if row is None:
                return False
            job = DurableJob(**{**dict(row), "payload": json.loads(row["payload"])})
            if job.origin == "schedule" and job.kind in ("audit", "decision_audit") and not self.settings(db)[f"{job.kind}_enabled"]:
                self._progress(db, job.id, "Scheduled audit disabled", status="cancelled", finished_at=now())
                self.events.emit("job_cancelled", job_id=job.id, kind=job.kind, reason="scheduled_audit_disabled", outcome="pending_commit")
                return True
            self._progress(db, job.id, "Starting", status="running")
            if job.kind in ("poll", "replay") and job.payload.pop("rerun_requested", False):
                db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(job.payload), job.id))
        started = time.monotonic()
        self.events.emit("job_claimed", lane=lane, job_id=job.id, kind=job.kind, origin=job.origin, scan_id=job.scan_id, run_id=job.payload.get("run_id"), round=job.payload.get("round"))
        try:
            if job.kind == "poll":
                self._poll(int(job.payload["scan_id"]), job.id)
            elif job.kind == "replay":
                self._replay_job(int(job.payload["scan_id"]))
            elif job.kind == "setup_review":
                self._setup_review_job(job)
            else:
                self._model_job(job.kind, job.payload, job.id, job.origin)
            if job.kind in ("poll", "replay"):
                with self.transaction() as db:
                    self._finish_deterministic(db, job)
        except Exception as exc:
            self.events.emit("job_failed", "ERROR", lane=lane, job_id=job.id, kind=job.kind, scan_id=job.scan_id,
                             run_id=job.payload.get("run_id"), round=job.payload.get("round"), error_type=type(exc).__name__, error=str(exc), stack=traceback.format_exc(limit=8), duration_ms=round((time.monotonic() - started) * 1000))
            with self.transaction() as db:
                if job.kind == "setup_review":
                    self._fail_setup(db, job, str(exc))
                if job.kind in ("poll", "replay"):
                    self._finish_deterministic(db, job, str(exc))
                else:
                    self._progress(db, job.id, str(exc), status="failed", finished_at=now())
                self.activity(db, "job_error", f"{job.kind} failed: {exc}", details={"job_id": job.id})
        with self.transaction() as db:
            status = db.execute("SELECT status FROM jobs WHERE id=?", (job.id,)).fetchone()[0]
        self.events.emit("job_finished", job_id=job.id, kind=job.kind, scan_id=job.scan_id, outcome=status, duration_ms=round((time.monotonic() - started) * 1000))
        return True

    def _loop(self) -> None:
        try:
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="wirewatch-feed") as executor:
                explicit = automatic = None
                while not self.stop_event.is_set():
                    for future in (explicit, automatic):
                        if future is not None and future.done():
                            try:
                                future.result()
                            except Exception as exc:
                                self.events.emit("collector_failed", "ERROR", error_type=type(exc).__name__, error=str(exc), stack=traceback.format_exc(limit=8))
                    if explicit is None or explicit.done():
                        explicit = executor.submit(self.run_next, "collector")
                    if automatic is None or automatic.done():
                        automatic = executor.submit(self.collect_due)
                    self._schedule()
                    self.stop_event.wait(0.2)
        except Exception as exc:
            self.events.emit("worker_stopped", "ERROR", lane="collector", error=str(exc))
            raise
        finally:
            self.events.emit("worker_stopped", lane="collector")

    def _model_loop(self) -> None:
        try:
            while not self.stop_event.is_set():
                if not self.run_next("model"):
                    self.stop_event.wait(0.2)
        except Exception as exc:
            self.events.emit("worker_stopped", "ERROR", lane="model", error=str(exc))
            raise
        finally:
            self.events.emit("worker_stopped", lane="model")

    def _schedule(self) -> None:
        with self.transaction() as db:
            settings = self.settings(db)
            if not db.execute("SELECT 1 FROM scans LIMIT 1").fetchone():
                return
            for kind in ("audit", "decision_audit"):
                if settings[f"{kind}_enabled"] and time.time() - float(settings[f"last_{kind}_at"]) >= int(settings[f"{kind}_interval_minutes"]) * 60:
                    self._enqueue(db, kind, {}, "schedule")
                    db.execute("UPDATE settings SET value=? WHERE key=?", (json.dumps(time.time()), f"last_{kind}_at"))

    def collect_due(self, at: float | None = None) -> int:
        current = time.time() if at is None else at
        with self.transaction() as db:
            row = db.execute("""SELECT fs.url FROM feed_state fs WHERE claimed=0 AND next_check_at<=? AND retry_at<=?
                AND EXISTS (SELECT 1 FROM feeds f JOIN scans s ON s.id=f.scan_id
                    WHERE f.url=fs.url AND s.mode='live' AND s.status='active')
                ORDER BY next_check_at,fs.url LIMIT 1""", (current, current)).fetchone()
        if row is None:
            return 0
        self._check_url(row["url"], due_at=current)
        return 1

    def _replay_job(self, scan_id: int, subscriptions: list[dict[str, object]] | None = None) -> bool:
        while True:
            with self.transaction() as db:
                scan = db.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
                if scan is None or not self._subscriptions_current(db, scan, subscriptions):
                    return False
                revision = scan["revision"]
                through = db.execute("SELECT coalesce(max(id),0) FROM articles").fetchone()[0]
            after = count = 0
            while after < through:
                with self.transaction() as db:
                    scan = db.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
                    if scan is None or not self._subscriptions_current(db, scan, subscriptions):
                        return False
                    articles = db.execute("SELECT * FROM articles WHERE id>? AND id<=? ORDER BY id LIMIT 64", (after, through)).fetchall()
                    if not articles:
                        break
                    rules = Rules.parse(json.loads(scan["rules"]))
                    for article in articles:
                        self._decide(db, scan, article, rules)
                    after = articles[-1]["id"]
                    count += len(articles)
            with self.transaction() as db:
                scan = db.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
                if scan is None or not self._subscriptions_current(db, scan, subscriptions):
                    return False
                if scan["revision"] == revision:
                    if scan["mode"] == "historical":
                        db.execute("UPDATE scans SET archive_through_id=? WHERE id=?", (through, scan_id))
                    self.activity(db, "replay", f"Replayed {count} stored articles at revision {revision}", scan_id, ScanWindow.parse(dict(scan)).to_dict())
                    decision_count, kept_count = db.execute("SELECT count(*),coalesce(sum(kept),0) FROM decisions WHERE scan_id=?", (scan_id,)).fetchone()
                    completed = True
                else:
                    completed = False
            if completed:
                self.events.emit("replay_completed", scan_id=scan_id, article_count=count, revision=revision, archive_through_id=through,
                                 decision_count=decision_count, kept_count=kept_count, discarded_count=decision_count - kept_count)
                return True

    @staticmethod
    def _subscriptions_current(db: sqlite3.Connection, scan: sqlite3.Row, subscriptions: list[dict[str, object]] | None) -> bool:
        if subscriptions is None:
            return True
        actual = [(row["id"], row["token"]) for row in db.execute("SELECT id,token FROM feeds WHERE scan_id=? ORDER BY id", (scan["id"],))]
        return scan["status"] == "active" and actual == [(feed["id"], feed["token"]) for feed in subscriptions]

    def _check_url(self, url: str, for_scan: int | None = None, since: float | None = None, due_at: float | None = None, token: str | None = None) -> str | None:
        while True:
            with self.transaction() as db:
                subscriptions = db.execute("""SELECT f.id,f.scan_id,f.token,f.last_checked FROM feeds f JOIN scans s ON s.id=f.scan_id
                    WHERE f.url=? AND s.status='active' AND (s.mode='live' OR s.id=?)
                    AND (? IS NULL OR s.id!=? OR f.token=?)""", (url, for_scan, token, for_scan, token)).fetchall()
                if not subscriptions:
                    return None
                row = db.execute("SELECT * FROM feed_state WHERE url=?", (url,)).fetchone()
                if not row["claimed"]:
                    state = FeedState(url, Validators(row["etag"], row["last_modified"]), row["next_check_at"], row["failures"], row["retry_at"])
                    current = time.time()
                    if state.retry_at > current:
                        return f"Server retry floor until {timestamp(state.retry_at)}"
                    if due_at is not None and state.next_check_at > due_at:
                        return None
                    if since is not None and row["checked_at"] >= since:
                        error = db.execute("SELECT error FROM feeds WHERE url=? AND error IS NOT NULL LIMIT 1", (url,)).fetchone()
                        return error["error"] if error else None
                    targets = [(scan["id"], scan["status_token"],
                        {(feed["id"], feed["token"]) for feed in db.execute("SELECT id,token FROM feeds WHERE scan_id=?", (scan["id"],))})
                        for scan in db.execute("SELECT id,status_token FROM scans WHERE status='active' AND (mode='live' OR id=?)", (for_scan,)).fetchall()
                        if scan["id"] != for_scan or any(sub["scan_id"] == for_scan for sub in subscriptions)]
                    db.execute("UPDATE feed_state SET claimed=1 WHERE url=?", (url,))
                    break
            if self.stop_event.wait(0.05):
                return "Server stopped before feed check"
        started = time.monotonic()
        diagnostic: dict[str, object] = {"outcome": "discarded"}
        self.events.emit("feed_fetch_started", "DEBUG", feed_url=url, scan_id=for_scan)
        try:
            try:
                result = fetch_conditional(url, state.validators)
                error = None
            except Exception as exc:
                result = None
                error = str(exc)
                retry_at = exc.retry_at if isinstance(exc, FeedHTTPError) else None
            with self.transaction() as db:
                valid = [sub for sub in subscriptions if db.execute("""SELECT 1 FROM feeds f JOIN scans s ON s.id=f.scan_id
                    WHERE f.id=? AND f.scan_id=? AND f.token=? AND f.url=? AND s.status='active'""",
                    (sub["id"], sub["scan_id"], sub["token"], url)).fetchone()]
                if not valid:
                    return None
                current = time.time()
                if result is None:
                    failures = state.failures + 1
                    floor = retry_at or 0
                    db.execute("UPDATE feed_state SET failures=?,retry_at=?,next_check_at=?,checked_at=? WHERE url=?",
                        (failures, floor, max(current + next_check_delay(failures), floor), current, url))
                    for sub in valid:
                        db.execute("UPDATE feeds SET last_checked=?,error=? WHERE id=?", (now(), error, sub["id"]))
                        if state.failures == 0:
                            self.activity(db, "feed_error", f"{url}: {error}", sub["scan_id"])
                    diagnostic = {"outcome": "backoff", "error": error, "failures": failures, "retry_at": floor, "next_check_at": max(current + next_check_delay(failures), floor)}
                    return error
                articles = result.articles
                inserted = 0
                if articles is not None:
                    for article in articles:
                        inserted += db.execute("""INSERT OR IGNORE INTO articles(stable_key,title,url,source,published_at,summary,feed_url,fetched_at)
                            VALUES (?,?,?,?,?,?,?,?)""", (*asdict(article).values(), now())).rowcount
                db.execute("UPDATE feed_state SET etag=?,last_modified=?,next_check_at=?,failures=0,retry_at=0,checked_at=? WHERE url=?",
                    (result.validators.etag, result.validators.last_modified, current + COLLECTION_INTERVAL_SECONDS, current, url))
                for sub in valid:
                    db.execute("UPDATE feeds SET last_checked=?,error=NULL,item_count=coalesce(?,item_count) WHERE id=?",
                        (now(), len(articles) if articles is not None else None, sub["id"]))
                    db.execute("UPDATE scans SET last_polled=? WHERE id=?", (now(), sub["scan_id"]))
                    if inserted or state.failures:
                        self.activity(db, "feed_polled", f"Fetched {len(articles or [])} items from {url}" if not state.failures else f"Feed recovered: {url}", sub["scan_id"])
                arrivals = [db.execute("SELECT * FROM articles WHERE stable_key=?", (article.stable_key,)).fetchone() for article in articles or []]
                decision_counts = []
                for scan_id, status_token, subscriptions in targets:
                    scan = db.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
                    if scan is None or scan["status"] != "active" or scan["status_token"] != status_token:
                        continue
                    actual = {(feed["id"], feed["token"]) for feed in db.execute("SELECT id,token FROM feeds WHERE scan_id=?", (scan_id,))}
                    if not subscriptions <= actual:
                        continue
                    rules = Rules.parse(json.loads(scan["rules"]))
                    if any(sub["scan_id"] == scan_id and sub["last_checked"] is None for sub in valid):
                        stored_articles = db.execute("SELECT * FROM articles").fetchall()
                    else:
                        stored_articles = arrivals
                    for stored in stored_articles:
                        self._decide(db, scan, stored, rules)
                    decision_count, kept_count = db.execute("SELECT count(*),coalesce(sum(kept),0) FROM decisions WHERE scan_id=?", (scan_id,)).fetchone()
                    decision_counts.append({"scan_id": scan_id, "decision_count": decision_count,
                                            "kept_count": kept_count, "discarded_count": decision_count - kept_count})
                diagnostic = {"outcome": "recovered" if state.failures else "changed" if inserted else "unchanged",
                              "item_count": len(articles or []), "inserted_count": inserted,
                              "scan_count": len(targets), "not_modified": articles is None, "decisions": decision_counts}
                return None
        finally:
            with self.transaction() as db:
                db.execute("UPDATE feed_state SET claimed=0 WHERE url=?", (url,))
            outcome = diagnostic["outcome"]
            self.events.emit("feed_fetch_finished", "WARNING" if outcome == "backoff" else "DEBUG" if outcome in ("unchanged", "discarded") else "INFO",
                             feed_url=url, scan_id=for_scan, duration_ms=round((time.monotonic() - started) * 1000), **diagnostic)

    def _poll(self, scan_id: int, job_id: str) -> None:
        with self.transaction() as db:
            scan = db.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
            if scan is None or scan["status"] == "paused":
                self._progress(db, job_id, "Scan paused or deleted", status="cancelled", finished_at=now())
                return
            window = ScanWindow.parse(dict(scan))
            db.execute("UPDATE scans SET last_polled=?,completed_at=NULL,last_error=NULL WHERE id=?", (now(), scan_id))
            feeds = [dict(row) for row in db.execute("SELECT * FROM feeds WHERE scan_id=? ORDER BY id", (scan_id,))]
            job = db.execute("SELECT created_at FROM jobs WHERE id=?", (job_id,)).fetchone()
            since = publication_time(job["created_at"]).timestamp() if job else time.time()
        errors = [] if feeds else ["No feed subscriptions remain"]
        for index, feed in enumerate(feeds):
            with self.transaction() as db:
                valid = db.execute("SELECT 1 FROM feeds f JOIN scans s ON s.id=f.scan_id WHERE f.id=? AND f.token=? AND s.status='active'", (feed["id"], feed["token"])).fetchone()
            if not valid:
                continue
            self.progress(job_id, f"Fetching feed {index + 1} of {len(feeds)}")
            error = self._check_url(feed["url"], for_scan=scan_id, since=since, token=feed["token"])
            if error:
                errors.append(f"{feed['url']}: {error}")
        if not self._replay_job(scan_id, feeds):
            self.progress(job_id, "Scan or subscriptions changed during poll", status="cancelled", finished_at=now())
            return
        error = "; ".join(errors) or None
        with self.transaction() as db:
            current = db.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
            if current is None or not self._subscriptions_current(db, current, feeds):
                self._progress(db, job_id, "Scan or subscriptions changed during poll", status="cancelled", finished_at=now())
                return
            db.execute("UPDATE scans SET completed_at=?,last_error=? WHERE id=?", (now() if window.mode == "historical" else None, error, scan_id))
        if error:
            raise RuntimeError(error)

    def _setup_state(self, db: sqlite3.Connection, scan_id: int) -> dict[str, object] | None:
        run = db.execute("SELECT * FROM setup_runs WHERE scan_id=? ORDER BY rowid DESC LIMIT 1", (scan_id,)).fetchone()
        if run is None:
            return None
        history = [dict(row) for row in db.execute("""SELECT id,run_id,round,status,started_at,finished_at,
            reviewed_count,total_count,calls_used,rationale,error,actions,probes FROM setup_rounds
            WHERE scan_id=? ORDER BY id""", (scan_id,))]
        for item in history:
            item["actions"] = json.loads(item["actions"])
            item["probes"] = json.loads(item["probes"])
        return {"run_id": run["run_id"], "phase": run["phase"], "round": run["round"],
            "max_rounds": review.MAX_ROUNDS, "calls_used": run["calls_used"], "call_limit": run["call_limit"],
            "lifetime_calls_used": db.execute("SELECT coalesce(sum(calls_used),0) FROM setup_runs WHERE scan_id=?", (scan_id,)).fetchone()[0],
            "next_review_at": timestamp(run["next_review_at"]) if run["next_review_at"] is not None else None,
            "reviewed_count": run["reviewed_count"], "total_count": run["total_count"],
            "archive_through_id": run["archive_through_id"], "reason": run["reason"], "history": history}

    def setup_history(self, scan_id: object) -> dict[str, object] | None:
        with self.transaction() as db:
            ident = self._id(scan_id)
            result = self._setup_state(db, ident)
            if result is None:
                self.scan(db, ident)
            return result

    def _setup_hash(self, db: sqlite3.Connection, scan_id: int) -> str | None:
        scan = db.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
        if scan is None:
            return None
        value = {key: scan[key] for key in ("id", "topic", "objective", "rules", "revision", "status", "status_token",
            "mode", "start_date", "end_date", "poll_interval_minutes")}
        value["subscriptions"] = [list(row) for row in db.execute("SELECT url,token FROM feeds WHERE scan_id=? ORDER BY id", (scan_id,))]
        return hashlib.sha256(review.encoded(value)).hexdigest()

    def _invalidate_setup(self, db: sqlite3.Connection, scan_id: int, reason: str,
                          phase: str = "needs_attention", round_status: str = "stale") -> None:
        run = db.execute("SELECT * FROM setup_runs WHERE scan_id=? ORDER BY rowid DESC LIMIT 1", (scan_id,)).fetchone()
        if run is None or run["phase"] not in ("collecting", "reviewing", "ready"):
            return
        db.execute("UPDATE setup_runs SET phase=?,reason=?,next_review_at=NULL WHERE run_id=?", (phase, reason, run["run_id"]))
        db.execute("UPDATE setup_rounds SET status=?,finished_at=?,error=? WHERE run_id=? AND finished_at IS NULL",
            (round_status, now(), reason, run["run_id"]))
        db.execute("UPDATE jobs SET status=?,finished_at=?,message=? WHERE kind='setup_review' AND scan_id=? AND status IN ('queued','running')",
            ("failed" if round_status == "failed" else "cancelled", now(), reason, scan_id))
        self.activity(db, "setup_review", reason, scan_id, {"run_id": run["run_id"], "phase": phase})
        self.events.emit("setup_phase_changed", scan_id=scan_id, run_id=run["run_id"], round=run["round"],
                         phase=phase, round_status=round_status, reason=reason, outcome="pending_commit")

    def control_setup_review(self, data: dict[str, object]) -> None:
        scan_id = self._id(data.get("scan_id"))
        action = data.get("action")
        if action not in ("start", "stop"):
            raise ValueError("Review action must be start or stop")
        with self.transaction() as db:
            self.scan(db, scan_id)
            if action == "stop":
                self._invalidate_setup(db, scan_id, "Setup review stopped; collection continues", phase="stopped", round_status="cancelled")
            else:
                self._start_setup(db, scan_id)
            state = self._setup_state(db, scan_id)
        self.events.emit("setup_control", scan_id=scan_id, action=action, run_id=state["run_id"] if state else None, phase=state["phase"] if state else None)

    def _start_setup(self, db: sqlite3.Connection, scan_id: int, initial_calls: int = 0) -> None:
        scan = self.scan(db, scan_id)
        prior = db.execute("SELECT phase FROM setup_runs WHERE scan_id=? ORDER BY rowid DESC LIMIT 1", (scan_id,)).fetchone()
        if prior and prior["phase"] in ("collecting", "reviewing"):
            return
        if scan["status"] != "active":
            raise ValueError("Resume this scan before starting setup review")
        if not 1 <= db.execute("SELECT count(*) FROM feeds WHERE scan_id=?", (scan_id,)).fetchone()[0] <= 20:
            raise ValueError("Setup review requires 1 to 20 subscribed feeds")
        run_id = uuid.uuid4().hex
        current = time.time()
        db.execute("""INSERT INTO setup_runs(run_id,scan_id,phase,round,calls_used,call_limit,started_at,
            collection_started,config_hash,reason) VALUES (?,?,'collecting',1,?,?,?,?,?,?)""",
            (run_id, scan_id, initial_calls, review.CALL_LIMIT + initial_calls, now(), current,
             self._setup_hash(db, scan_id), "Collecting and replaying the available archive before review"))
        self._setup_collect(db, run_id, current)
        self.activity(db, "setup_review", "Started bounded setup review", scan_id, {"run_id": run_id, "initial_calls": initial_calls})

    def _setup_collect(self, db: sqlite3.Connection, run_id: str, current: float) -> None:
        run = db.execute("SELECT * FROM setup_runs WHERE run_id=?", (run_id,)).fetchone()
        scan_id = run["scan_id"]
        work = {kind: self._enqueue(db, kind, {"scan_id": scan_id}, "model") for kind in ("poll", "replay")}
        db.execute("""UPDATE setup_runs SET phase='collecting',collection_started=?,next_review_at=?,settle_deadline=?,work=?,
            archive_through_id=NULL,reviewed_count=0,total_count=0,config_hash=? WHERE run_id=?""",
            (current, current + review.COLLECT_SECONDS, current + review.COLLECT_SECONDS + review.SETTLE_SECONDS,
             json.dumps(work), self._setup_hash(db, scan_id), run_id))
        self._enqueue(db, "setup_review", {"scan_id": scan_id, "run_id": run_id, "round": run["round"]}, "model")
        self.events.emit("setup_phase_changed", scan_id=scan_id, run_id=run_id, round=run["round"], phase="collecting",
                         next_review_at=current + review.COLLECT_SECONDS, outcome="pending_commit")

    def _setup_current(self, db: sqlite3.Connection, job: DurableJob) -> sqlite3.Row | None:
        run = db.execute("SELECT * FROM setup_runs WHERE scan_id=? ORDER BY rowid DESC LIMIT 1", (job.scan_id,)).fetchone()
        status = db.execute("SELECT status FROM jobs WHERE id=?", (job.id,)).fetchone()
        if (run is None or run["run_id"] != job.payload["run_id"] or run["round"] != job.payload["round"]
                or run["phase"] not in ("collecting", "reviewing") or status is None or status["status"] != "running"):
            return None
        if self.stop_event.is_set():
            self._invalidate_setup(db, job.scan_id, "Server stopped during setup review; no automatic retry", round_status="failed")
            return None
        if self._setup_hash(db, job.scan_id) != run["config_hash"]:
            self._invalidate_setup(db, job.scan_id, "Target configuration changed during review; restart setup review")
            return None
        return run

    def _setup_due(self, db: sqlite3.Connection, row: sqlite3.Row) -> bool:
        data = json.loads(row["payload"])
        run = db.execute("SELECT * FROM setup_runs WHERE run_id=?", (data["run_id"],)).fetchone()
        if run is None or run["phase"] != "collecting" or run["round"] != data["round"]:
            self._progress(db, row["id"], "Setup run is no longer active", status="cancelled", finished_at=now())
            return False
        if self._setup_hash(db, run["scan_id"]) != run["config_hash"]:
            self._invalidate_setup(db, run["scan_id"], "Target configuration changed; restart setup review")
            return False
        current = time.time()
        if current < run["next_review_at"]:
            return False
        work = json.loads(run["work"])
        busy = any(db.execute("SELECT status FROM jobs WHERE id=?", (ident,)).fetchone()[0] in ("queued", "running")
            for ident in work.values())
        if busy:
            if current >= run["settle_deadline"]:
                self._invalidate_setup(db, run["scan_id"], "Collection/replay did not settle within the 300-second limit", round_status="failed")
            return False
        if run["archive_through_id"] is None:
            highwater = db.execute("SELECT coalesce(max(id),0) FROM articles").fetchone()[0]
            work["replay"] = self._enqueue(db, "replay", {"scan_id": run["scan_id"]}, "model")
            db.execute("UPDATE setup_runs SET archive_through_id=?,total_count=?,work=?,settle_deadline=? WHERE run_id=?",
                (highwater, db.execute("SELECT count(*) FROM articles WHERE id<=?", (highwater,)).fetchone()[0],
                 json.dumps(work), current + review.SETTLE_SECONDS, run["run_id"]))
            return False
        return True

    def _fail_setup(self, db: sqlite3.Connection, job: DurableJob, reason: str) -> None:
        run = self._setup_current(db, job)
        if run is None:
            return
        db.execute("UPDATE setup_rounds SET status='failed',finished_at=?,error=? WHERE run_id=? AND round=? AND finished_at IS NULL",
            (now(), reason, run["run_id"], run["round"]))
        db.execute("UPDATE setup_runs SET phase='needs_attention',reason=?,next_review_at=NULL WHERE run_id=?", (reason, run["run_id"]))
        self.activity(db, "setup_review", reason, job.scan_id, {"run_id": run["run_id"], "phase": "needs_attention"})

    @staticmethod
    def _probe_stats(probes: list[dict[str, object]]) -> list[dict[str, object]]:
        return [{key: probe[key] for key in ("url", "query", "item_count", "error")} for probe in probes]

    def _setup_probes(self, job: DurableJob) -> bool:
        with self.transaction() as db:
            run = self._setup_current(db, job)
            if run is None:
                return False
            pending = json.loads(run["pending_probes"])
            window = ScanWindow.parse(dict(self.scan(db, job.scan_id)))
        for request in pending:
            started = time.monotonic()
            self.events.emit("setup_probe_started", job_id=job.id, scan_id=job.scan_id, run_id=job.payload["run_id"], round=job.payload["round"], feed_url=request["url"])
            with self.transaction() as db:
                if self._setup_current(db, job) is None:
                    return False
            try:
                articles = [asdict(article) for article in fetch_feed(request["url"])]
                error = None
            except Exception as exc:
                articles, error = [], str(exc)
            for article in articles:
                article["window_reason"] = window.date_reason(article["published_at"])
            with self.transaction() as db:
                run = self._setup_current(db, job)
                if run is None:
                    return False
                probes = json.loads(run["probes"])
                probes.append({**request, "id": len(probes) + 1, "articles": articles,
                    "item_count": len(articles), "in_window_count": sum(article["window_reason"] is None for article in articles),
                    "error": error, "executed_at": now()})
                remaining = json.loads(run["pending_probes"])[1:]
                db.execute("UPDATE setup_runs SET probes=?,pending_probes=? WHERE run_id=?",
                    (json.dumps(probes, ensure_ascii=False), json.dumps(remaining), run["run_id"]))
            self.events.emit("setup_probe_finished", "WARNING" if error else "INFO", job_id=job.id, scan_id=job.scan_id, run_id=job.payload["run_id"], round=job.payload["round"], probe_id=probes[-1]["id"], item_count=len(articles), error=error, duration_ms=round((time.monotonic() - started) * 1000))
        return True

    def _setup_evidence(self, db: sqlite3.Connection, scan: sqlite3.Row, highwater: int,
                        probes: list[dict[str, object]]) -> tuple[list[dict[str, object]], dict[str, int]]:
        rules = Rules.parse(json.loads(scan["rules"]))
        window = ScanWindow.parse(dict(scan))
        records = db.execute("""SELECT a.*,d.kept,d.reasons,d.matched_terms,d.rule_revision,d.decided_at
            FROM articles a LEFT JOIN decisions d ON d.article_id=a.id AND d.scan_id=? WHERE a.id<=? ORDER BY a.id""",
            (scan["id"], highwater)).fetchall()
        evidence = []
        missing = inconsistent = kept = 0
        for row in records:
            record = dict(row)
            actual = None if record["kept"] is None else bool(record["kept"])
            reasons = None if record["reasons"] is None else json.loads(record["reasons"])
            terms = None if record["matched_terms"] is None else json.loads(record["matched_terms"])
            expected = evaluate(rules, record["title"], record["summary"], window, record["published_at"])
            missing += actual is None
            inconsistent += actual is not None and (actual != expected.kept or reasons != expected.reasons
                or terms != expected.matched_terms or record["rule_revision"] != scan["revision"])
            kept += actual is True
            evidence.append({"type": "decision", "article_id": record["id"],
                **{key: record[key] for key in ("title", "summary", "url", "source", "published_at", "feed_url", "fetched_at", "rule_revision", "decided_at")},
                "kept": actual, "reasons": reasons, "matched_terms": terms})
        for probe in probes:
            evidence.append({"type": "probe", **{key: value for key, value in probe.items() if key != "articles"}})
            evidence.extend({"type": "probe_article", "probe_id": probe["id"], "item_index": index, **article}
                for index, article in enumerate(probe["articles"]))
        return evidence, {"article_count": len(records), "kept_count": kept, "missing_count": missing, "inconsistent_count": inconsistent}

    def _setup_review_job(self, job: DurableJob) -> None:
        if not self._setup_probes(job):
            return
        with self.transaction() as db:
            run = self._setup_current(db, job)
            if run is None:
                return
            scan = self.scan(db, job.scan_id)
            probes = json.loads(run["probes"])
            highwater = run["archive_through_id"]
            evidence, report = self._setup_evidence(db, scan, highwater, probes)
            db.execute("UPDATE setup_runs SET phase='reviewing',total_count=?,reason=? WHERE run_id=?",
                (report["article_count"], "Reviewing every kept and discarded decision through the frozen archive boundary", run["run_id"]))
            db.execute("""INSERT INTO setup_rounds(run_id,scan_id,round,status,started_at,total_count,probes)
                VALUES (?,?,?,'running',?,?,?)""", (run["run_id"], job.scan_id, run["round"], now(), report["article_count"], json.dumps(self._probe_stats(probes))))
            feeds = [dict(row) for row in db.execute("SELECT url,last_checked,error,item_count FROM feeds WHERE scan_id=? ORDER BY id", (job.scan_id,))]
            allowed = {row[0] for row in db.execute("SELECT DISTINCT url FROM feeds")}
            allowed.update(probe["url"] for probe in probes if probe["error"] is None)
            prior = [dict(row) for row in db.execute("SELECT round,status,rationale,actions,findings FROM setup_rounds WHERE run_id=? AND finished_at IS NOT NULL ORDER BY round", (run["run_id"],))]
            for item in prior:
                item["actions"], item["findings"] = json.loads(item["actions"]), json.loads(item["findings"])
            metadata = {"mode": "setup_review", "run_id": run["run_id"], "round": run["round"], "max_rounds": review.MAX_ROUNDS,
                "scan": {**{key: scan[key] for key in ("id", "topic", "objective", "revision", "mode", "start_date", "end_date")}, "rules": json.loads(scan["rules"])},
                "archive_through_id": highwater, "frozen_at": now(), "report": report, "feeds": feeds,
                "poll_error": scan["last_error"], "authorized_feed_urls": sorted(allowed), "previous_rounds": prior,
                "manifest": {"article_ids": [item["article_id"] for item in evidence if item["type"] == "decision"],
                    "probes": [{key: probe[key] for key in ("id", "url", "query", "item_count", "in_window_count", "error", "independent")} for probe in probes]}}
            carry = json.loads(run["findings"])
            remaining = run["call_limit"] - run["calls_used"]
        if report["missing_count"] or report["inconsistent_count"]:
            raise ValueError(f"Deterministic evidence has {report['missing_count']} missing and {report['inconsistent_count']} inconsistent decisions through #{highwater}; no model call")
        evidence, metadata = review.model_input(evidence, metadata)
        pages = review.pages(evidence, metadata)
        if len(pages) > remaining:
            raise ValueError(f"Full review needs {len(pages)} CLI invocations for {report['article_count']} articles; only {remaining} remain. No partial review spending")
        seen_articles = {ident for finding in carry for ident in finding["article_ids"]}
        seen_probes = {ident for finding in carry for ident in finding["probe_ids"]}
        rationales = []
        reviewed = 0
        self.events.emit("setup_round_started", job_id=job.id, scan_id=job.scan_id, run_id=job.payload["run_id"],
                         round=job.payload["round"], page_count=len(pages), **report)
        for number, page in enumerate(pages, 1):
            payload = review.context(metadata, page, number, len(pages), carry)
            key = AttemptKey(job.id, job.kind, job.scan_id, job.payload["run_id"], job.payload["round"], number)
            with self.traces.open(key, payload, review.SCHEMA, review.PROMPT) as attempt:
                with self.transaction() as db:
                    run = self._setup_current(db, job)
                    if run is not None:
                        if run["calls_used"] >= run["call_limit"]:
                            raise ValueError("Setup CLI invocation limit reached before invocation")
                        db.execute("UPDATE setup_runs SET calls_used=calls_used+1 WHERE run_id=?", (run["run_id"],))
                        db.execute("UPDATE setup_rounds SET calls_used=calls_used+1 WHERE run_id=? AND round=? AND finished_at IS NULL", (run["run_id"], run["round"]))
                if run is None:
                    attempt.save(stage="stale_snapshot", outcome="stale")
                    return
                result = run_codex(payload, lambda message: self.progress(job.id, message), attempt=attempt,
                                   schema=review.SCHEMA, system_prompt=review.PROMPT)
                seen_articles.update(item["article_id"] for item in page if item["type"] == "decision")
                seen_probes.update(item["id"] if item["type"] == "probe" else item["probe_id"] for item in page if item["type"] in ("probe", "probe_article"))
                attempt.save(stage="page_validation", response=result,
                             validation={"supplied_article_ids": sorted(seen_articles), "supplied_probe_ids": sorted(seen_probes), "carried_findings": carry})
                output = review.parse_page(result, number, number == len(pages), carry, seen_articles, seen_probes)
                carry = output.findings
                rationales.append(f"Page {number}\n{output.rationale}")
                reviewed += sum(item["type"] == "decision" for item in page)
                attempt.save(stage="application" if number == len(pages) else "page_accepted")
                with self.transaction() as db:
                    current = self._setup_current(db, job)
                    if current is not None:
                        db.execute("UPDATE setup_runs SET reviewed_count=? WHERE run_id=?", (reviewed, job.payload["run_id"]))
                        db.execute("UPDATE setup_rounds SET reviewed_count=?,rationale=?,findings=? WHERE run_id=? AND round=? AND finished_at IS NULL",
                            (reviewed, "\n\n".join(rationales), json.dumps(carry, ensure_ascii=False), job.payload["run_id"], job.payload["round"]))
                        if number == len(pages):
                            current = self._setup_current(db, job)
                            self._apply_setup_round(db, job, current, output, carry, probes, allowed, report)
                if current is None:
                    attempt.save(stage="stale_snapshot", outcome="stale")
                    return
                if number == len(pages):
                    attempt.committed = True
            self.events.emit("setup_page_accepted", job_id=job.id, scan_id=job.scan_id, run_id=key.run_id,
                             round=key.round, page=number, attempt_id=attempt.id, reviewed_count=reviewed)
        self.events.emit("setup_round_applied", job_id=job.id, scan_id=job.scan_id, run_id=key.run_id, round=key.round,
                         action_count=len(output.actions), finding_count=len(carry))

    def _apply_setup_round(self, db: sqlite3.Connection, job: DurableJob, run: sqlite3.Row,
                           output: review.PageResult, findings: list[dict[str, object]], probes: list[dict[str, object]],
                           allowed: set[str], report: dict[str, int]) -> None:
        scan = self.scan(db, job.scan_id)
        feeds = {row[0] for row in db.execute("SELECT url FROM feeds WHERE scan_id=?", (job.scan_id,))}
        scoped = review.parse_actions(output.actions, Rules.parse(json.loads(scan["rules"])), feeds, allowed, ScanWindow.parse(dict(scan)))
        prior_sources = {review.source_identity(prior["url"]) for prior in probes}
        if any(review.probe_source(probe["url"]) in prior_sources for probe in scoped.probes):
            raise Rejected("A new probe must use a fresh URL/query, not repeat an executed probe", path="actions.probes",
                           expected="fresh source identity", received=scoped.probes, prior_sources=sorted(prior_sources), stage="scoped_validation")
        independent = any(probe["independent"] and probe["error"] is None and probe["in_window_count"] > 0 for probe in probes)
        if not independent and not any(probe["independent"] for probe in scoped.probes):
            raise Rejected("Review must request a fresh independent probe before stability can be assessed", path="actions.probes",
                           expected="a fresh independent probe or successful inspected independent evidence", received=scoped.probes, stage="scoped_validation")
        self._update_rules(db, job.scan_id, scoped.rules)
        for url in feeds - scoped.feeds:
            db.execute("DELETE FROM feeds WHERE scan_id=? AND url=?", (job.scan_id, url))
            self.activity(db, "feed_removed", f"Setup review removed {url}; article history retained", job.scan_id)
        for url in scoped.feeds - feeds:
            self._subscribe(db, job.scan_id, url)
        for action in output.actions:
            self.activity(db, "setup_tool", action["rationale"], job.scan_id, {"run_id": run["run_id"], **action})
        phase, status = "needs_attention", "needs_attention"
        reason = "Review has unresolved findings or did not report stable evidence"
        if output.actions:
            status = "changed"
            reason = "Changes or requested probes require a fresh collection and unchanged verification round"
            if run["round"] < review.MAX_ROUNDS:
                phase = "collecting"
            else:
                reason = "Round 3 changed rules, feeds, or probes; changes remain unverified at the round limit"
                if scoped.feeds != feeds or json.loads(scan["rules"]) != scoped.rules.to_dict():
                    self._enqueue(db, "poll", {"scan_id": job.scan_id}, "model")
                    self._enqueue(db, "replay", {"scan_id": job.scan_id}, "model")
        elif output.stable:
            _, actual = self._setup_evidence(db, scan, run["archive_through_id"], [])
            healthy = bool(feeds) and all(row["last_checked"] and not row["error"] for row in db.execute("SELECT last_checked,error FROM feeds WHERE scan_id=?", (job.scan_id,)))
            gates = []
            if run["round"] < 2:
                gates.append("at least two review rounds required")
            if not independent:
                gates.append("no successful nonempty independent probe inspected")
            if not actual["article_count"] or not actual["kept_count"]:
                gates.append("no kept relevant evidence in the actual archive")
            if actual["missing_count"] or actual["inconsistent_count"]:
                gates.append("deterministic decisions changed or have gaps")
            if not healthy or scan["last_error"]:
                gates.append("unresolved feed errors or unchecked feeds")
            if run["reviewed_count"] != report["article_count"]:
                gates.append("incomplete input coverage")
            if not gates:
                phase, status = "ready", "stable"
                reason = f"Stable on reviewed evidence through #{run['archive_through_id']} at {now()}. Coverage incomplete."
            else:
                reason = "Cannot mark ready; " + "; ".join(gates)
        db.execute("""UPDATE setup_rounds SET status=?,finished_at=?,actions=?,findings=?,error=?
            WHERE run_id=? AND round=? AND finished_at IS NULL""",
            (status, now(), json.dumps(output.actions, ensure_ascii=False), json.dumps(findings, ensure_ascii=False),
             reason if phase == "needs_attention" else None, run["run_id"], run["round"]))
        db.execute("UPDATE setup_runs SET phase=?,reason=?,next_review_at=NULL,pending_probes=?,findings=?,config_hash=? WHERE run_id=?",
            (phase, reason, json.dumps(scoped.probes), json.dumps(findings, ensure_ascii=False), self._setup_hash(db, job.scan_id), run["run_id"]))
        if phase == "collecting":
            db.execute("UPDATE setup_runs SET round=round+1 WHERE run_id=?", (run["run_id"],))
            self._setup_collect(db, run["run_id"], time.time())
        self._progress(db, job.id, reason, status="completed", finished_at=now())
        self.activity(db, "setup_review", reason, job.scan_id, {"run_id": run["run_id"], "phase": phase, "round": run["round"]})

    def _decision_context(self, state: dict[str, object], history: list[dict[str, object]]) -> tuple[dict[str, object], str]:
        scans = [{key: scan[key] for key in ("id", "topic", "objective", "rules", "revision", "status", "mode", "start_date", "end_date", "completed_at", "archive_through_id")}
            for scan in state["scans"]]
        decisions = [{"article_id": record["id"], **{key: record[key] for key in ("scan_id", "kept", "reasons", "matched_terms", "rule_revision", "decided_at")}}
            for record in state["records"]]
        by_pair = {(row["scan_id"], row["article_id"]): row for row in decisions}
        missing, inconsistent = [], []
        for scan in scans:
            rules = Rules.parse(scan["rules"])
            window = ScanWindow.parse(scan)
            for article in history:
                pair = {"scan_id": scan["id"], "article_id": article["id"]}
                old = by_pair.get((scan["id"], article["id"]))
                if old is None:
                    eligible = scan["status"] == "active"
                    if scan["mode"] == "historical" and scan["completed_at"]:
                        if scan["archive_through_id"] is not None:
                            eligible = eligible and article["id"] <= scan["archive_through_id"]
                        else:
                            fetched = publication_time(article["fetched_at"])
                            completed = publication_time(scan["completed_at"])
                            eligible = eligible and fetched is not None and completed is not None and fetched <= completed
                    if eligible:
                        missing.append(pair)
                    continue
                result = evaluate(rules, article["title"], article["summary"], window, article["published_at"])
                if old["kept"] != result.kept or old["rule_revision"] != scan["revision"] or old["reasons"] != result.reasons or old["matched_terms"] != result.matched_terms:
                    inconsistent.append({**pair, "expected": asdict(result)})
        payload = {"mode": "decision_audit", "objective": state["settings"]["audit_objective"], "scans": scans,
            "history": history, "decisions": decisions, "report": {"gap_definition": "No decision for an active scan. Paused scans are excluded. Completed historical scans include articles through their last replay boundary. Legacy completed scans use archive timestamps through completion. Later arrivals are excluded.",
                "missing_count": len(missing), "inconsistent_count": len(inconsistent),
                "missing_decisions": missing, "inconsistent_decisions": inconsistent}}
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        return payload, digest

    def _model_job(self, kind: str, data: dict[str, object], job_id: str, origin: str = "user") -> None:
        audit_id: int | None = None
        try:
            with self.transaction() as db:
                state = self.state()
                history = [dict(row) for row in db.execute("SELECT * FROM articles ORDER BY id")]
            scans = state["scans"]
            expected = self._snapshot(scans)
            deletion_id = max((event["id"] for event in state["activity"] if event["type"] == "scan_deleted"), default=0)
            digest = None
            feed_options = []
            setup_urls = None
            if kind in ("audit", "decision_audit"):
                if kind == "decision_audit":
                    payload, digest = self._decision_context(state, history)
                    if origin == "schedule":
                        with self.transaction() as db:
                            prior = db.execute("SELECT input_hash FROM audits WHERE kind='decision' AND status='completed' ORDER BY id DESC LIMIT 1").fetchone()
                            if prior and prior["input_hash"] == digest:
                                self._progress(db, job_id, "Decision inputs unchanged since last successful review", status="cancelled", finished_at=now())
                                return
                else:
                    payload = {}
                with self.transaction() as db:
                    windows = [{key: scan[key] for key in ("id", "topic", "mode", "start_date", "end_date", "kept", "discarded")} for scan in scans]
                    cursor = db.execute("INSERT INTO audits(started_at,status,windows,kind,input_hash,report,coverage_count,decision_count) VALUES (?,'running',?,?,?,?,?,?)",
                        (now(), json.dumps(windows), "decision" if kind == "decision_audit" else "coverage", None,
                         json.dumps(payload.get("report", {})), len(history), len(state["records"])))
                    audit_id = cursor.lastrowid
                    db.execute("UPDATE settings SET value=? WHERE key=?", (json.dumps(time.time()), f"last_{kind}_at"))
                if not scans:
                    raise ValueError("Create a scan before running an audit")
                if len(scans) > 20:
                    raise ValueError("Audits support at most 20 scans in this prototype. No scans were silently omitted")
                objective = state["settings"]["audit_objective"]
                if kind == "audit":
                    feed_options = list({feed["url"]: feed for scan in scans
                        for feed in discovery(scan["topic"], ScanWindow.parse(scan)) + [{"url": f["url"], "name": "Subscribed source"} for f in scan["feeds"]]}.values())
                    fresh: list[dict[str, object]] = []
                    for index, scan in enumerate(scans):
                        window = ScanWindow.parse(scan)
                        self.progress(job_id, f"Fetching broader context for scan {index + 1} of {len(scans)}")
                        fresh.extend({**asdict(article), "scan_id": scan["id"], **window.to_dict()}
                            for article in fetch_feed(discovery(scan["topic"], window)[0]["url"]))
                    decisions = [{"article_id": record["id"], **{key: record[key] for key in ("scan_id", "mode", "start_date", "end_date", "kept", "reasons", "matched_terms", "rule_revision", "decided_at")}} for record in state["records"]]
                    payload = {"mode": kind, "objective": objective, "scans": scans,
                        "history": history, "decisions": decisions, "broader_headlines": fresh, "discovery": feed_options}
                    with self.transaction() as db:
                        db.execute("UPDATE audits SET fresh_count=? WHERE id=?", (len(fresh), audit_id))
            else:
                topic = required_text(data.get("topic"), "Topic", 300)
                objective = required_text(data.get("objective"), "Objective")
                window = ScanWindow.parse(data)
                supplied = feed_urls(data.get("feed_urls", []))
                setup_urls = supplied or [query_url(topic, window)]
                feed_options = list({feed["url"]: feed for feed in discovery(topic, window)
                    + [{"url": url, "name": "User source"} for url in supplied]}.values())
                self.progress(job_id, "Fetching setup source previews")
                preview = [asdict(article) for url in setup_urls for article in fetch_feed(url)]
                payload = {"mode": kind, "scan_window": window.to_dict(), "topic": topic, "objective": objective,
                    "discovery": feed_options, "user_feed_urls": supplied, "preview": preview, "existing_scans": scans}
            if origin == "schedule":
                with self.transaction() as db:
                    if not self.settings(db)[f"{kind}_enabled"]:
                        if audit_id is not None:
                            db.execute("UPDATE audits SET status='failed',finished_at=?,error='Scheduled audit disabled before model execution' WHERE id=?", (now(), audit_id))
                        self._progress(db, job_id, "Scheduled audit disabled", status="cancelled", finished_at=now())
                        return
            with self.traces.open(AttemptKey(job_id, kind, audit_id=audit_id), payload, PLAN_SCHEMA, MODEL_PROMPT) as attempt:
                if kind == "setup":
                    with self.transaction() as db:
                        reserved = db.execute("SELECT status,payload FROM jobs WHERE id=?", (job_id,)).fetchone()
                        if reserved["status"] == "running":
                            reserved_payload = json.loads(reserved["payload"])
                            reserved_payload["cli_calls_used"] = 1
                            db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(reserved_payload), job_id))
                    if reserved["status"] != "running":
                        attempt.save(stage="stale_snapshot", outcome="stale")
                        return
                plan = run_codex(payload, lambda message: self.progress(job_id, message), attempt=attempt)
                attempt.save(stage="application", response=plan, validation={"expected_snapshot": expected, "expected_deletion_id": deletion_id})
                self.progress(job_id, "Validating and applying tool plan atomically")
                with self.transaction() as db:
                    existing_ids = {row[0] for row in db.execute("SELECT id FROM scans")}
                    self._apply_plan(db, plan, feed_options, kind, str(objective), expected,
                        ScanWindow.parse(data) if kind == "setup" else None, deletion_id, setup_urls)
                    if kind == "setup":
                        for scan_id in {row[0] for row in db.execute("SELECT id FROM scans")} - existing_ids:
                            self._start_setup(db, scan_id, initial_calls=1)
                    self.activity(db, "model_plan", str(plan["rationale"]), details=plan["actions"])
                    if audit_id is not None:
                        db.execute("UPDATE audits SET status='completed',finished_at=?,rationale=?,actions=?,input_hash=? WHERE id=?",
                            (now(), plan["rationale"], json.dumps(plan["actions"]), digest, audit_id))
                    self._progress(db, job_id, "Completed", status="completed", finished_at=now())
                attempt.committed = True
            self.events.emit("model_plan_applied", job_id=job_id, attempt_id=attempt.id, kind=kind,
                             audit_id=audit_id, action_count=len(plan["actions"]))
        except Exception as exc:
            if audit_id is not None:
                with self.transaction() as db:
                    db.execute("UPDATE audits SET status='failed',finished_at=?,error=? WHERE id=? AND status='running'", (now(), str(exc), audit_id))
            raise

    @staticmethod
    def _snapshot(scans: list[dict[str, object]]) -> dict[int, tuple[object, ...]]:
        return {s["id"]: (s["revision"], s["status"], s["objective"], s["poll_interval_minutes"],
            s["mode"], s["start_date"], s["end_date"], [(f["url"], f["token"]) for f in s["feeds"]]) for s in scans}

    def apply_plan(self, plan: dict[str, object], discovery_feeds: list[dict[str, str]], kind: str, objective: str,
                   expected: dict[int, tuple[object, ...]] | None = None, setup_window: ScanWindow | None = None,
                   expected_deletion_id: int | None = None) -> set[int]:
        with self.transaction() as db:
            return self._apply_plan(db, plan, discovery_feeds, kind, objective, expected, setup_window, expected_deletion_id)

    def _apply_plan(self, db: sqlite3.Connection, plan: dict[str, object], discovery_feeds: list[dict[str, str]], kind: str, objective: str,
                    expected: dict[int, tuple[object, ...]] | None, setup_window: ScanWindow | None,
                    expected_deletion_id: int | None, setup_urls: list[str] | None = None) -> set[int]:
        try:
            rationale = required_text(plan.get("rationale"), "Plan rationale", 20000)
        except ValueError as exc:
            raise Rejected(str(exc), path="rationale", expected="nonempty text, at most 20000 characters", received=plan.get("rationale")) from exc
        actions = plan.get("actions")
        if not isinstance(actions, list) or len(actions) > 24:
            raise Rejected("Model plan requires at most 24 actions", path="actions", expected="array, at most 24 actions", received=actions)
        affected: set[int] = set()
        setup_hashes = {row[0]: self._setup_hash(db, row[0]) for row in db.execute("SELECT id FROM scans").fetchall()}
        aliases: dict[str, int] = {}
        if expected_deletion_id is not None and db.execute("SELECT coalesce(max(id),0) FROM activity WHERE type='scan_deleted'").fetchone()[0] != expected_deletion_id:
            raise Rejected("A scan was deleted while Codex was planning. No tools applied; run the job again", path="snapshot.deletion_id", expected=expected_deletion_id, received=db.execute("SELECT coalesce(max(id),0) FROM activity WHERE type='scan_deleted'").fetchone()[0], stage="stale_snapshot")
        if expected is not None:
            scans = [{**dict(s), "feeds": [dict(f) for f in db.execute("SELECT url,token FROM feeds WHERE scan_id=? ORDER BY id", (s["id"],))]} for s in db.execute("SELECT * FROM scans")]
            actual = self._snapshot(scans)
            if actual != expected:
                raise Rejected("Scans changed while Codex was planning. No tools applied; run the job again", path="snapshot.scans", expected=expected, received=actual, stage="stale_snapshot")
        allowed_urls = {safe_url(feed["url"]) for feed in discovery_feeds}
        allowed_urls.update(row["url"] for row in db.execute("SELECT url FROM feeds"))
        for index, action in enumerate(actions):
            try:
                if not isinstance(action, dict):
                    raise ValueError("Tool actions must be objects")
                tool = action.get("tool")
                if kind == "decision_audit" and tool not in ("update_scan", "replay_history"):
                    raise ValueError("Decision audits may only update rules and replay existing scans")
                ref = required_text(action.get("scan_ref"), "scan_ref", 100)
                reason = required_text(action.get("rationale"), "Tool rationale", 5000)
                if tool == "create_scan":
                    if ref.isdecimal() or ref in aliases:
                        raise ValueError("New scans need a unique nonnumeric alias")
                    topic = required_text(action.get("topic"), "Topic", 300)
                    scan_objective = objective if kind in ("audit", "decision_audit") else required_text(action.get("objective"), "Objective")
                    window = (setup_window or ScanWindow()) if kind == "setup" else ScanWindow()
                    source_ref = action.get("source_scan_ref")
                    if source_ref is not None:
                        if kind != "audit" or not isinstance(source_ref, str) or not source_ref.isdecimal():
                            raise ValueError("Historical audit creation requires an existing numeric source_scan_ref")
                        window = ScanWindow.parse(dict(self.scan(db, int(source_ref))))
                        if window.mode != "historical":
                            raise ValueError("source_scan_ref must identify a historical scan")
                    if kind == "audit" and any(key in action and action[key] != value for key, value in window.to_dict().items()):
                        raise ValueError("Audit-created dates must inherit source_scan_ref exactly")
                    scan_id = self._create(db, topic, scan_objective, Rules.parse(action.get("rules")), minutes(5 if action.get("poll_interval_minutes") is None else action["poll_interval_minutes"]), window)
                    aliases[ref] = scan_id
                else:
                    scan_id = aliases.get(ref) or (int(ref) if ref.isdecimal() else 0)
                    scan = self.scan(db, scan_id)
                    if kind == "setup" and ScanWindow.parse(dict(scan)) != (setup_window or ScanWindow()):
                        raise ValueError("Setup tools cannot target a scan with another mode or date window")
                    if any(key in action and action[key] != scan[key] for key in ("mode", "start_date", "end_date")):
                        raise ValueError("Model tools cannot change a scan's mode or date window")
                    if tool == "update_scan":
                        interval = action.get("poll_interval_minutes")
                        self._update_rules(db, scan_id, Rules.parse(action.get("rules")), minutes(interval) if interval is not None else None)
                    elif tool == "subscribe_feed":
                        url = safe_url(action.get("feed_url"))
                        if kind == "setup" and setup_urls is not None and url not in setup_urls:
                            raise ValueError("Setup must use the requested custom feeds or default topic search")
                        if url not in allowed_urls:
                            raise ValueError("Model selected a feed outside the supplied discovery catalog")
                        review.probe_source(url)
                        self._subscribe(db, scan_id, url)
                    elif tool == "replay_history":
                        self._enqueue(db, "replay", {"scan_id": scan_id}, "model")
                    else:
                        raise ValueError(f"Unknown tool: {tool}")
                affected.add(scan_id)
                self.activity(db, "model_tool", f"{tool}: {reason}", scan_id, {**action, **ScanWindow.parse(dict(self.scan(db, scan_id))).to_dict()})
            except ValueError as exc:
                raise Rejected(str(exc), path=f"actions[{index}]", expected="valid authorized tool action", received=action, stage="plan_validation") from exc
        if kind == "setup" and (not aliases or not any(db.execute("SELECT 1 FROM feeds WHERE scan_id=?", (scan_id,)).fetchone() for scan_id in aliases.values())):
            raise ValueError("Setup must create a scan and subscribe a feed. No tools applied")
        if kind == "setup" and setup_urls is not None:
            for scan_id in aliases.values():
                actual = {row["url"] for row in db.execute("SELECT url FROM feeds WHERE scan_id=?", (scan_id,))}
                if actual != set(setup_urls):
                    raise ValueError("Setup must subscribe all requested feeds. No tools applied")
        for scan_id in affected:
            scan = self.scan(db, scan_id)
            if scan_id in setup_hashes and setup_hashes[scan_id] != self._setup_hash(db, scan_id):
                self._invalidate_setup(db, scan_id, "Model plan changed target configuration; restart setup review")
            if scan["status"] == "active" and (scan["mode"] == "live" or scan["last_polled"] is None) and kind != "decision_audit":
                self._enqueue(db, "poll", {"scan_id": scan_id}, "model")
        self.activity(db, "plan_applied", rationale, details={"tool_count": len(actions)})
        return affected
