# Wirewatch

A single-user news scanner with a shared SQLite archive, deterministic rules, and optional Codex setup and audits.

## Run with Docker

Docker supplies Python, Node, Codex CLI `0.163.0`, and the compiled UI. No host language runtime or bind mount is needed.

```sh
docker compose build
```

To retain an existing local archive, import it using the next section before the first start. Otherwise the first start creates an empty archive.

```sh
docker compose run --rm wirewatch codex login --device-auth
docker compose up -d
```

Open <http://127.0.0.1:8765>. Login is optional for rule-only scans. The login command stores credentials in the private `codex` named volume. Linux uses file authentication storage. Credentials and local data are excluded from the image build.

The `archive` volume stores `/data/wirewatch.sqlite3`. Restarting or recreating the container preserves both volumes. `docker compose down` preserves them too. Do not use `down -v` if you want to keep them.

The image runs as the nonroot `node` user. Compose publishes only `127.0.0.1:8765`, includes an HTTP healthcheck, and restarts the service unless stopped.

### Import an existing local archive

Stop the original Python server and any Wirewatch container first. Build the image, then run this from the repository root before its first start. The command reads the stopped local database, uses SQLite backup, checks its integrity, and refuses to replace an existing container archive. It leaves the original file untouched.

```sh
docker compose stop
docker compose run --rm --no-deps -T wirewatch python3 -c '
import os, pathlib, sqlite3, sys, tempfile
with tempfile.TemporaryDirectory(dir="/data") as directory:
    source = pathlib.Path(directory) / "source.sqlite3"
    backup = pathlib.Path(directory) / "backup.sqlite3"
    source.write_bytes(sys.stdin.buffer.read())
    with sqlite3.connect(source) as old, sqlite3.connect(backup) as new:
        assert old.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "Invalid database"
        old.execute("SELECT id FROM scans LIMIT 1")
        old.backup(new)
    os.link(backup, "/data/wirewatch.sqlite3")
print("Imported archive without replacing existing data")
' < data/wirewatch.sqlite3
docker compose up -d
```

Adjust the input path if the original server used `--db`. A separate SQLite writer or uncheckpointed WAL file requires a proper SQLite backup before import.

## Use scans

Active live scans check RSS and Atom subscriptions continuously on a best-effort 15-second base interval. Slow feeds and HTTP backoff can delay checks. This is polling, not instantaneous publisher delivery. Shared URLs are fetched once per due cycle. Conditional requests use offered ETag and Last-Modified validators.

Each legitimate feed arrival is evaluated for every active live scan, even scans subscribed to different sources. Fetches capture eligible scans before the request. Pausing, deleting, or removing a captured subscription prevents its in-flight decisions from changing that scan. Decisions use the rules current when the response is saved.

Pause stops collection and incoming decisions for that scan. Resume replays the shared archive, including articles archived while paused. Creation, new subscriptions, rule edits, reruns, and explicit replay also evaluate archived articles. Replay still works while paused. The legacy `poll_interval_minutes` value remains stored but does not control collection.

Historical scans require `start_date` and `end_date` in `YYYY-MM-DD` format. Both dates are inclusive UTC publication dates. The end cannot be in the future. Mode and dates cannot change after creation. Create another scan to change the window.

A historical scan runs once at creation, combines its subscribed sources with replay of the entire saved archive, and then stops collecting. Run now explicitly reruns it. Success or failure persists in `completed_at` and `last_error`. It never ingests later live feed arrivals automatically. Replay and rule edits still respect its window. Unknown publication dates and dates outside the window have explicit discard reasons. All fetched metadata, including discarded items, stays in the shared archive.

Both creation methods and both modes accept optional custom RSS or Atom feeds. An empty feed list defaults to the topic Google News search, with `after:` and `before:` terms for historical scans. The `before:` bound is the day after the selected end. Historical topics containing their own date operators are rejected. Sources can be added or removed in either mode. Adding a source to completed historical work does not start a recurring collection schedule. Use Run now to fetch it.

Custom feeds often omit old reporting. Google News RSS is also capped and incomplete. A completed historical scan means the request finished, not that every published story was found. Every source uses the same publication-date gate. An explicit historical replay judges the whole archive, including later arrivals published within its window.

Rules match case-insensitive substrings in the title and stored excerpt. Any terms require one match; all terms require every match; exclude terms reject on any match. Empty positive lists accept everything unless an exclusion or date rule rejects it. Rule edits replay the shared archive and increment the rule revision when terms change.

Deleting a scan atomically removes that scan, its subscriptions, and its decisions. Shared articles, activity, and audits remain. Deletion records its topic and ID, cancels queued work, and prevents an in-flight fetch from writing results for the deleted scan. Scan IDs never reuse a committed deleted ID.

## Use Codex and audits

Setup, explicit setup review, and audits invoke Codex. Incoming articles use deterministic rules. Initial setup and audits return a bounded JSON tool plan for `create_scan`, `update_scan`, `subscribe_feed`, and `replay_history`. The server validates and applies the entire plan in one SQLite transaction. Invalid or stale plans apply nothing. Initial setup enforces the requested mode, dates, and source selection independently of the model. Accepted plan effects, downstream jobs, audit outcome, and model job completion commit together.

### Autonomous setup review

Successful **Create with Codex** automatically starts a setup-review run for each newly created scan. **Create with rules** makes zero model calls and starts no review. Both live and historical scans offer explicit Start, Stop, and Restart. Stop ends review while collection continues. Starting an active run is idempotent. Restart after a terminal result creates a fresh run and budget; lifetime usage remains visible.

A run has `collecting`, `reviewing`, `ready`, `needs_attention`, or `stopped` phase. Each round requests an explicit poll and complete archive replay, waits a persisted 60-second collection window, and waits for deterministic work to finish. Initial collection has another 300 seconds to settle. The replay queued when the archive boundary freezes gets its own persisted 300-second allowance. Waiting for the model lane does not consume that replay allowance. Restart preserves the collection window and gives resumed deterministic work 300 seconds to settle. Hung work still needs attention when its deadline expires. Failing feeds become review evidence, so the model can remove poor sources. Routine live collection continues during model calls and independent probe requests. Completed historical scans explicitly poll and replay again during setup review; they retain their finite UTC publication window and never acquire a recurring collection schedule.

Each review freezes one archive highwater mark. Every stored article through that boundary appears exactly once per round with its full stored title and excerpt, source, dates, actual kept or discarded decision, reasons, terms, and rule revision. Every executed independent probe retains all fetched titles, excerpts, sources, dates, eligibility, execution details, and errors. A shared source catalog holds actual feed URLs and subscription authorization; records reference it by `source_ref`. Model input uses a shared `evidence_columns` catalog and dense rows. Each row starts with its record type, followed by every value in that type's column order. This lossless format avoids repeating field names without shortening text or removing rows. Model input omits opaque article URLs and deduplication keys, which do not affect matching. `article_id` maps decisions to the archive's stored clickable URLs. Probe items use `probe_id` and `item_index`. Pages contain at most 450000 UTF-8 bytes of serialized evidence rows. The complete prompt, column catalog, metadata, prior findings, and evidence must fit within 600000 bytes per invocation. A round has at most four pages. Oversized records, a fifth page, context overflow, or insufficient remaining calls stop review visibly before partial spending for that round. There is no sampling or per-article model call.

The model can replace this scan's rules, add or remove its feeds, and request up to six fresh query or feed probes per round. It cannot change another scan, the objective, dates, or either audit setting. Query probe text cannot supply date operators. Historical Google feed probes accept existing date bounds and replace them with the scan's authoritative window. Equivalent dated or canonical Google subscription URLs do not count as independent sources. Existing accepted Google subscriptions with missing, empty, repeated, or date-only queries keep their validated literal URL as identity. They do not prevent empty plans from being accepted. Newly requested Google probes and model subscriptions still require a valid nonempty query. Custom feed URLs and live Google feed date queries retain their existing semantics. Probe results include every fetched title and excerpt, failures, and historical date eligibility. Probes do not enter the archive until their feed is subscribed. The next round assesses the actual results before using them. A new subscription requires an already authorized feed URL or a successfully executed probe. Every rule or feed change requires another collection, replay, and unchanged verification round.

Ready requires at least two rounds, a successful nonempty independent probe inspected by the model, a nonempty archive with kept evidence, checked healthy subscriptions, complete input coverage, consistent deterministic decisions, and a stable final plan without actions or unresolved findings. The result says "Stable on reviewed evidence through #H/time. Coverage incomplete." Locale and query selection do not prove national coverage. Empty relevant results and unresolved feed errors cannot become ready. A change in round three remains unverified and needs attention.

Each manually started run permits at most 12 Codex CLI invocations across three rounds. Automatic creation permits 13, including its initial planning invocation. The engine first retains required trace input, then reserves usage durably before each model attempt; failed or interrupted attempts retain their reservation. These limits cap application CLI invocations, not provider requests or tokens inside the CLI. Responses permit at most 24 actions, 32 cumulative findings totaling 50000 bytes, and 80000 bytes of output. A finding's reason permits 6000 characters and a page rationale permits 12000. Bound failures remain explicit instead of discarding evidence or rationales.

Target edits, pause, deletion, Stop, or a replacement run invalidate the current run before another page can spend or apply effects. Normal arrivals, feed check timestamps, and unrelated scan edits do not invalidate frozen evidence. Ready configuration edits invalidate readiness without automatic spending. Finished round history remains immutable and survives scan deletion. A queued review survives restart with its original collection window; a running review becomes `needs_attention` without retry. Plan effects, finished round, next phase, downstream jobs, and model job completion commit atomically. The jobs-table migration also commits its rename, copy, and index replacement in one explicit transaction. The engine is the only SQL owner.

Decision and coverage audits have independent toggles, intervals, and manual runs. Both default to off with a 15-minute interval. Both use the shared audit objective and each scan's objective.

A decision audit reviews every stored kept and discarded decision, article, rule, date window, and reason. Its deterministic report lists inconsistent decisions and eligible missing decisions. Paused scans are excluded from missing-decision checks. Completed historical scans exclude arrivals beyond their last replay boundary. The model can update rules and replay existing scans only. Decision review does not fetch feeds. Scheduled reviews skip model execution when their full decision input matches the last successful review. Manual runs may review unchanged inputs.

A coverage audit checks subscription health and independently fetched broader headlines against the full archive and decisions. Feed errors, failure counts, next-check times, and missing subscriptions remain visible without a model. Coverage can tune rules, subscribe feeds, replay, and create related scans. New scans default to live and inherit the audit objective. A coverage `create_scan` action can set `source_scan_ref` to an existing historical scan's numeric ID to inherit that immutable window. Neither audit automatically reruns completed historical collection.

Codex runs in an ephemeral temporary directory with a read-only sandbox, no user configuration, a JSON output schema, file authentication storage, and a 240-second timeout. No API key is required after CLI login. Initial setup and audit subscriptions must come from the supplied discovery catalog or existing subscriptions. User feeds enter the setup catalog and preview in both modes. Setup review additionally accepts successfully executed probe URLs.

## API

Mutations require JSON and `X-Wirewatch: 1`. Host and browser Origin checks require the loopback origin even when the container binds internally to `0.0.0.0`.

- `GET /api/state` retains scans, records, activity, audits, settings, jobs, and counts. It adds `collection_interval_seconds: 15`. Feeds add `next_check_at` and `failures`. Audit rows add `kind: "coverage" | "decision"` and a parsed `report` object. Decision reports contain `gap_definition`, `missing_count`, `inconsistent_count`, `missing_decisions`, and `inconsistent_decisions`. Each scan also has `mode`, `start_date`, `end_date`, `completed_at`, and `last_error`. Records and JSON exports include their scan window. Legacy scans migrate as live without changing their data.
- `GET /api/discover?topic=...&mode=historical&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD` returns the dated catalog and preview. Omitting mode defaults to live.
- `POST /api/scans` and `POST /api/plan` accept the same window fields plus existing topic, objective, rules, and subscription fields. Both endpoints accept optional `feed_urls` in either mode. Empty lists select the topic search. Custom lists select those sources.
- `POST /api/scans/update` rejects window changes. `POST /api/scans/delete` accepts `{"scan_id":123}`. Missing, invalid, or nonexistent IDs return a clear 400 error.
- `POST /api/scans/review` accepts `{"scan_id":123,"action":"start"}` or `"stop"` and returns `{"ok":true}`. No model work starts from viewing state. Each scan adds nullable `setup_review` with run ID, phase, round and call limits, lifetime calls, UTC next review time, reviewed and total counts, archive boundary, reason, and round history. History includes each round's status, timestamps, counts, calls, rationale, error, actions, and probe URL/query/count/error summaries. `GET /api/scans/review?scan_id=123` returns `{"setup_review":...}`, including retained review history after deletion. Jobs add `kind:"setup_review"`.
- `POST /api/audit` remains coverage. `POST /api/decision-audit` runs decision review. `POST /api/settings` accepts partial updates. Existing `audit_enabled` and `audit_interval_minutes` control coverage. `decision_audit_enabled` and `decision_audit_interval_minutes` control decision review.
- `POST /api/poll` and `POST /api/replay` accept `{"scan_id":123}`. Poll allows an explicit historical rerun after completion.
- `GET /api/health` returns `{"ok":true}`. Packaged Vite files load from `/assets/` with safe path resolution.

## Local diagnostics

`diagnostics.py` owns JSON formatting, redaction, private file permissions, rotation, and model trace retention. Its files live under `<db.parent>/diagnostics`. In Docker this is `/data/diagnostics` in the existing archive volume. No HTTP route serves diagnostic files. Git and Docker build exclusions cover them.

Events are JSONL on stderr and in `events.jsonl`. Each file permits 5000000 bytes with four numbered backups. Each record permits 16384 UTF-8 bytes and marks truncation explicitly. INFO is the default. `WIREWATCH_LOG_LEVEL=DEBUG` adds successful state and health polling, unchanged feed checks, and request starts. Failures remain visible at WARNING or ERROR. Rotation preserves mode `0600`; diagnostic directories use `0700`. Symlink file targets and diagnostic directories are refused.

Events include UTC `at`, `level`, `event`, and `thread`. Each boundary supplies existing IDs and safe counts, durations, statuses, or error messages. HTTP completion joins a request to its returned `job_id`. Jobs join to scans, setup runs, rounds, and model attempt IDs. Events staged within a SQLite transaction use `outcome: "pending_commit"`; a later committed job or application event confirms the result. Business activity remains separate SQLite history.

Events cover application start and stop, migration and recovery, HTTP completion and errors, discovery errors, scan/rules/feeds/settings changes, job enqueue/coalescing/claim/progress/finish/failure, worker termination, feed fetch/backoff/recovery, decision counts, replay completion, setup control/phases/rounds/pages/probes, and model application. Quiet state/health requests and unchanged fetches use DEBUG.

`ModelTraces.open(AttemptKey(...), payload, schema, instructions)` persists complete structured input before the engine reserves a model attempt. `AttemptKey` carries `job_id`, `kind`, and nullable `scan_id`, `run_id`, `round`, `page`, and `audit_id`. `run_codex` requires `attempt=`. The attempt context covers CLI invocation, JSON decoding, domain validation, and final application commit. No new model call or retry exists for diagnostics.

Each attempt has a unique JSON file in `traces/<attempt-id>.json`. It contains correlation, input, schema, instructions, actual stdin and command, CLI timing/exit/timeout, raw response, stdout/stderr, validation evidence, stage, and final outcome. Setup input retains its dense rows, column and source catalogs, manifest, and carried findings. Validation records cumulative supplied article/probe IDs. `Rejected` preserves human messages and adds private `path`, `expected`, `received`, bad and duplicate references, or stale snapshot details. Failed JSON, CLI, domain validation, and application stages remain distinguishable.

Each stream retains at most 65536 bytes and the raw plan at most 262144 bytes, with observed total bytes, retained bytes, and truncation flags. Counts refer to bytes before redaction. The CLI writes its last message through a drained FIFO, so no unbounded output file remains in the temporary directory. The 240-second timeout kills and reaps its process group, retaining partial output. The independent 200000-byte plan acceptance limit is unchanged. Complete traces permit 4000000 bytes; oversized preflight fails explicitly. Completed traces, including successful inputs, remain until the 50-file or 128000000-byte store limit evicts them. Active attempts are protected by a local lock. Orphaned attempts and interrupted atomic-write files become eligible for retention on the next write. One process owns these files; there is no cross-process locking.

Redaction covers nested credential-shaped keys and text, authorization and cookie material, bearer tokens, URL userinfo, and sensitive URL query parameters. No code reads credential files or logs environment or request-header dumps. Ordinary events omit prompts and article bodies. Private traces still contain user objectives and news text, so they must remain local.

Redaction is best effort. Assignments nested beyond five JSON serialization layers can retain credentials. Some suffix-glued names, such as `passwordhash` and `secretaccesskey`, and backslash-prefixed token assignments can also remain visible. Conservative matching can hide ports or stack-line numbers in sensitive-word host and file names. Keep diagnostic files private.

Ordinary diagnostic failures emit bounded stderr emergency events and do not stop deterministic collection. Required trace writes fail closed before model spending or before subsequent validation/application. Failed reserved attempts retain their existing call count. Context finalization preserves the original exception. If final persistence fails after a real transaction commits, the completed application job stays completed and is not retried. `model_trace_outcome_unconfirmed` reports the attempt/job identity and `application_committed`; the retained file may still say pending. That status is diagnostic uncertainty and does not imply rollback. Retention deletions and failures have explicit events. The earlier discarded CPSC rejection is not recoverable from these new files.

### HTTP correlation and browser reports

Every handled response includes `X-Wirewatch-Request`. A valid inbound value of 16 lowercase hex characters is echoed; otherwise the server generates one. Existing response bodies and state/job schemas remain compatible.

`POST /api/client-errors` uses the loopback, Origin, JSON, and `X-Wirewatch: 1` guards. Its body permits at most 8192 bytes and exactly these required fields:

- `kind` is `api`, `schema`, `runtime`, `rejection`, or `render`.
- `message` is nonempty text, redacted and clipped to 2000 characters.
- `request_id` is null or a 16-character lowercase hex ID of the original API failure.
- `path` is null or an application route beginning `/`, without query or fragment, at most 256 characters.
- `status` is null or an integer from 100 through 599.
- `issue_path` is null or text, redacted and clipped to 300 characters.
- `stack` is null or text, redacted and clipped to 4000 characters.
- `count` is an integer from 1 through 1000.

Unknown fields and wrong types return 400. Oversized bodies return 413. A locked per-process limiter accepts at most 30 reports in 60 seconds and returns 429 on excess. Accepted reports return `200 {"ok":true}` and produce a `client_error` event. Its `request_id` refers to the original failure; `reporting_request_id` refers to the report request itself.

## Prototype limits and local development

One Engine process owns the database. SQLite stores accepted jobs, setup runs and round history, URL validators, due times, and backoff. A model lane runs setup, setup review, and audits sequentially. A review awaiting its collection window does not block other queued model work. The collector reserves one HTTP slot for live due checks and one for explicit poll and replay jobs, so a slow model or many historical feeds cannot occupy all collection capacity. Network and model execution run outside SQLite transactions. Replay releases the transaction lock between 64-article batches.

Restart requeues interrupted poll and replay jobs idempotently. A model job interrupted while running fails visibly and is never retried automatically. A model job still queued may execute once. Scheduled audits recheck their toggle before starting. A removed, paused, or deleted subscription invalidates its in-flight results, including removal followed by re-addition with the same feed ID. An unchanged or empty collection tick creates no job or activity entry.

Poll and replay requests accepted during a running pass persist in its job payload. Completion atomically requeues that job when another pass is needed. Restart retains the request, and deletion keeps the job cancelled.

Local HTTP failures back off exponentially from 30 seconds to 30 minutes. Retry-After seconds and HTTP dates set a persistent server floor, even above that local cap. Manual reruns bypass local backoff but respect the server floor. HTTP success resets failures.

RSS and Atom store metadata and excerpts up to 2000 characters, without full article scraping. Feed requests have a 12-second timeout and a 2 MB limit. URLs deduplicate globally; the first fetched metadata wins. Counts measure scan decisions, so the same article can count for several scans.

Audits support at most 20 scans, 600000 UTF-8 bytes of context, and 24 actions. Larger inputs fail visibly without silently sampling history. `/api/state` returns the complete archive and activity, which limits archive size. This prototype has no accounts or hosted deployment.

For direct development, use Python 3.10 or newer and Node 22.12 or newer. Build the Blueprint React UI before starting Python. Run these commands from the repository root.

```sh
npm --prefix ui ci
npm --prefix ui run build
python3 server.py --port 8765
python3 check.py
```

The generated `static` directory is ignored by Git. Docker builds it inside the image.

The direct server defaults to `127.0.0.1` and `data/wirewatch.sqlite3`. `--db` selects a separate archive. `--host 0.0.0.0` is intended for the container; HTTP loopback Host and Origin checks remain in place. Checks use temporary databases, controlled collection time, and real loopback HTTP RSS and Atom fixtures. Only the model boundary is stubbed. They retain all prior 66 check groups and cover complete 1800-record review pages, six independent 100-item probes, exact UTF-8 bounds, late discarded misses, source changes, ready gates, budgets, schema bounds, atomic rollback, stop/edit/pause/delete/restart between pages, persisted collection windows, late model-lane availability, resumed deterministic deadlines, dated Google probes, and jobs-migration failure and process-crash rollback. Collection and replay also run during a blocked model review. Diagnostic regressions also execute fake CLI subprocesses for malformed JSON, nonzero exits, timeouts, bounded streams, and temporary cleanup. They exercise real HTTP ingress, request IDs, retention, permissions, redaction, concurrent writes, and unavailable storage before and after real commits. No check invokes a real Codex process or modifies a running container or user archive.
