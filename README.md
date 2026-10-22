# Wirewatch

A local news-monitoring prototype with a shared article archive, deterministic filters, and optional model-assisted setup and review.

Give a scan a topic and an objective. Wirewatch collects RSS and Atom entries, keeps or discards them with saved rules, and stores the decisions in SQLite. The web UI shows scans, records, activity, jobs, and review history.

## Status

This is a single-user prototype. The implementation is submitted separately from this README in the first pull request. Check out `feat/initial-prototype` to run it before that PR merges.

Collection works independently of model work. Autonomous setup can tune rules and feeds, but it is not yet reliable: a real run stopped during verification when its model response failed evidence-reference validation. It did not reach Ready.

**Logging is the next priority.** Activity history does not capture enough diagnostic detail across the application, and rejected model responses are not retained. That gap must be addressed before the setup failure can be diagnosed reliably.

## Run with Docker

From a checkout containing the implementation:

```sh
docker compose build
docker compose up -d
```

Open <http://127.0.0.1:8765>. Python, Node, the Blueprint React UI, and Codex CLI are packaged in the image. No host language runtime is required.

Manual rule-based scans need no model login. For model-assisted setup or audits:

```sh
docker compose run --rm wirewatch codex login --device-auth
```

The archive and Codex credentials live in separate Docker volumes. `docker compose down` keeps them. `docker compose down -v` deletes them. The repository contains no user archive or credentials.

## What uses a model

- Feed collection, matching, archive replay, and feed-health checks use no model. Active live scans check feeds on a best-effort 15-second interval, with conditional requests and error backoff.
- Codex setup can create rules and subscriptions, then collect, review, adjust, and verify them. Review is limited to three rounds and 12 CLI invocations, plus one initial creation invocation. Manual setup review is optional.
- Decision audits examine kept and discarded decisions. Coverage audits examine subscriptions and missing sources. They have separate switches and both default to off.

Live and historical scans share rule, feed, review, record, export, and deletion tools. Historical scans use an inclusive UTC date window and finite collection runs. Feed retention and Google News limits mean historical coverage is incomplete.

## Development

Use Python 3.10 or newer and Node 22.12 or newer. Python uses the standard library. UI dependencies are locked in `ui/package-lock.json`.

```sh
npm --prefix ui ci
npm --prefix ui run build
python3 server.py --port 8765
python3 check.py
```

The checks use temporary databases and local feed fixtures. They stub the model boundary and make no real Codex calls. Docker builds the UI itself.

The implementation includes `docs/reference.md` with API details, lifecycle rules, persistence behavior, and prototype limits.

## Limits

- One application process owns one local SQLite database. There are no user accounts or hosted deployment features.
- Matching uses substrings in titles and stored excerpts, not full article understanding.
- RSS and search feeds cannot guarantee complete or instantaneous coverage.
- A review marked Ready means stable on the evidence it reviewed, not proven product defects or complete national coverage.
- Model calls can fail or return invalid evidence references. Failed reviews stop visibly without automatic paid retries.
