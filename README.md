# Event Platform

A distributed event processing platform: high-volume web events are accepted over
HTTP, buffered in an SQS-style queue, persisted to MongoDB by a background worker,
indexed into Elasticsearch for full-text search, and summarised through a Redis
cache.

* **[ARCHITECTURE.md](ARCHITECTURE.md)**: system diagram, component responsibilities, queue guarantees, storage, indexing and caching rationale, failure modes, scaling, SQS drop-in design and what I would change. Start there.
* Python 3.12 · FastAPI · MongoDB 7 (PyMongo async) · Elasticsearch 8 · Redis 7 · pytest

## Contents

- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API](#api)
- [Project structure](#project-structure)
- [Testing](#testing)
- [AI in My Workflow](#ai-in-my-workflow)

## Quick start

### Everything in Docker

```bash
make up        # docker compose up -d --build --wait  (app + MongoDB + Elasticsearch + Redis)
```

The API is then on <http://localhost:8000> and interactive docs on
<http://localhost:8000/docs>. `make down` stops everything.

### Local development

Requires [uv](https://docs.astral.sh/uv/) and Docker (for the backing services).

```bash
make install                                         # uv sync (creates .venv)
docker compose up -d --wait mongo elasticsearch redis
cp .env.example .env                                 # optional, defaults work locally
make run                                             # uvicorn with --reload on :8000
```

### Try it

```bash
curl -X POST localhost:8000/events -H 'content-type: application/json' -d '{
  "event_type": "pageview",
  "timestamp": "2026-10-01T12:00:00Z",
  "user_id": "user-123",
  "source_url": "https://example.com/pricing",
  "metadata": {"browser": "firefox", "device": "desktop", "campaign": "Spring Sale"}
}'
# {"event_id":"8f0c…","status":"queued"}

curl 'localhost:8000/events?user_id=user-123'
curl 'localhost:8000/events/stats?bucket=hour&start=2026-10-01T00:00:00Z&end=2026-10-02T00:00:00Z'
curl 'localhost:8000/events/search?q=spring+sale'
curl -i 'localhost:8000/events/stats/realtime'
```

## Configuration

All settings are environment variables (or `.env`), defined in
[config.py](src/event_platform/config.py). The most relevant ones:

| Variable | Default | Purpose |
|---|---|---|
| `MONGO_URL` / `MONGO_DATABASE` | `mongodb://localhost:27017` / `event_platform` | Primary store |
| `ELASTICSEARCH_URL` / `ELASTICSEARCH_INDEX` | `http://localhost:9200` / `events` | Search index |
| `REDIS_URL` | `redis://localhost:6379/0` | Cache and rate-limit counters |
| `QUEUE_MAX_SIZE` | `10000` | Queue capacity; beyond it `POST /events` returns 503 |
| `QUEUE_VISIBILITY_TIMEOUT_SECONDS` | `30` | How long a received message stays hidden before redelivery |
| `QUEUE_MAX_RECEIVE_COUNT` | `5` | Deliveries before a message moves to the dead-letter queue |
| `WORKER_ENABLED` / `WORKER_BATCH_SIZE` | `true` / `10` | Background worker |
| `RETRY_BASE_DELAY_SECONDS` / `RETRY_MAX_DELAY_SECONDS` | `1` / `60` | Exponential backoff bounds |
| `REALTIME_STATS_TTL_SECONDS` | `10` | Redis TTL of the realtime summary |
| `REALTIME_STATS_WINDOW_SECONDS` | `3600` | Window the realtime summary covers |
| `RATE_LIMIT_ENABLED` / `RATE_LIMIT_REQUESTS` / `RATE_LIMIT_WINDOW_SECONDS` | `true` / `600` / `60` | Per-IP rate limit |
| `ADMIN_API_KEY` | unset | Enables `/admin/*`; unset means those routes return 404 |

## API

Full OpenAPI docs at `/docs`. Error bodies are always `{"detail": ...}`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/events` | Validate and enqueue an event. **202** `{event_id, status}` · 422 invalid · 429 rate limited · 503 queue full (`Retry-After`) |
| `GET` | `/events` | Filtered listing, newest first, keyset-paginated |
| `GET` | `/events/stats` | Counts by event type per time bucket (MongoDB aggregation) |
| `GET` | `/events/search` | Full-text search over metadata (Elasticsearch) |
| `GET` | `/events/stats/realtime` | Per-type counts over the last window, served from Redis |
| `GET` | `/health` | Liveness |
| `GET` | `/health/ready` | Readiness with per-dependency status |
| `GET` | `/admin/dead-letters` | List dead-lettered events (`X-Admin-Key` required) |
| `POST` | `/admin/dead-letters/redrive` | Re-enqueue dead-lettered events (`X-Admin-Key` required) |

### `POST /events`

| Field | Type | Rules |
|---|---|---|
| `event_type` | string | lowercase snake_case, starts with a letter, ≤ 64 chars (`pageview`, `click`, `conversion`, …) |
| `timestamp` | ISO 8601 | **must include a timezone offset**; stored in UTC |
| `user_id` | string | non-blank, ≤ 128 chars |
| `source_url` | string | absolute `http`/`https` URL, ≤ 2048 chars |
| `metadata` | object | any JSON object, ≤ 16 KB serialised |
| `event_id` | string, optional | idempotency key; generated when omitted. Resubmitting the same `event_id` stores the event once. |

Unknown fields are rejected. `202` means *accepted for processing*: the event
becomes visible to queries once the worker has persisted it, normally within
milliseconds.

### `GET /events`

Query parameters (all optional, combinable): `event_type`, `user_id`, `source_url`
(exact match), `start` (inclusive), `end` (exclusive), `limit` (1–500, default
50), `cursor`.

```json
{
  "items": [{"event_id": "…", "event_type": "click", "timestamp": "2026-10-01T12:00:00Z",
             "user_id": "user-123", "source_url": "https://…", "metadata": {}}],
  "next_cursor": "eyJ0cyI6…"
}
```

Pass `next_cursor` back as `cursor` to get the next page; it is `null` on the last
page. Dates without an offset are read as UTC.

### `GET /events/stats`

Same filters as `/events`, plus `bucket=hour|day|week` (default `day`; weeks start
on Monday, UTC). If `start`/`end` are omitted the window defaults to the last 24
hours, 30 days or 12 weeks. Requests covering more than 1 000 buckets get a 422.

```json
{
  "bucket": "day", "start": "2026-09-01T00:00:00Z", "end": "2026-10-01T00:00:00Z",
  "items": [{"bucket_start": "2026-09-30T00:00:00Z", "event_type": "click", "count": 42}]
}
```

### `GET /events/search`

`q` (required, ≤ 256 chars) plus the same filters as `/events` and `limit` (1–100,
default 20). Terms are AND-ed; `"exact phrase"`, `-exclude`, `prefix*` and `a | b`
are supported. Matching is case- and accent-insensitive across all metadata values
and the parts of the source URL.

```json
{"total": 1, "items": [{"score": 1.38, "event": {"event_id": "…", "…": "…"}}]}
```

### `GET /events/stats/realtime`

```json
{
  "generated_at": "2026-10-01T12:00:00Z",
  "window_start": "2026-10-01T11:00:00Z", "window_end": "2026-10-01T12:00:00Z",
  "total": 7, "counts_by_type": {"click": 2, "pageview": 5},
  "cache": {"hit": true, "ttl_seconds": 10}
}
```

The `X-Cache: HIT|MISS` header reports where the answer came from. Values can be up
to `ttl_seconds` old by design; see [Caching strategy](ARCHITECTURE.md#6-caching-strategy).

### Rate limiting and errors

Every non-health route returns `X-RateLimit-Limit`, `X-RateLimit-Remaining` and
`X-RateLimit-Reset`. When a backing service is down, affected endpoints return
**503 with `Retry-After`**, never an unhandled 500. `/health/ready` reports which
service is degraded.

## Project structure

```
src/event_platform/
├── domain/              Event entity and invariants (no I/O, no frameworks)
├── application/         Use cases + ports (Protocols): ingestion, processing, worker,
│                        querying, search, realtime stats, rate limiting, readiness
├── infrastructure/
│   ├── queue/           In-memory SQS-style queue
│   ├── mongo/           Repository, reader, query builders, indexes
│   ├── elasticsearch/   Mapping, query DSL builders, index adapter
│   └── redis/           Realtime stats cache, rate limiter
├── api/                 Routes, schemas, dependencies, middleware, error mapping
├── container.py         Composition root
└── main.py              App factory and lifespan
tests/
├── unit/                Fast, no services needed
├── integration/         Real MongoDB / Elasticsearch / Redis; skipped when unreachable
├── factories.py         Test data builders
└── fakes.py             In-memory implementations of the ports
```

## Testing

```bash
make test-unit                                        # ~130 tests, no services needed, ~3 s
docker compose up -d --wait mongo elasticsearch redis
make test                                             # full suite (167 tests) with coverage
make check                                            # ruff lint + format check, mypy --strict, tests
```

Integration tests look for services on the default ports (override with
`TEST_MONGO_URL`, `TEST_ELASTICSEARCH_URL`, `TEST_REDIS_URL`). Each test gets its own
MongoDB database, Elasticsearch index and Redis key prefix, so tests are isolated and
can run against a shared dev stack. If a service is unreachable, its tests are
**skipped, not failed**, so the unit suite runs anywhere.

Current state: 167 tests, 99 % line and branch coverage, `mypy --strict` clean.

### Philosophy

* **Test behaviour at the seams that matter, not implementation details.** Unit tests target the domain rules, the queue's delivery semantics, the processor's ack/retry decisions, pagination and the HTTP contract (status codes, headers, error bodies).
* **Fakes over mocks.** Ports are small `Protocol`s, so tests use real in-memory implementations ([fakes.py](tests/fakes.py)) and assert on outcomes ("the event is stored once and acked") rather than on call sequences. Refactoring internals does not break tests.
* **Never mock the database to test database behaviour.** Aggregation pipelines, `$dateTrunc` week boundaries, index selection, analyzers and mappings only mean something against the real engines, so those are integration tests against real MongoDB, Elasticsearch and Redis.
* **Lock in non-functional decisions.** Index usage is asserted through `explain()` plans (no collection scans, no in-memory sorts). The strict ES mapping and the accent-folding analyzer have tests. Stampede protection is tested with concurrent requests.
* **Deterministic time.** The queue, backoff, query defaults and rate limiter take injectable clocks or random sources, so timeout and window behaviour is tested exactly, without `sleep`.
* **Failure paths are first-class.** Outages are simulated by pointing adapters at a closed port, and every adapter must surface them as `DependencyUnavailableError` → 503. Retries, the DLQ, redrive, worker crash and stop timeouts are all covered.
* **Full lifecycles**: ingest → worker → MongoDB; ingest → `GET /events` and `/events/stats`; ingest → `/events/search`; ingest → realtime stats after cache expiry; duplicate submissions stored once.

These tests caught real bugs during development. The clearest one: an Elasticsearch
race where the worker could index an event before the index existed, which would
have created it with a dynamic mapping (see [ARCHITECTURE.md §5](ARCHITECTURE.md#5-indexing-strategy)).

### With more time I would add

1. **Load tests** (k6 / Locust) against docker-compose to find where throughput degrades and validate the "what breaks first" analysis.
2. **Chaos tests**: kill the worker mid-batch and restart MongoDB / Elasticsearch under load, asserting no loss and no duplicates end to end.
3. **Property-based tests** (Hypothesis) for keyset pagination (every event seen exactly once under random inserts and duplicate timestamps) and for the domain validation rules.
4. **Contract tests** running the same queue test suite against the in-memory queue and an SQS adapter on LocalStack.
5. **CI pipeline** (GitHub Actions with service containers) running `make check` on every push.

## AI in My Workflow

<!-- TODO(author): review and personalise this section before submitting. The items
below are factual for how this repository was built; add your own judgement calls,
the places you pushed back, and your time estimates. -->

**Tools.** I used Claude Code (Anthropic's Claude Opus model, in the Claude desktop
app) as a pair programmer for implementation, testing and documentation.

**How I structured the collaboration.**

* I gave the assistant the assignment and constraints (FastAPI, clean architecture, SOLID / DRY / KISS, tests) and had it propose an **8-stage plan** (scaffold → ingestion → worker → querying → search → caching → hardening → docs).
* Each stage ended with the changes staged, a quality gate run (ruff, `mypy --strict`, unit and integration tests) and a **proposed commit message that I reviewed and approved** before anything was committed. The git history is one reviewed commit per stage.
* I used it to turn requirements into concrete semantics. For example, modelling the in-memory queue on real SQS behaviour (visibility timeout, receipt handles, redrive policy) instead of an `asyncio.Queue`, so the worker would be written against realistic at-least-once guarantees.

**Where AI output was corrected, and why.** Every stage went through tooling and
tests, which caught real problems:

* **Elasticsearch race condition**: the first implementation created the index only at startup. The ingest → search integration test showed the worker could index before the index existed, which would have left Elasticsearch with a dynamic mapping. The fix: lazy creation before the first write, an empty result for searches against a missing index, and auto-creation disabled in docker-compose.
* **Flaky test removed rather than "fixed" with a sleep**: a Redis TTL-expiry test depended on sub-millisecond timing and only tested Redis itself; another test already checked that we set the TTL.
* **Design smells removed**: an `assert` used for type narrowing in production code was replaced by an explicit return value; a `TYPE_CHECKING` circular-import workaround was replaced by moving the search models next to the other read models.
* **Tooling side effects caught**: an automatic `ruff --fix` removed imports that looked unused because a code edit had silently not applied; the failing integration run exposed it.
* **Process and attribution**: I asked for the AI co-author trailer to be left out of commit messages and for every commit to wait for my approval.

<!-- TODO(author): add the decisions where you overrode or redirected the assistant. -->

**How it shaped the approach.** The biggest effect was not speed of typing code
but the ability to keep a high bar cheaply: strict typing, 99 % coverage with
behaviour-focused tests, `explain()`-based index assertions and outage tests were
affordable within the timeline. I spent my own time on the architecture document,
on the trade-offs (what to deliberately *not* build, such as write-path cache
invalidation, extra indexes and FIFO queues), and on reviewing every stage.
