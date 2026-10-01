# Architecture

This document explains how the event platform is put together, why each piece is
where it is, how it fails, and what I would change before running it in production.
It is meant to be read alongside the code; file references point to the
implementation of each decision.

## Contents

1. [System overview](#1-system-overview)
2. [Component responsibilities](#2-component-responsibilities)
3. [Async ingestion pipeline and queue design](#3-async-ingestion-pipeline-and-queue-design)
4. [Storage rationale](#4-storage-rationale)
5. [Indexing strategy](#5-indexing-strategy)
6. [Caching strategy](#6-caching-strategy)
7. [Failure modes and graceful degradation](#7-failure-modes-and-graceful-degradation)
8. [Scaling: what breaks first at 10x](#8-scaling-what-breaks-first-at-10x)
9. [AWS SQS drop-in design](#9-aws-sqs-drop-in-design)
10. [What I would do differently](#10-what-i-would-do-differently)

---

## 1. System overview

**Write path** (asynchronous):

```
 POST /events
      │
      ▼
 ┌────────────────────┐   INCR / EXPIRE   ┌───────┐
 │ RateLimit          │──────────────────▶│ Redis │      429 + Retry-After
 │ middleware         │                   └───────┘
 └─────────┬──────────┘
           ▼
 ┌────────────────────┐                                  422 invalid event
 │ Event entity       │
 │ (validation)       │
 └─────────┬──────────┘
           ▼ publish                                     503 + Retry-After if full
 ┌──────────────────────────────┐                        202 {event_id} otherwise
 │ In-memory SQS-style queue    │── max receives ──▶ DLQ ──▶ /admin/dead-letters
 │ visibility timeout, receipts │                            (list / redrive)
 └─────────┬────────────────────┘
           │ receive (long poll, batches of 10)
           ▼
 ┌──────────────────────────────┐  1. insert (_id = event_id)  ┌───────────────┐
 │ EventWorker / EventProcessor │─────────────────────────────▶│ MongoDB       │
 │ ack on success,              │  2. index (id = event_id)    ├───────────────┤
 │ retry with backoff on error  │─────────────────────────────▶│ Elasticsearch │
 └──────────────────────────────┘                              └───────────────┘
```

**Read path** (synchronous):

```
 GET /events, /events/stats ─▶ EventQueryService ────▶ MongoDB (compound indexes, aggregation)
 GET /events/search ─────────▶ EventSearchService ───▶ Elasticsearch
 GET /events/stats/realtime ─▶ RealtimeStatsService ─▶ Redis ── hit ─▶ cached summary
                                                         └──── miss ─▶ MongoDB, then SET with TTL
```

Data flows in one direction: **HTTP → queue → worker → MongoDB → Elasticsearch**.
Reads go to whichever store is built for the query: MongoDB for filtered listing
and aggregations, Elasticsearch for full-text search, Redis for the hot summary.

### Code structure (clean architecture)

```
src/event_platform/
├── domain/          Event entity and its invariants. No framework or I/O imports.
├── application/     Use cases (ingestion, processing, querying, search, realtime stats,
│                    readiness) and the ports (Protocols) they depend on.
├── infrastructure/  Adapters implementing the ports: queue/, mongo/, elasticsearch/, redis/.
├── api/             FastAPI routes, request/response schemas, middleware, error mapping.
├── container.py     Composition root: the only module that picks concrete adapters.
└── main.py          App factory and lifespan (start worker, close clients).
```

Dependencies point inwards: `api → application → domain`, and
`infrastructure → application/domain`. The application layer never imports
`pymongo`, `elasticsearch` or `redis`; it depends on `EventPublisher`,
`EventConsumer`, `EventRepository`, `EventReader`, `EventIndexer`, `EventSearcher`,
`RealtimeStatsCache` and `RateLimiter` ([ports.py](src/event_platform/application/ports.py)).
That is what lets the unit tests use in-memory fakes, and what makes the SQS
swap in [section 9](#9-aws-sqs-drop-in-design) a new adapter rather than a rewrite.

Producer and consumer sides of the queue are separate protocols on purpose: the
API can only publish, the worker can only consume.

## 2. Component responsibilities

| Component | Owns | Does not own |
|---|---|---|
| **API** (FastAPI) | Transport: parsing, schema checks, status codes, rate limiting, translating domain/application errors into HTTP in one place ([errors.py](src/event_platform/api/errors.py)). | Business rules. Validation rules live in the `Event` entity so they exist exactly once. |
| **Domain** (`Event`) | What a valid event is: snake_case `event_type`, http(s) `source_url`, timezone-aware timestamp normalised to UTC, bounded id lengths, metadata ≤ 16 KB ([events.py](src/event_platform/domain/events.py)). | Persistence or serialisation. |
| **Queue** | Decoupling request latency from storage latency; buffering during downstream outages; delivery semantics (visibility, retries, DLQ); backpressure. | Durability (in-memory — see failure modes). |
| **Worker** | Draining the queue, persisting then indexing each event, acking on success, scheduling retries with backoff on failure, graceful shutdown. | Deciding what is valid; it trusts the domain entity. |
| **MongoDB** | The system of record. Filtered listing, keyset pagination, time-bucketed aggregations. | Free-text relevance search. |
| **Elasticsearch** | A derived, rebuildable read model for full-text search over metadata and URLs. | Being the source of truth. Anything in ES can be rebuilt from MongoDB. |
| **Redis** | Ephemeral, shared state: the realtime stats cache and rate-limit counters. Configured without persistence, LRU-bounded. | Any data that cannot be lost. |

## 3. Async ingestion pipeline and queue design

### Flow

1. `POST /events` validates the payload by constructing the `Event` entity. Invalid → `422`; nothing is enqueued.
2. The event is published to the queue and the API answers `202 {"event_id", "status": "queued"}`. The request never waits on MongoDB or Elasticsearch.
3. The worker long-polls the queue in batches (default 10) and processes the batch concurrently.
4. For each message: insert into MongoDB, then index into Elasticsearch, then ack (delete). Any exception → `retry_later` with backoff; the message is *not* acked.

### Queue guarantees ([in_memory.py](src/event_platform/infrastructure/queue/in_memory.py))

The queue deliberately reproduces the semantics of an SQS **standard** queue, so
that the rest of the system is written against real-world guarantees, not the
convenient ones of an in-process list:

| Property | Behaviour |
|---|---|
| Delivery | **At-least-once.** A received message stays in the queue, invisible for `visibility_timeout` (30 s). If not acked in time — worker crashed, hung, or was slow — it becomes visible again and is redelivered. |
| Receipt handles | Each delivery gets a new handle; handles from earlier deliveries are stale and cannot ack. A slow worker cannot delete a message that has already been handed to someone else. |
| Ordering | Best-effort FIFO only. Retries and redeliveries reorder events. Nothing downstream depends on order: events carry their own timestamp. |
| Retries | `retry_later(handle, delay)` is `ChangeMessageVisibility`. The worker uses exponential backoff with *equal jitter*: ceiling doubles per attempt (1 s, 2 s, 4 s … capped at 60 s), actual delay uniform in `[ceiling/2, ceiling]`. Jitter avoids a synchronised thundering herd on a recovering database; the lower bound prevents back-to-back retries ([backoff.py](src/event_platform/application/backoff.py)). |
| Dead-letter queue | Redrive policy: a message received `max_receive_count` (5) times is moved to the DLQ instead of being delivered again. `GET /admin/dead-letters` lists them, `POST /admin/dead-letters/redrive` re-enqueues them once the cause is fixed. |
| Capacity | Bounded (`QUEUE_MAX_SIZE`, 10 000). When full, `publish` fails fast and the API returns `503` with `Retry-After`: explicit backpressure instead of unbounded memory growth. |
| Durability | **None.** Messages live in process memory and are lost on restart. This is the main simplification of the exercise; see failure modes and section 9. |

### Idempotency and deduplication

At-least-once delivery means every consumer side effect must be idempotent:

* **MongoDB**: `event_id` is stored as `_id`. A redelivered or resubmitted event hits the unique primary key, `insert_one` raises `DuplicateKeyError`, and the repository returns `False` ("already stored"). Deduplication costs no extra read and no extra index.
* **Elasticsearch**: documents are indexed with `id = event_id`, so re-indexing overwrites instead of duplicating.
* **Producers**: `event_id` is optional in the API. A client that supplies its own id gets end-to-end idempotent retries: submitting the same event three times stores it once (covered by an integration test).

Because both writes are idempotent, a message that failed half-way (saved to
MongoDB, failed to index) is simply processed again from the top. There is no
compensation logic and no partial state to reason about.

## 4. Storage rationale

**MongoDB is the source of truth** because events are semi-structured (free-form
`metadata`), append-heavy, and queried with equality filters, time ranges and
aggregations. The aggregation pipeline (`$match` → `$group` by `$dateTrunc` bucket
and type → `$sort`) expresses the stats endpoint directly, and documents round-trip
the metadata object without a schema migration per feature.

**Elasticsearch is a derived read model**, used only for what MongoDB is bad at:
relevance-ranked full-text search with proper analysis (tokenisation, case and
accent folding). I considered and rejected:

* *MongoDB `$text` index*: one text index per collection, weak analysis, no relevance tuning, and it competes for write throughput and RAM with the operational indexes.
* *Elasticsearch as the only store*: near-real-time refresh, weaker guarantees for a system of record, and aggregations over raw events are not where I want correctness to live. Keeping ES derived means it can be dropped and rebuilt from MongoDB at any time (new mapping, new analyzer, corruption).

**Redis holds only disposable state**: cached summaries and rate-limit counters.
It runs without persistence and with `allkeys-lru` eviction. Losing it costs a few
cache misses and briefly unenforced rate limits, nothing more.

The split follows one rule: *each store answers the queries it is good at, and
only one store is authoritative.*

### Document shapes

MongoDB ([documents.py](src/event_platform/infrastructure/mongo/documents.py)):

```json
{ "_id": "<event_id>", "event_type": "click", "timestamp": ISODate("…Z"),
  "user_id": "user-123", "source_url": "https://…", "metadata": { … },
  "ingested_at": ISODate("…Z") }
```

Elasticsearch adds a derived `metadata_text` field (see below) and keeps the full
`_source`, so search results do not need a second round trip to MongoDB.

## 5. Indexing strategy

### MongoDB ([indexes.py](src/event_platform/infrastructure/mongo/indexes.py))

Every list query sorts newest first by `(timestamp, _id)`. The `_id` tie-breaker
makes the order total, which keyset pagination requires. Indexes follow the
**ESR rule** (Equality, Sort, Range): equality field first, then the sort keys,
which also serve the time range.

| Index | Serves |
|---|---|
| `{timestamp: -1, _id: -1}` | Unfiltered listing, date-range-only listing, the `$match` of stats and realtime stats over a time window. |
| `{event_type: 1, timestamp: -1, _id: -1}` | Listing by type (+ range); stats filtered by type. |
| `{user_id: 1, timestamp: -1, _id: -1}` | A user's activity timeline. |
| `{source_url: 1, timestamp: -1, _id: -1}` | Events on a given page. |

Integration tests run `explain()` on each list query shape and assert that the
expected index wins, with **no `COLLSCAN` and no in-memory `SORT` stage**
([test_mongo_event_reader.py](tests/integration/test_mongo_event_reader.py)). An
index change that silently degrades a query fails the build.

Indexes are created idempotently at startup in a background task, so a MongoDB
outage does not block the API from accepting events. In production this belongs
in a migration step.

**Pagination** is keyset-based: the opaque `cursor` encodes the `(timestamp,
event_id)` of the last row returned, and the next page queries strictly after
it. Unlike `skip`/offset it costs O(page size) on page 1 000 and does not skip or
repeat rows while new events are being inserted. The service fetches `limit + 1`
rows to know whether another page exists without a `count` query.

**Indexes deliberately not created:**

* **Compound combinations** (`user_id + event_type`, …): user and URL filters are already highly selective; MongoDB filters the remainder cheaply. Each extra index adds write amplification on the hottest path.
* **`metadata.*`**: arbitrary keys would need a wildcard index, which is large and slows every insert. Metadata querying is Elasticsearch's job.
* **Text index**: search is in Elasticsearch (see section 4).
* **`ingested_at`**: nothing queries it; it exists for debugging and lag analysis.
* **A covered index for stats** (`{timestamp, event_type}`): it would make the aggregation index-only, but the right fix for stats at scale is pre-aggregation (section 8), not a fifth index on raw events.
* **TTL index for retention**: how long to keep raw events is a product and compliance decision, not something to bake in silently.

### Elasticsearch ([mapping.py](src/event_platform/infrastructure/elasticsearch/mapping.py))

| Field | Type | Why |
|---|---|---|
| (root) | `dynamic: strict` | Unknown top-level fields are rejected. Free-form metadata must never grow the mapping. |
| `event_id`, `event_type`, `user_id` | `keyword` | Exact filters and aggregations; never analysed. |
| `timestamp` | `date` | Range filters, sort. |
| `source_url` | `keyword` + `text` sub-field (`url_text` analyzer) | Exact filtering on the keyword; the sub-field splits on punctuation so `checkout` matches `https://shop.io/checkout/payment`. |
| `metadata` | `flattened` | One field mapping for arbitrary JSON: no mapping explosion, still supports exact `metadata.<key>` lookups. |
| `metadata_text` | `text`, custom `metadata_text` analyzer | All metadata leaf values concatenated at index time. Analyzer = standard tokenizer + `lowercase` + `asciifolding` (`BOGOTA` matches `Bogotá`). **No stemming**: metadata values are mostly identifiers (browsers, devices, campaign names) where stemming would conflate distinct values. |

Queries ([queries.py](src/event_platform/infrastructure/elasticsearch/queries.py))
use `simple_query_string` with `default_operator: and`. Unlike `query_string` it
never throws on malformed user input, yet still supports phrases, negation,
prefixes and OR. Structured filters (type, user, URL, date range) go in `bool.filter`:
non-scoring and cacheable.

The index is created explicitly with this mapping at startup **and lazily before
the first write**. docker-compose also sets `action.auto_create_index=false`. This
came from an integration test that failed: the worker could index an event before
the startup task had created the index, and Elasticsearch would have auto-created
it with a dynamic mapping, silently and permanently. Searching an index that does
not exist yet returns an empty result instead of an error.

## 6. Caching strategy

`GET /events/stats/realtime` returns counts per event type over the last hour,
served **cache-aside** from Redis ([realtime_stats.py](src/event_platform/application/realtime_stats.py)).

* **TTL: 10 s** (`REALTIME_STATS_TTL_SECONDS`). The consumer is a dashboard that polls every few seconds. Ten seconds of staleness is invisible at that granularity, and it caps MongoDB load at **one aggregation per TTL per instance, regardless of how many clients poll**. Shorter buys freshness nobody perceives; longer makes "realtime" feel broken.
* **Invalidation: TTL expiry only, no write-path invalidation.** Every ingested event changes the counts, so invalidating on write would drive the hit rate to roughly zero at any meaningful volume and couple the worker to the cache. Bounded staleness is the explicit contract, and the response says so (`generated_at`, `cache.hit`, `cache.ttl_seconds`, `X-Cache: HIT|MISS`).
* **Stampede protection**: concurrent misses in one process wait on a lock and re-check the cache, so ten simultaneous misses run one aggregation (unit-tested). Across instances, each instance may recompute once per TTL, which is acceptable at this scale.
* **Fail open**: Redis errors are caught; the summary is computed from MongoDB and the request still succeeds. Short socket timeouts (0.5 s) keep a sick Redis from adding latency.
* **Keys** are namespaced, schema-versioned and window-specific: `event-platform:stats:realtime:v1:3600s`. A rolling deploy that changes the payload shape bumps `v1` instead of failing to deserialise old entries.

**Under higher write volume** I would invert the model: instead of aggregating raw
events on a miss, the worker would `INCRBY` per-minute counters
(`stats:{type}:{yyyymmddhhmm}`, expiring after the window) in a pipeline, and the
endpoint would sum the last 60 buckets. Reads become O(window × types) in Redis
with no MongoDB involvement. The trade-off is approximate counts: a crash between
the MongoDB insert and the `INCR` undercounts, a retry after `INCR` overcounts.
Making it exact needs the increment to happen only on a *first* insert (the
repository already reports that) plus a reconciliation job against MongoDB. Beyond
that: stale-while-revalidate (serve the expired value while one caller refreshes)
or a distributed lock (`SET NX PX`) for cross-instance single-flight.

### Rate limiting

The same Redis backs a fixed-window limiter (default 600 requests/min per client
IP): `INCR` + `EXPIRE` in one `MULTI/EXEC`, one round trip per request, shared by
every instance ([rate_limiter.py](src/event_platform/infrastructure/redis/rate_limiter.py)).
Fixed windows can admit up to 2× the limit across a window boundary; a sliding
window counter (weighted previous + current window) is the upgrade if that matters.
It also **fails open**: during a Redis outage, availability of ingestion beats
abuse protection. Behind a load balancer the app runs with `--proxy-headers` so
the client IP is the real one.

## 7. Failure modes and graceful degradation

| Scenario | What happens | Degradation |
|---|---|---|
| **MongoDB unavailable** | `POST /events` keeps returning `202`: the queue buffers. The worker's saves fail and retry with backoff. Reads (`/events`, `/events/stats`) return `503` with `Retry-After` (driver errors are translated to `DependencyUnavailableError`, never a raw 500). Realtime stats return `503` on a cache miss but keep serving cached values until the TTL expires. Readiness reports `degraded`. | Writes are accepted while the queue has room; once full, ingestion returns `503 Retry-After`. **Caveat:** with the default 5 receives and backoff, a message dead-letters after about 15–30 s of failures. A longer outage moves events to the DLQ (recoverable with `/admin/dead-letters/redrive`). The real fix is a circuit breaker that pauses consumption instead of burning receive counts (section 10). |
| **Worker crashes mid-batch** | Messages already acked are done. Messages in flight are not acked, so they become visible again after the visibility timeout and are redelivered. A message saved to MongoDB but not acked is redelivered and hits the duplicate key; indexing runs again and overwrites. | No duplicates, no loss, a delay of up to `visibility_timeout`. On graceful shutdown the worker finishes the current batch (with a timeout) before exiting. |
| **Whole process crashes / redeploy** | The in-memory queue and DLQ are lost. | **Events accepted but not yet persisted are lost.** This is the biggest limitation of the in-process queue and the first thing a real queue fixes (section 9). |
| **Elasticsearch unavailable** | Saves to MongoDB succeed, indexing fails, the message is retried and can end up in the DLQ even though it is safely in MongoDB. `/events/search` returns `503`; every other endpoint is unaffected. | Search is the only feature lost. Recovery: redrive the DLQ, or reindex from MongoDB. In production indexing should be decoupled from persistence (section 10). |
| **Redis unavailable** | Realtime stats are computed from MongoDB on every request; rate limiting is skipped. Both are logged. | Higher MongoDB load from realtime stats, temporarily no rate limiting. No errors to clients. |
| **Queue full** (downstream slower than ingest) | `POST /events` returns `503` with `Retry-After: 1`; readiness returns `503` so a load balancer can shift traffic. | Explicit backpressure rather than unbounded memory growth and an OOM crash. |
| **Invalid / poison events** | Rejected at the edge with `422`, so they never enter the queue. Anything that still fails repeatedly lands in the DLQ after 5 attempts instead of blocking the queue. | Isolated per message. |

Readiness ([health.py](src/event_platform/application/health.py)) fails only when
*this instance* cannot accept events (queue full). A shared-dependency outage is
reported as `degraded` with HTTP 200: every instance shares MongoDB, ES and Redis,
so pulling instances out of rotation would turn "reads are failing" into "everything
is failing".

## 8. Scaling: what breaks first at 10x

In the order I expect things to break:

1. **The single process.** The API, queue and worker share one event loop and one process's memory. At 10x, CPU on that loop (JSON parsing, validation, driver work) saturates first, and the bounded queue fills and starts shedding with `503`s. You cannot fix this with `uvicorn --workers` (each process would get its own queue). **Fix:** move to a real queue (SQS/Kafka) and run API and workers as separate, independently scaled deployments. Scale workers on queue depth / age of oldest message.
2. **One write per event.** The worker does one `insert_one` and one ES `index` call per event: two round trips per event, and an ES refresh cost per write. **Fix:** batch writes. `insert_many(ordered=False)` (duplicates show up as per-document errors, still idempotent) and the Elasticsearch `_bulk` API, sized by count and bytes. Raise ES `refresh_interval` (e.g. 5–30 s): search freshness of seconds is fine for analytics.
3. **Stats over raw events.** `/events/stats` scans every matching event in the window. Fine at thousands per bucket, slow at hundreds of millions. **Fix:** pre-aggregate. The worker (or a change-stream consumer) maintains hourly rollups `{type, hour} → count` with `$inc` upserts; daily and weekly roll up from hourly. Stats then read hundreds of documents instead of millions. A MongoDB time-series collection is an alternative for the raw store.
4. **MongoDB write throughput and index size.** Five indexes per insert, and RAM to keep them hot. **Fix:** shard the collection. A hashed `user_id` shard key spreads writes evenly and keeps a user's timeline on one shard; time-range queries become scatter-gather, which the rollups above make rare. Revisit indexes against real query statistics.
5. **Elasticsearch index growth.** One ever-growing index. **Fix:** time-based indices (daily or weekly) behind a write alias, ILM for rollover / warm / delete, and shard sizing by data volume. Searches default to recent indices.

Redis is not on this list: the cache and rate-limit workloads are O(requests) with
tiny values, and a single Redis node handles 100k+ ops/s.

## 9. AWS SQS drop-in design

Because the application depends only on `EventPublisher`, `EventConsumer` and
`DeadLetterQueue`, replacing the in-memory queue means writing an `SqsEventQueue`
adapter and changing one line in [container.py](src/event_platform/container.py).
The worker, processor and API do not change. What does change:

| Concern | In-memory queue | With SQS |
|---|---|---|
| Durability | Lost on restart | Durable, replicated across AZs. The biggest single improvement. |
| `publish` | O(1) local | `SendMessage` network call (~10–20 ms). Use `SendMessageBatch` (10 per call) if ingesting batches, and keep the API's own timeout and retry budget small. |
| `receive` | `wait_seconds` arbitrary | `ReceiveMessage` with `WaitTimeSeconds` ≤ 20 (long polling) and `MaxNumberOfMessages` ≤ 10. |
| `ack` | dict delete | `DeleteMessage(ReceiptHandle)`, or `DeleteMessageBatch`. |
| `retry_later` | visibility change | `ChangeMessageVisibility` (max 12 h). Same semantics, which is why the port was modelled on it. |
| Long processing | n/a | Extend visibility with a heartbeat if processing can approach the timeout. |
| DLQ | in-process list | A separate SQS queue configured as the source queue's redrive policy (`maxReceiveCount`). Redrive with `StartMessageMoveTask`. |
| Ordering / dedup | best-effort | Standard queues give at-least-once and no ordering, which matches what the code already assumes. A FIFO queue (with `MessageDeduplicationId = event_id`) is unnecessary: ordering is not required and dedup is already handled by idempotent writes. FIFO would also cap throughput. |
| Message size | unbounded | 256 KB. The 16 KB metadata limit keeps events well under it. |
| Capacity / backpressure | `QUEUE_MAX_SIZE` → 503 | Effectively unbounded. Backpressure moves to monitoring `ApproximateAgeOfOldestMessage` and autoscaling workers on it. |
| Deployment | worker inside the API process | Worker becomes its own process / deployment (`python -m event_platform.worker`), scaled independently. |
| Local dev & tests | — | LocalStack or ElasticMQ in docker-compose; contract tests running the same suite against both adapters. |

The serialisation boundary also becomes explicit: the message body becomes the
event's JSON (the same shape as the API's `EventIn`), with a schema version field
so producers and consumers can be deployed independently.

## 10. What I would do differently

Given more time, or a real production environment, in rough priority order:

* **Durable queue and separate worker deployment** (section 9). The in-memory queue is the one design choice I would not ship.
* **Decouple search indexing from persistence.** Today an Elasticsearch outage holds up acks and can dead-letter events that are already safely in MongoDB. Better: the worker only writes MongoDB, and a separate consumer tails a MongoDB change stream (or an outbox) and bulk-indexes into ES with its own retry and lag metric. ES lag then never affects ingestion.
* **Circuit breaker in the worker.** When MongoDB is down every message burns its receive count and heads to the DLQ. A breaker that stops receiving while the dependency is failing (and resumes on a probe) keeps messages safely in the queue for the duration of the outage.
* **Batching** in the worker (`insert_many`, ES `_bulk`), sized by count and bytes.
* **Pre-aggregated stats** (hourly rollups) instead of aggregating raw events.
* **Observability**: structured JSON logs with `event_id` / request id correlation, metrics (queue depth, oldest message age, processing latency, retry and DLQ rates, cache hit ratio, 429/503 rates), tracing across API → queue → worker. Today there are logs only.
* **Multi-tenancy and auth.** A real event platform has customers: a `tenant_id` on every event, as the leading key of every index and the ES routing key, and API-key authentication per tenant. Rate limits per API key rather than per IP.
* **Schema evolution**: a `schema_version` on events and a registry of known `event_type`s with per-type metadata schemas, instead of free-form validation only.
* **Retention**: an explicit policy (TTL index or time-partitioned collections, ES ILM) agreed with product and compliance.
* **Index migrations** as a deploy step rather than at application startup.
* **Load and chaos testing**: k6 or Locust against docker-compose to find the actual knee of the curve; kill the worker mid-batch and restart Mongo / ES during load to verify the failure-mode table above end to end.
