I kept running into the same nagging question in projects that use Redis in
front of Postgres: when a read hits the cache, how do I actually know it's safe
to trust? Most setups just assume it is, and deal with staleness via a TTL.
That's fine until the two writes — one to Postgres, one to Redis — land on
either side of a crash or a rejected update, and now the cache is confidently
wrong with no way to tell.

Rather than accept that as a cost of doing business, I wanted a cache that could
prove whether it was trustworthy on every single read. That's what this repo
demonstrates: a small "fencing" pattern (BEFORE/AFTER UUIDs) that turns every
failure mode — a rejected write, an app crash mid-update, a stale confirmation
arriving late — into a harmless cache miss instead of a wrong answer. The demo
app lets you trigger each of those failures yourself and watch the fallback
happen live.

# Redis BEFORE/AFTER Fencing Demo

When an application writes to both Postgres and Redis, it cannot make those two
writes atomic without extra infrastructure. If Redis is updated and Postgres
rejects the change, Redis may contain data that was never committed. If Postgres
commits and the app crashes before Redis is updated, Redis may be stale or
half-confirmed. A normal cache-aside setup has no built-in way to tell the
difference.

Example of the problem without fencing:

```text
Initial state:

Postgres:
  user:42 name = "Ada Lovelace", version = 1

Redis:
  value = { "name": "Ada Lovelace", "version": 1 }
```

Now the app tries to update both systems:

```text
1. App writes Redis:
     value = { "name": "Katherine Johnson", "version": 2 }

2. App tries to update Postgres.

3. Postgres rejects the update:
     stale version, constraint failure, timeout, or another conflict
```

Redis now contains `"Katherine Johnson"`, but Postgres still contains
`"Ada Lovelace"`. A normal read from Redis has no way to know the cached value
was never committed, so the app may serve a wrong user profile.

The reverse failure is also possible:

```text
1. App updates Postgres successfully.
2. App crashes before updating Redis.
3. Redis keeps serving the old value.
```

Both cases are forms of the dual-write problem: the app tried to coordinate two
systems, but only one side finished.

This demo uses a small fencing pattern that makes Redis admit when it might be
wrong. Instead of treating a cached value as automatically safe, Redis separates
an in-flight marker from the last confirmed value:

- `BEFORE`: a UUID marker written before the Postgres update is attempted. It
  does not contain a value.
- `AFTER`: the UUID and full row that Postgres has confirmed, written only
  after Postgres commits.

The read rule is:

```text
BEFORE.uuid == AFTER.uuid   -> Redis is trusted
BEFORE.uuid != AFTER.uuid   -> Redis is not trusted; read Postgres
```

Postgres remains the source of truth. Redis is allowed to answer only when the
cached value can prove it belongs to a completed Postgres write. Failures become
cache misses instead of wrong answers.

Example with BEFORE/AFTER fencing:

```text
Initial trusted cache:

Postgres:
  user:42 name = "Ada Lovelace", version = 1

Redis:
  before_uuid = UUID-A
  after_uuid  = UUID-A
  value       = { "name": "Ada Lovelace", "version": 1 }

Read result:
  before_uuid == after_uuid
  -> serve Redis
```

Now suppose the app updates the user, Postgres commits, and the app crashes
before Redis receives `AFTER`:

```text
Postgres:
  user:42 name = "Katherine Johnson", version = 2

Redis:
  before_uuid = UUID-B
  after_uuid  = UUID-A
  value       = { "name": "Ada Lovelace", "version": 1 }

Read result:
  before_uuid != after_uuid
  -> do not trust Redis
  -> read Postgres
```

Redis still contains only the last confirmed value. The app refuses to serve it
because a newer in-flight `BEFORE` marker exists and Redis cannot prove that
attempt was confirmed. After the repair worker catches up, Redis writes a
matching `AFTER` UUID with the confirmed row from Postgres, and future reads can
use the cache again.

The longer design write-up is in
[`redis-before-after-consistency-pattern.md`](redis-before-after-consistency-pattern.md).
This README focuses on running and understanding the demo app.

## What The Demo Shows

The app manages one user profile:

```json
{
  "id": "42",
  "name": "Ada Lovelace",
  "phone_number": "+44-20-7946-0100",
  "email": "ada.lovelace@example.com",
  "version": 1
}
```

It uses:

- FastAPI for the backend
- Postgres as the source of truth
- Redis as the fenced cache
- a separate repair worker
- Redis Lua scripts for atomic cache updates
- Docker Compose to run everything

The default UI shows the final user-facing result: user data, version, and
whether the response was served from Redis or Postgres.

The debug UI shows the internals: Postgres state, Redis hash fields, UUIDs,
confirmed versions, dirty keys, and scenario logs.

## Run

```bash
docker compose up --build
```

Open:

```text
http://localhost:8000/ui
```

Useful pages:

```text
http://localhost:8000/ui        default user-facing UI
http://localhost:8000/debug-ui  debug/demo console
http://localhost:8000/docs      FastAPI docs
```

## Demo Flow

The first load after reset demonstrates a cold cache:

1. Click **Reset**.
2. Click **Refresh** or reload `/ui`.
3. The first read is served from Postgres.
4. Reload again.
5. The next read is served from Redis.

That proves ordinary cache warming still works.

Then use the scenario buttons:

- **Save changes**: normal successful update. Redis becomes trusted after
  Postgres commits and `AFTER` is written.
- **Rejected write**: Redis receives `BEFORE`, Postgres rejects the stale
  update, and the final read falls back to Postgres.
- **Crash after DB commit**: Postgres commits, Redis never receives `AFTER`,
  and the final read falls back to Postgres.
- **Repair once**: repairs dirty Redis entries from Postgres and makes Redis
  trusted again.
- **Delayed AFTER race**: proves an older delayed confirmation cannot clobber a
  newer confirmed cache entry.

The important behavior is that the final UI never needs to understand Redis
internals. It only sees:

```json
{
  "served_from": "postgres",
  "trusted_cache": false,
  "user": {
    "id": "42",
    "name": "Ada Lovelace",
    "phone_number": "+44-20-7946-0100",
    "email": "ada.lovelace@example.com",
    "version": 1
  }
}
```

or:

```json
{
  "served_from": "redis",
  "trusted_cache": true,
  "user": {
    "id": "42",
    "name": "Ada Lovelace",
    "phone_number": "+44-20-7946-0100",
    "email": "ada.lovelace@example.com",
    "version": 1
  }
}
```

## API Endpoints

Real app endpoints:

```text
GET   /users/{user_id}
PATCH /users/{user_id}
```

Demo endpoints:

```text
POST /demo/reset
POST /demo/rejected-write
POST /demo/crash-after-db-commit
POST /demo/delayed-after-race
POST /repair/run-once
GET  /debug/users/{user_id}
```

## How The Pattern Works

Redis stores the cached user profile as a hash:

```text
key: user:42

value              JSON user profile
before_uuid        UUID marker for the latest attempted write
after_uuid         UUID for the latest confirmed write
confirmed_version  DB row version corresponding to value
```

On update:

1. The backend writes `BEFORE` to Redis with a fresh UUID marker only.
2. Redis is now untrusted because `before_uuid != after_uuid`.
3. The backend updates Postgres using optimistic concurrency and gets the final
   row back from `RETURNING`.
4. If Postgres commits, the backend writes `AFTER` with the same UUID and the
   confirmed full row.
5. Redis is trusted again only if the UUIDs match.

If Postgres rejects the update, or the app crashes after Postgres commits but
before writing `AFTER`, Redis stays untrusted and reads fall back to Postgres.

The delayed confirmation case is protected by `confirmed_version`: Redis only
accepts an `AFTER` write if its Postgres version is newer than the current
confirmed version.

## Worker

The worker scans Redis `dirty_keys`, reads the current row from Postgres, and
repairs untrusted cache entries using the same version-gated Redis script.
The API and worker coordinate through a short-lived per-user Redis lease. The
lease covers normal writes and repairs, so a worker cannot repair a key while
an API update is changing its fencing markers. If several requests encounter
the same dirty key, one request repairs it while the others briefly wait for a
trusted cache entry before falling back to Postgres.

The worker interval is intentionally slow in the demo so you have time to see
the fallback behavior. Use **Repair once** in the UI when you want to repair
immediately.

## Singleflight Coordination

Fencing prevents an unsafe Redis read, but many simultaneous reads of the same
dirty key could still overload Postgres. The demo limits that work with a
short-lived Redis lease per user:

```text
1. One request acquires lock:user:42.
2. It reads Postgres and repairs Redis.
3. Other requests briefly wait for Redis to become trusted.
4. They serve the repaired value from Redis, or fall back to Postgres after
   the wait period.
```

Normal API writes, request-triggered repairs, and background worker repairs all
use the same lease. This prevents a worker from changing a key while an API
update is between its `BEFORE` and `AFTER` steps. The lease has an owner token,
is released atomically, and expires automatically if its owner crashes.

The defaults are suitable for the demo:

```text
USER_LOCK_TTL_MS=5000   lock lifetime
USER_LOCK_WAIT_MS=300   reader/writer wait time
```

These settings are coordination controls, not correctness controls. UUID
matching, database versions, and Postgres remain responsible for deciding
whether a cached value is safe.

## How This Compares To Outbox/CDC

Outbox and CDC are the established production patterns for avoiding direct
dual-writes. They keep Postgres as the only write path, then update Redis
asynchronously from an outbox table or database log.

The cost is operational complexity and propagation lag. You need to run and
monitor the outbox processor or CDC pipeline, handle retries and poison events,
manage replication slots or polling, and make sure Redis catches up quickly
enough for your freshness needs. When that worker or pipeline is slow, worker
lag becomes cache lag: Redis may keep serving older data until it catches up.

This demo explores a different tradeoff. Instead of eliminating the second
write, it makes Redis prove whether a cached value is safe to serve. If Redis
cannot prove that, the read path falls back to Postgres.

Hypothetically, this can be useful when:

- you do not already operate CDC infrastructure
- you want successful writes to update Redis immediately
- you prefer worker lag to become a cache miss instead of silent cache lag
- you want each cache entry to carry its own trust signal
- your system is small enough that adding Debezium, Kafka, or outbox processing
  would be operationally heavier than this guarded cache pattern

The caveat: this repository is a demo, not a production-proven replacement for
outbox or CDC. A production version would need testing around Redis failover,
concurrent writes, repair lag, cache stampedes, monitoring, and operational
recovery.

## Limitations

This pattern improves cache correctness, but it does not make Redis and
Postgres a single atomic system.

- **Redis failover can still lose cache writes.** If Redis acknowledges a
  `BEFORE` or `AFTER` write and then fails over before that write is replicated,
  the cache may move backward. The read path still treats mismatches as unsafe,
  but production deployments should test Redis Sentinel/Cluster failover
  behavior deliberately.
- **Dirty entries can stay dirty if repair is down.** A crash after Postgres
  commit is safe because reads fall back to Postgres, but the cache may remain
  cold for that key until the worker catches up. Production systems should
  monitor dirty-key age and mismatch-driven cache misses.
- **The repair lease is best-effort coordination.** A short lease reduces
  cache stampedes and coordinates API repairs with the worker, but it is not a
  replacement for the UUID and version checks. Production systems should size
  the lease for their database latency, handle lease expiry, and monitor
  fallback volume for hot keys.

If you already run CDC or an outbox pipeline, that is often the cleaner
long-term architecture: one write path into Postgres, then asynchronous cache
population from the database log. This repo is about making a direct
Redis-plus-Postgres write path safer when you are not using that infrastructure.

## Commands

Start:

```bash
docker compose up --build
```

Stop:

```bash
docker compose down
```

Reset the demo:

```bash
curl -X POST http://localhost:8000/demo/reset
```

Read the user:

```bash
curl http://localhost:8000/users/42
```

Update the user:

```bash
curl -X PATCH http://localhost:8000/users/42 \
  -H 'content-type: application/json' \
  -d '{"name":"Katherine Johnson","phone_number":"+1-202-555-0182"}'
```

Simulate crash after Postgres commit:

```bash
curl -X POST http://localhost:8000/demo/crash-after-db-commit \
  -H 'content-type: application/json' \
  -d '{"name":"Crash Demo","phone_number":"+1-303-555-0101"}'
```
