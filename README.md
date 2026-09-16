# Redis BEFORE/AFTER Fencing Demo

This is a runnable version of the pattern described in
[`redis-before-after-consistency-pattern.md`](redis-before-after-consistency-pattern.md).

It uses:

- FastAPI for the backend
- Postgres as the source of truth
- Redis as the fenced cache
- a separate repair worker
- Redis Lua scripts for atomic cache state transitions

Redis caches the whole user profile as one JSON snapshot:

```json
{
  "id": "42",
  "name": "Ada Lovelace",
  "phone_number": "+44-20-7946-0100",
  "email": "ada.lovelace@example.com",
  "version": 1
}
```

## Run it

```bash
docker compose up --build
```

Open the API docs:

```text
http://localhost:8000/docs
```

Open the default user UI:

```text
http://localhost:8000/ui
```

Open the debug/demo console:

```text
http://localhost:8000/debug-ui
```

## Endpoints

Real app endpoints:

```text
GET   /users/{user_id}
PATCH /users/{user_id}
```

Demo scenario endpoints:

```text
POST /demo/reset
POST /demo/rejected-write
POST /demo/crash-after-db-commit
POST /demo/delayed-after-race
POST /repair/run-once
GET  /debug/users/{user_id}
```

## Demo flow

You can run this flow from the UI buttons, or with curl.

Reset to a trusted starting point:

```bash
curl -X POST http://localhost:8000/demo/reset
```

Read from a cold cache. The first read is served from Postgres and warms Redis:

```bash
curl http://localhost:8000/users/42
```

Read again. This time the cache is trusted and the response is served from
Redis:

```bash
curl http://localhost:8000/users/42
```

The response includes the source indicator:

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

Perform a normal successful update:

```bash
curl -X PATCH http://localhost:8000/users/42 \
  -H 'content-type: application/json' \
  -d '{"name":"Ada Byron","phone_number":"+1-555-0199"}'
```

Simulate a rejected write. Redis receives `BEFORE`, Postgres rejects the stale
version, and Redis is not trusted:

```bash
curl -X POST http://localhost:8000/demo/rejected-write
curl http://localhost:8000/users/42
```

Simulate a crash after Postgres commits but before Redis receives `AFTER`:

```bash
curl -X POST http://localhost:8000/demo/crash-after-db-commit
curl http://localhost:8000/users/42
```

Repair manually, or wait for the worker container to do it:

```bash
curl -X POST http://localhost:8000/repair/run-once
curl http://localhost:8000/users/42
```

The worker polls every 5 minutes by default so there is time to inspect the
mismatch before it repairs Redis.

Show that a delayed older `AFTER` cannot clobber a newer confirmation:

```bash
curl -X POST http://localhost:8000/demo/delayed-after-race
```

Every response includes a `debug` snapshot of Postgres, Redis, and `dirty_keys`
so you can see exactly when Redis is trusted:

```json
{
  "debug": {
    "redis": {
      "value": {
        "id": "42",
        "name": "Ada Byron",
        "phone_number": "+1-555-0199",
        "email": "ada.lovelace@example.com",
        "version": 2
      },
      "before_uuid": "...",
      "after_uuid": "...",
      "version": 2,
      "confirmed_version": 2,
      "trusted": true
    }
  }
}
```
