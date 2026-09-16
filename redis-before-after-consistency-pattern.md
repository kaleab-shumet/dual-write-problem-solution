# Fencing a Redis Cache Against Its Own Lies

*How to make a cache admit when it might be wrong — a BEFORE/AFTER pattern for Redis + Postgres*

---

Every system that caches data in front of a database eventually runs into the same uncomfortable fact: **you cannot write to two systems atomically.** Postgres and Redis don't share a transaction. Somewhere between "write the cache" and "write the database," a crash, a timeout, or a lost network packet can leave the two disagreeing about the truth — and nothing about a normal cache-aside setup tells you when that's happened.

I ran into this while thinking about caching account balances for a payments service, and ended up designing a small pattern that lets the cache *prove* whether it's safe to trust, rather than just hoping it usually is. This post walks through the problem, the pattern (I'm calling it BEFORE/AFTER fencing), the bugs in the naive version of it, and how it compares to the more conventional CDC-based answer to the same problem.

## The dual-write problem, concretely

Say the app updates Redis, then Postgres:

```
1. App writes new value to Redis
2. App writes new value to Postgres
3. Postgres commits
4. App crashes before it can re-confirm anything in Redis
```

Postgres now has the correct value. Redis has *a* value — but nothing distinguishes "this was confirmed" from "this was an attempt that got interrupted." The reverse is just as bad: Redis gets updated, and then Postgres *rejects* the write (a constraint failure, an optimistic-lock conflict). Now Redis is confidently wrong, and every read of it says so.

This is the well-known **dual-write problem**, and most caching strategies don't actually solve it — TTLs and write-through caching just shrink the window of staleness, they don't let the cache *know* it's stale.

The question I wanted the read path to be able to answer, on every single read, was:

> Can I trust this cached value right now, or do I need to go to Postgres?

## Two production-grade answers to this problem exist already

Before getting into my approach, it's worth being upfront about the two standard ways people solve this, because a systems-minded reader's first reaction is going to be "why not just do X":

**Change Data Capture (CDC).** The app writes only to Postgres. A separate process (Debezium, Postgres logical replication, or an outbox table) tails the write-ahead log and asynchronously pushes changes into Redis. There's exactly one write path, so the dual-write race disappears by construction. The tradeoff is infrastructure — you're now running and operating a CDC pipeline — and an eventual-consistency window between commit and cache update.

**Fencing tokens.** Used in distributed locking to reject a lock-holder that's since been superseded, and in optimistic concurrency generally: attach a monotonically increasing token to an operation, and refuse to act on a token that's been superseded by a newer one.

What follows is basically an application of the second idea (fencing tokens), applied directly inside the cache entry itself, as a way to get most of CDC's safety without standing up a CDC pipeline. It's not a replacement for CDC in every case — if you're already running Debezium, use it — but if you want a self-contained fix without new infrastructure, this is one way to get there.

## The pattern: BEFORE/AFTER fencing

Instead of storing a Redis entry as a single value, split it into two fields that must agree before the entry is trusted:

| Field | Contents | Written |
|---|---|---|
| `BEFORE` | tentative value + a UUID | *before* attempting the Postgres write |
| `AFTER` | the UUID only | *after* Postgres confirms the write succeeded |

The trust rule is one comparison:

```
BEFORE.uuid == AFTER.uuid   →  trust Redis
BEFORE.uuid != AFTER.uuid   →  don't trust it, read Postgres
```

The UUID tags one specific update attempt. `AFTER` only ever advances once *that* attempt is confirmed durable. If anything goes wrong in between — a rejected write, a crash, a lost process — `AFTER` just never catches up, and the mismatch is itself the signal that something's unresolved. Postgres stays the single source of truth throughout; Redis is only ever allowed to answer when it can prove it reflects a completed Postgres transaction.

## What BEFORE and AFTER actually mean

Before the example, it's worth being precise about the two names, because they're doing exactly what they say:

- **BEFORE** = the state written *before* the Postgres write is attempted. It holds the tentative new value plus a fresh UUID that identifies this specific attempt. Writing it is how the cache immediately marks itself "don't trust me yet" the moment a change starts.
- **AFTER** = the state written *after* the Postgres write is confirmed to have succeeded. It holds only the UUID — no value — and its only job is to say "the attempt tagged with this UUID is now durable in Postgres."

So `BEFORE` is set at the *start* of a change, and `AFTER` is set at the *end*, only if that change actually landed. As long as those two UUIDs match, every part of the update — tentative write, database commit, confirmation — has completed for the *same* attempt, and the cached value can be trusted. The moment they diverge, you know something in that sequence is incomplete, and you fall back to Postgres until it's resolved.

## Walking through it: updating a user's phone number

### Initial state

```
Postgres (users table):
  id = user-42
  phone_number = "+1-555-0100"
  version = 1

Redis (hash key: user:42):
  BEFORE = { value: "+1-555-0100", uuid: "UUID-A" }
  AFTER  = { uuid: "UUID-A" }
```

`BEFORE.uuid == AFTER.uuid` → Redis is trusted. Any read of the user's phone number (an SMS-sending job, a profile page) is served straight from cache, no Postgres round-trip needed.

### Step 1 — the user asks to change their phone number to `+1-555-0199`

Generate a new UUID for this attempt: `UUID-B`.

### Step 2 — write BEFORE, immediately marking the cache untrusted

```
Redis: BEFORE = { value: "+1-555-0199", uuid: "UUID-B" }
       AFTER  = { uuid: "UUID-A" }   ← unchanged, still the old attempt
```

At this instant, `UUID-B != UUID-A`. Any request that reads the cache right now — even microseconds after this write — correctly detects the mismatch and falls back to Postgres, which still returns the *old* number, `+1-555-0100`. The new, unconfirmed number is never served as if it were real. This matters concretely here: an SMS 2FA job that fires mid-update should never send a code to a number that was typed in but never actually saved.

### Step 3 — attempt the Postgres write, using optimistic concurrency

```sql
UPDATE users
SET phone_number = '+1-555-0199',
    version = version + 1
WHERE id = 'user-42'
  AND version = 1;
```

### Step 4a — if Postgres succeeds (1 row updated, version is now 2)

Write AFTER, confirming this specific attempt:

```
Redis: BEFORE = { value: "+1-555-0199", uuid: "UUID-B" }
       AFTER  = { uuid: "UUID-B" }   ← now matches
```

`BEFORE.uuid == AFTER.uuid` again → Redis is trusted, and it now correctly serves `+1-555-0199`.

### Step 4b — if Postgres rejects the write (0 rows updated — e.g. a concurrent update already bumped the version)

`AFTER` is simply never touched. Redis is left as:

```
BEFORE = { value: "+1-555-0199", uuid: "UUID-B" }
AFTER  = { uuid: "UUID-A" }
```

The mismatch persists indefinitely. Every read falls through to Postgres, which still correctly holds `+1-555-0100`. The rejected change never leaks into a served response.

### Step 4c — if the app crashes right after Postgres commits, but before writing AFTER

```
Postgres: phone_number = "+1-555-0199"   ← committed
Redis:    BEFORE = { value: "+1-555-0199", uuid: "UUID-B" }
          AFTER  = { uuid: "UUID-A" }     ← stale, never got the update
```

`UUID-B != UUID-A` still holds, so every read correctly falls back to Postgres (which is right, just slower) until a background repair process notices the mismatch and catches Redis up by writing `AFTER = { uuid: "UUID-B" }`.

In all three branches (success, rejection, crash), the worst possible outcome is a temporary cache miss that costs an extra Postgres read — never a stale or half-applied phone number served as if it were confirmed.

Concurrent updates to the same user are a separate concern, handled entirely by Postgres: if two requests both try to change `user-42`'s phone number at once, both will read `version = 1`, but only one `UPDATE ... WHERE version = 1` can succeed — the other gets zero rows affected and must retry against the new version rather than silently overwrite. Postgres's version column protects against concurrent writes; the UUID fence protects against trusting an *unconfirmed* cache write. They're doing different jobs.

## The naive version has three bugs — here's how to fix them

I want to flag these clearly, because the pattern above sounds airtight in prose and isn't, until you close these gaps.

**1. BEFORE and AFTER have to be read and written atomically.** If they're separate keys or separate commands, a reader can catch them mid-update and see a false state. Fix: store both as fields of one Redis hash, and mutate them through a single Lua script so Redis treats the update as one atomic step. Reads use one `HGETALL` against the same hash.

**2. A delayed AFTER write can clobber a newer, valid one.** Picture this interleaving on the same key:

```
t0: Request 1 writes BEFORE(uuid=B, version=2), Postgres commits version 2
t1: Request 2 writes BEFORE(uuid=C, version=3), Postgres commits version 3
t2: Request 2's AFTER write lands first → AFTER = C   [correct, matches latest]
t3: Request 1's delayed AFTER write finally arrives → AFTER = B   [wrong — overwrites a valid newer state]
```

This doesn't cause a *wrong answer* (the system just falls back to Postgres on the resulting mismatch), but it does cause unnecessary cache misses under concurrent writes. The fix is to make the AFTER write conditional on version, via a small Lua script:

```lua
-- KEYS[1] = cache key, ARGV[1] = uuid, ARGV[2] = postgres version for this uuid
local current = tonumber(redis.call('HGET', KEYS[1], 'confirmed_version') or '0')
if tonumber(ARGV[2]) > current then
  redis.call('HSET', KEYS[1], 'after_uuid', ARGV[1], 'confirmed_version', ARGV[2])
  return 1
end
return 0
```

**3. The repair worker shouldn't scan the whole keyspace.** Running `SCAN` over every key checking for a mismatch doesn't scale past a small dataset. Instead, maintain a `dirty_keys` set: add a key when `BEFORE` is written, remove it once `AFTER` catches up. The worker then only ever touches genuinely inconsistent keys:

```python
for key in redis.smembers("dirty_keys"):
    before, after = redis.hmget(key, "before_uuid", "after_uuid")
    if before == after:
        redis.srem("dirty_keys", key)
        continue
    row = postgres.query("SELECT phone_number, version FROM users WHERE id = %s", extract_id(key))
    redis.eval(UPDATE_AFTER_SCRIPT, 1, key, generate_uuid(), row.version)
    redis.srem("dirty_keys", key)
```

Note the worker writes through the *same* version-gated script — it must never blindly overwrite, in case the value has moved on again since the key was queued as dirty.

## What this does and doesn't cover

With the three fixes in place, the pattern guarantees Redis is never served as authoritative unless it demonstrably reflects a committed Postgres write, and that crashes, rejected writes, and races all degrade into cache misses rather than wrong answers, without the request path ever blocking on the repair worker.

It does *not* cover everything on its own. Redis failing over mid-write (Sentinel/Cluster) can still lose a `BEFORE` or `AFTER` write independently — the fallback behavior absorbs this, but it's worth deliberately testing (kill Redis, kill the app, kill the worker, check nothing corrupted gets served). A repair worker that's down for a long stretch can leave a key permanently mismatched, so a safety TTL is worth adding. Many concurrent readers hitting a key mid-update can all fall through to Postgres at once — a short-lived "in-flight" marker smooths that out. And it's worth instrumenting the mismatch-driven cache-miss rate separately from ordinary TTL misses, so you can tell whether the cache is actually doing its job under real write load or just constantly falling back.

## Where this leaves you

If you're already running a CDC pipeline, that's still the cleaner long-term answer — one write path, no dual-write race by construction. If you're not, and you want a way to make a Redis-in-front-of-Postgres setup provably safe without adopting new infrastructure, BEFORE/AFTER fencing is a reasonably small, self-contained way to get there. It trades a bit of write-path complexity (an atomic hash, a version-gated confirm, a dirty set) for the guarantee that matters most: **the cache is either provably right, or it says so.**
