# Fencing a Redis Cache Against Its Own Lies

*How to make a cache admit when it might be wrong — a BEFORE/AFTER pattern for Redis + Postgres*

---

Every system that caches data in front of a database eventually runs into the same uncomfortable fact: **you cannot write to two systems atomically.** Postgres and Redis don't share a transaction. Somewhere between "write the cache" and "write the database," a crash, a timeout, or a lost network packet can leave the two disagreeing about the truth — and nothing about a normal cache-aside setup tells you when that's happened.

I ran into this while thinking about caching account balances for a payments service, and ended up designing a small pattern that lets the cache *prove* whether it's safe to trust, rather than just hoping it usually is. This post walks through the problem, the pattern (I'm calling it BEFORE/AFTER fencing), the bugs in the naive design, and how it compares to the more conventional CDC-based answer to the same problem.

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

Instead of storing a Redis entry as a single value, split it into an in-flight marker and a confirmed value that must agree before the entry is trusted:

| Field | Contents | Written |
|---|---|---|
| `BEFORE` | UUID marker only | *before* attempting the Postgres write |
| `AFTER` | UUID + confirmed value | *after* Postgres confirms the write succeeded |

The trust rule is one comparison:

```
BEFORE.uuid == AFTER.uuid   →  trust Redis
BEFORE.uuid != AFTER.uuid   →  don't trust it, read Postgres
```

The UUID tags one specific update attempt. `BEFORE` is only a marker; it never carries the tentative value. `AFTER` is the only place the cached value lives, and it only advances once *that* attempt is confirmed durable by Postgres. If anything goes wrong in between — a rejected write, a crash, a lost process — `AFTER` just never catches up, and the mismatch is itself the signal that something's unresolved. Postgres stays the single source of truth throughout; Redis is only ever allowed to answer when it can prove it reflects a completed Postgres transaction.

The protocol is identified by an opaque **cache key**, not by a particular table
or entity type. A cache key identifies the complete value protected by the
fence:

```text
user:42
order:99:summary
account:42:dashboard
```

The business operation supplies the complete value for that cache key; the
fencing layer does not need to know how the value was produced.

The database keeps a small attempt-claim table:

```sql
cache_attempts(
  cache_key TEXT,
  attempt_uuid UUID,
  created_at TIMESTAMP,
  PRIMARY KEY (cache_key, attempt_uuid)
)
```

The attempt row is inserted in the same database transaction as the business
write. A repairer tries to insert the same `cache_key` and `attempt_uuid` before
rebuilding the cached value. A unique-key conflict means the original attempt
already committed; a successful insert means the original attempt did not
commit and the repairer has claimed that unresolved attempt. This gives the
repairer a database-backed answer instead of guessing from Redis timing.

## What BEFORE and AFTER actually mean

Before the example, it's worth being precise about the two names, because they're doing exactly what they say:

- **BEFORE** = the marker written *before* the Postgres write is attempted. It holds only a fresh UUID that identifies this specific attempt. Writing it is how the cache immediately marks itself "don't trust me yet" the moment a change starts.
- **AFTER** = the state written *after* the Postgres write is confirmed to have succeeded. It holds the UUID plus the full confirmed value returned by Postgres. It is the only place the cached value lives.

So `BEFORE` is set at the *start* of a change, and `AFTER` is set at the *end*, only if that change actually landed. As long as those two UUIDs match, every part of the update — in-flight marker, database commit, confirmation — has completed for the *same* attempt, and `AFTER.value` can be trusted. The moment they diverge, you know something in that sequence is incomplete, and you fall back to Postgres until it's resolved.

## Walking through it: updating a user's profile

### Initial state

```
Database:
  user-42 = { name: "Ada", phone: "+1-555-0100" }

Redis (hash key: user:42):
  BEFORE = { uuid: "UUID-A" }
  AFTER  = { uuid: "UUID-A", value: { name: "Ada", phone: "+1-555-0100" } }
```

`BEFORE.uuid == AFTER.uuid` → Redis is trusted. A profile read is served
straight from cache, with no database round-trip.

### Step 1 — the user asks to change their profile

Generate a new UUID for this attempt: `UUID-B`.

### Step 2 — write BEFORE, immediately marking the cache untrusted

```
Redis: BEFORE = { uuid: "UUID-B" }
       AFTER  = { uuid: "UUID-A", value: { name: "Ada", phone: "+1-555-0100" } }
```

At this instant, `UUID-B != UUID-A`. Any request that reads the cache right
now detects the mismatch and falls back to the database, which still returns
the last committed profile. The new, unconfirmed profile is not in Redis.

### Step 3 — attempt the database write

```sql
UPDATE users
SET name = 'Ada Byron',
    phone_number = '+1-555-0199'
WHERE id = 'user-42';
```

### Step 4a — if the database succeeds

Write AFTER, confirming this specific attempt:

```
Redis: BEFORE = { uuid: "UUID-B" }
       AFTER  = { uuid: "UUID-B", value: { name: "Ada Byron", phone: "+1-555-0199" } }
```

`BEFORE.uuid == AFTER.uuid` again → Redis is trusted, and it now correctly serves `+1-555-0199`.

### Step 4b — if the database rejects the write

`AFTER` is simply never touched. Redis is left as:

```
BEFORE = { uuid: "UUID-B" }
AFTER  = { uuid: "UUID-A", value: { name: "Ada", phone: "+1-555-0100" } }
```

The mismatch persists until repair clears the marker. Every read falls through
to the database, which still holds the last committed profile. The rejected
change never leaks into Redis or a served response.

### Step 4c — if the app crashes after the database commits, but before writing AFTER

```
Database: { name: "Ada Byron", phone: "+1-555-0199" }   ← committed
Redis:    BEFORE = { uuid: "UUID-B" }
          AFTER  = { uuid: "UUID-A", value: { name: "Ada", phone: "+1-555-0100" } }
```

`UUID-B != UUID-A` still holds, so every read correctly falls back to the
database until repair notices the mismatch and catches Redis up by writing
`AFTER = { uuid: "UUID-B", value: { name: "Ada Byron", phone: "+1-555-0199" } }`.

In all three branches (success, rejection, crash), the worst possible outcome
is a temporary cache miss that costs an extra database read — never an
unconfirmed profile served as if it were confirmed.

Concurrent updates to the same user are handled by the database's own
concurrency controls. The UUID fence protects the cache from an unconfirmed
write; the database protects the data from conflicting writes. They do
different jobs.

## The naive design has four gaps — here's how to fix them

I want to flag these clearly, because the pattern above sounds airtight in prose and isn't, until you close these gaps.

**1. BEFORE and AFTER have to be read and written atomically.** If they're separate keys or separate commands, a reader can catch them mid-update and see a false state. Fix: store both as fields of one Redis hash, and mutate them through a single Lua script so Redis treats the update as one atomic step. Reads use one `HGETALL` against the same hash.

**2. A delayed AFTER write can clobber a newer, valid one.** Picture this interleaving on the same key:

```
t0: Request 1 writes BEFORE(uuid=B), then commits its update
t1: Request 2 writes BEFORE(uuid=C), then commits a newer update
t2: Request 2's AFTER write lands first → AFTER = { uuid=C, value=newer state }   [correct]
t3: Request 1's delayed AFTER write arrives → AFTER = { uuid=B, value=older state }   [wrong — overwrites a valid newer state]
```

This doesn't cause a *wrong answer* (the system just falls back to the
database on the resulting mismatch), but it does cause unnecessary cache
misses under concurrent writes. The fix is to make the AFTER write conditional
on the database's ordering token:

The cache accepts a confirmation when its ordering token is at least as new as
the token already stored. An older delayed confirmation is rejected. The
comparison can be implemented atomically alongside the AFTER write.

**3. Repair must prove which attempt it is resolving.** A repairer must not
read the database and then invent a new matching UUID pair. That could erase a
newer in-flight `BEFORE` marker. Instead, it uses the cache key and the exact
UUID currently in `BEFORE`:

```text
1. Read cache_key and BEFORE = UUID-B from Redis.
2. Insert (cache_key, UUID-B) into the database attempt table.
3. If the insert conflicts, the original writer committed its attempt.
4. If the insert succeeds, the original writer did not commit and repair owns
   the unresolved attempt.
5. Read the complete current value for this cache key from the database.
6. Write AFTER = UUID-B with that value and its database ordering token.
```

The repairer writes through the *same* ordering-gated script — it must never
blindly overwrite, in case the value has moved on again since the key became
dirty. It confirms the exact `BEFORE` UUID it claimed; it does not invent a new
matching pair and it never overwrites a newer `BEFORE` marker. A dirty-key
index, queue, or equivalent notification mechanism can identify repair work;
that delivery mechanism is separate from the fencing strategy.

**4. A dirty hot key can create a cache stampede.** If many readers see the
same mismatch, they can all query Postgres at once. Optional per-cache-key
singleflight coordination lets one reader perform the repair while the others
briefly wait for a trusted value, then fall back to the database if the repair
does not finish within the wait window. This reduces load; it is not the
correctness mechanism. The database attempt claim and Redis ordering check still
protect correctness if multiple repairers or a writer race.

## What this does and doesn't cover

With the four protections in place, the pattern guarantees Redis is never
served as authoritative unless it demonstrably reflects a committed database
write, and that crashes, rejected writes, and races all degrade into cache
misses rather than wrong answers. A request may wait briefly for repair, but it
does not wait indefinitely; it falls back to the database when the wait window
expires.

It does *not* cover everything on its own. Redis failing over mid-write
(Sentinel/Cluster) can still lose a `BEFORE` or `AFTER` write independently —
the fallback behavior absorbs this, but it is worth testing deliberately. A
repair process that is down for a long stretch can leave a key permanently
mismatched, so attempt retention, dirty-key age, and repair health need
monitoring. Singleflight reduces stampede risk but does not eliminate database
fallbacks when repair is slow. It is also worth instrumenting mismatch-driven
cache misses separately from ordinary TTL misses.

## Where this leaves you

If you're already running a CDC pipeline, that's still the cleaner long-term answer — one write path, no dual-write race by construction. If you're not, and you want a way to make a Redis-in-front-of-Postgres setup provably safe without adopting new infrastructure, BEFORE/AFTER fencing is a reasonably small, self-contained way to get there. It trades a bit of write-path complexity (an atomic hash, an ordering-gated confirm, a dirty set) for the guarantee that matters most: **the cache is either provably right, or it says so.**
