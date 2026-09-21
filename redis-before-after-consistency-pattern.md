# Redis BEFORE/AFTER Consistency Pattern

Redis cannot prove that a cached value agrees with the database merely because
it has not expired. A process can write Redis, fail before the database
commits, or commit the database and fail before Redis is updated. This pattern
makes that uncertainty visible.

Redis stores an in-flight marker (`BEFORE`) and a confirmed value (`AFTER`). A
read trusts the cached value only when both markers identify the same attempt.
This document describes the core protocol and does not depend on a particular
worker or message queue.

The protocol here uses a Redis-required cached-write policy. A writer must
successfully record `BEFORE` before changing the database. If Redis cannot
record `BEFORE`, the database write stops. Allowing database writes while
Redis is unavailable is a separate degraded-mode design and is intentionally
outside this document.

## Core Rule

Each protected cache entry has one opaque key, such as `user:42`. Redis stores
one hash:

```text
before_uuid  - latest attempted write
after_uuid   - latest database-confirmed attempt
value        - value confirmed by the database
```

```text
before_uuid == after_uuid  -> trust Redis and serve value
before_uuid != after_uuid  -> do not trust Redis; read the database
```

Missing fields are untrusted. `BEFORE` is a marker only; it never holds a
tentative value. `AFTER` is the only place the cached value lives.

## Database Coordinator

The database stores the current attempt for each cache key:

```sql
CREATE TABLE cache_attempts (
    cache_key TEXT PRIMARY KEY,
    attempt_uuid UUID NOT NULL
);
```

There is no required initial row. This is current coordinator state, not an
append-only history.

For the first write, when Redis has no `AFTER`, the expected attempt is NULL:

```sql
INSERT INTO cache_attempts(cache_key, attempt_uuid)
VALUES (:cache_key, :new_attempt)
ON CONFLICT (cache_key) DO NOTHING
RETURNING attempt_uuid;
```

A returned row means this transaction initialized the key. No returned row
means another transaction initialized it first, so this transaction rolls back
and retries.

For later writes, the expected `AFTER` must still be current:

```sql
UPDATE cache_attempts
SET attempt_uuid = :new_attempt
WHERE cache_key = :cache_key
  AND attempt_uuid = :expected_after
RETURNING attempt_uuid;
```

The business update and this transition share one database transaction. A
zero-row result means the expected attempt is stale; the business update must
not run or commit. This conditional operation replaces a separate `SELECT`
followed by an unconditional update, which would let two writers pass the same
check.

## Write Protocol

For a writer with fresh UUID `B`:

```text
1. Generate B.
2. Write Redis BEFORE=B and mark the key dirty.
3. Read Redis AFTER. This is the expected database attempt.
4. Begin one database transaction.
5. Initialize or conditionally advance cache_attempts to B.
6. If the transition fails, roll back and retry with a fresh UUID.
7. Execute the business update in the same transaction.
8. Commit the transaction.
9. Write Redis AFTER=B with the confirmed value.
```

The `AFTER` write is conditional. It only applies when Redis still contains
`BEFORE=B`; otherwise Redis remains dirty. This prevents a late writer from
overwriting a newer `BEFORE` marker. A Redis Lua script or equivalent
compare-and-set transaction can implement this check.

If Redis cannot record `BEFORE`, the write must stop. Continuing with the
database update could leave an old matching Redis pair looking trusted.

## Empty Initial Setup

Initial state:

```text
Redis: no BEFORE, no AFTER, no value
Database: no cache_attempts row
Business row: user 42 = Ada
```

W1 wants to write Grace:

```text
W1 generates B1
W1 writes BEFORE=B1
W1 reads AFTER=missing, so expected=NULL
W1 inserts (user:42, B1)
W1 updates the business row to Grace
W1 commits
W1 writes AFTER=B1 and value=Grace
```

The cache is now trusted. No seed row or seed UUID was required.

If W1 and W2 initialize simultaneously, the unique `cache_key` constraint
allows only one initialization. The loser rolls back and retries.

## Slow W1, Fast W2

Initial state:

```text
Redis: no BEFORE, no AFTER
Database: no cache_attempts row
Business row: name=Ada
```

The timeline is:

```text
t0  W1 generates B1 and writes BEFORE=B1.
t1  W1 reads AFTER=missing, so expected=NULL.
t2  W1 pauses before reaching the database.
t3  W2 generates B2 and writes BEFORE=B2.
t4  W2 reads AFTER=missing, so expected=NULL.
t5  W2 inserts (user:42, B2).
t6  W2 updates the business row to Luna and commits.
t7  W2 writes AFTER=B2 and value=Luna.
t8  W1 tries to initialize with B1 and gets no row from ON CONFLICT.
t9  W1 rolls back; Grace never reaches the business table.
```

The final state is:

```text
Database: name=Luna, current attempt=B2
Redis: BEFORE=B2, AFTER=B2, value=Luna
```

W1 wrote `BEFORE` first, but the database claim decides which writer can
continue.

## Three Writers Arrive Out Of Order

Concurrent writers read the same confirmed `AFTER` until one of them commits.
Overwriting `BEFORE` does not advance the database expectation by itself:

```text
Initial: AFTER=B0

W1 writes BEFORE=B1 and reads expected AFTER=B0.
W2 writes BEFORE=B2 and reads expected AFTER=B0.
W3 writes BEFORE=B3 and reads expected AFTER=B0.
```

Network timing may deliver W3, then W1, then W2 to the database. All three
attempt the same conditional transition from B0. Only the first transaction to
win the database row can update the business data; the other two see a stale
expected attempt and roll back.

If W3 starts after W2 has committed and written `AFTER=B2`, W3 reads expected
`AFTER=B2` and can advance the state from B2 to B3. W1's old expected B0 still
fails. The database does not trust network arrival order, and a writer whose
expected `AFTER` is not current cannot update the business row.

## Reader During an Uncommitted Writer

```text
t0  Redis: BEFORE=A, AFTER=A, value=A. Trusted.
t1  W1 writes BEFORE=B. Redis becomes dirty.
t2  W1 starts a database transaction but has not committed.
t3  R1 sees BEFORE=B and AFTER=A, so it ignores Redis.
t4  R1 reads the database and receives the last committed value A.
```

Repair uses the same expected-attempt transition as a writer. If repair claims
B first, W1's later transition fails and W1 rolls back. If W1 claims B first,
repair waits for the transaction and then reads the committed result or the
previous value after rollback. The database transaction decides; repair does
not guess from timing.

## Rejected Write And Crashes

Rejected business operation:

```text
t0  BEFORE=A, AFTER=A, value=A.
t1  Writer writes BEFORE=B.
t2  The transaction rejects and rolls back its attempt transition.
t3  Redis remains BEFORE=B, AFTER=A.
t4  Reads fall back to the database and receive A.
```

Crash before the database leaves the same mismatch. A repair can initialize or
advance the attempt, read the committed business value, and restore a matching
pair.

If the database commits but the process crashes before `AFTER`:

```text
t0  Writer writes BEFORE=B.
t1  The database attempt and business update commit.
t2  The process crashes before writing AFTER.
t3  Redis remains mismatched, so reads use the database.
t4  Repair locks and reads the committed database state.
t5  Repair restores AFTER and the confirmed value.
```

If a newer `BEFORE` replaced B, repair only changes Redis when the marker it
observed is still current. Otherwise it leaves Redis untrusted.

## Late AFTER

```text
t0  W1 commits and will eventually write AFTER=B1.
t1  W2 writes BEFORE=B2.
t2  W2 commits and writes AFTER=B2.
t3  W1's delayed AFTER=B1 arrives.
```

The Redis compare-and-set sees `BEFORE=B2`, not B1, and rejects W1's old
confirmation. A late confirmation cannot overwrite a newer marker.

## Repair And Singleflight

Repair:

```text
1. Read BEFORE and AFTER from Redis.
2. Start a database transaction.
3. Attempt the same expected-attempt transition as a writer.
4. Lock and read the committed current attempt and business value.
5. Commit the repair transaction.
6. Restore Redis only if the observed BEFORE is unchanged.
```

Singleflight is a load optimization for many readers of one dirty key. One
request or worker performs the repair while the others briefly wait. If the
owner fails, another request can retry. Correctness comes from the database
transition and the Redis `BEFORE` check, not from the singleflight lock.

## Assumptions And Limits

- Every write to the protected data uses the coordinator.
- The business update and attempt transition share one transaction.
- Redis `BEFORE` failures stop the write rather than silently proceeding.
- The database provides transactional conditional updates and row locking.
- Redis failover is configured so acknowledged hash writes are not silently
  lost. If a matching marker may be lost, the cache must be rebuilt.

This is a cache-coordination pattern, not a replacement for an outbox or CDC
pipeline. It keeps the database transaction authoritative and makes Redis fail
closed: when the protocol cannot prove the cache is current, reads go to the
database.

## Race Coverage

The runnable demo exercises these cases:

- **Sequential handoff:** W1 confirms its attempt; W2 starts afterward, reads
  W1's UUID, and commits the next attempt successfully.
- **Two writers with one expected attempt:** both writers start from the same
  `AFTER`; the first database claim commits and the other writer rolls back.
- **Three writers:** all six arrival orders are tested. The first database
  claimant commits, the other two lose the conditional attempt transition, and
  repair restores a trusted Redis value.
- **Reader before writer commit:** the reader rejects dirty Redis and returns
  the last committed database value.
- **Repair during a writer transaction:** repair waits for the database claim;
  it cannot hide the active writer and eventually observes the committed row.
- **Many dirty readers:** one singleflight owner repairs the key while the
  followers wait and then read the trusted Redis value.
- **Rejected write and crash after commit:** the business rollback or missing
  `AFTER` leaves Redis untrusted until repair runs.
- **Late `AFTER`:** an older confirmation cannot overwrite a newer `BEFORE`.
- **Redis outage:** a failed `BEFORE` stops the cached write; a failure after
  the database commit leaves a repairable dirty entry. Redis outage recovery
  is tested separately because this core protocol intentionally does not allow
  database-only writes.
