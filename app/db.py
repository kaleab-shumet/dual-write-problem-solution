from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

import asyncpg


T = TypeVar("T")


CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT 'Ada Lovelace',
  phone_number TEXT NOT NULL,
  email TEXT NOT NULL DEFAULT 'ada.lovelace@example.com',
  version BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS cache_attempts (
  cache_key TEXT PRIMARY KEY,
  attempt_uuid UUID NOT NULL
);

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT 'Ada Lovelace';

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS email TEXT NOT NULL DEFAULT 'ada.lovelace@example.com';
"""


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url, min_size=1, max_size=10)


async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SCHEMA_SQL)


async def reset_user(pool: asyncpg.Pool, user_id: str = "42") -> asyncpg.Record:
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM cache_attempts WHERE cache_key = $1",
            f"user:{user_id}",
        )
        return await conn.fetchrow(
            """
            INSERT INTO users (id, name, phone_number, email, version)
            VALUES ($1, $2, $3, $4, 1)
            ON CONFLICT (id) DO UPDATE
            SET name = EXCLUDED.name,
                phone_number = EXCLUDED.phone_number,
                email = EXCLUDED.email,
                version = 1
            RETURNING id, name, phone_number, email, version
            """,
            user_id,
            "Ada Lovelace",
            "+44-20-7946-0100",
            "ada.lovelace@example.com",
        )


async def get_user(pool: asyncpg.Pool, user_id: str) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT id, name, phone_number, email, version FROM users WHERE id = $1",
            user_id,
        )


async def update_user(
    pool: asyncpg.Pool,
    user_id: str,
    name: str | None,
    phone_number: str | None,
) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            UPDATE users
            SET name = COALESCE($1, name),
                phone_number = COALESCE($2, phone_number),
                version = version + 1
            WHERE id = $3
            RETURNING id, name, phone_number, email, version
            """,
            name,
            phone_number,
            user_id,
        )


async def insert_cache_attempt(
    conn: asyncpg.Connection,
    cache_key: str,
    attempt_uuid: str,
) -> bool:
    row = await conn.fetchrow(
        """
        INSERT INTO cache_attempts (cache_key, attempt_uuid)
        VALUES ($1, $2::uuid)
        ON CONFLICT (cache_key) DO NOTHING
        RETURNING attempt_uuid
        """,
        cache_key,
        attempt_uuid,
    )
    return row is not None


async def advance_cache_attempt(
    conn: asyncpg.Connection,
    cache_key: str,
    expected_attempt_uuid: str | None,
    attempt_uuid: str,
) -> bool:
    if expected_attempt_uuid is None:
        return await insert_cache_attempt(conn, cache_key, attempt_uuid)

    row = await conn.fetchrow(
        """
        UPDATE cache_attempts
        SET attempt_uuid = $3::uuid
        WHERE cache_key = $1
          AND attempt_uuid = $2::uuid
        RETURNING attempt_uuid
        """,
        cache_key,
        expected_attempt_uuid,
        attempt_uuid,
    )
    return row is not None


async def current_cache_attempt(
    conn: asyncpg.Connection,
    cache_key: str,
) -> str | None:
    row = await conn.fetchrow(
        """
        SELECT attempt_uuid::text AS attempt_uuid
        FROM cache_attempts
        WHERE cache_key = $1
        FOR UPDATE
        """,
        cache_key,
    )
    return row["attempt_uuid"] if row else None


async def bootstrap_cache_attempt(
    pool: asyncpg.Pool,
    cache_key: str,
    candidate_attempt_uuid: str,
) -> str:
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            await insert_cache_attempt(conn, cache_key, candidate_attempt_uuid)
            attempt_uuid = await current_cache_attempt(conn, cache_key)
            await tx.commit()
            if attempt_uuid is None:
                raise RuntimeError(f"cache attempt was not initialized: {cache_key}")
            return attempt_uuid
        except Exception:
            await tx.rollback()
            raise


async def run_with_cache_attempt(
    pool: asyncpg.Pool,
    cache_key: str,
    expected_attempt_uuid: str | None,
    attempt_uuid: str,
    write_fn: Callable[[asyncpg.Connection], Awaitable[T | None]],
) -> T | None:
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            claimed = await advance_cache_attempt(
                conn,
                cache_key,
                expected_attempt_uuid,
                attempt_uuid,
            )
            if not claimed:
                await tx.rollback()
                return None
            result = await write_fn(conn)
            if result is None:
                await tx.rollback()
                return None
            await tx.commit()
            return result
        except Exception:
            await tx.rollback()
            raise


async def repair_entity_for_attempt(
    pool: asyncpg.Pool,
    cache_key: str,
    expected_attempt_uuid: str | None,
    attempt_uuid: str,
    fetch_fn: Callable[[asyncpg.Connection], Awaitable[T | None]],
) -> tuple[T | None, str | None, str]:
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            claimed = await advance_cache_attempt(
                conn,
                cache_key,
                expected_attempt_uuid,
                attempt_uuid,
            )
            current_attempt = attempt_uuid if claimed else await current_cache_attempt(conn, cache_key)
            row = await fetch_fn(conn)
            await tx.commit()
            action = "repair_claimed_attempt" if claimed else "read_current_attempt"
            return row, current_attempt, action
        except Exception:
            await tx.rollback()
            raise


async def update_user_in_tx(
    conn: asyncpg.Connection,
    user_id: str,
    name: str | None,
    phone_number: str | None,
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        UPDATE users
        SET name = COALESCE($1, name),
            phone_number = COALESCE($2, phone_number),
            version = version + 1
        WHERE id = $3
        RETURNING id, name, phone_number, email, version
        """,
        name,
        phone_number,
        user_id,
    )


async def update_user_with_attempt(
    pool: asyncpg.Pool,
    user_id: str,
    expected_attempt_uuid: str | None,
    attempt_uuid: str,
    name: str | None,
    phone_number: str | None,
) -> asyncpg.Record | None:
    return await run_with_cache_attempt(
        pool,
        f"user:{user_id}",
        expected_attempt_uuid,
        attempt_uuid,
        lambda conn: update_user_in_tx(conn, user_id, name, phone_number),
    )


async def repair_user_for_attempt(
    pool: asyncpg.Pool,
    user_id: str,
    expected_attempt_uuid: str | None,
    attempt_uuid: str,
) -> tuple[asyncpg.Record | None, str | None, str]:
    return await repair_entity_for_attempt(
        pool,
        f"user:{user_id}",
        expected_attempt_uuid,
        attempt_uuid,
        lambda conn: conn.fetchrow(
            "SELECT id, name, phone_number, email, version FROM users WHERE id = $1",
            user_id,
        ),
    )
