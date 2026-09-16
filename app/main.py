from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from app import cache, db
from app.settings import DATABASE_URL, REDIS_URL


class UserPatchRequest(BaseModel):
    name: str | None = Field(default=None, examples=["Ada Byron"])
    phone_number: str | None = Field(default=None, examples=["+1-555-0199"])
    expected_version: int | None = None


class DemoProfileUpdateRequest(BaseModel):
    name: str = Field(default="Ada Lovelace")
    phone_number: str = Field(default="+44-20-7946-0100")
    expected_version: int | None = None


class AppState:
    db_pool: asyncpg.Pool
    redis: Redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await db.create_pool(DATABASE_URL)
    app.state.redis = await cache.create_redis(REDIS_URL)
    await db.init_db(app.state.db_pool)
    await reset_demo_state(app.state.db_pool, app.state.redis)
    yield
    await app.state.redis.aclose()
    await app.state.db_pool.close()


app = FastAPI(
    title="Redis BEFORE/AFTER Fencing Demo",
    description="A runnable demo of a Redis cache that refuses to serve unconfirmed dual writes.",
    lifespan=lifespan,
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def pool() -> asyncpg.Pool:
    return app.state.db_pool


def redis() -> Redis:
    return app.state.redis


def row_to_dict(row: asyncpg.Record | None) -> dict[str, Any] | None:
    return dict(row) if row else None


async def snapshot(user_id: str = "42") -> dict[str, Any]:
    return {
        "postgres": row_to_dict(await db.get_user(pool(), user_id)),
        "redis": await cache.get_cache(redis(), user_id),
        "dirty_keys": await cache.dirty_keys(redis()),
    }


async def reset_demo_state(db_pool: asyncpg.Pool, redis_client: Redis) -> dict[str, Any]:
    await redis_client.flushdb()
    row = await db.reset_user(db_pool, "42")
    return {
        "postgres": row_to_dict(row),
        "redis": await cache.get_cache(redis_client, row["id"]),
        "dirty_keys": await cache.dirty_keys(redis_client),
    }


async def current_expected_version(user_id: str, explicit: int | None) -> int:
    if explicit is not None:
        return explicit
    row = await db.get_user(pool(), user_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"user {user_id} not found")
    return int(row["version"])


async def repair_one_key(key: str) -> dict[str, Any]:
    user_id = cache.user_id_from_cache_key(key)
    cached = await cache.get_cache(redis(), user_id)
    if cached.get("trusted"):
        await cache.remove_dirty(redis(), key)
        return {"key": key, "action": "already_trusted"}

    row = await db.get_user(pool(), user_id)
    if row is None:
        return {"key": key, "action": "missing_postgres_row"}

    repaired = await cache.repair_from_db(
        redis(),
        user_id,
        dict(row),
        row["version"],
    )
    return {
        "key": key,
        "action": {
            0: "skipped_newer_confirmed_cache",
            1: "repaired_confirmed_newer_db_value",
            2: "skipped_newer_in_flight_attempt",
            3: "cleared_rejected_attempt",
        }.get(repaired, "unknown"),
    }


@app.get("/")
async def index() -> dict[str, Any]:
    return {
        "message": "Open /ui for the user UI, /debug-ui for the demo console, or /docs for API docs.",
        "happy_path": [
            "POST /demo/reset",
            "GET /users/42",
            "PATCH /users/42",
            "POST /demo/crash-after-db-commit",
            "POST /repair/run-once",
            "POST /demo/delayed-after-race",
        ],
    }


@app.get("/ui", include_in_schema=False)
async def ui() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/debug-ui", include_in_schema=False)
async def debug_ui() -> FileResponse:
    return FileResponse(STATIC_DIR / "debug.html")


@app.post("/demo/reset")
async def reset_demo() -> dict[str, Any]:
    state = await reset_demo_state(pool(), redis())
    return {"outcome": "reset", **state}


@app.get("/users/{user_id}")
async def read_user(user_id: str) -> dict[str, Any]:
    cached = await cache.get_cache(redis(), user_id)
    if cached.get("trusted"):
        return {
            "served_from": "redis",
            "trusted_cache": True,
            "user": cached["value"],
            "debug": await snapshot(user_id),
        }

    row = await db.get_user(pool(), user_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"user {user_id} not found")

    if not cached.get("before_uuid") and not cached.get("after_uuid") and "value" not in cached:
        await cache.set_trusted(redis(), user_id, dict(row), row["version"])

    return {
        "served_from": "postgres",
        "trusted_cache": False,
        "user": row_to_dict(row),
        "debug": await snapshot(user_id),
    }


@app.patch("/users/{user_id}")
async def update_user(user_id: str, body: UserPatchRequest) -> dict[str, Any]:
    if body.name is None and body.phone_number is None:
        raise HTTPException(status_code=422, detail="Provide name, phone_number, or both.")

    expected_version = await current_expected_version(user_id, body.expected_version)
    current = await db.get_user(pool(), user_id)
    if current is None:
        raise HTTPException(status_code=404, detail=f"user {user_id} not found")

    attempt_uuid = cache.new_uuid()
    await cache.write_before(
        redis(),
        user_id,
        attempt_uuid,
    )

    row = await db.update_user(
        pool(),
        user_id,
        body.name,
        body.phone_number,
        expected_version,
    )
    if row is None:
        return {
            "outcome": "postgres_rejected_write",
            "why_it_is_safe": "AFTER was not written, so Redis remains untrusted.",
            "debug": await snapshot(user_id),
        }

    confirm_result = await cache.confirm_after(
        redis(),
        user_id,
        attempt_uuid,
        dict(row),
        row["version"],
    )
    return {
        "outcome": "committed_and_confirmed",
        "attempt_uuid": attempt_uuid,
        "confirm_result": confirm_result,
        "user": row_to_dict(row),
        "debug": await snapshot(user_id),
    }


@app.post("/demo/rejected-write")
async def rejected_write(
    body: DemoProfileUpdateRequest = DemoProfileUpdateRequest(
        name="Ada Lovelace",
        phone_number="+44-20-7946-0100",
    ),
) -> dict[str, Any]:
    row = await db.get_user(pool(), "42")
    if row is None:
        raise HTTPException(status_code=404, detail="user 42 not found")

    stale_version = max(int(row["version"]) - 1, 0)
    attempt_uuid = cache.new_uuid()
    await cache.write_before(
        redis(),
        "42",
        attempt_uuid,
    )
    rejected = await db.update_user(
        pool(),
        "42",
        body.name,
        body.phone_number,
        stale_version,
    )
    return {
        "outcome": "postgres_rejected_write",
        "attempt_uuid": attempt_uuid,
        "postgres_returned_row": row_to_dict(rejected),
        "why_it_is_safe": "BEFORE only contains an unconfirmed UUID marker. Reads fall back because BEFORE.uuid != AFTER.uuid.",
        "debug": await snapshot("42"),
    }


@app.post("/demo/crash-after-db-commit")
async def crash_after_db_commit(
    body: DemoProfileUpdateRequest = DemoProfileUpdateRequest(),
) -> dict[str, Any]:
    expected_version = await current_expected_version("42", body.expected_version)
    current = await db.get_user(pool(), "42")
    if current is None:
        raise HTTPException(status_code=404, detail="user 42 not found")

    attempt_uuid = cache.new_uuid()
    await cache.write_before(
        redis(),
        "42",
        attempt_uuid,
    )
    row = await db.update_user(
        pool(),
        "42",
        body.name,
        body.phone_number,
        expected_version,
    )
    if row is None:
        return {"outcome": "postgres_rejected_write", "debug": await snapshot("42")}

    return {
        "outcome": "simulated_crash_after_postgres_commit",
        "attempt_uuid": attempt_uuid,
        "important_part": "The API intentionally did not write AFTER. Reads are safe because Redis is now untrusted.",
        "user": row_to_dict(row),
        "debug": await snapshot("42"),
    }


@app.post("/demo/delayed-after-race")
async def delayed_after_race(
    body: DemoProfileUpdateRequest = DemoProfileUpdateRequest(
        name="Ada Lovelace",
        phone_number="+44-20-7946-0100",
    ),
) -> dict[str, Any]:
    row = await db.get_user(pool(), "42")
    if row is None:
        raise HTTPException(status_code=404, detail="user 42 not found")

    starting_version = int(row["version"])

    uuid_b = cache.new_uuid()
    await cache.write_before(redis(), "42", uuid_b)
    row_b = await db.update_user(
        pool(),
        "42",
        body.name,
        body.phone_number,
        starting_version,
    )
    if row_b is None:
        raise HTTPException(status_code=409, detail="first simulated update failed")

    uuid_c = cache.new_uuid()
    await cache.write_before(redis(), "42", uuid_c)
    row_c = await db.update_user(
        pool(),
        "42",
        body.name,
        body.phone_number,
        starting_version + 1,
    )
    if row_c is None:
        raise HTTPException(status_code=409, detail="second simulated update failed")

    confirm_c = await cache.confirm_after(redis(), "42", uuid_c, dict(row_c), row_c["version"])
    delayed_confirm_b = await cache.confirm_after(redis(), "42", uuid_b, dict(row_b), row_b["version"])

    return {
        "outcome": "newer_confirm_won",
        "first_update": {"uuid": uuid_b, "version": row_b["version"]},
        "second_update": {"uuid": uuid_c, "version": row_c["version"]},
        "confirm_second_result": confirm_c,
        "delayed_confirm_first_result": delayed_confirm_b,
        "why_it_is_safe": "The late AFTER for the older version returned 0 and could not clobber the newer confirmed cache.",
        "debug": await snapshot("42"),
    }


@app.post("/repair/run-once")
async def repair_run_once() -> dict[str, Any]:
    keys = await cache.dirty_keys(redis())
    results = [await repair_one_key(key) for key in keys]
    return {"results": results, "debug": await snapshot("42")}


@app.get("/debug/users/{user_id}")
async def debug_user(user_id: str) -> dict[str, Any]:
    return await snapshot(user_id)
