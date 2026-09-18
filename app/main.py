from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import asyncpg
from bullmq import Queue
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from app import cache, db
from app.cache_coordinator import CacheCoordinator
from app.settings import DATABASE_URL, REDIS_URL, REPAIR_QUEUE_NAME


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
    repair_queue: Queue
    cache_coordinator: CacheCoordinator


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await db.create_pool(DATABASE_URL)
    app.state.redis = await cache.create_redis(REDIS_URL)
    app.state.repair_queue = Queue(REPAIR_QUEUE_NAME, {"connection": REDIS_URL})
    app.state.cache_coordinator = CacheCoordinator(
        app.state.db_pool,
        app.state.redis,
        app.state.repair_queue,
    )
    await db.init_db(app.state.db_pool)
    await reset_demo_state(app.state.db_pool, app.state.redis)
    yield
    await app.state.repair_queue.close()
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


def user_cache_key(user_id: str) -> str:
    return cache.user_cache_key(user_id)


def coordinator() -> CacheCoordinator:
    return app.state.cache_coordinator


def row_to_dict(row: asyncpg.Record | None) -> dict[str, Any] | None:
    return dict(row) if row else None


async def snapshot(user_id: str = "42") -> dict[str, Any]:
    return {
        "postgres": row_to_dict(await db.get_user(pool(), user_id)),
        "redis": await cache.get_cache(redis(), user_cache_key(user_id)),
        "dirty_keys": await cache.dirty_keys(redis()),
    }


async def reset_demo_state(db_pool: asyncpg.Pool, redis_client: Redis) -> dict[str, Any]:
    await redis_client.flushdb()
    row = await db.reset_user(db_pool, "42")
    return {
        "postgres": row_to_dict(row),
        "redis": await cache.get_cache(redis_client, user_cache_key(row["id"])),
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
    cached = await cache.get_cache(redis(), key)
    before_uuid = cached.get("before_uuid")
    if cached.get("trusted"):
        await cache.remove_dirty(redis(), key)
        return {"key": key, "action": "already_trusted"}
    if not before_uuid:
        return {"key": key, "action": "missing_before_uuid"}

    repaired = await coordinator().repair_attempt(
        key,
        before_uuid,
        lambda: db.get_user(pool(), user_id),
    )
    if repaired.get("value") is None:
        return {"key": key, "action": "missing_postgres_row"}
    return {
        "key": key,
        "action": "repaired_after_from_db",
        "db_action": repaired.get("db_action"),
        "confirm_result": repaired.get("confirm_result"),
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
    result = await coordinator().read_with_worker(
        user_cache_key(user_id),
        lambda: db.get_user(pool(), user_id),
    )
    if result.get("served_from") == "missing":
        raise HTTPException(status_code=404, detail=f"user {user_id} not found")
    result["user"] = result.pop("value")
    result["debug"] = await snapshot(user_id)
    status_code = result.pop("status_code", None)
    if status_code:
        return JSONResponse(status_code=status_code, content=result)
    return result


@app.get("/users/{user_id}/singleflight")
async def read_user_with_singleflight(user_id: str) -> dict[str, Any]:
    result = await coordinator().read_with_singleflight(
        user_cache_key(user_id),
        lambda: db.get_user(pool(), user_id),
    )
    if result.get("served_from") == "missing":
        raise HTTPException(status_code=404, detail=f"user {user_id} not found")
    result["user"] = result.pop("value")
    result["debug"] = await snapshot(user_id)
    status_code = result.pop("status_code", None)
    if status_code:
        return JSONResponse(status_code=status_code, content=result)
    return result


@app.patch("/users/{user_id}")
async def update_user(user_id: str, body: UserPatchRequest) -> dict[str, Any]:
    if body.name is None and body.phone_number is None:
        raise HTTPException(status_code=422, detail="Provide name, phone_number, or both.")

    expected_version = await current_expected_version(user_id, body.expected_version)
    current = await db.get_user(pool(), user_id)
    if current is None:
        raise HTTPException(status_code=404, detail=f"user {user_id} not found")

    result = await coordinator().cached_write(
        user_cache_key(user_id),
        lambda conn: db.update_user_in_tx(
            conn,
            user_id,
            body.name,
            body.phone_number,
            expected_version,
        ),
    )
    result["user"] = result.pop("value")
    if result["user"] is None:
        result["why_it_is_safe"] = "AFTER was not written, so Redis remains untrusted until the worker confirms this UUID."
    result["debug"] = await snapshot(user_id)
    return result


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
    result = await coordinator().cached_write(
        user_cache_key("42"),
        lambda conn: db.update_user_in_tx(
            conn,
            "42",
            body.name,
            body.phone_number,
            stale_version,
        ),
    )
    return {
        "outcome": "postgres_rejected_write",
        "attempt_uuid": result["attempt_uuid"],
        "postgres_returned_row": result["value"],
        "why_it_is_safe": "The rejected DB transaction rolled back its attempt insert. The worker can claim the same BEFORE UUID and confirm the still-current DB row.",
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
    result = await coordinator().cached_write(
        user_cache_key("42"),
        lambda conn: db.update_user_in_tx(
            conn,
            "42",
            body.name,
            body.phone_number,
            expected_version,
        ),
        confirm_after=False,
        enqueue_repair_after_commit=True,
    )
    if result["value"] is None:
        return {"outcome": "postgres_rejected_write_or_attempt_lost", "debug": await snapshot("42")}
    return {
        "outcome": "simulated_crash_after_postgres_commit",
        "attempt_uuid": result["attempt_uuid"],
        "important_part": "The API intentionally did not write AFTER. The worker will try the same BEFORE UUID; duplicate means the DB transaction committed, so it repairs from the committed row.",
        "user": result["value"],
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

    first = await coordinator().cached_write(
        user_cache_key("42"),
        lambda conn: db.update_user_in_tx(
            conn,
            "42",
            body.name,
            body.phone_number,
            starting_version,
        ),
        confirm_after=False,
    )
    if first["value"] is None:
        raise HTTPException(status_code=409, detail="first simulated update failed")

    second = await coordinator().cached_write(
        user_cache_key("42"),
        lambda conn: db.update_user_in_tx(
            conn,
            "42",
            body.name,
            body.phone_number,
            starting_version + 1,
        ),
    )
    if second["value"] is None:
        raise HTTPException(status_code=409, detail="second simulated update failed")

    delayed_confirm_b = await cache.confirm_after(
        redis(),
        user_cache_key("42"),
        first["attempt_uuid"],
        first["value"],
        first["value"]["version"],
    )

    return {
        "outcome": "newer_confirm_won",
        "first_update": {"uuid": first["attempt_uuid"], "version": first["value"]["version"]},
        "second_update": {"uuid": second["attempt_uuid"], "version": second["value"]["version"]},
        "confirm_second_result": second["confirm_result"],
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
