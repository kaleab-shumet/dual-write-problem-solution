from __future__ import annotations

import os


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DIRTY_KEYS_SET = "dirty_keys"
WORKER_INTERVAL_SECONDS = float(os.getenv("WORKER_INTERVAL_SECONDS", "300"))
USER_LOCK_TTL_MS = int(os.getenv("USER_LOCK_TTL_MS", "5000"))
USER_LOCK_WAIT_MS = int(os.getenv("USER_LOCK_WAIT_MS", "300"))
