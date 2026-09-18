from __future__ import annotations

import os


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DIRTY_KEYS_SET = "dirty_keys"
REPAIR_QUEUE_NAME = os.getenv("REPAIR_QUEUE_NAME", "cache-repairs")
REPAIR_WAIT_MS = int(os.getenv("REPAIR_WAIT_MS", "600"))
SINGLEFLIGHT_TTL_MS = int(os.getenv("SINGLEFLIGHT_TTL_MS", "5000"))
