#!/usr/bin/env python3
"""Demonstrate PostgreSQL xmin preventing two writers from updating one row."""

from __future__ import annotations

import asyncio
import os

import asyncpg


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")
TABLE = "xmin_race_demo"


async def read_xmin(conn: asyncpg.Connection) -> str:
    row = await conn.fetchrow(
        f"SELECT xmin::text AS row_token, name FROM {TABLE} WHERE id = 1"
    )
    assert row is not None
    return str(row["row_token"])


async def main() -> None:
    admin = await asyncpg.connect(DATABASE_URL)
    writer_one = await asyncpg.connect(DATABASE_URL)
    writer_two = await asyncpg.connect(DATABASE_URL)

    try:
        await admin.execute(f"DROP TABLE IF EXISTS {TABLE}")
        await admin.execute(
            f"""
            CREATE TABLE {TABLE} (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            )
            """
        )
        await admin.execute(f"INSERT INTO {TABLE} (id, name) VALUES (1, 'Ada')")

        tx_one = writer_one.transaction()
        tx_two = writer_two.transaction()
        await tx_one.start()
        await tx_two.start()

        token_one = await read_xmin(writer_one)
        token_two = await read_xmin(writer_two)
        print(f"Both writers read xmin={token_one} and xmin={token_two}")

        first_update = await writer_one.fetchrow(
            f"""
            UPDATE {TABLE}
            SET name = 'Grace'
            WHERE id = 1 AND xmin = $1::xid
            RETURNING name, xmin::text AS new_xmin
            """,
            int(token_one),
        )
        assert first_update is not None
        print(
            "W1 updates successfully but has not committed: "
            f"name={first_update['name']}, new xmin={first_update['new_xmin']}"
        )

        second_update = asyncio.create_task(
            writer_two.fetchrow(
                f"""
                UPDATE {TABLE}
                SET name = 'Linus'
                WHERE id = 1 AND xmin = $1::xid
                RETURNING name, xmin::text AS new_xmin
                """,
                int(token_two),
            )
        )
        await asyncio.sleep(0.2)
        print("W2 is still waiting because W1 holds the row lock.")

        await tx_one.commit()
        print("W1 commits.")

        second_result = await second_update
        if second_result is None:
            print("W2 returns zero rows: xmin changed, so W2 cannot commit.")
            await tx_two.rollback()
        else:
            print(f"Unexpected W2 update: {dict(second_result)}")
            await tx_two.commit()

        final_row = await admin.fetchrow(
            f"SELECT name, xmin::text AS row_token FROM {TABLE} WHERE id = 1"
        )
        print(f"Final database row: name={final_row['name']}, xmin={final_row['row_token']}")
    finally:
        await writer_one.close()
        await writer_two.close()
        await admin.execute(f"DROP TABLE IF EXISTS {TABLE}")
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
