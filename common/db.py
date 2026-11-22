"""Tiny SQLite helper. Each service opens ITS OWN file (database per service)."""
import os
import sqlite3

DB_PATH = os.getenv("DB_PATH", "/data/service.db")


def connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # Every consumer records the message IDs it has handled (idempotent consumer).
    conn.execute(
        "CREATE TABLE IF NOT EXISTS processed_messages (message_id TEXT PRIMARY KEY)"
    )
    return conn
