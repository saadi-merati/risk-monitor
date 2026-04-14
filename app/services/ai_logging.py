import json
import sqlite3
from pathlib import Path
from typing import Any, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
STATE_DB_PATH = ROOT_DIR / "data" / "app_state.sqlite"


def get_connection() -> sqlite3.Connection:
    STATE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(STATE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_ai_tables() -> None:
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                role TEXT NOT NULL,
                model TEXT,
                prompt_version TEXT,
                cache_key TEXT,
                input_json TEXT,
                output_json TEXT,
                success INTEGER NOT NULL,
                error_message TEXT,
                estimated_cost_usd REAL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_cache (
                cache_key TEXT PRIMARY KEY,
                user_id INTEGER,
                role TEXT NOT NULL,
                model TEXT,
                prompt_version TEXT,
                input_json TEXT,
                output_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    finally:
        conn.close()


def read_cache(cache_key: str) -> Optional[dict[str, Any]]:
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT output_json
            FROM ai_cache
            WHERE cache_key = ?
        """, (cache_key,)).fetchone()

        if row is None:
            return None

        return json.loads(row["output_json"])
    finally:
        conn.close()


def write_cache(
    cache_key: str,
    user_id: int,
    role: str,
    model: str,
    prompt_version: str,
    input_payload: dict[str, Any],
    output_payload: dict[str, Any],
) -> None:
    conn = get_connection()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO ai_cache (
                cache_key, user_id, role, model, prompt_version, input_json, output_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            cache_key,
            int(user_id),
            role,
            model,
            prompt_version,
            json.dumps(input_payload, ensure_ascii=False),
            json.dumps(output_payload, ensure_ascii=False),
        ))
        conn.commit()
    finally:
        conn.close()


def log_ai_call(
    user_id: int,
    role: str,
    model: str,
    prompt_version: str,
    cache_key: str,
    input_payload: dict[str, Any],
    output_payload: Optional[dict[str, Any]],
    success: bool,
    error_message: Optional[str] = None,
    estimated_cost_usd: Optional[float] = None,
) -> None:
    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO ai_calls (
                user_id, role, model, prompt_version, cache_key,
                input_json, output_json, success, error_message, estimated_cost_usd
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            int(user_id),
            role,
            model,
            prompt_version,
            cache_key,
            json.dumps(input_payload, ensure_ascii=False),
            json.dumps(output_payload, ensure_ascii=False) if output_payload is not None else None,
            1 if success else 0,
            error_message,
            estimated_cost_usd,
        ))
        conn.commit()
    finally:
        conn.close()