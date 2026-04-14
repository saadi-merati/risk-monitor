import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
STATE_DB_PATH = ROOT_DIR / "data" / "app_state.sqlite"


def get_connection() -> sqlite3.Connection:
    STATE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(STATE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_state_db() -> None:
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscriber_actions (
                user_id INTEGER PRIMARY KEY,
                action TEXT NOT NULL CHECK (action IN ('watch', 'block', 'none')),
                note TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    finally:
        conn.close()


def upsert_action(user_id: int, action: str, note: Optional[str] = None) -> None:
    if action not in {"watch", "block", "none"}:
        raise ValueError(f"Invalid action: {action}")

    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO subscriber_actions (user_id, action, note, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                action = excluded.action,
                note = excluded.note,
                updated_at = CURRENT_TIMESTAMP
        """, (int(user_id), action, note))
        conn.commit()
    finally:
        conn.close()


def get_action(user_id: int) -> dict:
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT user_id, action, note, updated_at
            FROM subscriber_actions
            WHERE user_id = ?
        """, (int(user_id),)).fetchone()

        if row is None:
            return {
                "user_id": int(user_id),
                "action": "none",
                "note": None,
                "updated_at": None,
            }

        return dict(row)
    finally:
        conn.close()


def get_all_actions() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query("""
            SELECT user_id, action, note, updated_at
            FROM subscriber_actions
        """, conn)
        return df
    finally:
        conn.close()


def merge_actions(scored_df: pd.DataFrame) -> pd.DataFrame:
    actions_df = get_all_actions()

    if actions_df.empty:
        result = scored_df.copy()
        result["operator_action"] = "none"
        result["operator_note"] = None
        result["action_updated_at"] = None
        return result

    result = scored_df.merge(actions_df, on="user_id", how="left")
    result["action"] = result["action"].fillna("none")

    result = result.rename(columns={
        "action": "operator_action",
        "note": "operator_note",
        "updated_at": "action_updated_at",
    })

    return result


if __name__ == "__main__":
    init_state_db()
    print(f"State DB initialized at: {STATE_DB_PATH}")