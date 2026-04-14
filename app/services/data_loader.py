import sqlite3
from pathlib import Path

import pandas as pd


def load_tables(db_path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        users = pd.read_sql_query("SELECT * FROM users", conn)
        subscriptions = pd.read_sql_query("SELECT * FROM subscriptions", conn)
        memberships = pd.read_sql_query("SELECT * FROM memberships", conn)
        payments = pd.read_sql_query("SELECT * FROM payments", conn)
        complaints = pd.read_sql_query("SELECT * FROM complaints", conn)
    finally:
        conn.close()

    return users, subscriptions, memberships, payments, complaints