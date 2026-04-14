import sqlite3
from pathlib import Path

DB_PATH = Path("data/raw/risk_monitor_dataset.sqlite")

if not DB_PATH.exists():
    raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

tables = [row[0] for row in cursor.execute("""
    SELECT name
    FROM sqlite_master
    WHERE type='table'
    ORDER BY name
""").fetchall()]

print("=" * 80)
print("ROW COUNTS")
print("=" * 80)
for table in tables:
    count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {count}")

print("\n" + "=" * 80)
print("SAMPLE ROWS")
print("=" * 80)
for table in tables:
    print(f"\n--- {table} ---")
    rows = cursor.execute(f"SELECT * FROM {table} LIMIT 5").fetchall()
    col_names = [col[1] for col in cursor.execute(f"PRAGMA table_info({table})").fetchall()]
    print("Columns:", col_names)
    for row in rows:
        print(row)

checks = {
    "users": ["status", "country", "phone_prefix"],
    "subscriptions": ["status", "brand", "currency"],
    "memberships": ["status", "reason"],
    "payments": ["status", "currency", "stripe_error_code"],
    "complaints": ["status", "type", "resolution"],
}

print("\n" + "=" * 80)
print("DISTINCT VALUES")
print("=" * 80)
for table, columns in checks.items():
    for col in columns:
        print(f"\n[{table}.{col}]")
        values = cursor.execute(f"""
            SELECT {col}, COUNT(*) as n
            FROM {table}
            GROUP BY {col}
            ORDER BY n DESC
            LIMIT 20
        """).fetchall()
        for value, n in values:
            print(f"{value!r}: {n}")

conn.close()
