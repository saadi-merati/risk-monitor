import sqlite3
from pathlib import Path

DB_PATH = Path("data/raw/risk_monitor_dataset.sqlite")

if not DB_PATH.exists():
    raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print(f"Base ouverte : {DB_PATH}\n")

tables = cursor.execute("""
    SELECT name
    FROM sqlite_master
    WHERE type='table'
    ORDER BY name
""").fetchall()

print("Tables trouvées :")
for (table_name,) in tables:
    print(f"- {table_name}")

print("\nColonnes par table :")
for (table_name,) in tables:
    print(f"\n[{table_name}]")
    columns = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
    for col in columns:
        col_id, name, col_type, notnull, default_value, pk = col
        print(
            f"  - {name} | type={col_type} | notnull={notnull} | pk={pk} | default={default_value}"
        )

conn.close()
