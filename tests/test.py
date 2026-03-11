import json
import sqlite3
from pathlib import Path

# =========================================================
# 1. URL JSON 確認
# =========================================================

# print("==== URL JSON 確認 ====")

# json_paths = [
#     Path("data/urls/2025/6/race_list_2025-06.json"),
#     Path("data/urls/2025/12/race_list_2025-12.json"),
# ]

# for path in json_paths:
#     print(f"\n[FILE] {path}")
#     if not path.exists():
#         print("  → 存在しません")
#         continue

#     with open(path, "r", encoding="utf-8") as f:
#         data = json.load(f)

#     print("  件数:", len(data))
#     for url in data:
#         print("   ", url)


# =========================================================
# 2. race.db 確認
# =========================================================

print("\n==== race.db 確認 ====")

race_db = Path("data/raw/race/race.db")

if race_db.exists():
    conn = sqlite3.connect(race_db)
    cur = conn.cursor()

    tables = ["races", "horse_entries"]

    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"{table} 件数:", count)

            print(f"{table} サンプル:")
            cur.execute(f"SELECT * FROM {table} LIMIT 3")
            for row in cur.fetchall():
                print("   ", row)

        except Exception as e:
            print(f"{table} 読込エラー:", e)

    conn.close()
else:
    print("race.db が存在しません")


# =========================================================
# 3. pedigree.db 確認
# =========================================================

print("\n==== pedigree.db 確認 ====")

pedigree_db = Path("data/raw/pedigree/pedigree.db")

if pedigree_db.exists():
    conn = sqlite3.connect(pedigree_db)
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM pedigree_info")
        count = cur.fetchone()[0]
        print("pedigree_info 件数:", count)

        print("pedigree_info サンプル:")
        cur.execute("SELECT * FROM pedigree_info LIMIT 3")
        for row in cur.fetchall():
            print("   ", row)

    except Exception as e:
        print("pedigree_info 読込エラー:", e)

    conn.close()
else:
    print("pedigree.db が存在しません")


# =========================================================
# 4. sire_lineage.db 確認
# =========================================================

print("\n==== sire_lineage.db 確認 ====")

sire_db = Path("data/raw/pedigree/sire_lineage.db")

if sire_db.exists():
    conn = sqlite3.connect(sire_db)
    cur = conn.cursor()

    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cur.fetchall()
        print("テーブル一覧:", tables)

        for table in tables:
            table_name = table[0]
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            print(f"{table_name} 件数:", count)

            cur.execute(f"SELECT * FROM {table_name} LIMIT 3")
            for row in cur.fetchall():
                print("   ", row)

    except Exception as e:
        print("sire_lineage.db 読込エラー:", e)

    conn.close()
else:
    print("sire_lineage.db が存在しません")


print("\n==== 確認終了 ====")

# python -m tests.test
