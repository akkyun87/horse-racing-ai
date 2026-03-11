import sqlite3
from collections import Counter


class InbreedingCalculator:
    def __init__(self, db_path="data/pedigree.db"):
        self.db_path = db_path

    def calculate_cross(self, horse_id: int):
        """
        血統表内の重複する祖先（インブリード）を算出する。
        世代数(n)に対する血量は (1/2)^(n+1) で計算。
        """
        conn = sqlite3.connect(self.db_path)
        # 62頭の祖先カラムを動的に取得（sire_name_ で始まるもの）
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(pedigree_info)")
        columns = [col[1] for col in cursor.fetchall() if "sire_name_" in col[1]]

        query = f"SELECT {', '.join([f'[{c}]' for c in columns])} FROM pedigree_info WHERE horse_id = ?"
        cursor.execute(query, (horse_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return {}

        # 祖先の出現回数と位置（世代）を記録
        ancestor_map = {}
        for col_name, name in zip(columns, row):
            if not name or name == "Unknown":
                continue

            # カラム名から世代を判定（父=1代, 父父=2代...）
            generation = self._get_generation_from_label(col_name)
            if name not in ancestor_map:
                ancestor_map[name] = []
            ancestor_map[name].append(generation)

        # 重複（クロス）している馬のみ抽出
        crosses = {name: gens for name, gens in ancestor_map.items() if len(gens) > 1}

        return self._format_cross_result(crosses)

    def _get_generation_from_label(self, label):
        # ラベルの文字数等から世代を判定するロジック（父=1, 父父=2...）
        clean_label = label.replace("sire_name_", "").replace(" ", "")
        return len(clean_label)

    def _format_cross_result(self, crosses):
        results = []
        for name, gens in crosses.items():
            gen_str = " x ".join(map(str, sorted(gens)))
            results.append(f"{name} {gen_str}")
        return results
