"""
lineage_features.py (src/features/lineage_features.py)

血統情報から系統特徴量を生成するモジュール.
血量, 父母交差, 父系内交差, 母系内交差を算出する.

Usage:

    from src.features.lineage_features import generate_lineage_features

    features = generate_lineage_features(pedigree_df, lineage_master_df)
"""

import logging
import os
from typing import Dict, List

import pandas as pd

# =================================================================
# 定数定義
# =================================================================

LINEAGE_ORDER: List[str] = [
    "ND",
    "ND-NJ",
    "ND-VR",
    "ND-LY",
    "ND-DA",
    "ND-NR",
    "ND-SC",
    "ND-SW",
    "MP",
    "MP-FA",
    "MP-GW",
    "MP-FN",
    "MP-KM",
    "MP-KG-KK",
    "MP-SS",
    "MP-MA",
    "NA",
    "NA-GS",
    "NA-BR",
    "NA-NB",
    "NA-RG",
    "NA-PG",
    "RC",
    "RC-HA",
    "RC-SS",
    "RC-DI",
    "RC-RO",
    "RC-SG",
    "HE",
    "TS",
    "TS-HI",
    "TS-HY",
    "ST",
    "ST-OR",
    "ST-DA",
    "PH",
    "PH-TF",
    "BL",
    "SS",
    "SS-PR",
    "SS-RI",
    "MA",
]

PEDIGREE_POSITIONS: List[str] = [
    "父",
    "父父",
    "父父父",
    "父母父",
    "父父父父",
    "父父父母父",
    "父父母父父",
    "父父母母父",
    "父母父父父",
    "父母父母父",
    "父母母父父",
    "父母母母父",
    "母父",
    "母父父",
    "母母父",
    "母父父父",
    "母父母父",
    "母母父父",
    "母母母父",
]


# =================================================================
# ユーティリティ
# =================================================================


def calculate_generation_weight(position_name: str) -> float:
    """
    血統ポジションから世代重みを算出する.

    Args:
        position_name (str) : 血統ポジション名

    Returns:
        float : 世代重み

    Example:
        weight = calculate_generation_weight("父父")
    """
    generation: int = len(position_name)
    return 1.0 / (2**generation)


def initialize_feature_dict() -> Dict[str, float]:
    """
    系統順序に基づいた初期辞書を生成する.

    Returns:
        Dict[str, float] : 初期化済み辞書
    """
    return {lid: 0.0 for lid in LINEAGE_ORDER}


# =================================================================
# メインロジック
# =================================================================


def generate_lineage_features(
    pedigree_df: pd.DataFrame,
    lineage_master_df: pd.DataFrame,
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    血統データから系統特徴量を生成する.

    Args:
        pedigree_df (pd.DataFrame) : ID化済み血統データ
        lineage_master_df (pd.DataFrame) : 系統マスタ

    Returns:
        Dict[str, Dict[str, Dict[str, float]]] :
            horse_id をキーとした系統特徴量辞書

    Example:
        features = generate_lineage_features(pedigree_df, lineage_master_df)
    """
    logger = logging.getLogger(__name__)

    id_to_lineage: Dict[str, str] = dict(
        zip(
            lineage_master_df["horse_id"].astype(str),
            lineage_master_df["lineage_id"].astype(str),
        )
    )

    all_features: Dict[str, Dict[str, Dict[str, float]]] = {}

    for _, row in pedigree_df.iterrows():
        horse_id: str = str(row["horse_id"])
        logger.info("Start lineage feature generation, horse_id=%s", horse_id)

        blood_volume = initialize_feature_dict()
        cross_inter = {lid: 0 for lid in LINEAGE_ORDER}
        cross_sire = {lid: 0 for lid in LINEAGE_ORDER}
        cross_dam = {lid: 0 for lid in LINEAGE_ORDER}

        sire_lids: List[str] = []
        dam_lids: List[str] = []

        for pos in PEDIGREE_POSITIONS:
            column_name: str = f"pos_{pos}_id"
            if column_name not in pedigree_df.columns:
                continue

            target_id: str = str(row[column_name])
            if target_id == "0000000000":
                continue

            lineage_id: str = id_to_lineage.get(target_id, "UNKNOWN")
            if lineage_id not in LINEAGE_ORDER:
                continue

            weight: float = calculate_generation_weight(pos)
            blood_volume[lineage_id] += weight

            if pos.startswith("母"):
                dam_lids.append(lineage_id)
            else:
                sire_lids.append(lineage_id)

        for lid in LINEAGE_ORDER:
            sire_count: int = sire_lids.count(lid)
            dam_count: int = dam_lids.count(lid)

            if sire_count > 0 and dam_count > 0:
                cross_inter[lid] = 1
            if sire_count > 1:
                cross_sire[lid] = 1
            if dam_count > 1:
                cross_dam[lid] = 1

        all_features[horse_id] = {
            "blood_vol": blood_volume,
            "cross_inter": cross_inter,
            "cross_sire": cross_sire,
            "cross_dam": cross_dam,
        }

        logger.info("Finished lineage feature generation, horse_id=%s", horse_id)

    return all_features


# =================================================================
# テスト部
# =================================================================
if __name__ == "__main__":
    # 実行コマンド例:
    # python -m src.features.lineage_features

    from src.features.pedigree_features import generate_pedigree_features
    from src.utils.db_manager import load_from_db

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    PEDIGREE_DB = os.path.join("data", "raw", "pedigree", "pedigree.db")
    LINEAGE_DB = os.path.join("data", "raw", "pedigree", "sire_lineage.db")

    lineage_master = pd.DataFrame(
        load_from_db(LINEAGE_DB, "sire_lineage", logging.getLogger("DB"))
    )
    pedigree_info = pd.DataFrame(
        load_from_db(PEDIGREE_DB, "pedigree_info", logging.getLogger("DB"))
    )

    target_pedigree = pedigree_info.iloc[[0]]
    id_mapped = generate_pedigree_features(target_pedigree, lineage_master)
    results = generate_lineage_features(id_mapped, lineage_master)

    print("=" * 80)
    print("形式1: Raw 出力")
    print("=" * 80)
    print(results)

    print("\n" + "=" * 80)
    print("形式2: 整形出力")
    print("=" * 80)
    for horse_id, feature in results.items():
        print(f"対象馬 ID: {horse_id}")
        print("-" * 60)
        print(f"{'系統':<12} | {'血量':<8} | {'交差':<4} | {'父':<4} | {'母':<4}")
        print("-" * 60)

        for lid in LINEAGE_ORDER:
            bv = feature["blood_vol"][lid]
            if bv > 0:
                print(
                    f"{lid:<12} | {bv:<8.4f} | "
                    f"{feature['cross_inter'][lid]:<4} | "
                    f"{feature['cross_sire'][lid]:<4} | "
                    f"{feature['cross_dam'][lid]:<4}"
                )
