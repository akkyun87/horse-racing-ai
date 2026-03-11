"""
src/features/pedigree_features.py

血統構成情報に基づく特徴量生成ロジック.
pedigree.db から取得した血統情報を, 血統位置ごとの種牡馬IDへ変換し,
機械学習モデル入力用の特徴量テーブルを生成するモジュールである.

Usage:
    from src.features.pedigree_features import generate_pedigree_features

    pedigree_features_df = generate_pedigree_features(pedigree_df, lineage_master_df)
"""

from typing import Dict, List

import pandas as pd

# ---------------------------------------------------------
# 定数定義 : 血統位置
# ---------------------------------------------------------

_PEDIGREE_POSITIONS: List[str] = [
    "父",
    "父父",
    "父父父",
    "父母父",
    "父父父父",
    "父父母父",
    "父母父父",
    "父母母父",
    "父父父父父",
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
    "母父父父父",
    "母父父母父",
    "母父母父父",
    "母父母母父",
    "母母父父父",
    "母母父母父",
    "母母母父父",
    "母母母母父",
]

_UNKNOWN_ID = "0000000000"


# ---------------------------------------------------------
# 補助関数
# ---------------------------------------------------------


def _build_name_to_id_map(lineage_master_df: pd.DataFrame) -> Dict[str, str]:
    """
    血統マスタから馬名とIDの対応辞書を生成する.

    Args:
        lineage_master_df (pd.DataFrame): 血統マスタ DataFrame.

    Returns:
        Dict[str, str]: 馬名から馬IDへのマッピング辞書.
    """
    return dict(
        zip(
            lineage_master_df["horse_name"].astype(str).str.strip(),
            lineage_master_df["horse_id"].astype(str).str.strip(),
        )
    )


def _normalize_sire_name(name: object) -> str:
    """
    血統馬名を正規化する.

    Args:
        name (object): 血統馬名.

    Returns:
        str: 正規化後の馬名.
    """
    value = str(name).strip()
    if value in {"", "None", "nan", "Unknown"}:
        return ""
    return value


def _resolve_sire_id(sire_name: str, name_to_id: Dict[str, str]) -> str:
    """
    血統馬名から馬IDを解決する.

    Args:
        sire_name (str): 血統馬名.
        name_to_id (Dict[str, str]): 馬名とIDの対応辞書.

    Returns:
        str: 解決された馬ID.
    """
    if not sire_name:
        return _UNKNOWN_ID
    return name_to_id.get(sire_name, _UNKNOWN_ID)


# ---------------------------------------------------------
# メイン処理
# ---------------------------------------------------------


def generate_pedigree_features(
    pedigree_df: pd.DataFrame,
    lineage_master_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    血統情報を血統位置ごとの種牡馬IDへ変換する.

    Args:
        pedigree_df (pd.DataFrame): pedigree.db 由来の血統情報 DataFrame.
        lineage_master_df (pd.DataFrame): 血統マスタ DataFrame.

    Returns:
        pd.DataFrame: 馬単位の血統特徴量 DataFrame.
    """
    try:
        name_to_id = _build_name_to_id_map(lineage_master_df)
        records: List[Dict[str, str]] = []

        for _, row in pedigree_df.iterrows():
            horse_record: Dict[str, str] = {"horse_id": str(row["horse_id"])}

            for pos in _PEDIGREE_POSITIONS:
                column_name = f"sire_name_{pos}"
                sire_name = _normalize_sire_name(row.get(column_name))
                sire_id = _resolve_sire_id(sire_name, name_to_id)
                horse_record[f"pos_{pos}_id"] = sire_id

            records.append(horse_record)

        return pd.DataFrame(records)

    except Exception as exc:
        raise RuntimeError(f"血統特徴量生成失敗 : exception={exc}") from exc


# ---------------------------------------------------------
# テストコード部
# python -m src.features.pedigree_features
# ---------------------------------------------------------
if __name__ == "__main__":
    import logging
    import os

    from src.utils.db_manager import load_from_db

    logger = logging.getLogger("pedigree_features_test")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(handler)

    logger.info("pedigree_features 単体テスト開始")

    lineage_db_path = os.path.join("data", "raw", "pedigree", "sire_lineage.db")
    pedigree_db_path = os.path.join("data", "raw", "pedigree", "pedigree.db")

    lineage_raw = load_from_db(lineage_db_path, "sire_lineage", logger)
    pedigree_raw = load_from_db(pedigree_db_path, "pedigree_info", logger)

    lineage_df = pd.DataFrame(lineage_raw)
    pedigree_df = pd.DataFrame(pedigree_raw)

    features_df = generate_pedigree_features(pedigree_df, lineage_df)

    # 1. 全結果出力
    logger.info("生成結果全体")
    logger.info("\n" + features_df.head(5).to_string(index=False))

    # 2. 見やすい要約
    logger.info("生成結果サマリー")
    logger.info(f"レコード数: {len(features_df)}")
    logger.info(f"カラム数: {len(features_df.columns)}")
    logger.info(f"カラム一覧: {list(features_df.columns)[:5]} ...")

    logger.info("pedigree_features 単体テスト終了")
