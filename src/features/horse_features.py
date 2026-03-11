"""
src/features/horse_features.py

競走馬の個体属性・当日状態に基づく特徴量生成ロジック.
race.db の horse_entries テーブルを入力とし, 設計書に基づき数値・カテゴリ特徴量へ変換するモジュールである.

Usage:
    from src.features.horse_features import generate_horse_features

    horse_features_df = generate_horse_features(entries_df)
"""

from typing import List

import pandas as pd

# ---------------------------------------------------------
# 定数定義 : カテゴリマッピング
# ---------------------------------------------------------

# 性別のIDマッピング
_SEX_MAP = {
    "牡": 1,
    "牝": 2,
    "セ": 3,
    "騸": 3,
}

# ---------------------------------------------------------
# 補助関数
# ---------------------------------------------------------


def _classify_frame(frame_number: int) -> int:
    """
    枠番を内, 中, 外の3カテゴリへ分類する.

    Args:
        frame_number (int): 枠番.

    Returns:
        int: 枠カテゴリID.
    """
    try:
        frame = int(frame_number)

        if frame <= 2:
            return 1
        if frame <= 6:
            return 2
        return 3

    except (TypeError, ValueError):
        return 0


def _convert_numeric_columns(df: pd.DataFrame, columns: List[str]) -> None:
    """
    指定カラムを数値型へ一括変換する.

    Args:
        df (pd.DataFrame): 変換対象DataFrame.
        columns (List[str]): 数値変換対象カラム名リスト.

    Returns:
        None: DataFrame を直接更新する.
    """
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def _generate_text_id(series: pd.Series) -> pd.Series:
    """
    テキスト列を簡易的な整数IDへ変換する.

    Args:
        series (pd.Series): テキストSeries.

    Returns:
        pd.Series: ID化されたSeries.
    """
    return series.apply(lambda x: abs(hash(str(x))) % 10000 if pd.notnull(x) else 0)


# ---------------------------------------------------------
# メイン処理
# ---------------------------------------------------------


def generate_horse_features(entries_df: pd.DataFrame) -> pd.DataFrame:
    """
    競走馬の全属性情報を入力とし, 学習用特徴量テーブルを生成する.

    Args:
        entries_df (pd.DataFrame): race.db / horse_entries テーブルの全カラム.

    Returns:
        pd.DataFrame: 特徴量化された馬単位のDataFrame.
    """
    try:
        df = entries_df.copy()

        # ---------------------------------------------------------
        # 1. 数値型カラムの一括変換
        # ---------------------------------------------------------
        numeric_columns = [
            "age",
            "weight_carried",
            "body_weight",
            "diff_from_prev",
            "last_3f",
            "speed_index",
            "popularity",
            "time",
            "number",
        ]
        _convert_numeric_columns(df, numeric_columns)

        # ---------------------------------------------------------
        # 2. カテゴリID変換
        # ---------------------------------------------------------
        df["sex_id"] = df["sex"].map(_SEX_MAP).fillna(0)
        df["frame_category_id"] = df["frame"].apply(_classify_frame)

        # ---------------------------------------------------------
        # 3. テキスト情報のID化
        # ---------------------------------------------------------
        text_columns = [
            "jockey_name",
            "trainer_name",
            "owner",
            "breeder",
        ]
        for col in text_columns:
            new_col = col.replace("_name", "") + "_id"
            df[new_col] = _generate_text_id(df[col])

        # ---------------------------------------------------------
        # 4. 欠損値補完
        # ---------------------------------------------------------
        df["body_weight"] = df["body_weight"].fillna(470)
        df["diff_from_prev"] = df["diff_from_prev"].fillna(0)

        if "speed_index" in df.columns and not df["speed_index"].empty:
            df["speed_index"] = df["speed_index"].fillna(df["speed_index"].mean())
        else:
            df["speed_index"] = df["speed_index"].fillna(0)

        # ---------------------------------------------------------
        # 5. 出力カラム選択
        # ---------------------------------------------------------
        feature_columns = [
            "race_key",
            "horse_id",
            "number",
            "age",
            "weight_carried",
            "body_weight",
            "diff_from_prev",
            "sex_id",
            "frame_category_id",
            "jockey_id",
            "trainer_id",
            "owner_id",
            "breeder_id",
            "speed_index",
            "popularity",
            "finish_order",
        ]

        return df[feature_columns]

    except Exception as exc:
        raise RuntimeError(f"horse_features 生成処理失敗 : exception={exc}") from exc


# ---------------------------------------------------------
# テストコード部
# python -m src.features.horse_features
# ---------------------------------------------------------
if __name__ == "__main__":
    import logging

    logger = logging.getLogger("horse_features_test")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(handler)

    logger.info("horse_features 単体テスト開始")

    # テストデータ作成
    sample_entries = pd.DataFrame(
        [
            {
                "race_key": "2025-11-16_京都_11",
                "horse_id": 1337645,
                "finish_order": 1,
                "frame": 4,
                "number": 7,
                "sex": "牝",
                "age": 4,
                "jockey_name": "戸崎 圭太",
                "weight_carried": 56.0,
                "last_3f": 34.2,
                "speed_index": 111.7,
                "popularity": 1,
                "body_weight": 480,
                "diff_from_prev": 8,
                "trainer_name": "木村 哲也",
                "owner": "(有)サンデーレーシング",
                "breeder": "ノーザンファーム",
            }
        ]
    )

    # 特徴量生成
    features_df = generate_horse_features(sample_entries)

    # 1. 全結果出力
    logger.info("生成結果全体")
    logger.info("\n" + features_df.to_string(index=False))

    # 2. 見やすい要約
    logger.info("生成結果サマリー")
    logger.info(f"レコード数: {len(features_df)}")
    logger.info(f"カラム一覧: {list(features_df.columns)}")
    logger.info("\n" + features_df.head(1).to_string(index=False))

    logger.info("horse_features 単体テスト終了")
