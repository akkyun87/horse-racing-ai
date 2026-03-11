"""
src/features/race_features.py

レース条件に基づく特徴量生成ロジック.
JBIS等から取得したレース基本情報を, 数値特徴量やカテゴリIDへ変換し,
機械学習モデルの入力形式に整形するモジュールである.


Usage:
    from src.features.race_features import generate_race_features

    race_features_df = generate_race_features(races_df)
"""

import logging
from typing import Dict

import pandas as pd

# ---------------------------------------------------------
# 定数定義 : カテゴリマッピング
# ---------------------------------------------------------

# 格付の序列マッピング (GI を最高位とした数値化)
_GRADE_MAP: Dict[str, int] = {
    "G1": 4,
    "GI": 4,
    "G2": 3,
    "GII": 3,
    "G3": 2,
    "GIII": 2,
    "L": 1,
    "OP": 1,
    "(L)": 1,
    "1勝クラス": 0,
    "2勝クラス": 0,
    "3勝クラス": 0,
    "未勝利": 0,
    "新馬": 0,
}

# 競馬場名の ID マッピング
_VENUE_MAP: Dict[str, int] = {
    "札幌": 1,
    "函館": 2,
    "福島": 3,
    "新潟": 4,
    "東京": 5,
    "中山": 6,
    "中京": 7,
    "京都": 8,
    "阪神": 9,
    "小倉": 10,
}

# 天候の ID マッピング
_WEATHER_MAP: Dict[str, int] = {
    "晴": 1,
    "曇": 2,
    "小雨": 3,
    "雨": 4,
    "小雪": 5,
    "雪": 6,
}

# 馬場状態の ID マッピング
_CONDITION_MAP: Dict[str, int] = {
    "良": 1,
    "稍重": 2,
    "重": 3,
    "不良": 4,
}

# 馬場種別の ID マッピング
_TRACK_TYPE_MAP: Dict[str, int] = {
    "芝": 1,
    "ダート": 2,
    "障害": 3,
}


def generate_race_features(races_df: pd.DataFrame) -> pd.DataFrame:
    """
    レース単位の条件情報を, 数値またはカテゴリ特徴量へ変換する.

    Args:
        races_df (pd.DataFrame): race.db / races テーブルから取得したレース情報.

    Returns:
        pd.DataFrame: race_key を主キーとするレース特徴量テーブル.

    Example:
        race_features_df = generate_race_features(races_df)
    """

    # ---------------------------------------------------------
    # 入力データのコピー作成
    # ---------------------------------------------------------
    df: pd.DataFrame = races_df.copy()

    # ---------------------------------------------------------
    # 馬場種別のカテゴリ ID 変換
    # ---------------------------------------------------------
    df["track_type_id"] = df["track_type"].map(_TRACK_TYPE_MAP).fillna(0)

    # ---------------------------------------------------------
    # 距離の数値変換
    # ---------------------------------------------------------
    df["distance"] = pd.to_numeric(
        df["distance"],
        errors="coerce",
    ).fillna(0)

    # ---------------------------------------------------------
    # 格付の序列変換
    # ---------------------------------------------------------
    df["grade_rank"] = df["grade"].map(_GRADE_MAP).fillna(0)

    # ---------------------------------------------------------
    # 天候・馬場状態・競馬場の前処理
    # ---------------------------------------------------------
    df["weather"] = df["weather"].str.strip()
    df["track_condition"] = df["track_condition"].str.strip()

    # ---------------------------------------------------------
    # 天候・馬場状態・競馬場のカテゴリ ID 変換
    # ---------------------------------------------------------
    df["weather_id"] = df["weather"].map(_WEATHER_MAP).fillna(0)
    df["condition_id"] = df["track_condition"].map(_CONDITION_MAP).fillna(0)
    df["venue_id"] = df["venue_name"].map(_VENUE_MAP).fillna(0)

    # ---------------------------------------------------------
    # 特徴量カラム抽出
    # ---------------------------------------------------------
    feature_columns: list[str] = [
        "race_key",
        "track_type_id",
        "distance",
        "grade_rank",
        "weather_id",
        "condition_id",
        "venue_id",
    ]

    return df[feature_columns]


# ---------------------------------------------------------
# テストコード部
# python -m src.features.race_features
# ---------------------------------------------------------
if __name__ == "__main__":
    import logging

    # ロガー設定
    logger = logging.getLogger("race_features_test")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(handler)

    logger.info("race_features 単体テスト開始")

    # 1. テストデータ作成
    sample_races = pd.DataFrame(
        [
            {
                "race_key": "2025-11-16_京都_11",
                "track_type": "芝",
                "distance": 2200,
                "grade": "GI",
                "weather": "晴 ",
                "track_condition": "良",
                "venue_name": "京都",
            },
            {
                "race_key": "2025-12-01_中山_11",
                "track_type": "ダート",
                "distance": 1800,
                "grade": "OP",
                "weather": "曇",
                "track_condition": "重",
                "venue_name": "中山",
            },
        ]
    )

    # 2. 特徴量生成
    features_df = generate_race_features(sample_races)

    # 3. 出力結果をすべて表示
    logger.info("生成された特徴量 DataFrame 全体")
    logger.info("\n" + features_df.to_string(index=False))

    # 4. 見やすい要約表示
    logger.info("生成結果サマリー")
    logger.info(f"レコード数: {len(features_df)}")
    logger.info(f"カラム一覧: {list(features_df.columns)}")
    logger.info("先頭 1 件:")
    logger.info("\n" + features_df.head(1).to_string(index=False))

    logger.info("race_features 単体テスト終了")
