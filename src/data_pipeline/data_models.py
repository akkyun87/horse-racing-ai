# ファイルパス: src/data_pipeline/data_models.py

"""
src/data_pipeline/data_models.py

【概要】
競走馬予測モデルで使用する構造化データの定義モジュールです。
JBIS 等の外部ソースから取得した「Raw Data（生データ）」と、
ML モデルへ入力可能な形式に加工された「Model Input Data（特徴量）」を
明確に分離して定義します。

データは不変オブジェクト (frozen=True) として設計されており、
ML パイプライン中の意図しないデータ書き換えを構造レベルで防止します。
構造的整合性の検証は src/data_pipeline/data_validator.py に委譲します。

【外部依存】
- 特になし (標準ライブラリ dataclasses, typing のみ)

【Usage】
    from src.data_pipeline.data_models import (
        PedigreeInfo,
        RaceDetail,
        PedigreeFeature,
        RacePerformanceFeature,
    )

    pedigree = PedigreeInfo(
        horse_id="2021100001",
        name="テストホース",
        five_gen_ancestor_names=["祖先名"] * EXPECTED_ANCESTOR_COUNT,
        five_gen_ancestor_ids=[0] * EXPECTED_ANCESTOR_COUNT,
        five_gen_sire_names=["種牡馬名"] * EXPECTED_SIRE_COUNT,
        five_gen_sire_ids=[0] * EXPECTED_SIRE_COUNT,
    )
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final, List, Mapping, Optional, Sequence, Union

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# 5代血統表の全ノード数: 父母(2) + 祖父母(4) + 曾祖父母(8) + 高祖父母(16) + 5代祖(32) = 62
EXPECTED_ANCESTOR_COUNT: Final[int] = 62

# 種牡馬ラインのノード数: 父(1) + 祖父(2) + 曾祖父(4) + 高祖父(8) + 5代祖父(16) = 31
EXPECTED_SIRE_COUNT: Final[int] = 31


# ---------------------------------------------------------
# Raw Data: 血統情報
# ---------------------------------------------------------


@dataclass(slots=True, frozen=True)
class PedigreeInfo:
    """
    競走馬の5代血統表情報（生データ）。

    frozen=True により不変オブジェクトとして扱い、
    スクレイピング後のデータが下流処理で書き換えられることを防ぐ。
    構造的整合性の検証（要素数 62/31 チェック等）は
    src/data_pipeline/data_validator.py に委譲する。

    Args:
        horse_id (str): 馬固有ID (Primary Key, JBIS 採番, 10桁ゼロ埋め文字列)。
        name (str): 馬名。
        five_gen_ancestor_names (Sequence[str]): 5代全祖先の馬名リスト（計 62 箇所）。
        five_gen_ancestor_ids (Sequence[int]): 5代全祖先の馬IDリスト（計 62 箇所）。
        five_gen_sire_names (Sequence[str]): 5代種牡馬ラインの馬名リスト（計 31 箇所）。
        five_gen_sire_ids (Sequence[int]): 5代種牡馬ラインの馬IDリスト（計 31 箇所）。
        five_gen_sire_lineage_names (Optional[Sequence[str]]): 系統名リスト（31 箇所）。
                                                                 スクレイピング失敗時は None。
        five_gen_sire_lineage_ids (Optional[Sequence[int]]): 系統IDリスト（31 箇所）。
                                                               スクレイピング失敗時は None。

    Returns:
        PedigreeInfo: 不変の血統情報オブジェクト。

    Raises:
        なし（構造検証は data_validator.py に委譲）

    Example:
        pedigree = PedigreeInfo(
            horse_id="2021100001",
            name="テストホース",
            five_gen_ancestor_names=["祖先"] * EXPECTED_ANCESTOR_COUNT,
            five_gen_ancestor_ids=[0] * EXPECTED_ANCESTOR_COUNT,
            five_gen_sire_names=["種牡馬"] * EXPECTED_SIRE_COUNT,
            five_gen_sire_ids=[0] * EXPECTED_SIRE_COUNT,
        )
    """

    # ---------------------------------------------------------
    # 必須フィールド
    # ---------------------------------------------------------

    # 馬固有ID。race.db (INTEGER) と pedigree.db (TEXT 10桁) の型不一致を吸収するため
    # zfill(10) で統一した文字列として保持する
    horse_id: str

    # 馬名（空文字はバリデーター側でエラーとして検出する）
    name: str

    # 5代全祖先の名前・IDペア（要素数は EXPECTED_ANCESTOR_COUNT = 62 を期待）
    five_gen_ancestor_names: Sequence[str]
    five_gen_ancestor_ids: Sequence[int]

    # 5代種牡馬ラインの名前・IDペア（要素数は EXPECTED_SIRE_COUNT = 31 を期待）
    five_gen_sire_names: Sequence[str]
    five_gen_sire_ids: Sequence[int]

    # ---------------------------------------------------------
    # オプションフィールド (系統情報)
    # ---------------------------------------------------------

    # 系統名・系統IDは付帯情報であり、取得失敗時は None として許容する
    five_gen_sire_lineage_names: Optional[Sequence[str]] = None
    five_gen_sire_lineage_ids: Optional[Sequence[int]] = None


# ---------------------------------------------------------
# Raw Data: 開催情報
# ---------------------------------------------------------


@dataclass(slots=True, frozen=True)
class RaceVenue:
    """
    競馬場および開催回次に関する情報。

    Args:
        round (int): 開催回次（第○回）。
        place (str): 競馬場名（例: "東京", "阪神"）。
        day (int): 開催日次（第○日）。

    Returns:
        RaceVenue: 不変の開催情報オブジェクト。

    Raises:
        なし

    Example:
        venue = RaceVenue(round=1, place="東京", day=3)
    """

    round: int
    place: str
    day: int


# ---------------------------------------------------------
# Raw Data: レース基本情報
# ---------------------------------------------------------


@dataclass(slots=True, frozen=True)
class RaceInfo:
    """
    各レースの環境条件およびリザルト概要。

    Args:
        number (int): レース番号。
        name (str): レース名（例: "日本ダービー"）。
        grade (str): レースグレード（例: "GI", "GII", "オープン", "1勝クラス"）。
        surface (str): 馬場種別（例: "芝", "ダート"）。
        distance_m (int): 距離（メートル単位）。
        weather (str): 天候（例: "晴", "雨"）。
        track_condition (str): 馬場状態（例: "良", "重"）。
        race_time (float): レース総タイム（秒、ラップ合計値）。
        first_3f (float): 前半3ハロンタイム（秒、距離に応じて正規化済み）。
        last_3f (float): 後半3ハロンタイム（秒、末尾3ラップ合計）。
        lap_time (Sequence[float]): ラップタイムのシーケンス（秒単位）。
        corner_order (Mapping[str, Sequence[Union[str, Sequence[str]]]]): コーナー通過順。

    Returns:
        RaceInfo: 不変のレース情報オブジェクト。

    Raises:
        なし

    Example:
        race = RaceInfo(
            number=11,
            name="日本ダービー",
            grade="GI",
            surface="芝",
            distance_m=2400,
            weather="晴",
            track_condition="良",
            race_time=144.1,
            first_3f=37.2,
            last_3f=34.5,
            lap_time=[12.5, 11.8, 12.0],
            corner_order={"1": ["1", "2"]},
        )
    """

    number: int
    name: str
    grade: str
    surface: str
    distance_m: int
    weather: str
    track_condition: str

    # レース総タイム（秒）
    race_time: float

    # 前後半3F
    first_3f: float
    last_3f: float

    # ラップタイム
    lap_time: Sequence[float]

    # コーナー通過順: {"コーナー番号": [通過順位または集団リスト]}
    corner_order: Mapping[str, Sequence[Union[str, Sequence[str]]]]


# ---------------------------------------------------------
# Raw Data: 出走馬情報
# ---------------------------------------------------------


@dataclass(slots=True, frozen=True)
class HorseEntry:
    """
    出走馬ごとの詳細な競走成績データ。

    必須フィールドは着順のみとし、その他は Optional で欠損を許容する。
    スクレイピング失敗・出走取消等でデータが欠落する項目が多いため、
    各フィールドに安全なデフォルト値を設定している。

    Args:
        rank (int): 着順（取消・失格等の特殊ケースはモデル側で別途管理）。
        frame (Optional[int]): 枠番。
        number (Optional[int]): 馬番。
        horse_id (int): 馬固有ID。
        name (str): 馬名。
        url (str): JBIS 上の馬詳細ページ URL。
        sex (str): 性別（例: "牡", "牝", "騸"）。
        age (int): 馬齢（歳）。
        jockey (str): 騎手名。
        weight (float): 斤量（kg）。
        time (Optional[float]): 走破タイム（秒単位, 失格等は None）。
        margin (Optional[str]): 着差（例: "クビ", "ハナ"）。
        passing_order (Optional[Sequence[int]]): 各コーナー通過順位のシーケンス。
        last_3f (Optional[float]): 上がり3ハロンタイム（秒）。
        speed_index (Optional[float]): スピード指数。
        popularity (Optional[int]): 単勝人気順位。
        body_weight (Optional[int]): 馬体重（kg）。
        diff_from_prev (Optional[int]): 前走比馬体重差（kg）。
        trainer_name (Optional[str]): 調教師名。
        trainer_region (Optional[str]): 調教師所属地区（例: "栗東", "美浦"）。
        owner (Optional[str]): 馬主名。
        breeder (Optional[str]): 生産者名。

    Returns:
        HorseEntry: 不変の出走馬情報オブジェクト。

    Raises:
        なし

    Example:
        entry = HorseEntry(rank=1, horse_id=2021100001, name="テスト馬", sex="牡")
    """

    # ---------------------------------------------------------
    # 必須フィールド（着順は全ケースで存在する）
    # ---------------------------------------------------------

    rank: int

    # ---------------------------------------------------------
    # 識別情報フィールド
    # ---------------------------------------------------------

    frame: Optional[int] = None
    number: Optional[int] = None

    # 馬固有ID (スクレイピング失敗時は 0 として扱う)
    horse_id: int = 0
    name: str = ""
    url: str = ""

    # 性別は検証対象のため空文字をデフォルトとし、バリデーターで検出する
    sex: str = ""
    age: int = 0
    jockey: str = ""

    # 斤量は float（ハンデ戦等で小数点が付く場合がある）
    weight: float = 0.0

    # ---------------------------------------------------------
    # 競走成績フィールド（スクレイピング失敗・競走中止等で欠損あり）
    # ---------------------------------------------------------

    time: Optional[float] = None
    margin: Optional[str] = None

    # 各コーナーの通過順位（コーナー数はレース・コースにより変動）
    passing_order: Optional[Sequence[int]] = None

    last_3f: Optional[float] = None
    speed_index: Optional[float] = None
    popularity: Optional[int] = None

    # 馬体重・前走比（未出走馬・地方馬等で非公表の場合は None）
    body_weight: Optional[int] = None
    diff_from_prev: Optional[int] = None

    # ---------------------------------------------------------
    # 関係者情報フィールド
    # ---------------------------------------------------------

    trainer_name: Optional[str] = None
    trainer_region: Optional[str] = None
    owner: Optional[str] = None
    breeder: Optional[str] = None


# ---------------------------------------------------------
# Raw Data: 払戻情報
# ---------------------------------------------------------


@dataclass(slots=True, frozen=True)
class Payout:
    """
    払戻金データ（単勝・複勝・馬連等）。

    Args:
        type (str): 払戻種別（例: "単勝", "複勝", "馬連"）。
        target (str): 対象馬番または馬番組み合わせ（例: "3", "3-7"）。
        amount (int): 払戻金額（円単位）。

    Returns:
        Payout: 不変の払戻情報オブジェクト。

    Raises:
        なし

    Example:
        payout = Payout(type="単勝", target="3", amount=1250)
    """

    type: str
    target: str
    amount: int


# ---------------------------------------------------------
# Raw Data: レース全体構造
# ---------------------------------------------------------


@dataclass(slots=True, frozen=True)
class RaceDetail:
    """
    構造化済みのレース全体情報。

    RaceVenue・RaceInfo・HorseEntry・Payout を集約した
    1レース分の完結したデータコンテナ。

    Args:
        date (str): 開催日（YYYY-MM-DD 形式）。
        weekday (str): 曜日（例: "土", "日"）。
        venue (RaceVenue): 開催場所情報。
        race (RaceInfo): レース基本情報。
        horses (Sequence[HorseEntry]): 出走馬リスト。デフォルトは空リスト。
        payouts (Sequence[Payout]): 払戻金リスト。デフォルトは空リスト。

    Returns:
        RaceDetail: 不変のレース全体情報オブジェクト。

    Raises:
        なし

    Example:
        detail = RaceDetail(
            date="2024-05-26",
            weekday="日",
            venue=RaceVenue(round=3, place="東京", day=8),
            race=RaceInfo(...),
        )
    """

    date: str
    weekday: str
    venue: RaceVenue
    race: RaceInfo

    # frozen=True 環境での可変型デフォルト値は field(default_factory=...) で安全に初期化する
    horses: Sequence[HorseEntry] = field(default_factory=list)
    payouts: Sequence[Payout] = field(default_factory=list)


# ---------------------------------------------------------
# Model Input Data: 血統特徴量ベクトル
# ---------------------------------------------------------


@dataclass(slots=True, frozen=True)
class PedigreeFeature:
    """
    血統情報をベクトル化した ML モデル入力用データ。

    種牡馬 Embedding・系統ベクトル・クロス配合・インブリード係数・
    ニックス評価を統合した特徴量コンテナ。

    Args:
        horse_id (int): 馬固有ID（PedigreeInfo.horse_id と対応）。
        sire_vector (Sequence[float]): 種牡馬 Embedding ベクトル（31 × Embedding 次元）。
        lineage_vector (Sequence[float]): 系統 Embedding ベクトル（31 × Embedding 次元）。
        cross_vector (Sequence[int]): クロス配合の One-Hot ベクトル（42 次元）。
        inbreeding_vector (Sequence[float]): インブリード血量ベクトル（42 次元）。
        nick_vector (Sequence[float]): ニックス評価ベクトル（固定長）。

    Returns:
        PedigreeFeature: 不変の血統特徴量オブジェクト。

    Raises:
        なし

    Example:
        feature = PedigreeFeature(
            horse_id=2021100001,
            sire_vector=[0.1] * 128,
            lineage_vector=[0.2] * 128,
            cross_vector=[0] * 42,
            inbreeding_vector=[0.0] * 42,
            nick_vector=[0.5] * 16,
        )
    """

    horse_id: int

    # 種牡馬・系統の Embedding ベクトル（次元数はモデル設定に依存）
    sire_vector: Sequence[float]
    lineage_vector: Sequence[float]

    # クロス・インブリード（42 = 主要血統ノード数に対応）
    cross_vector: Sequence[int]
    inbreeding_vector: Sequence[float]

    # ニックス評価ベクトル（固定長、モデル設定に依存）
    nick_vector: Sequence[float]


# ---------------------------------------------------------
# Model Input Data: 競走成績特徴量ベクトル
# ---------------------------------------------------------


@dataclass(slots=True, frozen=True)
class RacePerformanceFeature:
    """
    競走成績をベクトル化した ML モデル入力用データ。

    連続値は z-score 正規化済み、カテゴリ変数は One-Hot / Embedding エンコーディング済み。

    Args:
        race_id (int): レース固有ID。
        horse_id (int): 馬固有ID（HorseEntry.horse_id と対応）。
        distance_vector (Sequence[int]): 距離カテゴリベクトル。
        surface_vector (Sequence[int]): 馬場種別ベクトル（芝/ダート等）。
        course_vector (Sequence[int]): コース形状ベクトル。
        draw_vector (Sequence[int]): 枠番ベクトル。
        ground_vector (Sequence[int]): 馬場状態ベクトル。
        running_style_vector (Sequence[int]): 脚質ベクトル。
        age_vector (Sequence[int]): 馬齢ベクトル。
        race_level_vector (Sequence[int]): レースグレードベクトル。
        season_vector (Sequence[int]): 季節ベクトル。
        time_continuous (float): 走破タイム（正規化済み連続値）。
        time_category_vector (Sequence[int]): 走破タイムカテゴリベクトル。
        body_weight_continuous (float): 馬体重（正規化済み連続値）。
        body_weight_category_vector (Sequence[int]): 馬体重カテゴリベクトル。
        final_time_continuous (float): 上がりタイム（正規化済み連続値）。
        final_time_category_vector (Sequence[int]): 上がりタイムカテゴリベクトル。
        sex_vector (Sequence[int]): 性別ベクトル。
        finish_position_continuous (float): 着順（正規化済み連続値）。

    Returns:
        RacePerformanceFeature: 不変の競走成績特徴量オブジェクト。

    Raises:
        なし

    Example:
        perf = RacePerformanceFeature(
            race_id=202405260511,
            horse_id=2021100001,
            distance_vector=[0, 1, 0],
            surface_vector=[1, 0],
            ...
        )
    """

    race_id: int
    horse_id: int

    # ---------------------------------------------------------
    # カテゴリ特徴量ベクトル群
    # ---------------------------------------------------------

    distance_vector: Sequence[int]
    surface_vector: Sequence[int]
    course_vector: Sequence[int]
    draw_vector: Sequence[int]
    ground_vector: Sequence[int]
    running_style_vector: Sequence[int]
    age_vector: Sequence[int]
    race_level_vector: Sequence[int]
    season_vector: Sequence[int]

    # ---------------------------------------------------------
    # 連続値・カテゴリペア特徴量
    # ---------------------------------------------------------

    # 走破タイム（連続値は z-score 正規化済み、カテゴリは分位数エンコード済み）
    time_continuous: float
    time_category_vector: Sequence[int]

    # 馬体重（連続値は z-score 正規化済み）
    body_weight_continuous: float
    body_weight_category_vector: Sequence[int]

    # 上がりタイム（連続値は z-score 正規化済み）
    final_time_continuous: float
    final_time_category_vector: Sequence[int]

    # ---------------------------------------------------------
    # その他特徴量
    # ---------------------------------------------------------

    sex_vector: Sequence[int]

    # 着順は回帰ターゲットとしても使用するため連続値として保持
    finish_position_continuous: float


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    全データモデルのインスタンス化・不変性・構造を確認する。

    frozen=True が正しく機能しているか、デフォルト値が安全に
    初期化されるかを正常系・異常系で検証する。
    外部依存（DB・ネットワーク）なしで単体実行が可能。
    テスト終了後はログファイルとロガーをすべて解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.data_pipeline.data_models
        _run_tests()
    """
    from src.utils.logger import close_logger_handlers, setup_logger

    TEST_LOG_DIR: Final[str] = "logs/_test_data_models_tmp"
    TEST_LOG_FILE: Final[str] = f"{TEST_LOG_DIR}/test.log"
    TEST_LOGGER_NAME: Final[str] = "test_data_models"

    print("=" * 60)
    print(" data_models.py 簡易単体テスト 開始")
    print("=" * 60)

    logger = setup_logger(
        log_filepath=TEST_LOG_FILE,
        log_level="DEBUG",
        logger_name=TEST_LOGGER_NAME,
    )

    try:
        # ---------------------------------------------------------
        # テスト 1: PedigreeInfo 正常系インスタンス化
        # ---------------------------------------------------------
        print("\n[TEST 1] 正常系: PedigreeInfo のインスタンス化")
        # 意図: horse_id は str 型フィールドのため zfill(10) 済み文字列を渡す
        pedigree = PedigreeInfo(
            horse_id="2021100001",
            name="テストホース",
            five_gen_ancestor_names=["祖先"] * EXPECTED_ANCESTOR_COUNT,
            five_gen_ancestor_ids=[0] * EXPECTED_ANCESTOR_COUNT,
            five_gen_sire_names=["種牡馬"] * EXPECTED_SIRE_COUNT,
            five_gen_sire_ids=[0] * EXPECTED_SIRE_COUNT,
        )
        assert (
            pedigree.horse_id == "2021100001"
        ), f"horse_id が一致しません: {pedigree.horse_id!r}"
        assert (
            pedigree.name == "テストホース"
        ), f"name が一致しません: {pedigree.name!r}"
        assert (
            pedigree.five_gen_sire_lineage_names is None
        ), "デフォルト None が設定されていません"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: PedigreeInfo frozen=True による書き換え防止
        # ---------------------------------------------------------
        print("\n[TEST 2] 正常系: PedigreeInfo frozen=True による書き換え防止")
        try:
            pedigree.horse_id = "9999999999"  # type: ignore[misc]
            assert False, "FrozenInstanceError が発生しませんでした"
        except Exception:
            # 意図: frozen=True によりフィールド書き換えは例外が発生することを確認する
            pass
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: RaceDetail デフォルト値の安全な初期化
        # ---------------------------------------------------------
        print("\n[TEST 3] 正常系: RaceDetail horses/payouts デフォルト値の独立性検証")
        venue = RaceVenue(round=1, place="東京", day=3)
        # 意図: RaceInfo の全必須フィールドを正しく渡す（final_time は削除済み）
        race = RaceInfo(
            number=11,
            name="日本ダービー",
            grade="GI",
            surface="芝",
            distance_m=2400,
            weather="晴",
            track_condition="良",
            race_time=144.1,
            first_3f=37.2,
            last_3f=34.5,
            lap_time=[12.5, 11.8, 12.0],
            corner_order={"1": ["1", "2"], "2": ["2", "1"]},
        )
        detail = RaceDetail(date="2024-05-26", weekday="日", venue=venue, race=race)

        assert isinstance(
            detail.horses, list
        ), f"horses がリスト型ではありません: {type(detail.horses)}"
        assert (
            len(detail.horses) == 0
        ), f"horses のデフォルト件数が一致しません: {len(detail.horses)}"
        assert isinstance(
            detail.payouts, list
        ), f"payouts がリスト型ではありません: {type(detail.payouts)}"
        assert (
            len(detail.payouts) == 0
        ), f"payouts のデフォルト件数が一致しません: {len(detail.payouts)}"

        # 意図: 複数インスタンス間でデフォルトリストが共有されていないことを確認する
        detail2 = RaceDetail(date="2024-06-02", weekday="日", venue=venue, race=race)
        assert (
            detail.horses is not detail2.horses
        ), "デフォルトリストが複数インスタンス間で共有されています"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 4: HorseEntry 最小引数インスタンス化
        # ---------------------------------------------------------
        print("\n[TEST 4] 正常系: HorseEntry の最小引数インスタンス化")
        # 意図: rank のみ必須、その他はデフォルト値で初期化されることを確認する
        entry = HorseEntry(rank=1)
        assert entry.rank == 1, f"rank が一致しません: {entry.rank}"
        assert (
            entry.horse_id == 0
        ), f"horse_id のデフォルト値が一致しません: {entry.horse_id}"
        assert entry.name == "", f"name のデフォルト値が一致しません: {entry.name!r}"
        assert (
            entry.time is None
        ), f"time のデフォルト値が None ではありません: {entry.time}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 5: Payout 正常系インスタンス化
        # ---------------------------------------------------------
        print("\n[TEST 5] 正常系: Payout のインスタンス化")
        payout = Payout(type="単勝", target="3", amount=1250)
        assert payout.type == "単勝", f"type が一致しません: {payout.type!r}"
        assert payout.amount == 1250, f"amount が一致しません: {payout.amount}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 6: 定数値の妥当性検証
        # ---------------------------------------------------------
        print(
            "\n[TEST 6] 正常系: EXPECTED_ANCESTOR_COUNT / EXPECTED_SIRE_COUNT の妥当性"
        )
        assert (
            EXPECTED_ANCESTOR_COUNT == 62
        ), f"EXPECTED_ANCESTOR_COUNT が一致しません: {EXPECTED_ANCESTOR_COUNT}"
        assert (
            EXPECTED_SIRE_COUNT == 31
        ), f"EXPECTED_SIRE_COUNT が一致しません: {EXPECTED_SIRE_COUNT}"
        print("  -> PASS")

    except AssertionError as e:
        print(f"\n[FAIL] アサーション失敗: {e}")
    except Exception as e:
        print(f"\n[FAIL] 予期しないエラー: {e}")
        import traceback

        traceback.print_exc()
    finally:
        close_logger_handlers(TEST_LOGGER_NAME)
        if Path(TEST_LOG_DIR).exists():
            shutil.rmtree(TEST_LOG_DIR)
            print(f"\nCLEANUP: {TEST_LOG_DIR} を削除しました。")

    print("\n" + "=" * 60)
    print(" 全テスト完了")
    print("=" * 60)


if __name__ == "__main__":
    # python -m src.data_pipeline.data_models
    _run_tests()
