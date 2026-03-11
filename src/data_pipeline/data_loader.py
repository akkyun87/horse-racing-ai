# ファイルパス: src/data_pipeline/data_loader.py

"""
src/data_pipeline/data_loader.py

【概要】
JBIS (jbis.or.jp) からレース結果および血統情報をスクレイピングし、
データの検証・整形を経て SQLite データベースへ保存する
データパイプライン統括モジュールです。

処理フローは以下の順で実行されます:
  1. 設定ファイルロード (YAML)
  2. 月次分割によるレース URL 一覧収集
  3. レース詳細スクレイピング
  4. 未登録馬の血統スクレイピング
  5. データ検証 (data_validator)
  6. DB 重複排除フィルタリング
  7. SQLite 保存 (race.db / pedigree.db)

【外部依存】
- ネットワーク: JBIS (https://www.jbis.or.jp) への HTTP リクエスト
- DB: SQLite (race.db, pedigree.db)
- 内部モジュール:
    src.data_pipeline.data_models       (PedigreeInfo, RaceDetail)
    src.data_pipeline.data_validator    (validate_dataset)
    src.data_pipeline.pedigree_scraper  (scrape_pedigree_data, LABELS_SIRE_INDEX)
    src.data_pipeline.race_detail_scraper (scrape_race_details)
    src.data_pipeline.race_list_scraper (get_race_list_urls)
    src.utils.db_manager                (save_to_db)
    src.utils.file_manager              (load_data)

【Usage】
  from src.data_pipeline.data_loader import fetch_and_store_race_and_pedigree_data
  import logging

  logger = logging.getLogger("DataLoader")
  fetch_and_store_race_and_pedigree_data(logger=logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import calendar
import json
import logging
import re
import sqlite3
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple, Union

from src.data_pipeline.data_models import PedigreeInfo, RaceDetail
from src.data_pipeline.data_validator import validate_dataset
from src.data_pipeline.pedigree_scraper import LABELS_SIRE_INDEX, scrape_pedigree_data
from src.data_pipeline.race_detail_scraper import scrape_race_details
from src.data_pipeline.race_list_scraper import get_race_list_urls
from src.utils.db_manager import save_to_db
from src.utils.file_manager import load_data

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# JBIS の URL 構造から馬固有ID (数字列) を抽出するパターン
# 例: "/horse/2021100001/" → "2021100001"
HORSE_ID_PATTERN: re.Pattern[str] = re.compile(r"/horse/(\d+)/")

# JBIS のレース結果 URL から開催日 (YYYYMMDD) を抽出するパターン
# 例: "/race/result/20250601/105/11/" → "20250601"
RACE_DATE_PATTERN: re.Pattern[str] = re.compile(r"/race/result/(\d{8})/")

# レース名からグレード・クラス区分を抽出するパターン
# 対象: G1〜G3, JG1〜JG3 (障害), Listed, オープン/OP
GRADE_PATTERN: re.Pattern[str] = re.compile(r"(G[I]{1,3}|J\s?G[I]{1,3}|L|オープン|OP)")

# 一般条件クラスのキーワード（グレード未該当時の検索順）
_CLASS_KEYWORDS: List[str] = [
    "新馬",
    "未勝利",
    "1勝クラス",
    "2勝クラス",
    "3勝クラス",
]


# ---------------------------------------------------------
# 内部補助関数: 設定ロード
# ---------------------------------------------------------


def _load_config(
    config_path: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    YAML 設定ファイルを読み込み、パイプラインに必要な設定値を抽出する。

    Args:
        config_path (str): 設定ファイルのパス（YAML 形式）。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Dict[str, Any]: 期間・保存先・スクレイピング等の設定辞書。
                        ロード失敗時は空辞書 {} を返す。

    Raises:
        なし（エラーは戻り値で通知）

    Example:
        cfg = _load_config("config/data_loader_config.yaml", logger)
        if not cfg:
            return
    """
    # ---------------------------------------------------------
    # 入力バリデーションとファイルロード
    # ---------------------------------------------------------

    # pathlib.Path で OS 差異を吸収しつつファイル存在を事前確認する
    path_obj = Path(config_path)
    config: Dict[str, Any] = load_data(str(path_obj), logger)

    if not config:
        logger.error("設定ファイルロード失敗: %s", config_path)
        return {}

    # ---------------------------------------------------------
    # 必須項目の存在チェック
    # ---------------------------------------------------------

    # data セクションから収集期間を取得する
    data_cfg: Dict[str, Any] = config.get("data", {})
    output_cfg: Dict[str, Any] = config.get("output", {})

    start_date: Optional[str] = data_cfg.get("start_date")
    end_date: Optional[str] = data_cfg.get("end_date")

    # 収集期間が未定義の場合はパイプライン続行不可のためエラーとする
    if not start_date or not end_date:
        logger.error("設定ファイル項目不足: start_date または end_date が未定義です。")
        return {}

    # ---------------------------------------------------------
    # 設定値の構成と返却
    # ---------------------------------------------------------

    return {
        "start_date": start_date,
        "end_date": end_date,
        # 保存先ディレクトリはデフォルト値を設けて省略可能にする
        "race_data_dir": output_cfg.get("race_data_dir", "data/raw/race"),
        "pedigree_data_dir": output_cfg.get("pedigree_data_dir", "data/raw/pedigree"),
        "scraping": config.get("scraping", {}),
        "logging": config.get("logging", {}),
    }


# ---------------------------------------------------------
# 内部補助関数: キー生成
# ---------------------------------------------------------


def _build_race_key(rd: RaceDetail) -> str:
    """
    RaceDetail から DB 主キー候補となる一意な識別文字列を生成する。

    開催日・競馬場・レース番号の組み合わせで一意性を担保する。

    Args:
        rd (RaceDetail): 対象レース情報。

    Returns:
        str: "{date}_{place}_{race_number}" 形式の識別キー。

    Raises:
        なし

    Example:
        key = _build_race_key(race_detail)
        # "2024-05-26_東京_11"
    """
    # レース識別キーを生成し、races / horse_entries テーブルの両方で共用する
    return f"{rd.date}_{rd.venue.place}_{rd.race.number}"


# ---------------------------------------------------------
# 内部補助関数: 日付・URL パース
# ---------------------------------------------------------


def _split_date_range(
    start_date: str,
    end_date: str,
) -> List[Tuple[str, str]]:
    """
    指定された期間を月次単位のタプルリストに分割する。

    スクレイピングを月単位で実行することでメモリ消費を抑制し、
    途中再開も容易にするために月次分割を採用している。

    Args:
        start_date (str): 収集開始日 (YYYY-MM-DD 形式)。
        end_date (str): 収集終了日 (YYYY-MM-DD 形式)。

    Returns:
        List[Tuple[str, str]]: 月次 (開始日, 終了日) タプルのリスト。

    Raises:
        ValueError: 日付フォーマットが不正な場合。

    Example:
        ranges = _split_date_range("2024-01-01", "2024-03-15")
        # [("2024-01-01","2024-01-31"), ("2024-02-01","2024-02-29"), ("2024-03-01","2024-03-15")]
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges: List[Tuple[str, str]] = []
    curr = start

    while curr <= end:
        # calendar.monthrange で月末日を取得し、end との小さい方を採用する
        _, last_day = calendar.monthrange(curr.year, curr.month)
        month_end = datetime(curr.year, curr.month, last_day)
        actual_end = min(month_end, end)
        ranges.append((curr.strftime("%Y-%m-%d"), actual_end.strftime("%Y-%m-%d")))

        # 翌月1日へ移動（12月の場合は翌年1月へ）
        if curr.month == 12:
            curr = datetime(curr.year + 1, 1, 1)
        else:
            curr = datetime(curr.year, curr.month + 1, 1)

    return ranges


def _extract_year_month_from_url(url: str) -> Tuple[str, str]:
    """
    JBIS のレース結果 URL から年 (YYYY) と月 (M) を抽出する。

    Args:
        url (str): JBIS レース結果 URL。

    Returns:
        Tuple[str, str]: (年文字列, 月文字列（ゼロ埋めなし）)。

    Raises:
        ValueError: URL に日付パターンが含まれない場合。

    Example:
        y, m = _extract_year_month_from_url(
            "https://www.jbis.or.jp/race/result/20250801/13/11/"
        )
        # ("2025", "8")
    """
    match = RACE_DATE_PATTERN.search(url)
    if not match:
        raise ValueError(f"URL から日付を抽出できません: {url}")

    # YYYYMMDD 形式から年・月を切り出す（月はゼロ埋めを除去して返す）
    val = match.group(1)
    return val[:4], str(int(val[4:6]))


def _extract_year_month_from_date(date_str: str) -> Tuple[str, str]:
    """
    YYYY-MM-DD 形式の日付文字列から年 (YYYY) と月 (M) を抽出する。

    Args:
        date_str (str): YYYY-MM-DD 形式の日付文字列。

    Returns:
        Tuple[str, str]: (年文字列, 月文字列（ゼロ埋めなし）)。

    Raises:
        ValueError: 日付フォーマットが不正な場合。

    Example:
        y, m = _extract_year_month_from_date("2024-05-26")
        # ("2024", "5")
    """
    try:
        parts = date_str.split("-")
        # 月のゼロ埋めを除去して返す（ディレクトリ名等への利用を想定）
        return parts[0], str(int(parts[1]))
    except (IndexError, ValueError) as e:
        raise ValueError(f"日付形式不正: {date_str}") from e


# ---------------------------------------------------------
# 内部補助関数: レコード抽出
# ---------------------------------------------------------


def _extract_race_records(
    race_details: List[RaceDetail],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    RaceDetail リストから races テーブル保存用のレコードを生成する。

    グレード・クラス判定は重賞 (GRADE_PATTERN) を優先し、
    未該当の場合は一般条件キーワードで順次検索する。

    Args:
        race_details (List[RaceDetail]): スクレイピング済みのレース詳細リスト。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[Dict[str, Any]]: races テーブルへの挿入レコードリスト。

    Raises:
        なし

    Example:
        records = _extract_race_records(race_details, logger)
    """
    records: List[Dict[str, Any]] = []

    for rd in race_details:

        # ---------------------------------------------------------
        # グレード・クラス判定
        # ---------------------------------------------------------

        race_name: str = rd.race.name
        grade: Optional[str] = None

        # 重賞グレード (G1〜G3, JG1〜JG3, Listed) を優先的に検索する
        grade_match = GRADE_PATTERN.search(race_name)
        if grade_match:
            grade = grade_match.group(0).strip()
        else:
            # 重賞未該当の場合は一般条件クラスキーワードで順次検索する
            for kw in _CLASS_KEYWORDS:
                if kw in race_name:
                    grade = kw
                    break

        # ---------------------------------------------------------
        # レコード組み立て
        # ---------------------------------------------------------

        # _build_race_key で一意キーを生成し、horse_entries との結合キーとする
        records.append(
            {
                "race_key": _build_race_key(rd),
                "date": rd.date,
                "venue_name": rd.venue.place,
                "race_number": rd.race.number,
                "race_name": race_name,
                "track_type": rd.race.surface,
                "distance": rd.race.distance_m,
                "grade": grade,
                "weather": rd.race.weather,
                "track_condition": rd.race.track_condition,
            }
        )

    logger.info("races レコード生成完了: %d 件", len(records))
    return records


def _extract_horse_entry_records(
    race_details: List[RaceDetail],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    RaceDetail リストから horse_entries テーブル保存用のレコードを生成する。

    `passing_order` はリスト型のためカンマ区切り文字列へ変換する。
    馬 ID は HorseEntry.url から正規表現で抽出する。

    Args:
        race_details (List[RaceDetail]): スクレイピング済みのレース詳細リスト。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[Dict[str, Any]]: horse_entries テーブルへの挿入レコードリスト。

    Raises:
        なし

    Example:
        records = _extract_horse_entry_records(race_details, logger)
    """
    records: List[Dict[str, Any]] = []

    for rd in race_details:
        # race_key は _build_race_key で一元生成し、races テーブルと整合させる
        race_key = _build_race_key(rd)

        for horse in rd.horses:

            # URL から馬 ID を抽出する（抽出失敗時は None として後続処理に委ねる）
            id_match = HORSE_ID_PATTERN.search(horse.url)
            horse_id: Optional[int] = int(id_match.group(1)) if id_match else None

            # passing_order はリスト型のため DB 保存可能なカンマ区切り文字列へ変換する
            passing_order_str: Optional[str] = (
                ",".join(map(str, horse.passing_order))
                if isinstance(horse.passing_order, (list, tuple))
                else None
            )

            records.append(
                {
                    "race_key": race_key,
                    "horse_id": horse_id,
                    "finish_order": horse.rank,
                    "frame": horse.frame,
                    "number": horse.number,
                    "name": horse.name,
                    "sex": horse.sex,
                    "age": horse.age,
                    "jockey_name": horse.jockey,
                    "weight_carried": horse.weight,
                    "time": horse.time,
                    "margin": horse.margin,
                    "passing_order": passing_order_str,
                    "last_3f": horse.last_3f,
                    "speed_index": horse.speed_index,
                    "popularity": horse.popularity,
                    "body_weight": horse.body_weight,
                    "diff_from_prev": horse.diff_from_prev,
                    "trainer_name": horse.trainer_name,
                    "trainer_region": horse.trainer_region,
                    "owner": horse.owner,
                    "breeder": horse.breeder,
                }
            )

    logger.info("horse_entries レコード生成完了: %d 件", len(records))
    return records


def _flatten_pedigree_info(
    pedigree_infos: List[PedigreeInfo],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    PedigreeInfo の階層構造を DB 保存用のフラットな辞書へ変換する。

    種牡馬名・系統名は LABELS_SIRE_INDEX のキー順に展開し、
    インデックス超過時はデフォルト値 ("未登録"/"不明") を代入する。

    Args:
        pedigree_infos (List[PedigreeInfo]): 血統情報リスト。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[Dict[str, Any]]: pedigree_info テーブルへの挿入レコードリスト。

    Raises:
        なし

    Example:
        records = _flatten_pedigree_info(pedigree_infos, logger)
    """
    flat_list: List[Dict[str, Any]] = []

    # LABELS_SIRE_INDEX のキー順序を固定してカラム名の一貫性を保証する
    labels = list(LABELS_SIRE_INDEX.keys())

    for info in pedigree_infos:

        record: Dict[str, Any] = {
            "horse_id": info.horse_id,
            "name": info.name,
            # 全祖先名はカンマ区切りで単一カラムに格納する
            "five_gen_ancestors": ",".join(info.five_gen_ancestor_names),
        }

        # ---------------------------------------------------------
        # ラベルに基づいた種牡馬名・系統名の展開
        # ---------------------------------------------------------

        for i, label in enumerate(labels):

            s_names = info.five_gen_sire_names
            l_names = info.five_gen_sire_lineage_names

            # インデックス超過時は未取得として明示的にデフォルト値を設定する
            record[f"sire_name_{label}"] = s_names[i] if i < len(s_names) else "未登録"
            record[f"lineage_name_{label}"] = (
                l_names[i] if (l_names is not None and i < len(l_names)) else "不明"
            )

        flat_list.append(record)

    logger.info("pedigree_info レコード生成完了: %d 件", len(flat_list))
    return flat_list


# ---------------------------------------------------------
# 内部補助関数: DB 整合性チェック
# ---------------------------------------------------------


def _extract_unique_horses(
    race_details: List[RaceDetail],
    pedigree_db_path: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    RaceDetail リストから未登録のユニークな馬情報を抽出する。

    既存の pedigree_info テーブルと照合し、未収集の馬のみを返す。
    これにより重複スクレイピングを防止しコスト削減を図る。

    Args:
        race_details (List[RaceDetail]): スクレイピング済みレース詳細リスト。
        pedigree_db_path (str): 血統 DB ファイルのパス。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[Dict[str, Any]]: 新規収集対象馬の {"id": int, "url": str} リスト。

    Raises:
        なし（DB 接続エラーは WARNING ログで通知し処理継続）

    Example:
        targets = _extract_unique_horses(race_details, "data/pedigree.db", logger)
    """
    # ---------------------------------------------------------
    # 既存馬 ID の取得 (DB 整合性確認)
    # ---------------------------------------------------------

    existing_ids: Set[int] = set()
    db_file = Path(pedigree_db_path)

    if db_file.exists():
        try:
            with sqlite3.connect(db_file) as conn:
                cursor = conn.cursor()

                # テーブル存在を確認してからクエリを実行する（テーブル未作成時のエラー防止）
                cursor.execute(
                    "SELECT name FROM sqlite_master"
                    " WHERE type='table' AND name='pedigree_info'"
                )
                if cursor.fetchone():
                    cursor.execute("SELECT horse_id FROM pedigree_info")
                    existing_ids = {int(row[0]) for row in cursor.fetchall()}
                    logger.info("既存 DB から馬 ID 取得成功: %d 件", len(existing_ids))

        except (sqlite3.Error, ValueError) as e:
            # DB 接続失敗時は全馬を新規収集対象として続行する
            logger.warning("既存 DB の馬情報取得失敗 (新規収集として続行): %s", e)

    # ---------------------------------------------------------
    # 重複除去と新規対象馬の抽出
    # ---------------------------------------------------------

    # 同一馬が複数レースに出走している場合の重複を除去するため辞書を使用する
    unique_map: Dict[int, Dict[str, Any]] = {}

    for detail in race_details:
        for horse in detail.horses:

            # url が空の馬はスクレイピング不完全として除外する
            if not horse.url:
                continue

            # URL から馬 ID を抽出し、失敗時は horse_id フィールドにフォールバックする
            id_match = HORSE_ID_PATTERN.search(horse.url)
            horse_id: int = (
                int(id_match.group(1))
                if id_match
                else (horse.horse_id or hash(horse.url) % 10_000_000)
            )

            # 既存 DB 未登録かつ今回バッチ内での重複でない場合のみ追加する
            if horse_id not in existing_ids and horse_id not in unique_map:
                unique_map[horse_id] = {"id": horse_id, "url": horse.url}

    result = list(unique_map.values())
    logger.info("新規収集対象馬抽出完了: %d 件", len(result))
    return result


def _filter_unique_entries(
    entries: List[Dict[str, Any]],
    db_file_path: str,
    table_name: str,
    key_columns: Union[str, List[str]],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    DB の既存キーと照合し、未登録レコードのみにフィルタリングする。

    重複保存によるデータ汚染を防止するための事前チェック関数。
    DB ファイル未存在またはテーブル未作成の場合は全件を新規扱いとする。

    Args:
        entries (List[Dict[str, Any]]): フィルタリング対象のレコードリスト。
        db_file_path (str): SQLite ファイルのパス。
        table_name (str): 対象テーブル名。
        key_columns (Union[str, List[str]]): 重複判定キー（単一または複合）。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[Dict[str, Any]]: 未登録レコードのみのリスト。

    Raises:
        なし（DB エラーは WARNING ログで通知し全件返却）

    Example:
        new_races = _filter_unique_entries(
            race_recs, "data/race.db", "races", "race_key", logger
        )
    """
    if not entries:
        return []

    # キーカラムをリスト形式に統一して複合キー対応を共通化する
    keys: List[str] = [key_columns] if isinstance(key_columns, str) else key_columns
    db_path = Path(db_file_path)

    # DB ファイル未存在時は全件を新規として返す
    if not db_path.exists():
        logger.info(
            "%s: DB ファイル未存在のため全 %d 件を新規保存対象とします。",
            table_name,
            len(entries),
        )
        return entries.copy()

    # ---------------------------------------------------------
    # 既存キーセットの構築
    # ---------------------------------------------------------

    existing_set: Set[Any] = set()

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # テーブル存在確認（初回起動時のテーブル未作成に備える）
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            if cursor.fetchone():
                select_clause = ", ".join(keys)
                cursor.execute(f"SELECT {select_clause} FROM {table_name}")

                # 複合キーはタプル、単一キーは文字列として格納する
                if len(keys) > 1:
                    existing_set = {
                        tuple(str(v) for v in row) for row in cursor.fetchall()
                    }
                else:
                    existing_set = {str(row[0]) for row in cursor.fetchall()}

    except sqlite3.Error as e:
        # DB 照合失敗時は全件保存を試みることで保守性を確保する
        logger.warning("重複判定データ取得失敗 [%s]: %s", table_name, e)
        return entries.copy()

    # ---------------------------------------------------------
    # 未登録レコードの抽出
    # ---------------------------------------------------------

    unique_list: List[Dict[str, Any]] = []

    for entry in entries:
        # 複合キー・単一キーいずれかのフォーマットで比較キーを生成する
        record_key: Any = (
            tuple(str(entry.get(k)) for k in keys)
            if len(keys) > 1
            else str(entry.get(keys[0]))
        )
        if record_key not in existing_set:
            unique_list.append(entry)

    logger.info(
        "%s: %d 件中 %d 件が新規データ",
        table_name,
        len(entries),
        len(unique_list),
    )
    return unique_list


# ---------------------------------------------------------
# ファイル出力処理
# ---------------------------------------------------------


def _save_race_urls(
    all_race_urls: List[str],
    start_date: str,
    logger: logging.Logger,
    base_dir: Optional[Path] = None,
) -> None:
    """
    レース URL を年月ごとにグループ化して JSON ファイルへ保存する。

    年月ディレクトリを自動生成し、後続の月次バッチ処理で
    再利用できるよう整理して保存する。

    Args:
        all_race_urls (List[str]): 保存対象の URL リスト。
        start_date (str): フォールバック用の基準日 (YYYY-MM-DD 形式)。
        logger (logging.Logger): ログ出力用ロガー。
        base_dir (Optional[Path]): 保存先ベースディレクトリ。
                                    省略時は "data/urls" を使用する。

    Returns:
        None

    Raises:
        OSError: ディレクトリ作成またはファイル書き込みに失敗した場合。

    Example:
        _save_race_urls(url_list, "2024-01-01", logger)
    """
    if not all_race_urls:
        logger.warning("保存対象 URL がありません。スキップします。")
        return

    save_base = base_dir or Path("data") / "urls"

    # URL パース失敗時のフォールバック用年月を用意する
    try:
        fb_year, fb_month = _extract_year_month_from_date(start_date)
    except ValueError:
        fb_year, fb_month = "0000", "0"

    # ---------------------------------------------------------
    # 年月キーによるグルーピング
    # ---------------------------------------------------------

    # defaultdict でキー未存在時の初期化を自動化する
    groups: DefaultDict[str, List[str]] = defaultdict(list)

    for url in all_race_urls:
        try:
            year, month = _extract_year_month_from_url(url)
        except ValueError:
            # URL パース失敗時はフォールバック年月へ振り分ける
            year, month = fb_year, fb_month
        groups[f"{year}/{month}"].append(url)

    # ---------------------------------------------------------
    # ディレクトリ作成とファイル書き込み
    # ---------------------------------------------------------

    for ym_key, urls in sorted(groups.items()):
        year, month = ym_key.split("/")
        out_dir = save_base / year / month

        try:
            # parents=True で中間ディレクトリも一括作成する
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"race_list_{year}-{month.zfill(2)}.json"

            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(urls, f, ensure_ascii=False, indent=2)

            logger.info("JSON 保存完了: %s (%d 件)", out_file, len(urls))

        except OSError as e:
            logger.error("ファイル出力失敗: %s / %s", ym_key, e)
            raise


# ---------------------------------------------------------
# メインパイプライン
# ---------------------------------------------------------


def fetch_and_store_race_and_pedigree_data(
    logger: logging.Logger,
    tests: bool = False,
) -> None:
    """
    JBIS データ収集・検証・DB 保存のメインフローを制御する。

    処理ステップ:
      1. 設定ロード
      2. レース URL 一覧収集（月次分割）
      3. URL の JSON 保存
      4. レース詳細スクレイピング
      5. 未登録馬の血統スクレイピング
      6. データ検証
      7. 重複排除フィルタリング
      8. SQLite 保存

    Args:
        logger (logging.Logger): ログ出力用ロガー。
        tests (bool): True の場合は静的テスト URL でスクレイピングをスキップする。
                      デフォルトは False。

    Returns:
        None

    Raises:
        なし（各ステップのエラーはログで通知し安全に終了）

    Example:
        logger = logging.getLogger("DataLoader")
        fetch_and_store_race_and_pedigree_data(logger=logger)
    """
    logger.info("=== データ収集パイプライン開始 ===")

    # ---------------------------------------------------------
    # 1. 設定ロードと DB パスの初期化
    # ---------------------------------------------------------

    config_path = str(Path("config") / "data_loader_config.yaml")
    cfg = _load_config(config_path, logger)

    if not cfg:
        logger.error("設定ロード失敗のためパイプラインを中断します。")
        return

    # DB ファイルのパスを pathlib.Path で組み立てて OS 差異を吸収する
    race_db = str(Path(cfg["race_data_dir"]) / "race.db")
    ped_db = str(Path(cfg["pedigree_data_dir"]) / "pedigree.db")

    # ---------------------------------------------------------
    # 2. レース URL 一覧収集
    # ---------------------------------------------------------

    if not tests:
        # 本番モード: 設定期間を月次分割して JBIS からレース URL を収集する
        date_ranges = _split_date_range(cfg["start_date"], cfg["end_date"])
        all_urls: List[str] = []

        for s, e in date_ranges:
            logger.info("期間スキャン中: %s ～ %s", s, e)
            urls = get_race_list_urls(s, e, logger)
            if urls:
                all_urls.extend(urls)
    else:
        # テストモード: 静的 URL でネットワーク依存なしに動作確認する
        all_urls = [
            "https://www.jbis.or.jp/race/result/20250601/105/11/",
            "https://www.jbis.or.jp/race/result/20251228/106/11/",
        ]

    if not all_urls:
        logger.warning(
            "処理対象のレース URL が見つかりませんでした。パイプラインを終了します。"
        )
        return

    # ---------------------------------------------------------
    # 3. URL の JSON 保存
    # ---------------------------------------------------------

    _save_race_urls(all_urls, cfg["start_date"], logger)

    # ---------------------------------------------------------
    # 4. レース詳細スクレイピング
    # ---------------------------------------------------------

    race_details = scrape_race_details(all_urls, logger)

    if not race_details:
        logger.warning(
            "レース詳細データの取得に失敗しました。パイプラインを終了します。"
        )
        return

    # ---------------------------------------------------------
    # 5. 未登録馬の血統スクレイピング
    # ---------------------------------------------------------

    # 既存 DB と照合して未収集馬のみを抽出し、無駄なリクエストを排除する
    target_horses = _extract_unique_horses(race_details, ped_db, logger)
    ped_infos: List[PedigreeInfo] = (
        scrape_pedigree_data(target_horses, logger) if target_horses else []
    )

    # ---------------------------------------------------------
    # 6. データ検証 (バリデーション)
    # ---------------------------------------------------------

    validate_dataset(race_details + ped_infos, logger)

    # ---------------------------------------------------------
    # 7 & 8. 重複排除フィルタリングと DB 保存
    # ---------------------------------------------------------

    # レース基本情報の保存
    race_recs = _extract_race_records(race_details, logger)
    new_races = _filter_unique_entries(race_recs, race_db, "races", "race_key", logger)
    if new_races:
        save_to_db(new_races, race_db, "races", logger)

    # 出走馬結果の保存
    entry_recs = _extract_horse_entry_records(race_details, logger)
    new_entries = _filter_unique_entries(
        entry_recs, race_db, "horse_entries", ["race_key", "horse_id"], logger
    )
    if new_entries:
        save_to_db(new_entries, race_db, "horse_entries", logger)

    # 血統情報の保存（新規収集分のみ）
    if ped_infos:
        ped_records = _flatten_pedigree_info(ped_infos, logger)
        save_to_db(ped_records, ped_db, "pedigree_info", logger)

    logger.info("=== データ収集パイプライン正常終了 ===")


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    主要関数の正常系・異常系動作を確認する。

    外部依存（DB・スクレイパー）はダミーデータで代替し、
    テストで生成したファイルは実行後に削除する。
    print は本ブロック内のみ許可。
    """
    import sys
    import tempfile

    print("=" * 60)
    print(" data_loader.py 簡易単体テスト 開始")
    print("=" * 60)

    # テスト用ロガーを標準出力へ接続する
    test_logger = logging.getLogger("test_data_loader")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)

    # ---------------------------------------------------------
    # テスト 1: _split_date_range 正常系
    # ---------------------------------------------------------
    print("\n[TEST 1] _split_date_range 正常系")
    ranges = _split_date_range("2024-01-01", "2024-03-15")
    print(f"  分割結果: {ranges}")
    assert len(ranges) == 3, f"[FAIL] 期待 3件 / 実際 {len(ranges)}件"
    assert ranges[0] == ("2024-01-01", "2024-01-31")
    assert ranges[2] == ("2024-03-01", "2024-03-15")
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 2: _split_date_range 単月範囲
    # ---------------------------------------------------------
    print("\n[TEST 2] _split_date_range 単月範囲")
    ranges_single = _split_date_range("2024-05-10", "2024-05-20")
    assert len(ranges_single) == 1, f"[FAIL] 期待 1件 / 実際 {len(ranges_single)}件"
    print(f"  単月: {ranges_single}")
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 3: _extract_year_month_from_url 正常系
    # ---------------------------------------------------------
    print("\n[TEST 3] _extract_year_month_from_url 正常系")
    y, m = _extract_year_month_from_url(
        "https://www.jbis.or.jp/race/result/20250801/13/11/"
    )
    print(f"  year={y}, month={m}")
    assert y == "2025" and m == "8", f"[FAIL] year={y}, month={m}"
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 4: _extract_year_month_from_url 異常系（パターン不一致）
    # ---------------------------------------------------------
    print("\n[TEST 4] _extract_year_month_from_url 異常系")
    try:
        _extract_year_month_from_url("https://www.jbis.or.jp/horse/2021100001/")
        print("  [FAIL] 例外が発生しませんでした。")
    except ValueError as e:
        print(f"  ValueError を正常に捕捉: {e}")
        print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 5: _filter_unique_entries DB ファイル未存在時の全件返却
    # ---------------------------------------------------------
    print("\n[TEST 5] _filter_unique_entries DB 未存在時")
    dummy_entries = [{"race_key": "2024-01-01_東京_11"}]
    result = _filter_unique_entries(
        dummy_entries,
        "/nonexistent/path/race.db",
        "races",
        "race_key",
        test_logger,
    )
    assert (
        result == dummy_entries
    ), f"[FAIL] DB 未存在時に全件返却されませんでした: {result}"
    print(f"  全件返却確認: {len(result)} 件")
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 6: _save_race_urls 正常系（一時ディレクトリを使用）
    # ---------------------------------------------------------
    print("\n[TEST 6] _save_race_urls 正常系")
    tmp_dir = Path(tempfile.mkdtemp())
    test_urls = [
        "https://www.jbis.or.jp/race/result/20240526/105/11/",
        "https://www.jbis.or.jp/race/result/20240602/105/11/",
    ]
    try:
        _save_race_urls(test_urls, "2024-05-01", test_logger, base_dir=tmp_dir)
        saved_files = list(tmp_dir.rglob("*.json"))
        assert len(saved_files) >= 1, "[FAIL] JSON ファイルが生成されませんでした。"
        print(f"  生成ファイル: {[str(f) for f in saved_files]}")
        print("  -> PASS")
    finally:
        # テストで生成した一時ファイルをクリーンアップする
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)
        print(f"  一時ディレクトリを削除: {tmp_dir}")

    # ---------------------------------------------------------
    # テスト 7: _build_race_key フォーマット確認
    # ---------------------------------------------------------
    print("\n[TEST 7] _build_race_key フォーマット確認")

    class _MockVenue:
        place = "東京"

    class _MockRace:
        number = 11

    class _MockRaceDetail:
        date = "2024-05-26"
        venue = _MockVenue()
        race = _MockRace()

    key = _build_race_key(_MockRaceDetail())  # type: ignore[arg-type]
    assert key == "2024-05-26_東京_11", f"[FAIL] 期待キーと不一致: {key}"
    print(f"  生成キー: {key}")
    print("  -> PASS")

    print("\n" + "=" * 60)
    print(" 全テスト完了")
    print("=" * 60)


if __name__ == "__main__":
    # python -m src.data_pipeline.data_loader
    _run_tests()
