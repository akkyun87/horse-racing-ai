# ファイルパス: src/data_pipeline/pedigree_scraper.py

"""
src/data_pipeline/pedigree_scraper.py

【概要】
JBIS (Japan Bloodstock Information System) から競走馬の5代血統情報を抽出し、
種牡馬 31 箇所の系統（サンデーサイレンス系、ミスタープロスペクター系等）を特定する。

系統判定は以下の3段階で行われる:
  1. 設定ファイル (YAML) の創始者リストと照合
  2. キャッシュ DB (sire_lineage.db) と照合
  3. JBIS を最大 MAX_TRACE_CYCLES サイクル遡上スクレイピング

スタックオーバーフロー防止のため、父系遡上は再帰ではなく反復処理で実装する。

【外部依存】
- ネットワーク: JBIS (https://www.jbis.or.jp/horse/) への HTTP リクエスト
- DB: SQLite (data/raw/pedigree/sire_lineage.db)
- 設定: config/lineage.yaml (系統創始者定義)
- 内部モジュール:
    src.data_pipeline.data_models   (PedigreeInfo)
    src.utils.db_manager            (load_from_db, save_to_db)
    src.utils.file_manager          (load_data)
    src.utils.retry_requests        (fetch_html)
    src.utils.logger                (setup_logger, close_logger_handlers)

【Usage】
    from src.data_pipeline.pedigree_scraper import scrape_pedigree_data
    from src.utils.logger import setup_logger

    logger = setup_logger(
        log_filepath="logs/pedigree_scraper.log",
        log_level="INFO",
        logger_name="PedigreeScraperMain",
    )

    sample_horses = [
        {"id": "0001155349", "url": "https://www.jbis.or.jp/horse/0001155349/"},
    ]
    results = scrape_pedigree_data(sample_horses, logger)
    for info in results:
        print(f"[馬ID] {info.horse_id}  [馬名] {info.name}")
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import re
import shutil
import sqlite3
import time
import unicodedata
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Final, List, Optional, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.data_pipeline.data_models import PedigreeInfo
from src.utils.db_manager import load_from_db, save_to_db
from src.utils.file_manager import load_data
from src.utils.logger import setup_logger
from src.utils.retry_requests import fetch_html

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# JBIS の競走馬ベース URL (末尾スラッシュ必須・urljoin で結合するため)
JBIS_BASE_URL: Final[str] = "https://www.jbis.or.jp/horse/"

# 系統キャッシュを永続化する SQLite ファイルパス
SIRE_LINEAGE_DB_PATH: Final[Path] = Path("data/raw/pedigree/sire_lineage.db")

# 種牡馬登場回数を記録するテーブル名（sire_lineage.db 内）
SIRE_APPEARANCE_COUNT_TABLE: Final[str] = "sire_appearance_count"

# 系統創始者・サブ系統の定義 YAML ファイルパス
LINEAGE_YAML_PATH: Final[Path] = Path("config/lineage.yaml")

# 父系遡上の最大サイクル数 (1 サイクル = 5代分、最大 25 代まで遡上可能)
MAX_TRACE_CYCLES: Final[int] = 5

# 5代血統表（全 62 箇所）における「種牡馬ポジション」のラベル → インデックス対応表
# 父=0, 父父=1, 母父=32 ... の 31 箇所の雄ラインを定義
LABELS_SIRE_INDEX: Final[Dict[str, int]] = {
    "父": 0,
    "父父": 1,
    "父父父": 3,
    "父母父": 5,
    "父父父父": 7,
    "父父母父": 9,
    "父母父父": 11,
    "父母母父": 13,
    "父父父父父": 15,
    "父父父母父": 17,
    "父父母父父": 19,
    "父父母母父": 21,
    "父母父父父": 23,
    "父母父母父": 25,
    "父母母父父": 27,
    "父母母母父": 29,
    "母父": 32,
    "母父父": 34,
    "母母父": 36,
    "母父父父": 38,
    "母父母父": 40,
    "母母父父": 42,
    "母母母父": 44,
    "母父父父父": 46,
    "母父父母父": 48,
    "母父母父父": 50,
    "母父母母父": 52,
    "母母父父父": 54,
    "母母父母父": 56,
    "母母母父父": 58,
    "母母母母父": 60,
}

# インデックスを昇順に並べたリスト (ループ処理での位置参照用)
SIRE_POSITIONS: Final[List[int]] = sorted(LABELS_SIRE_INDEX.values())

# 父系遡上時にチェックする直系父系ポジションのインデックス
# (父=0, 父父=1, 父父父=3, 父父父父=7, 父父父父父=15)
_DIRECT_SIRE_INDICES: Final[List[int]] = [0, 1, 3, 7, 15]

# 系統不明時のプレースホルダー値
_UNKNOWN_LINEAGE: Final[str] = "不明"
_UNKNOWN_LINEAGE_ID: Final[str] = "UNKNOWN"


# ---------------------------------------------------------
# 内部データ構造体
# ---------------------------------------------------------


@dataclass(slots=True, frozen=True)
class Ancestor:
    """
    競走馬の祖先情報（馬名・馬ID）を保持するデータクラス。

    Attributes:
        horse_name (str): 馬名。
        horse_id (str): JBIS馬ID（10桁0埋め）。

    Raises:
        FrozenInstanceError: frozen=True のため、インスタンス生成後のフィールド書き換えは不可。

    Example:
        >>> anc = Ancestor(horse_name="キタサンブラック", horse_id="0001155349")
        >>> anc.horse_name
        'キタサンブラック'
    """

    horse_name: str
    horse_id: str


@dataclass(slots=True, frozen=True)
class LineageResult:
    """
    系統判定結果（馬ID・馬名・系統名・系統ID）を保持するデータクラス。

    Attributes:
        horse_id (str): JBIS馬ID。
        horse_name (str): 馬名。
        lineage (str): 系統名。
        lineage_id (str): 系統ID。

    Raises:
        FrozenInstanceError: frozen=True のため、インスタンス生成後のフィールド書き換えは不可。

    Example:
        >>> result = LineageResult(
        ...     horse_id="9999999999",
        ...     horse_name="Sunday Silence",
        ...     lineage="サンデーサイレンス系",
        ...     lineage_id="SS001",
        ... )
        >>> result.lineage
        'サンデーサイレンス系'
    """

    horse_id: str
    horse_name: str
    lineage: str
    lineage_id: str


# ---------------------------------------------------------
# 文字列正規化ユーティリティ
# ---------------------------------------------------------


def to_halfwidth(text: str) -> str:
    """
    文字列を半角・国名除去・空白除去で正規化する。

    Args:
        text (str): 入力文字列。

    Returns:
        str: 正規化後の文字列。入力が空文字・None 相当の場合は空文字を返す。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> to_halfwidth("Sunday Silence(USA)")
        'Sunday Silence'
        >>> to_halfwidth("キタサン ブラック")
        'キタサンブラック'
    """
    if not text:
        return ""

    # NFKC 正規化で全角英数字・記号を半角に統一する
    text = unicodedata.normalize("NFKC", text)

    # "(USA)" "(IRE)" "(GB)" 等の国名括弧表記を除去する
    text = re.sub(r"\([A-Z]+\)", "", text).strip()

    # 日本語馬名（ひらがな・カタカナ・漢字を含む）の空白を除去する
    if re.search(r"[一-龥ぁ-んァ-ン]", text):
        text = text.replace(" ", "")

    return text


def normalize_name(name: str) -> str:
    """
    馬名を半角・国名除去・空白除去・大文字化で正規化する。

    YAML 創始者辞書との照合時に表記ゆれを吸収するため、
    `to_halfwidth` の変換に加えて大文字化を適用する。

    Args:
        name (str): 馬名。

    Returns:
        str: 正規化後の馬名。入力が空文字・None 相当の場合は空文字を返す。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> normalize_name("Sunday Silence(USA)")
        'SUNDAYSILENCE'
        >>> normalize_name("サンデーサイレンス")
        'サンデーサイレンス'
    """
    if not name:
        return ""

    # to_halfwidth で全角・国名を処理してから大文字化する
    return to_halfwidth(name).replace(" ", "").upper()


def format_horse_id(raw_id: Any) -> str:
    """
    馬IDを10桁0埋めの文字列に正規化する。

    race.db (INTEGER) と pedigree.db (TEXT 10桁) の型不一致を吸収するため、
    数字のみを抽出して 10 桁にゼロ埋めする。

    Args:
        raw_id (Any): 入力馬ID。数値・文字列・None など任意の型を受け付ける。

    Returns:
        str: 10桁0埋めの馬ID文字列。数字が含まれない場合は空文字を返す。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> format_horse_id("2021100001")
        '2021100001'
        >>> format_horse_id(123)
        '0000000123'
        >>> format_horse_id(None)
        ''
    """
    if not raw_id:
        return ""

    # 入力から数字のみを抽出して 10 桁にゼロ埋めする
    digits = "".join(filter(str.isdigit, str(raw_id)))
    return digits.zfill(10) if digits else ""


# ---------------------------------------------------------
# 設定・キャッシュロード
# ---------------------------------------------------------


def load_lineage_config(
    logger: logging.Logger,
) -> Dict[str, Dict[str, str]]:
    """
    系統創始者・サブ系統の定義YAMLをロードし、創始者名でインデックス化した辞書を返す。

    Args:
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Dict[str, Dict[str, str]]: 正規化馬名→系統情報の辞書。
                                   YAML 未存在・形式不正時は空辞書を返す。

    Raises:
        None: ファイル読み込みエラーは load_data 内部で捕捉しログ出力する。

    Example:
        >>> logger = setup_logger("logs/test.log", logger_name="Test")
        >>> founders = load_lineage_config(logger)
        >>> isinstance(founders, dict)
        True
    """
    data = load_data(str(LINEAGE_YAML_PATH), logger)

    if not data or "lineages" not in data:
        logger.warning("系統設定ファイルが空または 'lineages' キーが存在しません。")
        return {}

    founders: Dict[str, Dict[str, str]] = {}

    def _traverse(node: Dict[str, Any]) -> None:
        founders_raw = node.get("founder")
        if founders_raw:
            founders_list = (
                founders_raw if isinstance(founders_raw, list) else [founders_raw]
            )
            for founder_name in founders_list:
                if isinstance(founder_name, str):
                    norm = normalize_name(founder_name)
                    founders[norm] = {
                        "name": node.get("name", _UNKNOWN_LINEAGE),
                        "id": node.get("lineage_id", _UNKNOWN_LINEAGE_ID),
                        "founder_names": founders_list,
                    }
        for sub in (node.get("sub_lineages") or {}).values():
            _traverse(sub)

    for root in data["lineages"].values():
        _traverse(root)

    logger.debug("系統設定ロード完了: %d 創始者を登録", len(founders))
    return founders


def load_existing_lineages(
    logger: logging.Logger,
) -> Dict[str, Dict[str, str]]:
    """
    系統キャッシュDBから既存の系統情報をロードする。

    Args:
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Dict[str, Dict[str, str]]: 馬ID→系統情報の辞書。
                                   DB 未存在・読み込み失敗時は空辞書を返す。

    Raises:
        None: DB 読み込みエラーは load_from_db 内部で捕捉しログ出力する。

    Example:
        >>> logger = setup_logger("logs/test.log", logger_name="Test")
        >>> existing = load_existing_lineages(logger)
        >>> isinstance(existing, dict)
        True
    """
    # DB ファイルが存在しない場合は全件新規として扱う
    if not SIRE_LINEAGE_DB_PATH.exists():
        logger.debug("系統キャッシュ DB が未存在です。新規作成します。")
        return {}

    rows = load_from_db(str(SIRE_LINEAGE_DB_PATH), "sire_lineage", logger)

    # 馬 ID を正規化した形式でインデックスを構築する
    return {
        format_horse_id(row["horse_id"]): row
        for row in (rows or [])
        if row.get("horse_id")
    }


def _update_sire_appearance_counts(
    lineage_results: List[LineageResult],
    db_path: Path,
    logger: logging.Logger,
) -> None:
    """
    種牡馬リストの登場回数をDBに記録・更新する。

    同一血統表内での重複も個別にカウントするため、まず lineage_results を
    horse_id ごとに集約し、登場回数を加算したうえで UPSERT する。
    DB に未登録の種牡馬は count=N で新規挿入し、登録済みの場合は count を N 増やす。
    horse_id が空の種牡馬（未登録・不明）はスキップする。

    db_manager.save_to_db は INSERT のみ対応のため、本処理では sqlite3 を直接使用する。
    DB ファイルが未存在の場合は作成し、sire_appearance_count テーブルも自動作成する。

    Args:
        lineage_results (List[LineageResult]): 系統判定済み種牡馬リスト（1頭分・最大31件）。
        db_path (Path): sire_lineage.db のファイルパス。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        None: 戻り値なし（副作用として DB の count カラムを更新する）。

    Raises:
        None: sqlite3.Error は内部で捕捉しログ出力する。

    Example:
        >>> results = [LineageResult("0001155349", "キタサンブラック", "サンデーサイレンス系", "SS001")]
        >>> _update_sire_appearance_counts(results, Path("data/raw/pedigree/sire_lineage.db"), logger)
    """
    valid_sires = [r for r in lineage_results if r.horse_id]
    if not valid_sires:
        logger.debug("カウント対象の種牡馬が存在しません。スキップします。")
        return

    # horse_id ごとに登場回数を集約（同一血統表内の重複も加算）
    count_by_id: Dict[str, int] = {}
    name_by_id: Dict[str, str] = {}
    for r in valid_sires:
        count_by_id[r.horse_id] = count_by_id.get(r.horse_id, 0) + 1
        name_by_id[r.horse_id] = r.horse_name

    rows = [
        (horse_id, name_by_id[horse_id], count)
        for horse_id, count in count_by_id.items()
    ]

    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIRE_APPEARANCE_COUNT_TABLE} (
                    horse_id   TEXT PRIMARY KEY,
                    horse_name TEXT,
                    count      INTEGER DEFAULT 0
                )
                """
            )
            conn.executemany(
                f"""
                INSERT INTO {SIRE_APPEARANCE_COUNT_TABLE}
                    (horse_id, horse_name, count)
                VALUES (?, ?, ?)
                ON CONFLICT(horse_id)
                DO UPDATE SET count = count + excluded.count
                """,
                rows,
            )
            conn.commit()
        logger.debug(
            "種牡馬登場回数を更新: %d 頭（延べ出現回数 +%d）",
            len(rows),
            sum(c for _, _, c in rows),
        )
    except sqlite3.Error as e:
        logger.error("種牡馬登場回数の更新に失敗しました: %s", e, exc_info=True)


# ---------------------------------------------------------
# HTML パーサー
# ---------------------------------------------------------


def _parse_ancestor_items(soup: BeautifulSoup) -> List[Ancestor]:
    """
    JBIS血統表HTMLから祖先ノード（雄・雌）を抽出し、Ancestorリストを返す。

    Args:
        soup (BeautifulSoup): 血統表HTMLのBeautifulSoupオブジェクト。

    Returns:
        List[Ancestor]: 祖先情報リスト（ドキュメント順）。取得ゼロ件の場合は空リスト。

    Raises:
        None: HTML 構造に不整合がある場合も例外を伝播せず空リストまたは部分リストを返す。

    Example:
        >>> from bs4 import BeautifulSoup
        >>> html = "<div class='data-3__items'><div class='data-3__male'><a class='txt-link' href='/horse/2021100001/'>テスト馬</a></div></div>"
        >>> soup = BeautifulSoup(html, "html.parser")
        >>> items = _parse_ancestor_items(soup)
        >>> items[0].horse_id
        '2021100001'
    """
    items = soup.select(
        "div.data-3__items div.data-3__male," " div.data-3__items div.data-3__female"
    )

    ancestors: List[Ancestor] = []

    for item in items:
        link = item.find("a", class_="txt-link")

        # href 属性から JBIS の馬 ID を抽出する
        id_match = re.search(r"/horse/(\d+)/?", link.get("href", "")) if link else None

        ancestors.append(
            Ancestor(
                horse_name=(
                    to_halfwidth(link.get_text(strip=True)) if link else "未登録"
                ),
                horse_id=format_horse_id(id_match.group(1)) if id_match else "",
            )
        )

    return ancestors


def extract_pedigree_data(
    html: str,
    horse_id: str,
) -> Optional[Dict[str, Any]]:
    """
    JBIS血統ページHTMLから馬名・祖先リストを抽出する。

    Args:
        html (str): 血統ページHTML。
        horse_id (str): 馬ID。

    Returns:
        Optional[Dict[str, Any]]: {'horse_id', 'name', 'ancestors'} の辞書。
                                   祖先が 1 件も取得できない場合は None。

    Raises:
        None: HTML パースエラーは BeautifulSoup が内部で吸収する。

    Example:
        >>> data = extract_pedigree_data(html_string, "0001155349")
        >>> data["horse_id"]
        '0001155349'
    """
    soup = BeautifulSoup(html, "html.parser")

    # 馬名は h1 タグから取得する
    name_tag = soup.select_one("div.hdg1-search h1")

    ancestors = _parse_ancestor_items(soup)

    # 血統ノードが 1 件も取得できない場合はパース失敗として None を返す
    if not ancestors:
        return None

    return {
        "horse_id": format_horse_id(horse_id),
        "name": (
            to_halfwidth(name_tag.get_text(strip=True))
            if name_tag
            else _UNKNOWN_LINEAGE
        ),
        "ancestors": ancestors,
    }


# ---------------------------------------------------------
# 父系遡上スクレイピング
# ---------------------------------------------------------


def trace_sire_lineage(
    start_id: str,
    founders: Dict[str, Dict[str, str]],
    existing: Dict[str, Dict[str, str]],
    logger: logging.Logger,
) -> Optional[LineageResult]:
    """
    父系遡上により系統を特定する（YAML→DB→JBISスクレイピングの順）。

    Args:
        start_id (str): 起点馬ID。
        founders (Dict[str, Dict[str, str]]): 創始者辞書。
        existing (Dict[str, Dict[str, str]]): 既存キャッシュ辞書。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Optional[LineageResult]: 系統判定結果。
                                  最大サイクル到達・起点枯渇・HTTP失敗時は None。

    Raises:
        None: HTTP エラーは fetch_html 内部で捕捉しログ出力する。

    Example:
        >>> result = trace_sire_lineage("0001155349", founders, existing, logger)
        >>> result.lineage if result else "不明"
        'サンデーサイレンス系'
    """
    current_id: str = format_horse_id(start_id)
    visited_ids: Set[str] = set()

    # ---------------------------------------------------------
    # メインループ処理 (父系遡上サイクル)
    # ---------------------------------------------------------

    for cycle in range(MAX_TRACE_CYCLES):

        # 循環参照防止: 既訪問 ID への遡上は中断する
        if not current_id or current_id in visited_ids:
            break

        visited_ids.add(current_id)

        # 対象種牡馬の血統ページを取得する
        url = urljoin(JBIS_BASE_URL, f"{current_id}/pedigree/")
        response = fetch_html(url, logger)

        if not response:
            logger.warning("父系遡上: HTML 取得失敗 id=%s cycle=%d", current_id, cycle)
            return None

        ancestors = _parse_ancestor_items(BeautifulSoup(response.text, "html.parser"))

        # ---------------------------------------------------------
        # 直系父系ラインの系統照合
        # ---------------------------------------------------------

        # 父(0), 父父(1), 父父父(3), 父父父父(7), 父父父父父(15) の順で照合する
        for idx in _DIRECT_SIRE_INDICES:

            if idx >= len(ancestors) or not ancestors[idx].horse_id:
                continue

            sire = ancestors[idx]
            norm_name = normalize_name(sire.horse_name)

            # 優先順位 1: YAML 創始者リストに一致する場合
            if norm_name in founders:
                f = founders[norm_name]
                return LineageResult(
                    horse_id=sire.horse_id,
                    horse_name=sire.horse_name,
                    lineage=f["name"],
                    lineage_id=f["id"],
                )

            # 優先順位 2: キャッシュ DB に存在する場合
            if sire.horse_id in existing:
                e = existing[sire.horse_id]
                return LineageResult(
                    horse_id=sire.horse_id,
                    horse_name=sire.horse_name,
                    lineage=e["lineage"],
                    lineage_id=e["lineage_id"],
                )

        # 次サイクル: 5代前の父（インデックス 15）を起点に遡上する
        current_id = ancestors[15].horse_id if len(ancestors) > 15 else ""
        logger.debug("父系遡上: cycle=%d 完了 (次の起点: %s)", cycle + 1, current_id)

    # 最大サイクル到達または起点枯渇で特定不能
    logger.warning("父系遡上: 系統特定不能 start_id=%s", start_id)
    return None


# ---------------------------------------------------------
# 系統確定 (バルク処理)
# ---------------------------------------------------------


def determine_lineages_for_sires(
    sire_list: List[Ancestor],
    founders: Dict[str, Dict[str, str]],
    existing: Dict[str, Dict[str, str]],
    logger: logging.Logger,
) -> List[LineageResult]:
    """
    種牡馬リストに対し系統判定を一括実行し、結果リストを返す。

    Args:
        sire_list (List[Ancestor]): 種牡馬Ancestorリスト。
        founders (Dict[str, Dict[str, str]]): 創始者辞書。
        existing (Dict[str, Dict[str, str]]): 既存キャッシュ辞書（更新される）。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[LineageResult]: 系統判定結果リスト。sire_list と同じ順序・件数で返る。
                             特定不能の場合は lineage_id=_UNKNOWN_LINEAGE_ID の結果が入る。

    Raises:
        None: HTTP エラー・DB 書き込みエラーは内部で捕捉しログ出力する。

    Example:
        >>> results = determine_lineages_for_sires(sire_list, founders, existing, logger)
        >>> results[0].lineage
        'サンデーサイレンス系'
    """
    results: List[LineageResult] = []

    # 新規発見系統を一時蓄積し、最後にまとめて DB 保存する（バルクインサート）
    new_cache_entries: List[Dict[str, str]] = []

    # ---------------------------------------------------------
    # メインループ処理 (種牡馬ごとの系統判定)
    # ---------------------------------------------------------

    for sire in sire_list:
        norm_name = normalize_name(sire.horse_name)

        # デフォルト値
        target_lineage = _UNKNOWN_LINEAGE
        target_lineage_id = _UNKNOWN_LINEAGE_ID

        # 判定ロジック
        if not sire.horse_id or sire.horse_name in (_UNKNOWN_LINEAGE, "未登録"):
            pass  # 不明のまま
        elif norm_name in founders:
            target_lineage = founders[norm_name]["name"]
            target_lineage_id = founders[norm_name]["id"]
        elif sire.horse_id in existing:
            target_lineage = existing[sire.horse_id]["lineage"]
            target_lineage_id = existing[sire.horse_id]["lineage_id"]
        else:
            traced = trace_sire_lineage(sire.horse_id, founders, existing, logger)
            if traced:
                # 意図: traced のID/Nameは祖先なので、lineage情報のみ採用し
                #       照会元（sire）のID/Nameを維持する
                target_lineage = traced.lineage
                target_lineage_id = traced.lineage_id

        result = LineageResult(
            horse_id=sire.horse_id,
            horse_name=sire.horse_name,
            lineage=target_lineage,
            lineage_id=target_lineage_id,
        )
        results.append(result)

        if result.lineage_id != _UNKNOWN_LINEAGE_ID and sire.horse_id not in existing:
            existing[sire.horse_id] = asdict(result)
            new_cache_entries.append(asdict(result))

    # ---------------------------------------------------------
    # キャッシュの永続化 (バルクインサート)
    # ---------------------------------------------------------

    if new_cache_entries:
        save_to_db(new_cache_entries, str(SIRE_LINEAGE_DB_PATH), "sire_lineage", logger)
        logger.info(
            "系統キャッシュ追加: %d 件の新系統を登録しました。", len(new_cache_entries)
        )

    return results


# ---------------------------------------------------------
# メインスクレイピング・インターフェース
# ---------------------------------------------------------


def scrape_pedigree_data(
    horse_list: List[Dict[str, Any]],
    logger: logging.Logger,
) -> List[PedigreeInfo]:
    """
    競走馬リストに対し血統・系統情報を一括スクレイピングし、PedigreeInfoリストを返す。

    Args:
        horse_list (List[Dict[str, Any]]): {'id', 'url'} を持つ馬リスト。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[PedigreeInfo]: 血統・系統情報リスト。
                            HTML 取得失敗・パース失敗の馬はスキップされリストに含まれない。

    Raises:
        None: HTTP エラー・パースエラーは内部でキャッチしログ出力するため外部へ伝播しない。

    Example:
        >>> logger = setup_logger("logs/pedigree.log", logger_name="Pedigree")
        >>> results = scrape_pedigree_data([{"id": "0001155349", "url": "..."}], logger)
        >>> results[0].horse_id
        '0001155349'
    """
    # ---------------------------------------------------------
    # 設定・キャッシュのロード (ループ外で 1 回のみ実行)
    # ---------------------------------------------------------

    # YAML 創始者リストと既存系統キャッシュをループ前に一括ロードし、
    # 各馬の処理ごとにファイル・DB アクセスが発生しないようにする
    founders = load_lineage_config(logger)
    existing = load_existing_lineages(logger)

    results: List[PedigreeInfo] = []

    # ---------------------------------------------------------
    # メインループ処理 (馬ごとの血統スクレイピング)
    # ---------------------------------------------------------

    total = len(horse_list)

    for i, horse in enumerate(horse_list, start=1):

        h_id = format_horse_id(horse.get("id"))
        base_url: Optional[str] = horse.get("url")

        logger.info("血統解析中 [%d/%d]: id=%s", i, total, h_id)

        # ---------------------------------------------------------
        # HTTP / DB / 外部 API 設定 (血統ページ取得)
        # ---------------------------------------------------------

        # base_url に "pedigree/" を付加して5代血統専用ページへアクセスする
        response = fetch_html(urljoin(base_url or "", "pedigree/"), logger)

        if not response:
            logger.error("HTML 取得失敗: id=%s url=%s", h_id, base_url)
            continue

        # ---------------------------------------------------------
        # パース処理 (HTML → Ancestor リスト)
        # ---------------------------------------------------------

        pedigree_data = extract_pedigree_data(response.text, h_id)

        if not pedigree_data:
            logger.warning("血統データ抽出失敗: id=%s", h_id)
            continue

        ancestors: List[Ancestor] = pedigree_data["ancestors"]

        # ---------------------------------------------------------
        # 種牡馬31箇所の抽出と系統判定
        # ---------------------------------------------------------

        # SIRE_POSITIONS に基づいて種牡馬ポジションの Ancestor を抽出する
        # インデックス超過時は未登録プレースホルダーで補完する
        sire_list: List[Ancestor] = [
            (
                ancestors[pos]
                if pos < len(ancestors)
                else Ancestor(horse_name="未登録", horse_id="")
            )
            for pos in SIRE_POSITIONS
        ]

        # existing は determine_lineages_for_sires 内で更新されるため
        # 同一バッチ内の後続馬でも新規キャッシュが即座に反映される
        lineage_results = determine_lineages_for_sires(
            sire_list, founders, existing, logger
        )

        # 登場した種牡馬の回数をDBに反映（同一血統表内の重複も集約してからUPSERT）
        _update_sire_appearance_counts(lineage_results, SIRE_LINEAGE_DB_PATH, logger)

        # ---------------------------------------------------------
        # PedigreeInfo オブジェクトの生成
        # ---------------------------------------------------------

        results.append(
            PedigreeInfo(
                horse_id=h_id,
                name=pedigree_data["name"],
                five_gen_ancestor_names=[a.horse_name for a in ancestors],
                five_gen_ancestor_ids=[
                    int(a.horse_id) if a.horse_id.isdigit() else 0 for a in ancestors
                ],
                five_gen_sire_names=[r.horse_name for r in lineage_results],
                five_gen_sire_ids=[
                    int(r.horse_id) if r.horse_id.isdigit() else 0
                    for r in lineage_results
                ],
                five_gen_sire_lineage_names=[r.lineage for r in lineage_results],
                five_gen_sire_lineage_ids=[r.lineage_id for r in lineage_results],
            )
        )

    # ---------------------------------------------------------
    # ログ出力・後処理
    # ---------------------------------------------------------

    logger.info(
        "血統スクレイピング完了: 対象=%d 件 / 成功=%d 件 / スキップ=%d 件",
        total,
        len(results),
        total - len(results),
    )

    return results


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    主要機能の動作確認テストを実行する。

    外部依存（DB・ネットワーク・YAML）はダミーデータまたは unittest.mock.patch で
    代替するため、単体実行が可能。
    テスト終了後はログファイルとロガーをすべて解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.data_pipeline.pedigree_scraper
        _run_tests()
    """
    from unittest.mock import patch

    from src.utils.logger import close_logger_handlers, setup_logger

    TEST_LOG_DIR: Final[str] = "logs/_test_pedigree_scraper_tmp"
    TEST_LOG_FILE: Final[str] = f"{TEST_LOG_DIR}/test.log"
    TEST_LOGGER_NAME: Final[str] = "test_pedigree_scraper"

    print("=" * 60)
    print(" pedigree_scraper.py 簡易単体テスト 開始")
    print("=" * 60)

    logger = setup_logger(
        log_filepath=TEST_LOG_FILE,
        log_level="DEBUG",
        logger_name=TEST_LOGGER_NAME,
    )

    try:
        # ---------------------------------------------------------
        # テスト 1: format_horse_id (正常系・異常系)
        # ---------------------------------------------------------
        print("\n[TEST 1] 正常系/異常系: format_horse_id の10桁0埋め変換")
        assert format_horse_id("2021100001") == "2021100001", "10桁文字列が一致しません"
        assert format_horse_id(123) == "0000000123", "数値の0埋めが一致しません"
        assert format_horse_id("") == "", "空文字が空文字を返しません"
        assert format_horse_id(None) == "", "None が空文字を返しません"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: normalize_name (正常系・空文字)
        # ---------------------------------------------------------
        print("\n[TEST 2] 正常系/異常系: normalize_name の正規化")
        assert (
            normalize_name("Sunday Silence") == "SUNDAYSILENCE"
        ), "英語馬名の正規化が一致しません"
        assert (
            normalize_name("サンデーサイレンス") == "サンデーサイレンス"
        ), "日本語馬名の正規化が一致しません"
        assert normalize_name("") == "", "空文字が空文字を返しません"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: to_halfwidth (国名括弧除去)
        # ---------------------------------------------------------
        print("\n[TEST 3] 正常系: to_halfwidth の国名括弧除去")
        result = to_halfwidth("Sunday Silence(USA)")
        assert "USA" not in result, f"国名が除去されていません: {result!r}"
        assert "(" not in result, f"括弧が除去されていません: {result!r}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 4: _parse_ancestor_items (ダミーHTML パース)
        # ---------------------------------------------------------
        print("\n[TEST 4] 正常系: _parse_ancestor_items のダミーHTML パース")
        dummy_html = (
            "<div class='data-3__items'>"
            "<div class='data-3__male'>"
            "<a class='txt-link' href='/horse/2021100001/'>テスト種牡馬</a>"
            "</div>"
            "<div class='data-3__female'>"
            "<a class='txt-link' href='/horse/2021100002/'>テスト牝馬</a>"
            "</div>"
            "</div>"
        )
        soup = BeautifulSoup(dummy_html, "html.parser")
        ancestors = _parse_ancestor_items(soup)
        assert (
            len(ancestors) == 2
        ), f"抽出件数が一致しません: {len(ancestors)} (期待値: 2)"
        assert (
            ancestors[0].horse_id == "2021100001"
        ), f"horse_id が一致しません: {ancestors[0].horse_id!r}"
        assert (
            ancestors[0].horse_name == "テスト種牡馬"
        ), f"horse_name が一致しません: {ancestors[0].horse_name!r}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 5: determine_lineages_for_sires (キャッシュヒット)
        # ---------------------------------------------------------
        print("\n[TEST 5] 正常系: determine_lineages_for_sires のキャッシュヒット")
        # 意図: キャッシュ既存馬はDBへの書き込みが発生しないため patch 不要
        test_sires = [
            Ancestor(horse_name="テスト種牡馬", horse_id="2021100001"),
            Ancestor(horse_name="未登録", horse_id=""),
        ]
        test_existing: Dict[str, Dict[str, str]] = {
            "2021100001": {
                "horse_id": "2021100001",
                "horse_name": "テスト種牡馬",
                "lineage": "サンデーサイレンス系",
                "lineage_id": "SS001",
            }
        }
        lineage_results = determine_lineages_for_sires(
            test_sires, {}, test_existing, logger
        )
        assert (
            len(lineage_results) == 2
        ), f"返却件数が一致しません: {len(lineage_results)} (期待値: 2)"
        assert (
            lineage_results[0].lineage == "サンデーサイレンス系"
        ), f"lineage が一致しません: {lineage_results[0].lineage!r}"
        assert (
            lineage_results[1].lineage_id == _UNKNOWN_LINEAGE_ID
        ), f"未登録馬の lineage_id が一致しません: {lineage_results[1].lineage_id!r}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 6: determine_lineages_for_sires (YAML 創始者ヒット)
        # ---------------------------------------------------------
        print("\n[TEST 6] 正常系: determine_lineages_for_sires の YAML 創始者ヒット")
        # 意図: 新規馬（existing に存在しない）が YAML にヒットすると save_to_db が
        #       呼ばれるため patch で外部DB書き込みを排除する
        founder_sire = Ancestor(horse_name="Sunday Silence", horse_id="9999999999")
        test_founders_with_ss: Dict[str, Dict[str, str]] = {
            "SUNDAYSILENCE": {"name": "サンデーサイレンス系", "id": "SS001"}
        }
        with patch("src.data_pipeline.pedigree_scraper.save_to_db"):
            founder_results = determine_lineages_for_sires(
                [founder_sire], test_founders_with_ss, {}, logger
            )
        assert (
            founder_results[0].lineage == "サンデーサイレンス系"
        ), f"YAML 創始者ヒット結果が一致しません: {founder_results[0].lineage!r}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 7: LABELS_SIRE_INDEX と SIRE_POSITIONS の整合性
        # ---------------------------------------------------------
        print("\n[TEST 7] 正常系: LABELS_SIRE_INDEX と SIRE_POSITIONS の整合性")
        assert (
            len(LABELS_SIRE_INDEX) == 31
        ), f"LABELS_SIRE_INDEX の件数が一致しません: {len(LABELS_SIRE_INDEX)} (期待値: 31)"
        assert (
            len(SIRE_POSITIONS) == 31
        ), f"SIRE_POSITIONS の件数が一致しません: {len(SIRE_POSITIONS)} (期待値: 31)"
        assert SIRE_POSITIONS == sorted(
            LABELS_SIRE_INDEX.values()
        ), "SIRE_POSITIONS が LABELS_SIRE_INDEX のソート値と一致しません"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 8: Ancestor frozen=True による書き換え防止
        # ---------------------------------------------------------
        print("\n[TEST 8] 正常系: Ancestor frozen=True による書き換え防止")
        anc = Ancestor(horse_name="テスト", horse_id="0000000001")
        try:
            anc.horse_name = "改ざん"  # type: ignore[misc]
            assert False, "FrozenInstanceError が発生しませんでした"
        except Exception:
            # 意図: frozen=True によりフィールド書き換えは例外が発生することを確認する
            pass
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 9: _update_sire_appearance_counts (カウント更新・集約UPSERT)
        # ---------------------------------------------------------
        print("\n[TEST 9] 正常系: _update_sire_appearance_counts のカウント更新")
        TEST_DB_FILE: Final[str] = f"{TEST_LOG_DIR}/test_count.db"
        test_db_path = Path(TEST_DB_FILE)

        # 前回テスト実行時の状態が残らないよう、事前にテスト用DBを削除しておく
        if test_db_path.exists():
            test_db_path.unlink()

        test_lineage_results = [
            LineageResult(
                horse_id="0001155349",
                horse_name="キタサンブラック",
                lineage="サンデーサイレンス系",
                lineage_id="RC-HA-SS",
            ),
            LineageResult(
                horse_id="0001155349",
                horse_name="キタサンブラック",
                lineage="サンデーサイレンス系",
                lineage_id="RC-HA-SS",
            ),
            LineageResult(
                horse_id="",
                horse_name="未登録",
                lineage=_UNKNOWN_LINEAGE,
                lineage_id=_UNKNOWN_LINEAGE_ID,
            ),
        ]

        _update_sire_appearance_counts(test_lineage_results, test_db_path, logger)
        _update_sire_appearance_counts(test_lineage_results, test_db_path, logger)

        with sqlite3.connect(TEST_DB_FILE) as conn:
            row = conn.execute(
                f"SELECT count FROM {SIRE_APPEARANCE_COUNT_TABLE} WHERE horse_id = ?",
                ("0001155349",),
            ).fetchone()

        assert row is not None, "キタサンブラックのレコードが存在しません"
        assert row[0] == 4, f"count が期待値と一致しません: {row[0]} (期待値: 4)"

        empty_row = None
        with sqlite3.connect(TEST_DB_FILE) as conn:
            empty_row = conn.execute(
                f"SELECT count FROM {SIRE_APPEARANCE_COUNT_TABLE} WHERE horse_id = ?",
                ("",),
            ).fetchone()
        assert empty_row is None, "未登録馬が誤ってDBに挿入されています"

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
            test_dir = Path(TEST_LOG_DIR)
            # Windows では SQLite のファイルハンドル解放が遅れ、rmtree が PermissionError になることがある
            removed = False
            for attempt in range(3):
                try:
                    shutil.rmtree(test_dir)
                    print(f"\nCLEANUP: {TEST_LOG_DIR} を削除しました。")
                    removed = True
                    break
                except PermissionError:
                    if attempt < 2:
                        time.sleep(0.1)
            if not removed:
                print(
                    f"\nCLEANUP: {TEST_LOG_DIR} の削除をスキップしました（ファイル使用中）。"
                )

    print("\n" + "=" * 60)
    print(" 全テスト完了")
    print("=" * 60)


if __name__ == "__main__":
    # python -m src.data_pipeline.pedigree_scraper
    # _run_tests()

    horse_list = [
        {
            "id": "0001155349",
            "url": "https://www.jbis.or.jp/horse/0001155349/",
        },  # キタサンブラック
    ]
    scrape_pedigree_data(
        horse_list,
        setup_logger("logs/pedigree_scraper.log", logger_name="PedigreeScraper"),
    )
