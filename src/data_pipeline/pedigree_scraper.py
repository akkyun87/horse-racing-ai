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

【Usage】
  from src.data_pipeline.pedigree_scraper import scrape_pedigree_data
  import logging

  logger = logging.getLogger("PedigreeScraper")
  results = scrape_pedigree_data(horse_list, logger=logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Final, List, Optional, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.data_pipeline.data_models import PedigreeInfo
from src.utils.db_manager import load_from_db, save_to_db
from src.utils.file_manager import load_data
from src.utils.retry_requests import fetch_html

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# JBIS の競走馬ベース URL (末尾スラッシュ必須・urljoin で結合するため)
JBIS_BASE_URL: Final[str] = "https://www.jbis.or.jp/horse/"

# 系統キャッシュを永続化する SQLite ファイルパス
SIRE_LINEAGE_DB_PATH: Final[Path] = Path("data/raw/pedigree/sire_lineage.db")

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
    血統表の 1 ノードを表す内部データ構造。

    frozen=True により、スクレイピング後の書き換えを防止する。

    Args:
        horse_name (str): 馬名（正規化済み）。
        horse_id (str): 馬固有ID（10桁ゼロ埋め文字列）。
    """

    horse_name: str
    horse_id: str


@dataclass(slots=True, frozen=True)
class LineageResult:
    """
    系統判定結果を表す内部データ構造。

    frozen=True により、判定後の書き換えを防止する。

    Args:
        horse_id (str): 対象種牡馬の馬固有ID。
        horse_name (str): 対象種牡馬の馬名。
        lineage (str): 判定された系統名（例: "サンデーサイレンス系"）。
        lineage_id (str): 系統固有ID。未判定時は "UNKNOWN"。
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
    全角文字を半角に変換し、国名表記 (USA) 等を除去する。

    JBIS の馬名は全角カタカナ・英字が混在するため、
    系統照合前に正規化して表記ゆれを吸収する。

    Args:
        text (str): 変換対象の文字列。

    Returns:
        str: 正規化済み文字列。空文字入力時は空文字を返す。

    Raises:
        なし

    Example:
        result = to_halfwidth("サンデーサイレンス（ＵＳＡ）")
        # "サンデーサイレンス"
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
    馬名を系統照合用に正規化する（半角化 + 空白除去 + 大文字化）。

    YAML の創始者キーと照合するため、
    表記ゆれを完全に吸収した統一形式に変換する。

    Args:
        name (str): 正規化対象の馬名。

    Returns:
        str: 正規化済み文字列（大文字・空白なし）。

    Raises:
        なし

    Example:
        key = normalize_name("Sunday Silence")
        # "SUNDAYSILENCE"
    """
    if not name:
        return ""

    # to_halfwidth で全角・国名を処理してから大文字化する
    return to_halfwidth(name).replace(" ", "").upper()


def format_horse_id(raw_id: Any) -> str:
    """
    任意の形式の馬 ID を 10 桁ゼロ埋め文字列に統一する。

    スクレイピング元によって桁数が異なる場合があるため、
    DB キーとしての一貫性を確保するために正規化する。

    Args:
        raw_id (Any): 変換元の馬 ID（int / str / None 等）。

    Returns:
        str: 10 桁ゼロ埋め文字列。数字が含まれない場合は空文字。

    Raises:
        なし

    Example:
        result = format_horse_id("2021100001")
        # "2021100001"
        result = format_horse_id(123)
        # "0000000123"
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
    YAML 設定ファイルから系統創始者の定義を再帰的に読み込む。

    YAML 構造はネスト状のサブ系統を持つため、
    再帰トラバーサルで全ノードを展開して返す。

    Args:
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Dict[str, Dict[str, str]]: 正規化済み馬名をキー、
                                   {"name": 系統名, "id": 系統ID} を値とする辞書。

    Raises:
        なし（ロード失敗時は空辞書を返す）

    Example:
        founders = load_lineage_config(logger)
        # {"SUNDAYSILENCE": {"name": "サンデーサイレンス系", "id": "SS001"}, ...}
    """
    data = load_data(str(LINEAGE_YAML_PATH), logger)

    if not data or "lineages" not in data:
        logger.warning("系統設定ファイルが空または 'lineages' キーが存在しません。")
        return {}

    founders: Dict[str, Dict[str, str]] = {}

    def _traverse(node: Dict[str, Any]) -> None:
        """YAML ノードを再帰的に走査し、創始者情報を展開する。"""
        # founder キーが存在する場合は創始者として登録する
        if node.get("founder"):
            founders[normalize_name(node["founder"])] = {
                "name": node.get("name", _UNKNOWN_LINEAGE),
                "id": node.get("lineage_id", _UNKNOWN_LINEAGE_ID),
            }
        # sub_lineages が存在する場合は再帰的に展開する
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
    キャッシュ DB (sire_lineage.db) から既存の系統判定結果を読み込む。

    DB が存在しない場合は空辞書を返し、初回起動時も安全に動作する。

    Args:
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Dict[str, Dict[str, str]]: 馬 ID (10桁文字列) をキー、
                                   系統情報辞書を値とする辞書。

    Raises:
        なし（DB 未存在時は空辞書を返す）

    Example:
        cache = load_existing_lineages(logger)
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


# ---------------------------------------------------------
# HTML パーサー
# ---------------------------------------------------------


def _parse_ancestor_items(soup: BeautifulSoup) -> List[Ancestor]:
    """
    BeautifulSoup オブジェクトから血統表ノードを抽出し Ancestor リストを生成する。

    JBIS の血統ページは `div.data-3__male` と `div.data-3__female` で
    雄・雌を区別して配置されている。
    ドキュメント順に取得することで血統表上の位置（インデックス）を保持する。

    Args:
        soup (BeautifulSoup): パース済みの HTML オブジェクト。

    Returns:
        List[Ancestor]: 血統表上の全ノードの Ancestor リスト（最大 62 要素）。

    Raises:
        なし

    Example:
        ancestors = _parse_ancestor_items(soup)
    """
    # JBIS 特有のクラス構造から雄・雌ノードを取得する（ドキュメント順を維持）
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
    血統ページの HTML 文字列から馬名・全祖先情報を抽出する。

    ネットワーク処理とパース処理を分離するため、
    本関数は HTML 文字列を受け取り純粋な辞書を返す設計とする。

    Args:
        html (str): JBIS 血統ページの HTML 文字列。
        horse_id (str): 対象馬の固有 ID（ログ・識別用）。

    Returns:
        Optional[Dict[str, Any]]: 抽出成功時は
            {"horse_id": str, "name": str, "ancestors": List[Ancestor]}、
            失敗時（血統ノードが空）は None。

    Raises:
        なし

    Example:
        data = extract_pedigree_data(html_text, "2021100001")
        if data:
            ancestors = data["ancestors"]
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
    特定不能な種牡馬を JBIS で父系遡上し、系統を特定する。

    創始者またはキャッシュに当たるまで最大 MAX_TRACE_CYCLES サイクル繰り返す。
    スタックオーバーフロー防止のため再帰ではなく反復処理で実装する。

    Args:
        start_id (str): 遡上開始の種牡馬 ID。
        founders (Dict[str, Dict[str, str]]): 系統創始者辞書（YAML ロード済み）。
        existing (Dict[str, Dict[str, str]]): キャッシュ辞書（DB ロード済み）。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Optional[LineageResult]: 系統特定成功時は LineageResult、失敗時は None。

    Raises:
        なし（HTTP 失敗・系統未特定は None として返す）

    Example:
        result = trace_sire_lineage("2021100001", founders, existing, logger)
        if result:
            print(result.lineage)
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
    種牡馬リストに対して系統判定を一括で実行する。

    判定優先順位:
      1. 馬名が YAML 創始者リストに一致 → 即時確定
      2. 馬 ID がキャッシュ DB に存在  → キャッシュ返却
      3. 上記いずれも不一致            → JBIS 父系遡上スクレイピング

    新規判定結果はオンメモリキャッシュと DB の両方に反映する。

    Args:
        sire_list (List[Ancestor]): 系統判定対象の種牡馬リスト（31 件）。
        founders (Dict[str, Dict[str, str]]): 系統創始者辞書（YAML ロード済み）。
        existing (Dict[str, Dict[str, str]]): キャッシュ辞書（DB ロード済み・更新あり）。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[LineageResult]: 各種牡馬の系統判定結果リスト（入力と同順）。

    Raises:
        なし

    Example:
        results = determine_lineages_for_sires(sire_list, founders, existing, logger)
    """
    results: List[LineageResult] = []

    # 新規発見系統を一時蓄積し、最後にまとめて DB 保存する（バルクインサート）
    new_cache_entries: List[Dict[str, str]] = []

    # ---------------------------------------------------------
    # メインループ処理 (種牡馬ごとの系統判定)
    # ---------------------------------------------------------

    for sire in sire_list:

        norm_name = normalize_name(sire.horse_name)

        # 未登録・不明馬は判定不能として即確定する
        if not sire.horse_id or sire.horse_name in (_UNKNOWN_LINEAGE, "未登録"):
            result = LineageResult(
                horse_id=sire.horse_id,
                horse_name=sire.horse_name,
                lineage=_UNKNOWN_LINEAGE,
                lineage_id=_UNKNOWN_LINEAGE_ID,
            )

        # 優先順位 1: YAML 創始者リストに一致する場合
        elif norm_name in founders:
            f = founders[norm_name]
            result = LineageResult(
                horse_id=sire.horse_id,
                horse_name=sire.horse_name,
                lineage=f["name"],
                lineage_id=f["id"],
            )

        # 優先順位 2: キャッシュ DB に存在する場合
        elif sire.horse_id in existing:
            e = existing[sire.horse_id]
            result = LineageResult(
                horse_id=sire.horse_id,
                horse_name=sire.horse_name,
                lineage=e["lineage"],
                lineage_id=e["lineage_id"],
            )

        # 優先順位 3: JBIS 父系遡上スクレイピングで特定を試みる
        else:
            traced = trace_sire_lineage(sire.horse_id, founders, existing, logger)
            result = traced or LineageResult(
                horse_id=sire.horse_id,
                horse_name=sire.horse_name,
                lineage=_UNKNOWN_LINEAGE,
                lineage_id=_UNKNOWN_LINEAGE_ID,
            )

        results.append(result)

        # 新規判定済みの場合はオンメモリキャッシュと DB 登録候補に追加する
        if result.lineage_id != _UNKNOWN_LINEAGE_ID and sire.horse_id not in existing:
            # asdict を使用して frozen dataclass から辞書に変換する
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
    競走馬リストの血統情報を JBIS から一括取得し、系統判定済み PedigreeInfo を生成する。

    設定・キャッシュのロードはループ外で 1 回のみ実行し、
    ループ内での重複 I/O を排除する。

    Args:
        horse_list (List[Dict[str, Any]]): 収集対象馬のリスト。
                                           各要素は {"id": str, "url": str} を持つこと。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[PedigreeInfo]: 系統判定済みの血統情報オブジェクトリスト。
                            HTML 取得失敗・パース失敗の馬はリストから除外される。

    Raises:
        なし（各馬のエラーはログで通知しスキップ）

    Example:
        logger = logging.getLogger("PedigreeScraper")
        results = scrape_pedigree_data(horse_list, logger)
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

        # ---------------------------------------------------------
        # PedigreeInfo オブジェクトの生成
        # ---------------------------------------------------------

        results.append(
            PedigreeInfo(
                horse_id=int(h_id) if h_id.isdigit() else 0,
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
    主要関数の正常系・異常系動作を確認する。

    外部依存（JBIS HTTP・DB・YAML）はダミーデータで代替し、
    ネットワーク接続なしで実行できる。
    print は本ブロック内のみ許可。
    """
    import sys

    print("=" * 60)
    print(" pedigree_scraper.py 簡易単体テスト 開始")
    print("=" * 60)

    # テスト用ロガーをコンソールへ接続する
    test_logger = logging.getLogger("test_pedigree_scraper")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)

    # ---------------------------------------------------------
    # テスト 1: format_horse_id 正常系
    # ---------------------------------------------------------
    print("\n[TEST 1] format_horse_id 正常系")
    assert format_horse_id("2021100001") == "2021100001"
    assert format_horse_id(123) == "0000000123"
    assert format_horse_id("") == ""
    assert format_horse_id(None) == ""
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 2: normalize_name 正常系
    # ---------------------------------------------------------
    print("\n[TEST 2] normalize_name 正常系")
    assert normalize_name("Sunday Silence") == "SUNDAYSILENCE"
    assert normalize_name("サンデーサイレンス") == "サンデーサイレンス"
    assert normalize_name("") == ""
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 3: to_halfwidth 国名除去
    # ---------------------------------------------------------
    print("\n[TEST 3] to_halfwidth 国名括弧除去")
    result = to_halfwidth("Sunday Silence(USA)")
    assert "USA" not in result, f"[FAIL] 国名が除去されていません: {result}"
    print(f"  変換結果: '{result}'")
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 4: _parse_ancestor_items HTML パース
    # ---------------------------------------------------------
    print("\n[TEST 4] _parse_ancestor_items HTML パース")
    dummy_html = """
    <div class="data-3__items">
      <div class="data-3__male">
        <a class="txt-link" href="/horse/2021100001/">テスト種牡馬</a>
      </div>
      <div class="data-3__female">
        <a class="txt-link" href="/horse/2021100002/">テスト牝馬</a>
      </div>
    </div>
    """
    soup = BeautifulSoup(dummy_html, "html.parser")
    ancestors = _parse_ancestor_items(soup)
    assert len(ancestors) == 2, f"[FAIL] 期待 2件 / 実際 {len(ancestors)}件"
    assert ancestors[0].horse_id == "2021100001"
    assert ancestors[0].horse_name == "テスト種牡馬"
    print(f"  抽出件数: {len(ancestors)} 件")
    print(f"  ancestors[0]: {ancestors[0]}")
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 5: determine_lineages_for_sires キャッシュヒット
    # ---------------------------------------------------------
    print("\n[TEST 5] determine_lineages_for_sires キャッシュヒット")
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
    test_founders: Dict[str, Dict[str, str]] = {}

    lineage_results = determine_lineages_for_sires(
        test_sires, test_founders, test_existing, test_logger
    )
    assert len(lineage_results) == 2, f"[FAIL] 期待 2件 / 実際 {len(lineage_results)}件"
    assert lineage_results[0].lineage == "サンデーサイレンス系"
    assert lineage_results[1].lineage_id == _UNKNOWN_LINEAGE_ID
    print(f"  キャッシュヒット: {lineage_results[0].lineage}")
    print(f"  未登録馬: lineage_id={lineage_results[1].lineage_id}")
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 6: determine_lineages_for_sires YAML 創始者ヒット
    # ---------------------------------------------------------
    print("\n[TEST 6] determine_lineages_for_sires YAML 創始者ヒット")
    founder_sire = Ancestor(horse_name="Sunday Silence", horse_id="9999999999")
    test_founders_with_ss: Dict[str, Dict[str, str]] = {
        "SUNDAYSILENCE": {"name": "サンデーサイレンス系", "id": "SS001"}
    }
    founder_results = determine_lineages_for_sires(
        [founder_sire], test_founders_with_ss, {}, test_logger
    )
    assert founder_results[0].lineage == "サンデーサイレンス系"
    print(f"  YAML 創始者ヒット: {founder_results[0].lineage}")
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 7: LABELS_SIRE_INDEX と SIRE_POSITIONS の整合性
    # ---------------------------------------------------------
    print("\n[TEST 7] LABELS_SIRE_INDEX と SIRE_POSITIONS の整合性")
    assert (
        len(LABELS_SIRE_INDEX) == 31
    ), f"[FAIL] 期待 31件 / 実際 {len(LABELS_SIRE_INDEX)}件"
    assert len(SIRE_POSITIONS) == 31
    assert SIRE_POSITIONS == sorted(LABELS_SIRE_INDEX.values())
    print(f"  LABELS_SIRE_INDEX: {len(LABELS_SIRE_INDEX)} 件")
    print("  -> PASS")

    # ---------------------------------------------------------
    # テスト 8: Ancestor frozen=True による書き換え防止
    # ---------------------------------------------------------
    print("\n[TEST 8] Ancestor frozen=True による書き換え防止")
    anc = Ancestor(horse_name="テスト", horse_id="0000000001")
    try:
        anc.horse_name = "改ざん"  # type: ignore[misc]
        print("  [FAIL] frozen=True が機能していません。")
    except Exception:
        print("  FrozenInstanceError を正常に捕捉")
        print("  -> PASS")

    print("\n" + "=" * 60)
    print(" 全テスト完了")
    print("=" * 60)


if __name__ == "__main__":
    # python -m src.data_pipeline.pedigree_scraper
    _run_tests()
