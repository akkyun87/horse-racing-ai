# ファイルパス: src/utils/pedigree_visualizer.py

"""
src/utils/pedigree_visualizer.py

【概要】
競走馬の5代血統表をHTMLおよび画像として出力するユーティリティ。
DBから血統データを取得し、インブリード計算（馬名クロス・系統クロス）を統合したうえで
HTMLツリー構造として出力する。画像変換には wkhtmltoimage を利用する。

クロス対象馬は太字＋血量公式で強調表示し、系統背景色と組み合わせて
視覚的に血統関係を把握しやすくする。

【外部依存】
- DB: SQLite (data/raw/pedigree/pedigree.db)
- 画像変換: wkhtmltoimage (Windows 向けバイナリパス)
- 内部モジュール:
    src.utils.db_manager              (load_from_db)
    src.utils.inbreeding_calculator   (calculate_inbreeding_batch)
    src.utils.logger                  (setup_logger, close_logger_handlers)

【Usage】
    from src.utils.pedigree_visualizer import generate_pedigree_image
    from src.utils.logger import setup_logger

    logger = setup_logger(
        log_filepath="logs/pedigree_visualizer.log",
        log_level="INFO",
        logger_name="PedigreeVisualizer",
    )
    generate_pedigree_image(horse_id="0001352760", logger=logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Final, List, Optional

from src.utils import db_manager
from src.utils.inbreeding_calculator import calculate_inbreeding_batch
from src.utils.logger import setup_logger

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# 血統情報を格納する SQLite ファイルパス
DB_PATH: Final[Path] = Path("data/raw/pedigree/pedigree.db")

# HTML / 画像の出力先ディレクトリ
HTML_DIR: Final[Path] = Path("images/pedigree/html")
IMG_DIR: Final[Path] = Path("images/pedigree/img")

# 画像変換に使用する wkhtmltoimage の実行ファイルパス (Windows)
WKHTMLTOIMAGE: Final[Path] = Path(r"C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe")

# ---------------------------------------------------------
# 5代血統表インデックス → ラベル対応辞書
# インデックス 0〜61 の 62 ノードに日本語ラベルを割り当てる
# ---------------------------------------------------------

PEDIGREE_INDEX_LABELS: Final[Dict[int, str]] = {
    # 父系（0〜30）
    0: "父",
    1: "父父",
    2: "父母",
    3: "父父父",
    4: "父父母",
    5: "父母父",
    6: "父母母",
    7: "父父父父",
    8: "父父父母",
    9: "父父母父",
    10: "父父母母",
    11: "父母父父",
    12: "父母父母",
    13: "父母母父",
    14: "父母母母",
    15: "父父父父父",
    16: "父父父父母",
    17: "父父父母父",
    18: "父父父母母",
    19: "父父母父父",
    20: "父父母父母",
    21: "父父母母父",
    22: "父父母母母",
    23: "父母父父父",
    24: "父母父父母",
    25: "父母父母父",
    26: "父母父母母",
    27: "父母母父父",
    28: "父母母父母",
    29: "父母母母父",
    30: "父母母母母",
    # 母系（31〜61）
    31: "母",
    32: "母父",
    33: "母母",
    34: "母父父",
    35: "母父母",
    36: "母母父",
    37: "母母母",
    38: "母父父父",
    39: "母父父母",
    40: "母父母父",
    41: "母父母母",
    42: "母母父父",
    43: "母母父母",
    44: "母母母父",
    45: "母母母母",
    46: "母父父父父",
    47: "母父父父母",
    48: "母父父母父",
    49: "母父父母母",
    50: "母父母父父",
    51: "母父母父母",
    52: "母父母母父",
    53: "母父母母母",
    54: "母母父父父",
    55: "母母父父母",
    56: "母母父母父",
    57: "母母父母母",
    58: "母母母父父",
    59: "母母母父母",
    60: "母母母母父",
    61: "母母母母母",
}

# 系統名 → 背景色の対応辞書
COLOR_MAP: Final[Dict[str, str]] = {
    # Northern Dancer 系（青）
    "Northern Dancer 系": "#4A90E2",
    "Nijinsky 系": "#5A9BE6",
    "Vice Regent 系": "#6BA7EA",
    "Lyphard 系": "#7BB3EE",
    "Danzig 系": "#3F83D6",
    "Nureyev 系": "#8CBFF2",
    "Storm Cat 系": "#9CCBF5",
    "Sadler's Wells 系": "#3578CA",
    # Mr. Prospector 系（肌色）
    "Mr. Prospector 系": "#F5CBA7",
    "Fappiano 系": "#F6D2B1",
    "Gone West 系": "#F7D9BB",
    "Forty Niner 系": "#F4C29C",
    "Kingmambo 系": "#F3BA91",
    "King Kamehameha 系": "#F2B286",
    "Smart Strike 系": "#F8E0C7",
    "Machiavellian 系": "#EFB083",
    # Nasrullah 系（赤）
    "Nasrullah 系": "#E74C3C",
    "Grey Sovereign 系": "#EC7063",
    "Bold Ruler 系": "#CD6155",
    "Never Bend 系": "#D35454",
    "Red God 系": "#C0392B",
    "Princely Gift 系": "#F1948A",
    # Royal Charger 系（緑）
    "Royal Charger 系": "#27AE60",
    "Halo 系": "#2ECC71",
    "Sunday Silence 系": "#58D68D",
    "Deep Impact 系": "#82E0AA",
    "Roberto 系": "#239B56",
    "Sir Gaylord 系": "#1D8348",
    # Herod 系（茶）
    "Herod 系": "#8E5C42",
    # Touchstone 系（薄紫）
    "Touchstone 系": "#AF7AC5",
    "Himyar 系": "#BB8FCE",
    "Hyperion 系": "#C39BD3",
    # Stockwell 系（濃い紫）
    "Stockwell 系": "#6C3483",
    "Orme 系": "#7D3C98",
    "Damascus 系": "#5B2C6F",
    # Phalaris 系（濃い青）
    "Phalaris 系": "#1B4F72",
    "Tom Fool 系": "#21618C",
    # Blandford 系（灰）
    "Blandford 系": "#7F8C8D",
    # St. Simon 系（黄色）
    "St. Simon 系": "#F4D03F",
    "Princequillo 系": "#F7DC6F",
    "Ribot 系": "#F1C40F",
    # Matchem 系（オレンジ）
    "Matchem 系": "#E67E22",
}


# ---------------------------------------------------------
# データ取得 & 計算
# ---------------------------------------------------------


def get_pedigree_and_cross_data(
    horse_id: str,
    logger: logging.Logger,
) -> Optional[Dict[str, Any]]:
    """
    指定馬IDの血統レコードを取得し、インブリード計算結果を埋め込んで返す。

    DB アクセスを1回に集約し、取得したレコードリストを
    calculate_inbreeding_batch に渡すことで二重ロードを防止する。

    Args:
        horse_id (str): 馬ID（10桁ゼロ埋み文字列または数値文字列）。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Optional[Dict[str, Any]]: pedigree_info の1行レコードに
                                   "cross_info" キーを追加した辞書。
                                   DB 未接続・馬未登録の場合は None。

    Raises:
        None: DB アクセスエラーは内部で捕捉し ERROR ログを出力する。

    Example:
        >>> logger = setup_logger("logs/pedigree_visualizer.log", logger_name="PedigreeVisualizer")
        >>> data = get_pedigree_and_cross_data("0001352760", logger)
        >>> if data:
        ...     print(data["name"])
    """
    try:
        target_id = str(horse_id).zfill(10)

        # 意図: DB アクセスを1回に集約し、blood レコードを両処理で共用する
        records = db_manager.load_from_db(str(DB_PATH), "pedigree_info", logger)
        if not records:
            return None

        record = next(
            (r for r in records if str(r.get("horse_id")).zfill(10) == target_id),
            None,
        )
        if not record:
            return None

        # 意図: ロード済みの records を注入することで、インブリード計算側での
        #       二重 DB アクセスを排除する
        cross_results = calculate_inbreeding_batch(
            [target_id],
            pedigree_records=records,
            logger=logger,
        )

        # 意図: クロス計算結果をレコードに埋め込み、HTML 描画で参照できるようにする
        record["cross_info"] = cross_results.get(target_id, {})
        return record

    except Exception as e:
        logger.error("データ取得・計算エラー: %s", e, exc_info=True)
        return None


# ---------------------------------------------------------
# HTML 構築ユーティリティ
# ---------------------------------------------------------


def get_cell_html(
    idx: int,
    pedigree_dict: Dict[int, str],
    record: Optional[Dict[str, Any]],
) -> str:
    """
    指定インデックスのセル内容を HTML 文字列で返す。

    クロス対象馬は太字＋血量公式で強調表示し、系統ラベルに応じた
    背景色を適用する。濃い背景色の場合はテキストを白色に切り替えて
    視認性を確保する。

    Args:
        idx (int): 血統表インデックス（0〜61）。
        pedigree_dict (Dict[int, str]): インデックス→馬名の辞書。
        record (Optional[Dict[str, Any]]): pedigree_info の1行分の辞書。
                                            "cross_info" キーを含む。
                                            None の場合は系統色・クロス情報なしで描画する。

    Returns:
        str: セル1個分の HTML 文字列（display:table による中央揃え構造）。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> pedigree_dict = {0: "ブラックタイド"}
        >>> html = get_cell_html(0, pedigree_dict, None)
        >>> "ブラックタイド" in html
        True
    """
    name = pedigree_dict.get(idx, "").strip()
    label = PEDIGREE_INDEX_LABELS.get(idx, "")

    # 意図: クロス対象馬はインブリード計算済みの cross_info から取得して強調表示する
    cross_data = record.get("cross_info", {}).get("horse_crosses", {}) if record else {}
    target_cross = cross_data.get(name) if name and name != "Unknown" else None

    def _get_lineage(rec: Optional[dict], lbl: str) -> str:
        """record から lineage_name_{lbl} を取得する（空白差異を吸収）。"""
        if not rec:
            return ""
        k = f"lineage_name_{lbl}"
        v = rec.get(k, "")
        if v:
            return v
        # 意図: カラム名に含まれる空白の有無の違いを吸収するため、
        #       空白除去後の文字列でも照合する
        t = k.replace(" ", "")
        for rk, rv in rec.items():
            if isinstance(rk, str) and rk.replace(" ", "") == t:
                return rv or ""
        return ""

    # 意図: 系統色は「父」で終わる種牡馬ポジションにのみ適用する
    lineage = _get_lineage(record, label) if record and label.endswith("父") else ""
    bg_color = COLOR_MAP.get(lineage, "transparent")

    # 意図: 濃い背景色に対してテキストを白色にすることで視認性を確保する
    is_dark = bg_color in {
        "#6C3483",
        "#1B4F72",
        "#8E5C42",
        "#5B2C6F",
        "#21618C",
        "#7D3C98",
    }
    text_color = "#ffffff" if is_dark else "#000000"
    sub_color = "rgba(255,255,255,0.7)" if is_dark else "rgba(0,0,0,0.5)"
    cross_text_color = "#ffeb3b" if is_dark else "#d32f2f"

    # 意図: クロス対象馬は太字にして非クロス馬と視覚的に区別する
    name_style = (
        "font-weight: bold; font-size: 15px;"
        if target_cross
        else "font-weight: normal; font-size: 14px;"
    )

    cross_html = ""
    if target_cross:
        formula = target_cross["formula"]
        pct = target_cross["blood_pct"]
        # 意図: 血量公式（例: 4x5 (9.375%)）をセル下部に小さく表示する
        cross_html = (
            f'<div style="font-size: 10px; color: {cross_text_color}; '
            f'font-weight: bold; margin-top: 1px;">{formula} ({pct}%)</div>'
        )

    return f"""
    <div style="display: table; width: 100%; height: 100%; background-color: {bg_color}; border-collapse: collapse;">
        <div style="display: table-cell; vertical-align: middle; text-align: center; padding: 2px;">
            <div style="{name_style} color: {text_color}; line-height: 1.1;">{name}</div>
            {cross_html}
            {f'<div style="font-size: 9px; color: {sub_color}; font-weight: bold; margin-top: 2px;">{lineage}</div>' if lineage else ''}
        </div>
    </div>
    """


def build_html(
    name: str,
    pedigree_dict: Dict[int, str],
    record: Optional[Dict[str, Any]],
) -> str:
    """
    5代血統表を HTML テーブル構造で組み立て、文字列として返す。

    父系（上半分・sire-side）と母系（下半分・dam-side）を rowspan で結合し、
    横5列・縦32行の血統表レイアウトを生成する。
    `.wrapper` の padding により wkhtmltoimage での端の削れを防止する。

    Args:
        name (str): 馬名（HTML タイトル用に保持。本体表示には未使用）。
        pedigree_dict (Dict[int, str]): インデックス→馬名の辞書（0〜61）。
        record (Optional[Dict[str, Any]]): pedigree_info の1行分の辞書。
                                            系統色・クロス情報の適用に使用する。

    Returns:
        str: 完全な HTML ドキュメント文字列（DOCTYPE〜/html）。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> pedigree_dict = {i: f"馬{i}" for i in range(62)}
        >>> html = build_html("テスト馬", pedigree_dict, None)
        >>> html.startswith("<!DOCTYPE html>")
        True
    """
    css = """
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        html, body { background-color: #fff; display: inline-block; }
        .wrapper { padding: 15px; background-color: #fff; }
        table { border-collapse: collapse; border: 2px solid #000; table-layout: fixed; width: 1100px; height: 850px; }
        td { border: 1px solid #444; vertical-align: middle; text-align: center; }
        .sire-side { background-color: #f0faff; }
        .dam-side  { background-color: #fff5f7; }
    </style>
    """

    # 意図: 32行5列の血統表テーブル構造を rowspan で結合する
    g = pedigree_dict
    r = record
    table_rows = f"""
    <tr><td class="sire-side" rowspan="16">{get_cell_html(0,g,r)}</td><td rowspan="8">{get_cell_html(1,g,r)}</td><td rowspan="4">{get_cell_html(3,g,r)}</td><td rowspan="2">{get_cell_html(7,g,r)}</td><td>{get_cell_html(15,g,r)}</td></tr>
    <tr><td>{get_cell_html(16,g,r)}</td></tr>
    <tr><td rowspan="2">{get_cell_html(8,g,r)}</td><td>{get_cell_html(17,g,r)}</td></tr>
    <tr><td>{get_cell_html(18,g,r)}</td></tr>
    <tr><td rowspan="4">{get_cell_html(4,g,r)}</td><td rowspan="2">{get_cell_html(9,g,r)}</td><td>{get_cell_html(19,g,r)}</td></tr>
    <tr><td>{get_cell_html(20,g,r)}</td></tr>
    <tr><td rowspan="2">{get_cell_html(10,g,r)}</td><td>{get_cell_html(21,g,r)}</td></tr>
    <tr><td>{get_cell_html(22,g,r)}</td></tr>
    <tr><td rowspan="8">{get_cell_html(2,g,r)}</td><td rowspan="4">{get_cell_html(5,g,r)}</td><td rowspan="2">{get_cell_html(11,g,r)}</td><td>{get_cell_html(23,g,r)}</td></tr>
    <tr><td>{get_cell_html(24,g,r)}</td></tr>
    <tr><td rowspan="2">{get_cell_html(12,g,r)}</td><td>{get_cell_html(25,g,r)}</td></tr>
    <tr><td>{get_cell_html(26,g,r)}</td></tr>
    <tr><td rowspan="4">{get_cell_html(6,g,r)}</td><td rowspan="2">{get_cell_html(13,g,r)}</td><td>{get_cell_html(27,g,r)}</td></tr>
    <tr><td>{get_cell_html(28,g,r)}</td></tr>
    <tr><td rowspan="2">{get_cell_html(14,g,r)}</td><td>{get_cell_html(29,g,r)}</td></tr>
    <tr><td>{get_cell_html(30,g,r)}</td></tr>
    <tr><td class="dam-side" rowspan="16">{get_cell_html(31,g,r)}</td><td rowspan="8">{get_cell_html(32,g,r)}</td><td rowspan="4">{get_cell_html(34,g,r)}</td><td rowspan="2">{get_cell_html(38,g,r)}</td><td>{get_cell_html(46,g,r)}</td></tr>
    <tr><td>{get_cell_html(47,g,r)}</td></tr>
    <tr><td rowspan="2">{get_cell_html(39,g,r)}</td><td>{get_cell_html(48,g,r)}</td></tr>
    <tr><td>{get_cell_html(49,g,r)}</td></tr>
    <tr><td rowspan="4">{get_cell_html(35,g,r)}</td><td rowspan="2">{get_cell_html(40,g,r)}</td><td>{get_cell_html(50,g,r)}</td></tr>
    <tr><td>{get_cell_html(51,g,r)}</td></tr>
    <tr><td rowspan="2">{get_cell_html(41,g,r)}</td><td>{get_cell_html(52,g,r)}</td></tr>
    <tr><td>{get_cell_html(53,g,r)}</td></tr>
    <tr><td rowspan="8">{get_cell_html(33,g,r)}</td><td rowspan="4">{get_cell_html(36,g,r)}</td><td rowspan="2">{get_cell_html(42,g,r)}</td><td>{get_cell_html(54,g,r)}</td></tr>
    <tr><td>{get_cell_html(55,g,r)}</td></tr>
    <tr><td rowspan="2">{get_cell_html(43,g,r)}</td><td>{get_cell_html(56,g,r)}</td></tr>
    <tr><td>{get_cell_html(57,g,r)}</td></tr>
    <tr><td rowspan="4">{get_cell_html(37,g,r)}</td><td rowspan="2">{get_cell_html(44,g,r)}</td><td>{get_cell_html(58,g,r)}</td></tr>
    <tr><td>{get_cell_html(59,g,r)}</td></tr>
    <tr><td rowspan="2">{get_cell_html(45,g,r)}</td><td>{get_cell_html(60,g,r)}</td></tr>
    <tr><td>{get_cell_html(61,g,r)}</td></tr>
    """

    return (
        f'<!DOCTYPE html><html><head><meta charset="utf-8">{css}</head>'
        f'<body><div class="wrapper"><table>{table_rows}</table></div></body></html>'
    )


# ---------------------------------------------------------
# メイン実行処理
# ---------------------------------------------------------


def generate_pedigree_image(
    horse_id: str,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    指定馬IDの5代血統表（クロス情報付き）を HTML ファイルおよび PNG 画像として出力する。

    血統データとインブリード計算結果を統合したうえで HTML を生成し、
    wkhtmltoimage で PNG に変換する。

    Args:
        horse_id (str): 馬ID（10桁ゼロ埋み文字列または数値文字列）。
        logger (Optional[logging.Logger]): ログ出力用ロガー。
                                            None の場合は内部で setup_logger() を生成する。

    Returns:
        None: 戻り値なし（副作用として HTML / PNG ファイルを生成する）。

    Raises:
        None: DB エラー・subprocess エラーは内部で捕捉し ERROR ログを出力する。

    Example:
        >>> logger = setup_logger("logs/pedigree_visualizer.log", logger_name="PedigreeVisualizer")
        >>> generate_pedigree_image("0001352760", logger)
    """
    if logger is None:
        logger = setup_logger(
            log_filepath="logs/pedigree_visualizer.log",
            log_level="INFO",
            logger_name=__name__,
        )

    safe_id = str(horse_id).zfill(10)
    html_path = HTML_DIR / f"{safe_id}.html"
    img_path = IMG_DIR / f"{safe_id}.png"

    HTML_DIR.mkdir(parents=True, exist_ok=True)
    IMG_DIR.mkdir(parents=True, exist_ok=True)

    record = get_pedigree_and_cross_data(horse_id, logger)
    if not record:
        logger.error("血統データが取得できませんでした: %s", safe_id)
        return

    name = record["name"]
    raw_list = [
        x.strip() for x in record.get("five_gen_ancestors", "").split(",") if x.strip()
    ]

    # 意図: 本馬自身がインデックス 0 に含まれる場合（63件）は除去し 62件に揃える
    if len(raw_list) == 63:
        raw_list = raw_list[1:]

    # 意図: スクレイピング不完全で要素が不足している場合も空文字で補完して処理を継続する
    while len(raw_list) < 62:
        raw_list.append("")

    pedigree_dict: Dict[int, str] = {i: raw_list[i] for i in range(62)}

    html = build_html(name, pedigree_dict, record)
    with html_path.open("w", encoding="utf-8") as f:
        f.write(html)

    try:
        subprocess.run(
            [
                str(WKHTMLTOIMAGE),
                "--quiet",
                "--enable-local-file-access",
                str(html_path.resolve()),
                str(img_path.resolve()),
            ],
            check=True,
        )
        logger.info("画像生成成功: %s", img_path.name)
    except subprocess.CalledProcessError as e:
        logger.error("画像変換エラー (CalledProcessError): %s", e)
    except Exception as e:
        logger.error("予期せぬエラー: %s", e, exc_info=True)


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    主要機能の動作確認テストを実行する。

    DBアクセスは実 DB_PATH に接続する（モックなし）。
    DB ファイルが存在しない場合・対象馬が未登録の場合は該当テストを SKIP する。
    subprocess.run（wkhtmltoimage）は Windows 専用外部バイナリのため
    unittest.mock.patch でモックし OS 依存を排除する。
    テスト終了後はログファイルとロガーをすべて解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.utils.pedigree_visualizer
        _run_tests()
    """
    from unittest.mock import patch

    from src.utils.logger import close_logger_handlers, setup_logger

    TEST_LOG_DIR: Final[str] = "logs/_test_pedigree_visualizer_tmp"
    TEST_LOG_FILE: Final[str] = f"{TEST_LOG_DIR}/test.log"
    TEST_LOGGER_NAME: Final[str] = "test_pedigree_visualizer"
    TEST_HTML_DIR: Final[str] = f"{TEST_LOG_DIR}/html"
    TEST_IMG_DIR: Final[str] = f"{TEST_LOG_DIR}/img"
    TEST_HORSE_ID: Final[str] = "0001352760"

    print("=" * 60)
    print(" pedigree_visualizer.py 簡易単体テスト 開始")
    print("=" * 60)

    db_available: bool = DB_PATH.exists()
    if not db_available:
        print(f"\n  [INFO] DB ファイルが存在しません: {DB_PATH}")
        print("  DB 依存テストは SKIP されます。")

    logger = setup_logger(
        log_filepath=TEST_LOG_FILE,
        log_level="DEBUG",
        logger_name=TEST_LOGGER_NAME,
    )

    try:
        # ---------------------------------------------------------
        # テスト 1: get_cell_html (純粋ロジック・DB 不要)
        # ---------------------------------------------------------
        print("\n[TEST 1] 正常系: get_cell_html の HTML 構造検証")
        pedigree_dict_test = {0: "ブラックタイド"}
        html = get_cell_html(0, pedigree_dict_test, None)
        assert "ブラックタイド" in html, "馬名が HTML に含まれていません"
        assert "display: table" in html, "display:table が HTML に含まれていません"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: build_html (純粋ロジック・DB 不要)
        # ---------------------------------------------------------
        print("\n[TEST 2] 正常系: build_html の HTML ドキュメント構造検証")
        pedigree_dict_full = {i: f"馬{i}" for i in range(62)}
        html_doc = build_html("テスト馬", pedigree_dict_full, None)
        assert html_doc.startswith("<!DOCTYPE html>"), "DOCTYPE 宣言が含まれていません"
        assert "<table>" in html_doc, "<table> タグが含まれていません"
        assert 'class="sire-side"' in html_doc, "sire-side クラスが含まれていません"
        assert 'class="dam-side"' in html_doc, "dam-side クラスが含まれていません"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: get_pedigree_and_cross_data 実 DB 接続
        # ---------------------------------------------------------
        print("\n[TEST 3] 正常系: get_pedigree_and_cross_data の実 DB 接続・構造検証")
        if not db_available:
            print(f"  SKIP: DB ファイルが存在しません ({DB_PATH})")
        else:
            result = get_pedigree_and_cross_data(TEST_HORSE_ID, logger)
            if result is None:
                print(f"  SKIP: horse_id={TEST_HORSE_ID} は DB 未登録")
            else:
                assert "name" in result, "'name' キーが存在しません"
                assert (
                    "five_gen_ancestors" in result
                ), "'five_gen_ancestors' キーが存在しません"
                # 意図: インブリード計算結果が埋め込まれていることを確認する
                assert "cross_info" in result, "'cross_info' キーが存在しません"
                assert (
                    "horse_crosses" in result["cross_info"]
                ), "'horse_crosses' キーが cross_info に存在しません"
                print(
                    f"  DB 取得成功: name={result['name']} "
                    f"horse_crosses={len(result['cross_info']['horse_crosses'])}件"
                )
            print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 4: generate_pedigree_image 実 DB + subprocess モック
        # ---------------------------------------------------------
        print("\n[TEST 4] 正常系: generate_pedigree_image の実 DB + subprocess モック")
        with (
            patch("src.utils.pedigree_visualizer.HTML_DIR", Path(TEST_HTML_DIR)),
            patch("src.utils.pedigree_visualizer.IMG_DIR", Path(TEST_IMG_DIR)),
            patch("src.utils.pedigree_visualizer.subprocess.run"),
        ):
            generate_pedigree_image(TEST_HORSE_ID, logger)

        if not db_available:
            print(f"  SKIP: DB ファイルが存在しません ({DB_PATH})")
        else:
            expected_html = Path(TEST_HTML_DIR) / f"{TEST_HORSE_ID}.html"
            if not expected_html.exists():
                print(
                    f"  SKIP: horse_id={TEST_HORSE_ID} が DB 未登録のため HTML 非生成"
                )
            else:
                content = expected_html.read_text(encoding="utf-8")
                assert (
                    "<!DOCTYPE html>" in content
                ), "HTML に DOCTYPE 宣言が含まれていません"
                assert "<table>" in content, "HTML に <table> が含まれていません"
                print(f"  HTML ファイル生成・内容確認成功: {expected_html}")
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
    # python -m src.utils.pedigree_visualizer
    _run_tests()
