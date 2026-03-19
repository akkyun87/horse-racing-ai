"""
src/utils/pedigree_visualizer.py

【概要】
競走馬の5代血統表をHTMLおよび画像として出力するユーティリティ.
DBから血統データを取得し、正確なインデックス・ラベル対応で血統表をHTMLツリー構造として出力する.
画像変換には wkhtmltoimage を利用する.

【外部依存】
- DB: SQLite (src/utils/db_manager.py 経由)
- ログ: src/utils/logger.py
- 画像変換: wkhtmltoimage

【Usage】
    from src.utils.pedigree_visualizer import generate_pedigree_image
    import logging

    logger = logging.getLogger("example")
    generate_pedigree_image(horse_id="0001352760", logger=logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import subprocess
from pathlib import Path
from typing import Dict, Optional

from src.utils import db_manager
from src.utils.logger import setup_logger

# ---------------------------------------------------------
# 定数・設定
# ---------------------------------------------------------

# 保存先ディレクトリおよび外部ツールのパス
DB_PATH: Path = Path("data/raw/pedigree/pedigree.db")
HTML_DIR: Path = Path("images/pedigree/html")
IMG_DIR: Path = Path("images/pedigree/img")
WKHTMLTOIMAGE: str = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe"

# ---------------------------------------------------------
# 5代血統表インデックス → ラベル対応dict
# ---------------------------------------------------------

PEDIGREE_INDEX_LABELS: Dict[int, str] = {
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


# ---------------------------------------------------------
# 血統データ取得
# ---------------------------------------------------------


def get_pedigree_data(horse_id: str, logger: logging.Logger) -> Optional[dict]:
    """
    指定馬IDの pedigree_info テーブルのレコード全体を返す.

    Args:
        horse_id (str): 馬ID（数値/文字列どちらも可）.
        logger (logging.Logger): ロガー.

    Returns:
        Optional[dict]: レコード全体. 見つからない場合は None.

    Raises:
        例外は握りつぶさず logger で出力し、失敗時は None を返す.

    Example:
        row = get_pedigree_data("0001352760", logger)
    """

    # ---------------------------------------------------------
    # horse_id を10桁0埋めに統一する
    # ---------------------------------------------------------

    try:
        target_id = str(horse_id).zfill(10)
        records = db_manager.load_from_db(str(DB_PATH), "pedigree_info", logger)

        if not records:
            logger.error("pedigree_info テーブルが空、またはDBアクセス失敗")
            return None

        # 対象IDと一致するレコードを線形探索する
        row = next(
            (r for r in records if str(r.get("horse_id")).zfill(10) == target_id),
            None,
        )

    except Exception as e:
        logger.error(f"DBアクセスエラー : {e}")
        return None

    if not row:
        logger.error(f"horse_id {target_id} が見つかりません.")
        return None

    return row


# ---------------------------------------------------------
# HTML構築ユーティリティ
# ---------------------------------------------------------


def get_cell_html(
    idx: int,
    pedigree_dict: Dict[int, str],
    record: Optional[dict],
) -> str:
    """
    指定インデックスのセル内容（馬名＋系統情報）を返す.

    種牡馬（ラベルが「父」で終わる）の場合のみ系統情報を付与する.

    Args:
        idx (int): 血統インデックス.
        pedigree_dict (Dict[int, str]): インデックス → 馬名 dict.
        record (Optional[dict]): DBレコード全体（系統情報付与用）.

    Returns:
        str: セル表示用HTML文字列.

    Example:
        html = get_cell_html(0, pedigree_dict, record)
    """

    name = pedigree_dict.get(idx, "")
    label = PEDIGREE_INDEX_LABELS.get(idx, "")

    # 種牡馬セルにのみ系統情報をサブテキストとして付与する
    if record and label.endswith("父"):
        lineage = record.get(f"lineage_name_{label}", "")
        if lineage:
            return (
                f"{name}<br>"
                f"<span style='font-size:10px;color:#666'>{lineage}</span>"
            )

    return name


def build_html(
    name: str,
    pedigree_dict: Dict[int, str],
    record: Optional[dict],
) -> str:
    """
    血統表HTMLテーブルを組み立てる.

    Args:
        name (str): 馬名.
        pedigree_dict (Dict[int, str]): インデックス → 馬名 dict.
        record (Optional[dict]): DBレコード全体（系統情報付与用）.

    Returns:
        str: HTML文字列.

    Example:
        html = build_html("キタサンブラック", pedigree_dict, record)
    """

    # ---------------------------------------------------------
    # CSSスタイル定義
    # ---------------------------------------------------------

    html_content = f"""
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: 'MS Gothic', 'Meiryo', Arial, sans-serif; margin: 10px; }}
        h2 {{ font-size: 18px; color: #333; border-bottom: 2px solid #333; display: inline-block; }}
        table {{ border-collapse: collapse; border: 2px solid #000; table-layout: fixed; width: 1000px; }}
        td {{ border: 1px solid #444; height: 26px; padding: 2px 5px; font-size: 12px; text-align: center; }}
        .target {{ background: #eee; font-weight: bold; }}
        .sire-side {{ background: #f0faff; }}
        .dam-side {{ background: #fff5f7; }}
    </style>
</head>
<body>
    <h2>{name} 5代血統表</h2>
    <table>
        <tr>
            <td class="target" rowspan="32">{name}</td>
            <td class="sire-side" rowspan="16">{get_cell_html(0, pedigree_dict, record)}</td>
            <td rowspan="8">{get_cell_html(1, pedigree_dict, record)}</td>
            <td rowspan="4">{get_cell_html(3, pedigree_dict, record)}</td>
            <td rowspan="2">{get_cell_html(7, pedigree_dict, record)}</td>
            <td>{get_cell_html(15, pedigree_dict, record)}</td>
        </tr>
        <tr><td>{get_cell_html(16, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="2">{get_cell_html(8, pedigree_dict, record)}</td><td>{get_cell_html(17, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(18, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="4">{get_cell_html(4, pedigree_dict, record)}</td><td rowspan="2">{get_cell_html(9, pedigree_dict, record)}</td><td>{get_cell_html(19, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(20, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="2">{get_cell_html(10, pedigree_dict, record)}</td><td>{get_cell_html(21, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(22, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="8">{get_cell_html(2, pedigree_dict, record)}</td><td rowspan="4">{get_cell_html(5, pedigree_dict, record)}</td><td rowspan="2">{get_cell_html(11, pedigree_dict, record)}</td><td>{get_cell_html(23, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(24, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="2">{get_cell_html(12, pedigree_dict, record)}</td><td>{get_cell_html(25, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(26, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="4">{get_cell_html(6, pedigree_dict, record)}</td><td rowspan="2">{get_cell_html(13, pedigree_dict, record)}</td><td>{get_cell_html(27, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(28, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="2">{get_cell_html(14, pedigree_dict, record)}</td><td>{get_cell_html(29, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(30, pedigree_dict, record)}</td></tr>
        <tr>
            <td class="dam-side" rowspan="16">{get_cell_html(31, pedigree_dict, record)}</td>
            <td rowspan="8">{get_cell_html(32, pedigree_dict, record)}</td>
            <td rowspan="4">{get_cell_html(34, pedigree_dict, record)}</td>
            <td rowspan="2">{get_cell_html(38, pedigree_dict, record)}</td>
            <td>{get_cell_html(46, pedigree_dict, record)}</td>
        </tr>
        <tr><td>{get_cell_html(47, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="2">{get_cell_html(39, pedigree_dict, record)}</td><td>{get_cell_html(48, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(49, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="4">{get_cell_html(35, pedigree_dict, record)}</td><td rowspan="2">{get_cell_html(40, pedigree_dict, record)}</td><td>{get_cell_html(50, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(51, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="2">{get_cell_html(41, pedigree_dict, record)}</td><td>{get_cell_html(52, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(53, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="8">{get_cell_html(33, pedigree_dict, record)}</td><td rowspan="4">{get_cell_html(36, pedigree_dict, record)}</td><td rowspan="2">{get_cell_html(42, pedigree_dict, record)}</td><td>{get_cell_html(54, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(55, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="2">{get_cell_html(43, pedigree_dict, record)}</td><td>{get_cell_html(56, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(57, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="4">{get_cell_html(37, pedigree_dict, record)}</td><td rowspan="2">{get_cell_html(44, pedigree_dict, record)}</td><td>{get_cell_html(58, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(59, pedigree_dict, record)}</td></tr>
        <tr><td rowspan="2">{get_cell_html(45, pedigree_dict, record)}</td><td>{get_cell_html(60, pedigree_dict, record)}</td></tr>
        <tr><td>{get_cell_html(61, pedigree_dict, record)}</td></tr>
    </table>
</body>
</html>
"""
    return html_content


# ---------------------------------------------------------
# メイン実行関数
# ---------------------------------------------------------


def generate_pedigree_image(
    horse_id: str,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    指定馬IDの血統表をHTMLおよびPNG画像として生成・保存する.

    既にHTMLと画像が両方存在する場合はスキップする.

    Args:
        horse_id (str): 馬ID.
        logger (Optional[logging.Logger]): ロガー. 省略時は自動生成.

    Returns:
        None

    Example:
        generate_pedigree_image("0001352760", logger)
    """

    # ---------------------------------------------------------
    # ロガーの初期化（未指定時は自動生成）
    # ---------------------------------------------------------

    if logger is None:
        logger = setup_logger(
            log_filepath="logs/pedigree_visualizer.log",
            log_level="INFO",
            logger_name=__name__,
        )

    safe_id = str(horse_id).zfill(10)
    html_path = HTML_DIR / f"{safe_id}.html"
    img_path = IMG_DIR / f"{safe_id}.png"

    # 既に両ファイルが存在する場合は処理をスキップする
    if html_path.exists() and img_path.exists():
        logger.info(f"スキップ（既存）: {safe_id}")
        return

    HTML_DIR.mkdir(parents=True, exist_ok=True)
    IMG_DIR.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------
    # DBから血統データを取得する
    # ---------------------------------------------------------

    record = get_pedigree_data(horse_id, logger)
    if not record:
        return

    # ---------------------------------------------------------
    # 5代血統表リスト・dictを生成する
    # ---------------------------------------------------------

    name = record["name"]
    raw_list = [x.strip() for x in record["five_gen_ancestors"].split(",") if x.strip()]

    # 先頭に対象馬名が含まれている場合は除去する
    if len(raw_list) == 63:
        raw_list = raw_list[1:]

    # 62件に満たない場合は空文字で補完する
    while len(raw_list) < 62:
        raw_list.append("")

    pedigree_dict: Dict[int, str] = {i: raw_list[i] for i in range(62)}

    # ---------------------------------------------------------
    # HTML生成・ファイル保存
    # ---------------------------------------------------------

    html = build_html(name, pedigree_dict, record)

    with html_path.open("w", encoding="utf-8") as f:
        f.write(html)

    # ---------------------------------------------------------
    # wkhtmltoimage による画像変換
    # ---------------------------------------------------------

    try:
        subprocess.run(
            [
                WKHTMLTOIMAGE,
                "--width",
                "1150",
                "--disable-smart-width",
                str(html_path),
                str(img_path),
            ],
            check=True,
            capture_output=True,
        )
        logger.info(f"画像生成成功 : {img_path}")
    except Exception as e:
        logger.error(f"画像変換エラー : {e}")


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """主要機能の動作確認テストを実行する."""

    logger = setup_logger(
        log_filepath="logs/pedigree_visualizer_test.log",
        log_level="INFO",
        logger_name="pedigree_visualizer_test",
    )

    print("\n--- 正常系 : 画像生成テスト ---")
    generate_pedigree_image("0001352760", logger)

    print("\n--- 異常系 : 存在しないID ---")
    generate_pedigree_image("9999999999", logger)


if __name__ == "__main__":
    # 実行コマンド: python -m src.utils.pedigree_visualizer
    _run_tests()
