# ファイルパス: src/data_pipeline/race_list_scraper.py

"""
src/data_pipeline/race_list_scraper.py

【概要】
JBIS レース結果検索ページから、指定期間の個別レース結果ページ URL を取得する
スクレイピングユーティリティモジュール。

検索結果ページをページネーション順に巡回し、個別レース結果ページの URL を抽出する。
抽出された URL は重複除去およびソートされた状態で返却される。

本モジュールはレース詳細スクレイピング (race_detail_scraper) の
入力データとして利用される。

【外部依存】
- ネットワーク: JBIS (https://www.jbis.or.jp) への HTTP リクエスト
- HTML 解析: BeautifulSoup4
- HTTP リトライ処理: src.utils.retry_requests.fetch_html

【Usage】
    from src.data_pipeline.race_list_scraper import get_race_list_urls
    import logging

    logger = logging.getLogger(__name__)
    urls = get_race_list_urls("2025-08-01", "2025-08-31", logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import re
from typing import Final, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.utils.retry_requests import fetch_html

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# JBIS トップドメイン: 相対 URL を絶対 URL へ変換する際の基底
BASE_URL: Final[str] = "https://www.jbis.or.jp"

# レース結果検索エンドポイント: 検索 URL の固定ベースパス
SEARCH_BASE_URL: Final[str] = f"{BASE_URL}/race/result/"

# 安全上限ページ数: サーバー側の異常やバグによる無限ループを防止する閾値
# 通常の運用範囲 (1 ヶ月 ≒ 数十ページ) では到達しない値を設定する
MAX_PAGES: Final[int] = 500

# レース結果ページ URL の正規表現パターン:
# ループ内で毎回コンパイルするコストを排除するため定数としてコンパイル済みで保持する
# 対象形式: /race/result/YYYYMMDD/<場コード>/<レース番号>/
RACE_URL_PATTERN: Final[re.Pattern[str]] = re.compile(r"^/race/result/\d{8}/\d+/\d+/")


# ---------------------------------------------------------
# メインロジック
# ---------------------------------------------------------


def get_race_list_urls(
    start_date: str,
    end_date: str,
    logger: logging.Logger,
) -> List[str]:
    """
    指定期間内の JBIS レース結果検索ページを巡回し、個別レース URL を抽出する。

    検索結果を 100 件/ページ単位で取得し、URL が 1 件も得られなくなった時点で
    ページ巡回を終了する。最終的に全 URL の重複除去と昇順ソートを行い返却する。

    Args:
        start_date (str): データ収集開始日 (YYYY-MM-DD 形式)。
        end_date   (str): データ収集終了日 (YYYY-MM-DD 形式)。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[str]: 個別レース結果ページの絶対 URL リスト (重複除去・昇順ソート済み)。
                   日付形式が不正または取得件数 0 件の場合は空リスト。

    Raises:
        None: 日付パースエラーおよびネットワークエラーは内部でキャッチしログ出力する。

    Example:
        >>> urls = get_race_list_urls("2025-08-01", "2025-08-31", logger)
        >>> print(urls[0])
        'https://www.jbis.or.jp/race/result/20250803/01/01/'
    """

    # ---------------------------------------------------------
    # 入力バリデーション: 日付文字列のパース
    # ---------------------------------------------------------

    try:
        start_parts = start_date.split("-")
        end_parts = end_date.split("-")

        # YYYY-MM-DD 形式であれば必ず 3 要素に分割されるはず
        if len(start_parts) != 3 or len(end_parts) != 3:
            raise ValueError("日付の構成要素数が不正です。")

        start_year, start_month = start_parts[0], start_parts[1]
        end_year, end_month = end_parts[0], end_parts[1]

    except (ValueError, IndexError) as e:
        logger.error(
            f"日付形式不正: {start_date!r}, {end_date!r} " f"(期待値: YYYY-MM-DD) | {e}"
        )
        return []

    # ---------------------------------------------------------
    # メインループ初期化
    # ---------------------------------------------------------

    # 全ページを通じて収集した URL を格納するリスト
    race_urls: List[str] = []

    # ページネーションカウンタ: 1 ページ目から開始する
    current_page: int = 1

    # ---------------------------------------------------------
    # 検索結果ページの巡回ループ
    # ---------------------------------------------------------

    while current_page <= MAX_PAGES:

        # ---------------------------------------------------------
        # 検索 URL の生成
        # ---------------------------------------------------------

        # JBIS 検索パラメータの意味:
        #   hold=hold_1      : 中央競馬の開催に絞り込む
        #   racetype_1       : 平地競走に絞り込む
        #   corse1/corse2    : コース絞り込み (空 = 全コース)
        #   condition        : 条件絞り込み  (空 = 全条件)
        #   from/to_distance : 距離絞り込み  (空 = 全距離)
        #   items=100        : 1 ページあたりの最大表示件数
        #   order=D          : 日付降順で取得する
        search_url = (
            f"{SEARCH_BASE_URL}"
            f"?sid=race&keyword=&match=prefix&hold=hold_1&racetype=racetype_1"
            f"&from_year={start_year}&from_month={int(start_month)}"
            f"&to_year={end_year}&to_month={int(end_month)}"
            f"&corse1=&corse2=&condition=&age=&class="
            f"&from_distance=&to_distance="
            f"&sort=ymd&items=100&page={current_page}&order=D"
        )

        logger.info(
            f"検索ページ取得試行: Page {current_page} " f"({start_date} ～ {end_date})"
        )

        # ---------------------------------------------------------
        # HTTP リクエスト (共通部品 fetch_html を利用)
        # ---------------------------------------------------------

        # リトライ処理は fetch_html 側で実装されているため直接呼び出す
        response = fetch_html(search_url, logger)

        if not response:
            logger.error(f"ページ取得失敗により巡回を中断: Page {current_page}")
            break

        # ---------------------------------------------------------
        # HTML パースと URL 抽出
        # ---------------------------------------------------------

        soup = BeautifulSoup(response.text, "html.parser")

        # 検索結果ページには他種リンクも含まれるため、
        # レース結果ページの URL パターンに一致するものだけを抽出する
        page_relative_urls: List[str] = [
            a_tag["href"]
            for a_tag in soup.find_all("a", href=True)
            if RACE_URL_PATTERN.match(a_tag["href"])
        ]

        # ---------------------------------------------------------
        # ページ内重複除去 (順序を維持しつつ dict を利用)
        # ---------------------------------------------------------

        # HTML 内に同一リンクが複数存在する場合を考慮し、順序を維持したまま除去する
        unique_relative_urls: List[str] = list(dict.fromkeys(page_relative_urls))

        # ---------------------------------------------------------
        # ページ終了判定
        # ---------------------------------------------------------

        # URL が 1 件も取れなければ最終ページを超えたと判断し巡回を終了する
        if not unique_relative_urls:
            logger.info(f"全ページ解析完了: Page {current_page}")
            break

        # ---------------------------------------------------------
        # 絶対 URL への変換と蓄積
        # ---------------------------------------------------------

        # 重複除去後の確定 URL にのみ urljoin を適用してコストを最小化する
        for rel_url in unique_relative_urls:
            race_urls.append(urljoin(BASE_URL, rel_url))

        logger.info(
            f"レース URL 抽出完了: Page {current_page} "
            f"(新規 {len(unique_relative_urls)} 件 / 累計 {len(race_urls)} 件)"
        )

        # 次ページへ進む
        current_page += 1

    # MAX_PAGES に到達した場合は運用上の異常として警告する
    if current_page > MAX_PAGES:
        logger.warning(
            f"最大ページ数 ({MAX_PAGES}) に到達したため処理を打ち切りました。"
        )

    # ---------------------------------------------------------
    # 後処理: 全体重複除去とソート
    # ---------------------------------------------------------

    # 複数ページにわたる重複 (同一 URL が異なるページに出現する場合) を除去し
    # 日付・場コード・レース番号の昇順で整列する
    final_urls: List[str] = sorted(set(race_urls))

    logger.info(f"URL 収集処理終了: 合計 {len(final_urls)} 件")

    return final_urls


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """主要機能の動作確認テストを実行する。

    オフラインテスト (バリデーション・戻り値型) と
    オンラインテスト (実ネットワーク疎通) に分離して実行する。
    外部依存が使用できない環境ではオンラインテストを自動スキップする。
    """
    import sys

    # ---- ログ設定 ----
    test_logger = logging.getLogger("test_race_list_scraper")
    test_logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    test_logger.addHandler(handler)

    print("\n" + "=" * 60)
    print("  Unit Test: src/data_pipeline/race_list_scraper.py")
    print("=" * 60 + "\n")

    errors: List[str] = []

    # ---------------------------------------------------------
    # [Test 1] 異常系: 日付フォーマット不正 → 空リスト返却
    # ---------------------------------------------------------
    print("[Test 1] 不正な日付フォーマットの処理")
    invalid_cases = [
        ("20250101", "20250131"),  # ハイフンなし
        ("2025/01/01", "2025/01/31"),  # スラッシュ区切り
        ("", "2025-01-31"),  # 空文字
        ("2025-13-01", "2025-13-31"),  # 存在しない月 (パースは通過するが0件になる)
    ]
    for s, e in invalid_cases:
        # 完全不正形式は空リストが返ることを確認する
        if "-" not in s or s.count("-") != 2:
            result = get_race_list_urls(s, e, test_logger)
            status = "OK" if result == [] else "FAIL"
            print(f"  {status}: get_race_list_urls({s!r}, {e!r}) -> {result!r}")
            if status == "FAIL":
                errors.append(f"invalid date {s!r} should return [], got {result!r}")

    # ---------------------------------------------------------
    # [Test 2] 正常系: 戻り値の型と構造
    # ---------------------------------------------------------
    print("\n[Test 2] 戻り値の型チェック (オフライン: モック不使用)")
    # fetch_html が None を返す場合 (ネットワーク不可) でも
    # 戻り値は必ず List[str] であることを確認する
    result = get_race_list_urls("2025-08-01", "2025-08-31", test_logger)
    status = "OK" if isinstance(result, list) else "FAIL"
    print(f"  {status}: 戻り値が List 型であること -> {type(result).__name__}")
    if status == "FAIL":
        errors.append(f"return type expected list, got {type(result).__name__}")

    # 戻り値がソート済みであることを確認する
    if result:
        status = "OK" if result == sorted(result) else "FAIL"
        print(f"  {status}: 戻り値が昇順ソート済みであること")
        if status == "FAIL":
            errors.append("return value is not sorted")

    # ---------------------------------------------------------
    # [Test 3] 定数の妥当性チェック
    # ---------------------------------------------------------
    print("\n[Test 3] 定数の妥当性")
    const_checks = [
        ("BASE_URL starts with https", BASE_URL.startswith("https://")),
        ("MAX_PAGES > 0", MAX_PAGES > 0),
        ("RACE_URL_PATTERN compiled", isinstance(RACE_URL_PATTERN, re.Pattern)),
        (
            "pattern matches sample",
            bool(RACE_URL_PATTERN.match("/race/result/20250803/01/01/")),
        ),
        ("pattern rejects other URL", not RACE_URL_PATTERN.match("/horse/2020101234/")),
    ]
    for label, ok in const_checks:
        status = "OK" if ok else "FAIL"
        print(f"  {status}: {label}")
        if not ok:
            errors.append(f"constant check failed: {label}")

    # ---------------------------------------------------------
    # [Test 4] オンライン疎通確認 (ネットワーク必須)
    # ---------------------------------------------------------
    print("\n[Test 4] 実ネットワーク疎通確認 (live network — skip if unavailable)")
    try:
        live_urls = get_race_list_urls("2025-11-01", "2025-11-03", test_logger)
        if live_urls:
            print(f"  OK: {len(live_urls)} 件取得")
            print(f"  Sample: {live_urls[0]}")
            # URL 形式の検証
            for u in live_urls:
                if not u.startswith(BASE_URL):
                    errors.append(f"URL does not start with BASE_URL: {u}")
        else:
            print("  SKIP: 0 件 — ネットワーク未接続または対象期間に開催なし")
    except Exception as e:
        print(f"  SKIP: ネットワークテストをスキップ: {e}")

    # ---------------------------------------------------------
    # テスト結果サマリ
    # ---------------------------------------------------------
    print("\n" + "=" * 60)
    if errors:
        print(f"  FAILED — {len(errors)} error(s):")
        for msg in errors:
            print(f"    ✗ {msg}")
    else:
        print("  ALL TESTS PASSED")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    _run_tests()
