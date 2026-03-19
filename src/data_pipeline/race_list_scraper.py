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
- 内部モジュール:
    src.utils.retry_requests (fetch_html)
    src.utils.logger         (setup_logger, close_logger_handlers)

【Usage】
    from src.data_pipeline.race_list_scraper import get_race_list_urls
    from src.utils.logger import setup_logger

    logger = setup_logger(
        log_filepath="logs/race_list_scraper.log",
        log_level="INFO",
        logger_name="RaceListScraper",
    )
    urls = get_race_list_urls("2025-08-01", "2025-08-31", logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import re
import shutil
from pathlib import Path
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
        >>> logger = setup_logger("logs/race_list.log", logger_name="RaceListScraper")
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
    """
    主要機能の動作確認テストを実行する。

    fetch_html を unittest.mock.patch でモックすることで外部ネットワークへの
    依存を排除し、単体実行を可能にする。
    テスト終了後はログファイルとロガーをすべて解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.data_pipeline.race_list_scraper
        _run_tests()
    """
    from unittest.mock import MagicMock, patch

    from src.utils.logger import close_logger_handlers, setup_logger

    TEST_LOG_DIR: Final[str] = "logs/_test_race_list_scraper_tmp"
    TEST_LOG_FILE: Final[str] = f"{TEST_LOG_DIR}/test.log"
    TEST_LOGGER_NAME: Final[str] = "test_race_list_scraper"

    print("=" * 60)
    print(" race_list_scraper.py 簡易単体テスト 開始")
    print("=" * 60)

    logger = setup_logger(
        log_filepath=TEST_LOG_FILE,
        log_level="DEBUG",
        logger_name=TEST_LOGGER_NAME,
    )

    def _make_mock_response(html: str) -> MagicMock:
        """
        指定 HTML テキストを返すモック Response を生成する。
        `if not response:` の判定が False になるよう __bool__ は MagicMock デフォルト (True) を維持する。
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = html
        return mock_resp

    try:
        # ---------------------------------------------------------
        # テスト 1: 異常系 (不正日付形式 → 空リスト返却)
        # ---------------------------------------------------------
        print("\n[TEST 1] 異常系: 不正な日付形式で空リストが返ること")
        # 意図: ハイフン区切りでない日付はパース段階で弾かれることを検証する
        invalid_format_cases = [
            ("20250101", "20250131"),  # ハイフンなし
            ("2025/01/01", "2025/01/31"),  # スラッシュ区切り
            ("", "2025-01-31"),  # 空文字
        ]
        for s, e in invalid_format_cases:
            result = get_race_list_urls(s, e, logger)
            assert result == [], f"不正日付 {s!r} で空リスト以外が返りました: {result}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: 異常系 (fetch_html が None → 空リスト返却)
        # ---------------------------------------------------------
        print("\n[TEST 2] 異常系: fetch_html=None 時に空リストが返ること")
        # 意図: ネットワーク断等で fetch_html が None を返した場合のフォールバックを検証する
        with patch(
            "src.data_pipeline.race_list_scraper.fetch_html",
            return_value=None,
        ):
            result = get_race_list_urls("2025-08-01", "2025-08-31", logger)
        assert result == [], f"fetch_html=None のとき空リスト以外が返りました: {result}"
        assert isinstance(
            result, list
        ), f"戻り値が List 型ではありません: {type(result)}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: 正常系 (ダミーHTML → URL 抽出・重複除去・ソート検証)
        # ---------------------------------------------------------
        print("\n[TEST 3] 正常系: ダミー HTML からの URL 抽出・重複除去・ソート検証")
        # 意図: 重複リンク・レース以外のリンクが正しくフィルタされることを検証する
        dummy_html = (
            "<html><body>"
            '<a href="/race/result/20251103/01/02/">R2</a>'
            '<a href="/race/result/20251101/01/01/">R1</a>'
            # 意図: 同一URLの重複 → 最終的に1件のみカウントされるべき
            '<a href="/race/result/20251101/01/01/">R1(重複)</a>'
            # 意図: 馬情報ページは RACE_URL_PATTERN に一致しないため除外される
            '<a href="/horse/2020101234/">horse(除外対象)</a>'
            "</body></html>"
        )
        mock_resp = _make_mock_response(dummy_html)

        # page 1: ダミーHTML を返す / page 2: None → ループ終了
        with patch(
            "src.data_pipeline.race_list_scraper.fetch_html",
            side_effect=[mock_resp, None],
        ):
            result = get_race_list_urls("2025-11-01", "2025-11-03", logger)

        assert isinstance(
            result, list
        ), f"戻り値が List 型ではありません: {type(result)}"
        assert (
            len(result) == 2
        ), f"重複除去後の件数が一致しません: {len(result)} (期待値: 2)"
        # 意図: 全 URL が絶対 URL に変換されていることを確認する
        assert all(
            u.startswith(BASE_URL) for u in result
        ), f"絶対 URL への変換に失敗しています: {result}"
        # 意図: 昇順ソートの検証 (R1=20251101 が R2=20251103 より前に並ぶ)
        assert result == sorted(result), f"戻り値が昇順ソートされていません: {result}"
        assert f"{BASE_URL}/race/result/20251101/01/01/" in result
        assert f"{BASE_URL}/race/result/20251103/01/02/" in result
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 4: 正常系 (定数の妥当性検証)
        # ---------------------------------------------------------
        print("\n[TEST 4] 正常系: 定数の妥当性検証")
        assert BASE_URL.startswith(
            "https://"
        ), f"BASE_URL が https:// で始まりません: {BASE_URL}"
        assert MAX_PAGES > 0, f"MAX_PAGES が 0 以下です: {MAX_PAGES}"
        assert isinstance(
            RACE_URL_PATTERN, re.Pattern
        ), "RACE_URL_PATTERN が re.Pattern 型ではありません"
        # 意図: 正規パターンに合致するサンプル URL を検証する
        assert RACE_URL_PATTERN.match(
            "/race/result/20250803/01/01/"
        ), "RACE_URL_PATTERN が正規 URL に一致しません"
        # 意図: 馬情報ページが誤ってマッチしないことを検証する
        assert not RACE_URL_PATTERN.match(
            "/horse/2020101234/"
        ), "RACE_URL_PATTERN が馬情報 URL に誤マッチしています"
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
    # python -m src.data_pipeline.race_list_scraper
    _run_tests()
