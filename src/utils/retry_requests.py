# ファイルパス: src/utils/retry_requests.py

"""
src/utils/retry_requests.py

【概要】
ネットワークの不安定性・一時的なサーバーエラー・レート制限に対処するための
指数バックオフ付きリトライ HTTP リクエストユーティリティモジュール。

リトライ時には指数バックオフ戦略を用いてサーバー負荷を抑えながら再接続を試行する。
主に JBIS (https://www.jbis.or.jp/) など競馬データサイトのスクレイピングで使用する。

【外部依存】
- ネットワーク: 指定 URL (主に JBIS: jbis.or.jp 等) への HTTP リクエスト
- 外部ライブラリ: requests
- 内部モジュール:
    src.utils.logger (setup_logger, close_logger_handlers)

【Usage】
    from src.utils.retry_requests import fetch_html
    from src.utils.logger import setup_logger

    logger = setup_logger(
        log_filepath="logs/retry_requests.log",
        log_level="INFO",
        logger_name="RetryRequests",
    )

    response = fetch_html(
        url="https://www.jbis.or.jp/race/result/.../",
        logger=logger,
        max_retries=3,
    )
    if response is None:
        print("ネットワークエラー")
    elif response.status_code == 404:
        print("ページが見つかりません")
    elif response.status_code == 200:
        print("成功！", response.text)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import shutil
import time
from pathlib import Path
from typing import Callable, Dict, Final, Optional

import requests

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# デフォルト最大リトライ回数: 引数未指定時のフォールバック値
DEFAULT_MAX_RETRIES: Final[int] = 5

# HTTP ステータスコード境界値:
# マジックナンバーを排除し、条件分岐の意図を明確にする
HTTP_SERVER_ERROR_MIN: Final[int] = 500  # 5xx: サーバー側の一時的障害 → リトライ対象
HTTP_SERVER_ERROR_MAX: Final[int] = 599
HTTP_CLIENT_ERROR_MIN: Final[int] = 400  # 4xx: クライアント側の問題 → リトライ不要
HTTP_FORBIDDEN: Final[int] = 403  # 403: IP 制限・アクセス集中の可能性あり

# 競馬関連サイトからのブロックを回避するための標準ブラウザ模倣ヘッダー:
# 関数外で一元管理することで、変更箇所を一点に集約し再利用を可能にする
DEFAULT_HEADERS: Final[Dict[str, str]] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": "https://www.jbis.or.jp/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ---------------------------------------------------------
# メイン HTTP 取得処理
# ---------------------------------------------------------


def fetch_html(
    url: str,
    logger: logging.Logger,
    max_retries: int = DEFAULT_MAX_RETRIES,
    request_interval: float = 3.0,
    retry_interval: float = 2.0,
    timeout: float = 10.0,
    headers: Optional[Dict[str, str]] = None,
    backoff_strategy: Optional[Callable[[int], float]] = None,
) -> Optional[requests.Response]:
    """
    指定された URL に HTTP GET リクエストを送信し、失敗時は指数バックオフでリトライする。

    【返却値の意味】
    - 2xx/3xx：成功。内容を参照してください。
    - 4xx：クライアントエラー（リトライしても解決しない）。status_code で原因を判定。
    - 5xx：サーバーエラー（リトライ超過後）。status_code で原因を判定。
    - None：ネットワークエラー、タイムアウト、通信例外。

    5xx (サーバーエラー) は一時的な障害として最大 max_retries 回リトライする。
    4xx (クライアントエラー) はリトライしても解決しないため即時返却する。
    通信例外はリトライ対象として扱い、上限超過時に None を返す。

    Args:
        url (str): リクエスト対象 URL。空文字・空白のみの場合は即時 None を返す。
        logger (logging.Logger): ログ出力用ロガー。
        max_retries (int): 最大リトライ回数。0 の場合はリトライなし・1回のみ試行。
                           負の値は 0 に補正する。デフォルトは DEFAULT_MAX_RETRIES。
        request_interval (float): 初回リクエスト前の待機時間 (秒)。サーバー負荷軽減用。
        retry_interval (float): リトライ時の基準待機時間 (秒)。バックオフの基底値。
        timeout (float): HTTP タイムアウト秒数。
        headers (Optional[Dict[str, str]]): カスタム HTTP ヘッダー。
                                            None の場合は DEFAULT_HEADERS を使用。
        backoff_strategy (Optional[Callable[[int], float]]): 試行回数を受け取り
                                                              待機秒数を返す関数。
                                                              None の場合は指数バックオフ。

    Returns:
        Optional[requests.Response]:
            - 成功時（2xx/3xx）: Response オブジェクト
            - クライアントエラー時（4xx）: Response オブジェクト
            - サーバーエラー時（5xx、リトライ超過後）: Response オブジェクト
            - 通信エラー時: None

    Raises:
        None: 通信例外・バリデーションエラーは内部でキャッチしログ出力するため
              外部へ伝播しない。

    Example:
        >>> logger = setup_logger("logs/retry_requests.log", logger_name="RetryRequests")
        >>> response = fetch_html("https://www.jbis.or.jp/race/result/.../", logger)
        >>> if response is None:
        ...     print("ネットワークエラー")
        >>> elif response.status_code == 404:
        ...     print("ページが見つかりません")
        >>> elif response.status_code == 200:
        ...     print("成功！")
    """

    # ---------------------------------------------------------
    # 入力バリデーション
    # ---------------------------------------------------------

    # URL が空文字または空白のみの場合はリクエスト不能として早期リターンする
    if not url or not url.strip():
        logger.error("有効な URL が指定されていません。リクエストを中断します。")
        return None

    # 負のリトライ回数は論理的に無効なため 0 (リトライなし) に補正する
    # 0 は「1回だけ試行する」有効な指定のため補正対象外とする
    if max_retries < 0:
        logger.warning(f"不正なリトライ回数 ({max_retries}) を 0 に補正します。")
        max_retries = 0

    # ---------------------------------------------------------
    # リクエスト設定の確定
    # ---------------------------------------------------------

    # ヘッダーが未指定の場合はブラウザ模倣のデフォルトヘッダーを使用する
    req_headers = headers if headers is not None else DEFAULT_HEADERS

    # バックオフ戦略が未指定の場合は指数バックオフ (2^(attempt-1) 倍) を適用する
    # 例: attempt=1 → 2.0秒, attempt=2 → 4.0秒, attempt=3 → 8.0秒
    if backoff_strategy is None:
        backoff_strategy = lambda attempt: retry_interval * (2 ** (attempt - 1))

    # ---------------------------------------------------------
    # HTTP リクエスト実行ループ
    # ---------------------------------------------------------

    with requests.Session() as session:
        # セッション共通ヘッダーを設定し、全リクエストに自動付与する
        session.headers.update(req_headers)

        # attempt=0: 初回試行 / attempt=1〜max_retries: リトライ
        attempt = 0

        while attempt <= max_retries:
            try:

                # -------------------------------------------------
                # 待機処理: サーバー負荷軽減のため適切な Sleep を挿入
                # -------------------------------------------------

                if attempt == 0:
                    # 初回リクエスト前の固定インターバル
                    if request_interval > 0:
                        logger.debug(
                            f"初回リクエスト前の待機を適用: {request_interval} 秒"
                        )
                        time.sleep(request_interval)
                else:
                    # リトライ時は指数バックオフで待機時間を延ばす
                    wait_time = backoff_strategy(attempt)
                    logger.warning(
                        f"リトライ実行中 ({attempt}/{max_retries}): "
                        f"{url} — {wait_time} 秒待機"
                    )
                    time.sleep(wait_time)

                # -------------------------------------------------
                # HTTP GET リクエスト送信
                # -------------------------------------------------

                logger.debug(f"HTTP GET リクエスト送信: {url}")
                response = session.get(url, timeout=timeout)

                # -------------------------------------------------
                # ステータスコード判定
                # -------------------------------------------------
                # 【重要】ここでは例外を発生させず、条件分岐でリトライを制御する
                # こうすることで、5xxでも response オブジェクトを確実に保持できる

                status = response.status_code

                # 1. 成功 (2xx, 3xx)
                if status < HTTP_CLIENT_ERROR_MIN:
                    logger.info(f"リクエスト成功 ({status}): {url}")
                    return response

                # 2. クライアントエラー (4xx)
                if HTTP_CLIENT_ERROR_MIN <= status < HTTP_SERVER_ERROR_MIN:
                    if status == HTTP_FORBIDDEN:
                        logger.error(
                            f"アクセス拒否 (403): IP 制限またはアクセス集中の可能性があります: {url}"
                        )
                    else:
                        logger.error(f"クライアントエラー発生 ({status}): {url}")
                    return response

                # 3. サーバーエラー (5xx) → リトライ対象
                if HTTP_SERVER_ERROR_MIN <= status <= HTTP_SERVER_ERROR_MAX:
                    logger.warning(f"サーバーエラー ({status}) 発生: {url}")

                    # リトライ上限に達したか判定
                    if attempt >= max_retries:
                        logger.error(
                            f"最大リトライ回数 ({max_retries}) を超過しました。"
                            f"取得失敗: {url}"
                        )
                        logger.error(
                            f"最後に受け取った 5xx エラー ({status}) を返却します"
                        )
                        return response

                    # リトライ可能：カウントを進めてループ継続
                    attempt += 1
                    continue

            except (
                requests.exceptions.RequestException,
                requests.exceptions.Timeout,
            ) as ex:
                # 純粋なネットワークエラー・タイムアウト
                attempt += 1
                logger.debug(f"通信例外を捕捉: {ex} (試行回数: {attempt})")

                # 上限に達したか判定
                if attempt > max_retries:
                    logger.error(
                        f"最大リトライ回数 ({max_retries}) を超過しました。"
                        f"取得失敗: {url}"
                    )
                    return None

                # リトライ可能：ループ継続
                continue

    # ループを抜けた場合は全リトライが失敗したことを示す
    # （通常ここに到達することはない）
    return None


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    主要機能の動作確認テストを実行する。

    requests.Session および time.sleep を unittest.mock でモックすることで
    外部ネットワークへの依存を排除し、単体実行を可能にする。
    テスト終了後はログファイルとロガーをすべて解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.utils.retry_requests
        _run_tests()
    """
    from unittest.mock import MagicMock, patch

    from src.utils.logger import close_logger_handlers, setup_logger

    TEST_LOG_DIR: Final[str] = "logs/_test_retry_requests_tmp"
    TEST_LOG_FILE: Final[str] = f"{TEST_LOG_DIR}/test.log"
    TEST_LOGGER_NAME: Final[str] = "test_retry_requests"

    print("=" * 60)
    print(" retry_requests.py 簡易単体テスト 開始")
    print("=" * 60)

    logger = setup_logger(
        log_filepath=TEST_LOG_FILE,
        log_level="DEBUG",
        logger_name=TEST_LOGGER_NAME,
    )

    def _make_mock_session(status_code: int) -> MagicMock:
        """
        指定ステータスコードを返すモック Session を生成する。

        requests.Session をコンテキストマネージャとして使用するため、
        __enter__ / __exit__ を設定したうえで get の戻り値を差し替える。
        """
        mock_response = MagicMock()
        mock_response.status_code = status_code

        mock_instance = MagicMock()
        mock_instance.get.return_value = mock_response
        # 意図: `with requests.Session() as session` の session に mock_instance を束縛する
        mock_instance.__enter__ = MagicMock(return_value=mock_instance)
        mock_instance.__exit__ = MagicMock(return_value=False)
        return mock_instance

    try:
        # ---------------------------------------------------------
        # テスト 1: 異常系 (空 URL → None)
        # ---------------------------------------------------------
        print("\n[TEST 1] 異常系: 空 URL バリデーション")
        for invalid_url in ["", "   "]:
            result = fetch_html(invalid_url, logger)
            assert (
                result is None
            ), f"空 URL {invalid_url!r} に対して None が返りませんでした: {result}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: 異常系 (負の max_retries → 0 に補正されること)
        # ---------------------------------------------------------
        print("\n[TEST 2] 異常系: 負の max_retries を 0 に補正して例外なく動作すること")
        mock_session = _make_mock_session(200)
        with (
            patch("src.utils.retry_requests.time.sleep"),
            patch(
                "src.utils.retry_requests.requests.Session",
                return_value=mock_session,
            ),
        ):
            result = fetch_html(
                "https://www.jbis.or.jp/",
                logger,
                max_retries=-1,
                request_interval=0.0,
            )
        assert result is not None, "負の max_retries 補正後にリクエストが失敗しました"
        assert (
            result.status_code == 200
        ), f"ステータスコードが一致しません: {result.status_code}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: 正常系 (200 OK → Response 返却)
        # ---------------------------------------------------------
        print("\n[TEST 3] 正常系: 200 OK レスポンスの返却")
        mock_session = _make_mock_session(200)
        with (
            patch("src.utils.retry_requests.time.sleep"),
            patch(
                "src.utils.retry_requests.requests.Session",
                return_value=mock_session,
            ),
        ):
            result = fetch_html(
                "https://www.jbis.or.jp/",
                logger,
                max_retries=1,
                request_interval=0.0,
            )
        assert result is not None, "200 OK で None が返りました"
        assert (
            result.status_code == 200
        ), f"ステータスコードが一致しません: {result.status_code}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 4: 異常系 (4xx → 即時返却かつリトライなし)
        # ---------------------------------------------------------
        print("\n[TEST 4] 異常系: 4xx クライアントエラーの即時返却・リトライなし検証")
        mock_session = _make_mock_session(404)
        with (
            patch("src.utils.retry_requests.time.sleep"),
            patch(
                "src.utils.retry_requests.requests.Session",
                return_value=mock_session,
            ),
        ):
            result = fetch_html(
                "https://www.jbis.or.jp/",
                logger,
                max_retries=3,
                request_interval=0.0,
            )
        assert result is not None, "404 で None が返りました"
        assert (
            result.status_code == 404
        ), f"ステータスコードが一致しません: {result.status_code}"
        # 意図: 4xx はリトライしないため session.get の呼び出しが 1 回のみであることを検証する
        call_count = mock_session.get.call_count
        assert (
            call_count == 1
        ), f"4xx でリトライが発生しました: {call_count} 回呼び出し (期待値: 1)"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 5: 異常系 (5xx → リトライ後に Response(500) 返却)
        # ---------------------------------------------------------
        print("\n[TEST 5] 異常系: 5xx サーバーエラーのリトライ後 Response(500) 返却")
        mock_session = _make_mock_session(500)
        with (
            patch("src.utils.retry_requests.time.sleep"),
            patch(
                "src.utils.retry_requests.requests.Session",
                return_value=mock_session,
            ),
        ):
            result = fetch_html(
                "https://www.jbis.or.jp/",
                logger,
                max_retries=2,
                request_interval=0.0,
                retry_interval=0.0,
            )
        assert result is not None, "5xx リトライ超過後に None が返りました"
        assert (
            result.status_code == 500
        ), f"ステータスコードが一致しません: {result.status_code}"
        # 意図: max_retries=2 の場合、初回 + 2回リトライ = 計 3 回呼び出されることを検証する
        call_count = mock_session.get.call_count
        assert (
            call_count == 3
        ), f"5xx のリトライ回数が一致しません: {call_count} 回呼び出し (期待値: 3)"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 6: 正常系 (定数の妥当性検証)
        # ---------------------------------------------------------
        print("\n[TEST 6] 正常系: 定数の妥当性検証")
        assert (
            DEFAULT_MAX_RETRIES > 0
        ), f"DEFAULT_MAX_RETRIES が 0 以下です: {DEFAULT_MAX_RETRIES}"
        assert (
            HTTP_SERVER_ERROR_MIN == 500
        ), "HTTP_SERVER_ERROR_MIN が 500 ではありません"
        assert (
            HTTP_SERVER_ERROR_MAX == 599
        ), "HTTP_SERVER_ERROR_MAX が 599 ではありません"
        assert (
            HTTP_CLIENT_ERROR_MIN == 400
        ), "HTTP_CLIENT_ERROR_MIN が 400 ではありません"
        assert HTTP_FORBIDDEN == 403, "HTTP_FORBIDDEN が 403 ではありません"
        assert (
            "User-Agent" in DEFAULT_HEADERS
        ), "DEFAULT_HEADERS に User-Agent がありません"
        assert "Referer" in DEFAULT_HEADERS, "DEFAULT_HEADERS に Referer がありません"
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
    # python -m src.utils.retry_requests
    _run_tests()
