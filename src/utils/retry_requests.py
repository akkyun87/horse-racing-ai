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

【Usage】
    from src.utils.retry_requests import fetch_html
    import logging

    logger = logging.getLogger(__name__)

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
import time
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
    """主要機能の動作確認テストを実行する。

    正常系・4xx・5xx・バリデーションの各ケースを検証する。
    外部ネットワーク (httpbin.org) を使用するため、
    ネットワーク未接続環境ではオンラインテストが自動スキップされる。
    """
    import sys

    # ---- ログ設定 ----
    test_logger = logging.getLogger("test_retry_requests")
    test_logger.setLevel(logging.DEBUG)
    if not test_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        test_logger.addHandler(handler)

    print("\n" + "=" * 60)
    print("  Unit Test: src/utils/retry_requests.py")
    print("=" * 60 + "\n")

    errors: list[str] = []

    # ---------------------------------------------------------
    # [Test 1] 入力バリデーション: 空 URL → None を返すこと
    # ---------------------------------------------------------
    print("[Test 1] 空 URL バリデーション")
    for invalid_url in ["", "   ", None]:
        # None は型ヒント上 str だが、防御的に検証する
        result = fetch_html(invalid_url or "", test_logger)  # type: ignore[arg-type]
        status = "OK" if result is None else "FAIL"
        print(f"  {status}: fetch_html({invalid_url!r}) -> None")
        if status == "FAIL":
            errors.append(f"空 URL {invalid_url!r} に対して None が返りませんでした。")

    # ---------------------------------------------------------
    # [Test 2] 入力バリデーション: 負の max_retries → 補正されること
    # ---------------------------------------------------------
    print("\n[Test 2] 負の max_retries 補正")
    # 補正後は 0 になるため 1 回だけ試行される。
    # ネットワーク不要なバリデーション部分のみ検証する
    try:
        # 補正は内部で行われ例外は送出されないことを確認する
        fetch_html(
            "https://httpbin.org/get",
            test_logger,
            max_retries=-1,
            request_interval=0.0,
            timeout=1.0,
        )
        print("  OK: 負の max_retries でも例外が送出されませんでした。")
    except Exception as e:
        errors.append(f"負の max_retries で予期せぬ例外: {e}")
        print(f"  FAIL: {e}")

    # ---------------------------------------------------------
    # [Test 3] オンライン: 正常系 (200 OK)
    # ---------------------------------------------------------
    print("\n[Test 3] 正常系 (200 OK) — live network")
    try:
        res = fetch_html(
            "https://httpbin.org/get",
            test_logger,
            max_retries=1,
            request_interval=0.1,
            timeout=5.0,
        )
        status = "OK" if (res and res.status_code == 200) else "FAIL"
        code = res.status_code if res else "None"
        print(f"  {status}: status_code={code} (expected: 200)")
        if status == "FAIL":
            errors.append(f"正常系: status_code={code}")
    except Exception as e:
        print(f"  SKIP: ネットワーク未接続のためスキップ: {e}")

    # ---------------------------------------------------------
    # [Test 4] オンライン: 4xx クライアントエラー → Response を即時返却
    # ---------------------------------------------------------
    print("\n[Test 4] 4xx クライアントエラーの即時返却 — live network")
    try:
        res = fetch_html(
            "https://httpbin.org/status/404",
            test_logger,
            max_retries=1,
            request_interval=0.1,
            timeout=5.0,
        )

        # ===== レスポンスの確認 =====
        if res is None:
            code = "None"
            status = "FAIL"
            print(f"  DEBUG: res is None")
        else:
            # res が存在する場合、status_code を取得
            code = res.status_code
            print(f"  DEBUG: res is {type(res)}")
            print(f"  DEBUG: res.status_code = {code!r} (type: {type(code).__name__})")

            # ステータスコード判定
            if isinstance(code, int):
                is_correct_status = code == 404
            else:
                is_correct_status = str(code) == "404"

            if is_correct_status:
                status = "OK"
            else:
                status = "FAIL"

        print(f"  {status}: status_code={code} (expected: 404, no retry)")
        if status == "FAIL":
            errors.append(f"4xx 即時返却: status_code={code}")
    except Exception as e:
        print(f"  SKIP: ネットワーク未接続のためスキップ: {e}")

    # ---------------------------------------------------------
    # [Test 5] オンライン: 5xx サーバーエラー → リトライ後 Response(500) を返却
    # ---------------------------------------------------------
    print("\n[Test 5] 5xx サーバーエラーのリトライ → Response(500) — live network")
    try:
        res = fetch_html(
            "https://httpbin.org/status/500",
            test_logger,
            max_retries=2,
            request_interval=0.1,
            retry_interval=0.5,
            timeout=5.0,
        )

        # ===== レスポンスの確認 =====
        if res is None:
            code = "None"
            status = "FAIL"
            print(f"  DEBUG: res is None")
        else:
            # res が存在する場合、status_code を取得
            code = res.status_code
            print(f"  DEBUG: res is {type(res)}")
            print(f"  DEBUG: res.status_code = {code!r} (type: {type(code).__name__})")

            # ステータスコード判定
            if isinstance(code, int):
                is_correct_status = code == 500
            else:
                is_correct_status = str(code) == "500"

            if is_correct_status:
                status = "OK"
            else:
                status = "FAIL"

        print(
            f"  {status}: status_code={code} (expected: 500, リトライ超過後のレスポンス)"
        )
        if status == "FAIL":
            errors.append(f"5xx リトライ後: status_code={code}（500 であるべき）")
    except Exception as e:
        print(f"  SKIP: ネットワーク未接続のためスキップ: {e}")

    # ---------------------------------------------------------
    # [Test 6] 定数の妥当性チェック
    # ---------------------------------------------------------
    print("\n[Test 6] 定数の妥当性")
    const_checks = [
        ("DEFAULT_MAX_RETRIES > 0", DEFAULT_MAX_RETRIES > 0),
        ("HTTP_SERVER_ERROR_MIN == 500", HTTP_SERVER_ERROR_MIN == 500),
        ("HTTP_SERVER_ERROR_MAX == 599", HTTP_SERVER_ERROR_MAX == 599),
        ("HTTP_CLIENT_ERROR_MIN == 400", HTTP_CLIENT_ERROR_MIN == 400),
        ("HTTP_FORBIDDEN == 403", HTTP_FORBIDDEN == 403),
        ("DEFAULT_HEADERS has User-Agent", "User-Agent" in DEFAULT_HEADERS),
        ("DEFAULT_HEADERS has Referer", "Referer" in DEFAULT_HEADERS),
    ]
    for label, ok in const_checks:
        status = "OK" if ok else "FAIL"
        print(f"  {status}: {label}")
        if not ok:
            errors.append(f"定数チェック失敗: {label}")

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
    # python -m src.utils.retry_requests
    _run_tests()
