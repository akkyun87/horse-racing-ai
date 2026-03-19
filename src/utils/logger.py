# ファイルパス: src/utils/logger.py

"""
src/utils/logger.py

【概要】
プロジェクト全体で使用するロギング設定を構築するユーティリティです。
コンソール出力およびファイルへの永続化の両方に対応し、
競馬予測システムの各コンポーネントにおける実行ログの集約を支援します。

【外部依存】
- OS: ログ保存用ディレクトリの作成権限
- 標準ライブラリ: logging, pathlib, os, typing

【Usage】
    from src.utils.logger import setup_logger, close_logger_handlers

    logger = setup_logger(
        log_filepath="logs/data_collection.log",
        log_level="INFO",
        logger_name="DataCollector",
    )
    logger.info("処理を開始しました。")
    # 終了時
    close_logger_handlers("DataCollector")
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------
import logging
import os
import shutil
from pathlib import Path
from typing import Dict, Final, List, Literal, Optional, Union

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# ログ出力で使用する日時フォーマット（ISO 8601に近い形式で統一）
LOG_DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"

# 有効なログレベルの文字列→logging 定数マッピング
VALID_LOG_LEVELS: Final[Dict[str, int]] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

# ログレベルの型定義
LogLevelLiteral = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# コンソール向けデフォルトログフォーマット
# 意図: 関数シグネチャへのインライン埋め込みを避け、変更箇所をここに一元化する
DEFAULT_LOG_FORMAT_CONSOLE: Final[str] = (
    "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d] %(message)s"
)

# ファイル向けデフォルトログフォーマット（ロガー名を追加してファイル横断検索を容易にする）
DEFAULT_LOG_FORMAT_FILE: Final[str] = (
    "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d][%(name)s] %(message)s"
)

# ---------------------------------------------------------
# 入力バリデーション
# ---------------------------------------------------------


def validate_log_level(log_level: str) -> int:
    """
    ログレベル文字列を検証し logging 定数へ変換する。

    Args:
        log_level (str): ログレベル文字列 (DEBUG / INFO / WARNING / ERROR / CRITICAL)。

    Returns:
        int: logging モジュールのログレベル定数。

    Raises:
        ValueError: 指定された文字列が VALID_LOG_LEVELS に存在しない場合。

    Example:
        >>> level = validate_log_level("INFO")
        >>> print(level)
        20
    """
    # 大文字変換による表記揺れの吸収
    upper_level: str = log_level.upper()

    # 意図: マッピングに存在しない値は、後続の処理でエラーになる前に早期例外を発生させる
    if upper_level not in VALID_LOG_LEVELS:
        allowed_levels: List[str] = list(VALID_LOG_LEVELS.keys())
        raise ValueError(
            f"無効なログレベルが指定されました: {log_level!r}。 "
            f"許容される値: {allowed_levels}"
        )

    return VALID_LOG_LEVELS[upper_level]


# ---------------------------------------------------------
# 外部リソース準備 (ディレクトリ操作)
# ---------------------------------------------------------


def ensure_log_directory(log_filepath: Union[str, Path]) -> None:
    """
    ログファイルの出力先ディレクトリが存在することを保証する。

    Args:
        log_filepath (Union[str, Path]): ログファイルの保存先パス。

    Returns:
        None: 戻り値なし（副作用としてディレクトリを作成）。

    Raises:
        OSError: ディレクトリ作成時に権限不足やディスクフル等が発生した場合。

    Example:
        >>> ensure_log_directory("logs/app.log")
    """
    path_obj: Path = Path(log_filepath)
    parent_dir: Path = path_obj.parent

    # 意図: カレントディレクトリ直下やパス指定がない場合は作成を試みない
    if parent_dir == Path(".") or str(parent_dir) == "":
        return

    try:
        # 意図: 複数スレッド/プロセスからの同時実行を考慮し、exist_ok=True を指定
        parent_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        # 意図: 競馬予測システムなどバッチ処理において、ログ出力不能は致命的なため例外を再送する
        raise OSError(
            f"ログディレクトリの作成に失敗しました: {parent_dir} | 詳細: {e}"
        ) from e


# ---------------------------------------------------------
# ハンドラ生成フェーズ
# ---------------------------------------------------------


def create_console_handler(
    log_level: int,
    log_format: str,
) -> logging.StreamHandler:
    """
    コンソール出力用 StreamHandler を生成する。

    Args:
        log_level (int): 適用するログレベル (logging 定数)。
        log_format (str): ログフォーマット文字列。

    Returns:
        logging.StreamHandler: 設定済みコンソールハンドラ。

    Raises:
        None: 特筆すべき例外なし。

    Example:
        >>> handler = create_console_handler(logging.INFO, "%(message)s")
    """
    handler: logging.StreamHandler = logging.StreamHandler()
    handler.setLevel(log_level)

    # 意図: 日時フォーマットを LOG_DATE_FORMAT 定数で統一し、時系列解析を容易にする
    formatter: logging.Formatter = logging.Formatter(
        log_format, datefmt=LOG_DATE_FORMAT
    )
    handler.setFormatter(formatter)
    return handler


def create_file_handler(
    log_filepath: str,
    log_level: int,
    log_format: str,
) -> logging.FileHandler:
    """
    ファイル出力用 FileHandler を生成する。

    Args:
        log_filepath (str): ログファイルの保存パス。
        log_level (int): 適用するログレベル (logging 定数)。
        log_format (str): ログフォーマット文字列。

    Returns:
        logging.FileHandler: 設定済みファイルハンドラ。

    Raises:
        PermissionError: ログファイルへの書き込み権限がない場合。

    Example:
        >>> handler = create_file_handler("logs/app.log", logging.INFO, "%(message)s")
    """
    try:
        # 意図: 競馬データに含まれる馬名（外字/旧漢字等）を正しく記録するため UTF-8 を指定
        handler: logging.FileHandler = logging.FileHandler(
            log_filepath, encoding="utf-8"
        )
    except (PermissionError, FileNotFoundError) as e:
        raise PermissionError(
            f"ログファイルへのアクセスに失敗しました: {log_filepath} | 詳細: {e}"
        ) from e

    handler.setLevel(log_level)
    formatter: logging.Formatter = logging.Formatter(
        log_format, datefmt=LOG_DATE_FORMAT
    )
    handler.setFormatter(formatter)
    return handler


# ---------------------------------------------------------
# メインロガーセットアップフェーズ
# ---------------------------------------------------------


def setup_logger(
    log_filepath: str,
    log_level: LogLevelLiteral = "INFO",
    logger_name: Optional[str] = None,
    log_format_console: str = DEFAULT_LOG_FORMAT_CONSOLE,
    log_format_file: str = DEFAULT_LOG_FORMAT_FILE,
) -> logging.Logger:
    """
    コンソールおよびファイルへログ出力するロガーを生成・設定する。

    Args:
        log_filepath (str): ログファイルの保存パス。
        log_level (LogLevelLiteral): 出力最小ログレベル。デフォルトは "INFO"。
        logger_name (Optional[str]): ロガーの識別名。None の場合はモジュール名。
        log_format_console (str): コンソール向けログフォーマット。
                                  デフォルトは DEFAULT_LOG_FORMAT_CONSOLE。
        log_format_file (str): ファイル向けログフォーマット。
                               デフォルトは DEFAULT_LOG_FORMAT_FILE。

    Returns:
        logging.Logger: 設定完了済みの Logger インスタンス。

    Raises:
        ValueError: ログレベルが不正な場合。
        OSError: ディレクトリ作成に失敗した場合。

    Example:
        >>> logger = setup_logger("logs/test.log", log_level="DEBUG", logger_name="TestLogger")
        >>> logger.info("Setup complete")
    """
    # ロガーの初期化
    resolved_name: str = logger_name or __name__
    logger: logging.Logger = logging.getLogger(resolved_name)

    # ログレベルの設定
    level: int = validate_log_level(log_level)
    logger.setLevel(level)

    # 意図: 同一名のロガーが再定義された際、ハンドラが重複しログが二重・三重に出力されるのを防ぐ
    if logger.hasHandlers():
        return logger

    # 外部リソース（出力先）の準備
    ensure_log_directory(log_filepath)

    # ハンドラの登録
    # 意図: コンソールハンドラとファイルハンドラを個別に生成し、ロガーに紐付ける
    logger.addHandler(create_console_handler(level, log_format_console))
    logger.addHandler(create_file_handler(log_filepath, level, log_format_file))

    # 意図: 親ロガーへの伝播を抑制し、意図しない場所（ルートロガー等）での重複出力を防ぐ
    logger.propagate = False

    return logger


# ---------------------------------------------------------
# 終了処理（リソース解放）
# ---------------------------------------------------------


def close_logger_handlers(logger_name: Optional[str] = None) -> None:
    """
    指定されたロガーのすべてのハンドラをクローズし、ロガーから除去する。

    Args:
        logger_name (Optional[str]): 対象のロガー名。None の場合はルートロガー周辺。

    Returns:
        None: 戻り値なし。

    Raises:
        None: 終了処理中のエラーは安全のため内部で無視される。

    Example:
        >>> close_logger_handlers("DataCollector")
    """
    target_logger: logging.Logger = logging.getLogger(logger_name)

    # 意図: イテレート中のリストから要素を削除する際の不具合を避けるためスライスコピーを使用
    for handler in target_logger.handlers[:]:
        try:
            handler.close()
            target_logger.removeHandler(handler)
        except Exception:
            pass


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    主要機能の動作確認テストを実行する。

    外部依存（DB・ネットワーク）を持たず、一時ディレクトリのみを使用するため
    単体実行が可能。テスト終了後はファイル・ロガーを必ず解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.utils.logger
        _run_tests()
    """
    TEST_LOG_DIR: Final[str] = "logs/_test_logger_tmp"
    TEST_LOG_FILE: Final[str] = f"{TEST_LOG_DIR}/unit_test.log"
    TEST_LOGGER_NAME: Final[str] = "test_logger_internal"

    print("=" * 60)
    print(" logger.py 簡易単体テスト 開始")
    print("=" * 60)

    try:
        # ---------------------------------------------------------
        # テスト 1: 正常系 (ログレベル変換)
        # ---------------------------------------------------------
        print("\n[TEST 1] 正常系: ログレベル文字列の変換")
        assert validate_log_level("DEBUG") == logging.DEBUG
        assert validate_log_level("info") == logging.INFO  # 小文字許容
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: 正常系 (ロガー生成とファイル出力)
        # ---------------------------------------------------------
        print("\n[TEST 2] 正常系: ロガーセットアップと書き込み検証")
        logger = setup_logger(
            log_filepath=TEST_LOG_FILE,
            log_level="DEBUG",
            logger_name=TEST_LOGGER_NAME,
        )

        test_msg = "Test log message for horse_id: 2021100001 (🐴)"
        logger.debug(test_msg)

        # ファイルの存在確認
        assert os.path.exists(TEST_LOG_FILE), "ログファイルが生成されていません"

        # 内容の検証
        with open(TEST_LOG_FILE, "r", encoding="utf-8") as f:
            content = f.read()
            assert test_msg in content, "ログメッセージがファイルに書き込まれていません"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: 異常系 (無効なログレベル)
        # ---------------------------------------------------------
        print("\n[TEST 3] 異常系: 無効なログレベル指定")
        try:
            validate_log_level("INVALID")
            assert False, "ValueError が発生しませんでした"
        except ValueError:
            print("  -> PASS (Expected error caught)")

        # ---------------------------------------------------------
        # テスト 4: 正常系 (同一名ロガーの重複防止)
        # ---------------------------------------------------------
        print(
            "\n[TEST 4] 正常系: 同一ロガー名での再呼び出し時にハンドラが重複しないこと"
        )
        # 意図: hasHandlers() 分岐を経由して既存ロガーがそのまま返ることを検証する
        logger_again = setup_logger(
            log_filepath=TEST_LOG_FILE,
            log_level="DEBUG",
            logger_name=TEST_LOGGER_NAME,
        )
        handler_count: int = len(logger_again.handlers)
        assert handler_count == 2, (
            f"ハンドラ数が期待値(2)と異なります: {handler_count} "
            f"(重複登録が発生している可能性があります)"
        )
        print("  -> PASS")

    except AssertionError as e:
        print(f"\n[FAIL] アサーション失敗: {e}")
    except Exception as e:
        print(f"\n[FAIL] 予期しないエラー: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # テストリソースのクリーンアップ
        close_logger_handlers(TEST_LOGGER_NAME)
        if Path(TEST_LOG_DIR).exists():
            shutil.rmtree(TEST_LOG_DIR)
            print(f"\nCLEANUP: {TEST_LOG_DIR} を削除しました。")

    print("\n" + "=" * 60)
    print(" 全テスト完了")
    print("=" * 60)


if __name__ == "__main__":
    # python -m src.utils.logger
    _run_tests()
