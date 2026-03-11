"""
src/utils/logger.py

【概要】
プロジェクト全体で使用するロギング設定を構築するユーティリティモジュール。
コンソール出力およびファイルへの永続化の両方に対応し、
競馬予測システムの各コンポーネントにおける実行ログの集約を支援する。

【外部依存】
- OS: ログ保存用ディレクトリの作成権限
- 標準ライブラリ: logging, pathlib

【Usage】
    from src.utils.logger import setup_logger
    import logging

    logger = setup_logger(
        log_filepath="logs/data_collection.log",
        log_level="INFO",
        logger_name=__name__,
    )
    logger.info("処理を開始しました。")
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import os
import shutil
from pathlib import Path
from typing import Dict, Final, Literal, Optional

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# ログ出力で使用する日時フォーマット: 全ハンドラで統一するため定数として一元管理する
LOG_DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"

# 有効なログレベルの文字列→logging 定数マッピング:
# validate_log_level と VALID_LOG_LEVELS を分離することで
# 定数の参照とバリデーションロジックを独立させる
VALID_LOG_LEVELS: Final[Dict[str, int]] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


# ---------------------------------------------------------
# 入力バリデーション
# ---------------------------------------------------------


def validate_log_level(log_level: str) -> int:
    """
    ログレベル文字列を検証し logging 定数へ変換する。

    Args:
        log_level (str): ログレベル文字列 (DEBUG / INFO / WARNING / ERROR / CRITICAL)。
                         大文字・小文字の混在を許容する。

    Returns:
        int: logging モジュールのログレベル定数 (例: logging.INFO = 20)。

    Raises:
        ValueError: 指定された文字列が VALID_LOG_LEVELS に存在しない場合。

    Example:
        >>> validate_log_level("info")
        20
        >>> validate_log_level("INVALID")
        ValueError: 無効なログレベルが指定されました ...
    """
    # 大文字に統一してルックアップし、大小文字の差異を吸収する
    upper_level = log_level.upper()

    # マッピングに存在しない値は即座に例外として呼び出し元へ通知する
    if upper_level not in VALID_LOG_LEVELS:
        raise ValueError(
            f"無効なログレベルが指定されました: {log_level!r}。"
            f"許容される値: {list(VALID_LOG_LEVELS.keys())}"
        )

    return VALID_LOG_LEVELS[upper_level]


# ---------------------------------------------------------
# ログディレクトリ準備
# ---------------------------------------------------------


def ensure_log_directory(log_filepath: str) -> None:
    """
    ログファイルの出力先ディレクトリが存在することを保証する。

    ディレクトリが存在しない場合は中間ディレクトリを含めて再帰的に作成する。
    ルート直下などディレクトリ部分が空の場合は何もしない。

    Args:
        log_filepath (str): ログファイルのパス (例: "logs/app.log")。

    Returns:
        None

    Raises:
        OSError: ディレクトリ作成時に権限不足・ディスクフル等が発生した場合。

    Example:
        >>> ensure_log_directory("logs/subsystem/app.log")
        # logs/subsystem/ ディレクトリが存在しなければ作成される
    """
    parent_dir = Path(log_filepath).parent

    # ルート直下 (parent_dir == ".") の場合は作成不要のためスキップする
    if not parent_dir or parent_dir == Path("."):
        return

    try:
        # exist_ok=True: 既存ディレクトリへの競合エラーを抑制する
        parent_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(
            f"ログディレクトリの作成に失敗しました: {parent_dir} | {e}"
        ) from e


# ---------------------------------------------------------
# ハンドラ生成: コンソール
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
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> handler = create_console_handler(logging.INFO, "%(message)s")
    """
    handler = logging.StreamHandler()
    handler.setLevel(log_level)

    # LOG_DATE_FORMAT を使用して全ハンドラの日時表示を統一する
    handler.setFormatter(logging.Formatter(log_format, datefmt=LOG_DATE_FORMAT))

    return handler


# ---------------------------------------------------------
# ハンドラ生成: ファイル
# ---------------------------------------------------------


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
        >>> handler = create_file_handler("logs/app.log", logging.DEBUG, "%(message)s")
    """
    try:
        # encoding="utf-8" を明示し、日本語ログを文字化けなく出力する
        handler = logging.FileHandler(log_filepath, encoding="utf-8")
    except PermissionError as e:
        raise PermissionError(
            f"ログファイルへの書き込み権限がありません: {log_filepath}"
        ) from e

    handler.setLevel(log_level)

    # LOG_DATE_FORMAT を使用してコンソールハンドラと日時表示を統一する
    handler.setFormatter(logging.Formatter(log_format, datefmt=LOG_DATE_FORMAT))

    return handler


# ---------------------------------------------------------
# メインロガーセットアップ
# ---------------------------------------------------------


def setup_logger(
    log_filepath: str,
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO",
    logger_name: Optional[str] = None,
    log_format_console: str = "[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
    log_format_file: str = "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d] %(message)s",
) -> logging.Logger:
    """
    コンソールおよびファイルへログ出力するロガーを生成・設定する。

    同一名称のロガーが既に設定済みの場合は既存インスタンスをそのまま返し、
    ハンドラの二重登録による多重出力を防止する。

    Args:
        log_filepath (str): ログファイルの保存パス (例: "logs/app.log")。
        log_level (Literal): 出力最小ログレベル。デフォルトは "INFO"。
        logger_name (Optional[str]): ロガーの識別名。None の場合はモジュール名を使用。
        log_format_console (str): コンソール向けログフォーマット。
        log_format_file (str): ファイル向けログフォーマット (行番号等のデバッグ情報を含む)。

    Returns:
        logging.Logger: 設定完了済みの Logger インスタンス。

    Raises:
        ValueError: log_level に無効な文字列が指定された場合。
        OSError: ログディレクトリの作成に失敗した場合。

    Example:
        >>> logger = setup_logger("logs/app.log", log_level="DEBUG", logger_name="myapp")
        >>> logger.info("Logger ready")
    """

    # ---------------------------------------------------------
    # ロガー取得
    # ---------------------------------------------------------

    # 明示指定がない場合はモジュール名を使用し、名前空間の衝突を回避する
    resolved_name = logger_name or __name__
    logger = logging.getLogger(resolved_name)

    # ---------------------------------------------------------
    # ログレベル設定
    # ---------------------------------------------------------

    level = validate_log_level(log_level)
    logger.setLevel(level)

    # ---------------------------------------------------------
    # ハンドラ重複防止
    # ---------------------------------------------------------

    # setup_logger が複数回呼ばれた際の多重ログ出力を防止する
    # 既にハンドラが登録済みの場合はそのまま返す
    if logger.hasHandlers():
        return logger

    # ---------------------------------------------------------
    # ログディレクトリ準備
    # ---------------------------------------------------------

    ensure_log_directory(log_filepath)

    # ---------------------------------------------------------
    # ハンドラ生成・登録
    # ---------------------------------------------------------

    logger.addHandler(create_console_handler(level, log_format_console))
    logger.addHandler(create_file_handler(log_filepath, level, log_format_file))

    return logger


# ---------------------------------------------------------
# ハンドラクローズ補助関数
# ---------------------------------------------------------


def close_logger_handlers(logger_name: Optional[str] = None) -> None:
    """
    指定されたロガーのすべてのハンドラをクローズ・削除する。

    Windowsではファイルハンドラがクローズされていないと、
    ファイル削除時にPermissionErrorが発生するため、
    テスト終了時などにこの関数で明示的にクローズする。

    Args:
        logger_name (Optional[str]): ロガー名。None の場合は root ロガー。

    Returns:
        None

    Example:
        >>> close_logger_handlers("test_logger")
    """
    logger = logging.getLogger(logger_name)

    # 全ハンドラをクローズして logger から削除
    for handler in logger.handlers[:]:  # スライスコピーで安全に列挙
        handler.close()
        logger.removeHandler(handler)


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """主要機能の動作確認テストを実行する。

    正常系・異常系・ファイル内容検証の3軸でテストを実施する。
    テストで生成した一時ディレクトリは finally ブロックで必ず削除する。
    """
    # テスト用の一時ディレクトリ・ファイルパスを定義する
    test_dir = "logs_test_tmp"
    test_file = os.path.join(test_dir, "unit_test.log")

    print("\n" + "=" * 60)
    print("  Unit Test: src/utils/logger.py")
    print("=" * 60 + "\n")

    errors: list[str] = []

    try:
        # ---------------------------------------------------------
        # [Test 1] validate_log_level: 正常系
        # ---------------------------------------------------------
        print("[Test 1] validate_log_level — 正常系")
        level_cases = [
            ("DEBUG", logging.DEBUG),
            ("info", logging.INFO),  # 小文字でも通ること
            ("Warning", logging.WARNING),  # 混在ケース
            ("ERROR", logging.ERROR),
            ("CRITICAL", logging.CRITICAL),
        ]
        for raw, expected in level_cases:
            result = validate_log_level(raw)
            status = "OK" if result == expected else "FAIL"
            print(
                f"  {status}: validate_log_level({raw!r}) -> {result} (expected: {expected})"
            )
            if status == "FAIL":
                errors.append(
                    f"validate_log_level({raw!r}) = {result}, expected {expected}"
                )

        # ---------------------------------------------------------
        # [Test 2] validate_log_level: 異常系
        # ---------------------------------------------------------
        print("\n[Test 2] validate_log_level — 異常系")
        try:
            validate_log_level("INVALID_LEVEL")
            errors.append("ValueError が送出されるべきでしたが送出されませんでした。")
            print("  FAIL: ValueError が送出されませんでした。")
        except ValueError:
            print("  OK: ValueError を正しく捕捉しました。")

        # ---------------------------------------------------------
        # [Test 3] setup_logger: ロガー生成とファイル出力
        # ---------------------------------------------------------
        print("\n[Test 3] setup_logger — ファイル生成・内容検証")
        logger = setup_logger(
            log_filepath=test_file,
            log_level="DEBUG",
            logger_name="test_logger_unit",
        )
        logger.debug("DEBUG テストメッセージ")
        logger.info("INFO テストメッセージ")
        logger.warning("WARNING テストメッセージ")
        logger.error("ERROR テストメッセージ")

        # ファイルが生成されているか確認する
        if os.path.exists(test_file):
            print(f"  OK: ログファイルが生成されました: {test_file}")
        else:
            errors.append("ログファイルが生成されていません。")
            print("  FAIL: ログファイルが生成されていません。")

        # ファイルの内容が書き込まれているか確認する
        with open(test_file, "r", encoding="utf-8") as f:
            content = f.read()

        for marker in [
            "DEBUG テストメッセージ",
            "INFO テストメッセージ",
            "ERROR テストメッセージ",
        ]:
            status = "OK" if marker in content else "FAIL"
            print(f"  {status}: ログ内容に {marker!r} が存在すること")
            if status == "FAIL":
                errors.append(f"ログ内容に {marker!r} が見つかりません。")

        # ---------------------------------------------------------
        # [Test 4] setup_logger: 重複呼び出しでハンドラが増加しないこと
        # ---------------------------------------------------------
        print("\n[Test 4] setup_logger — ハンドラ重複防止")
        handler_count_before = len(logger.handlers)
        # 同一名称で再度呼び出す
        setup_logger(log_filepath=test_file, logger_name="test_logger_unit")
        handler_count_after = len(logger.handlers)
        status = "OK" if handler_count_before == handler_count_after else "FAIL"
        print(
            f"  {status}: ハンドラ数が増加しないこと ({handler_count_before} -> {handler_count_after})"
        )
        if status == "FAIL":
            errors.append(
                f"ハンドラが重複登録されました: {handler_count_before} -> {handler_count_after}"
            )

        # ---------------------------------------------------------
        # [Test 5] ensure_log_directory: ネスト済みディレクトリの作成
        # ---------------------------------------------------------
        print("\n[Test 5] ensure_log_directory — ネストディレクトリ作成")
        nested_path = os.path.join(test_dir, "nested", "deep", "test.log")
        ensure_log_directory(nested_path)
        nested_dir = os.path.dirname(nested_path)
        status = "OK" if os.path.isdir(nested_dir) else "FAIL"
        print(f"  {status}: {nested_dir} が作成されること")
        if status == "FAIL":
            errors.append(f"ネストディレクトリが作成されていません: {nested_dir}")

    except Exception as e:
        errors.append(f"テスト中に予期せぬ例外が発生: {e}")
        print(f"  ERROR: {e}")

    finally:
        # ---------------------------------------------------------
        # ハンドラのクローズ【重要】
        # ---------------------------------------------------------
        # Windowsではファイルハンドラがクローズされていないと、
        # ファイル削除時に PermissionError が発生するため、
        # finally ブロックの最初でハンドラをクローズする
        close_logger_handlers("test_logger_unit")

        # テストで生成した一時ディレクトリをすべて削除する
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
            print(f"\n  CLEANUP: 一時ディレクトリを削除しました: {test_dir}")

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
    # python -m src.utils.logger
    _run_tests()
