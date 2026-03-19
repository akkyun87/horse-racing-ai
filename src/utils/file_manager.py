# ファイルパス: src/utils/file_manager.py

"""
src/utils/file_manager.py

【概要】
JSON および YAML 形式のファイル入出力処理を共通化するユーティリティモジュール。

ログ出力・例外処理・ディレクトリ自動生成を内包し、
競馬予測システムの構成設定（config）や、スクレイピング工程の中間データ、
モデルのハイパーパラメータなどの保存・読込を安全に行う。

【外部依存】
- ライブラリ: PyYAML (yaml)
- ファイルシステム: 対象ディレクトリへの書き込み権限が必要
- 内部モジュール:
    src.utils.logger (setup_logger, close_logger_handlers)

【Usage】
    from src.utils.file_manager import save_data, load_data
    from src.utils.logger import setup_logger

    logger = setup_logger(
        log_filepath="logs/file_manager.log",
        log_level="INFO",
        logger_name="FileManager",
    )

    data = {"race_id": "20240101", "weather": "Sunny", "track_condition": "Good"}

    # 拡張子に基づき自動判定して保存
    save_data(data, "data/intermediate/race_info.json", logger)

    # 拡張子に基づき自動判定して読込
    config = load_data("config/settings.yaml", logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import json
import logging
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Final, Optional

import yaml

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# 対応している拡張子の定義
SUPPORTED_JSON_EXTS: Final[tuple[str, ...]] = (".json",)
SUPPORTED_YAML_EXTS: Final[tuple[str, ...]] = (".yaml", ".yml")


# ---------------------------------------------------------
# 内部補助関数 (JSON)
# ---------------------------------------------------------


def _save_json(
    data: Dict[str, Any],
    file_path: Path,
    indent: int = 4,
    ensure_ascii: bool = False,
) -> None:
    """
    辞書データを JSON 形式でファイルへ書き出す。

    Args:
        data (Dict[str, Any]): 保存対象の辞書データ。
        file_path (Path): 保存先の Path オブジェクト。
        indent (int): インデントの幅。デフォルトは 4。
        ensure_ascii (bool): True の場合、非 ASCII 文字をエスケープする。
                             日本語を保持するためデフォルトは False。

    Returns:
        None: 戻り値なし（副作用としてファイルを書き出す）。

    Raises:
        json.JSONEncodeError: データのシリアライズに失敗した場合。
        OSError: ファイルへの書き込みに失敗した場合。

    Example:
        >>> _save_json({"key": "val"}, Path("out.json"))
    """
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=ensure_ascii, indent=indent)


def _load_json(file_path: Path) -> Dict[str, Any]:
    """
    JSON ファイルを読み込み、辞書としてパースする。

    Args:
        file_path (Path): 読み込み対象の Path オブジェクト。

    Returns:
        Dict[str, Any]: パースされた辞書データ。

    Raises:
        json.JSONDecodeError: JSON のパースに失敗した場合。
        OSError: ファイルの読み込みに失敗した場合。

    Example:
        >>> data = _load_json(Path("input.json"))
    """
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------
# 内部補助関数 (YAML)
# ---------------------------------------------------------


def _save_yaml(data: Dict[str, Any], file_path: Path) -> None:
    """
    辞書データを YAML 形式でファイルへ書き出す。

    Args:
        data (Dict[str, Any]): 保存対象の辞書データ。
        file_path (Path): 保存先の Path オブジェクト。

    Returns:
        None: 戻り値なし（副作用としてファイルを書き出す）。

    Raises:
        yaml.YAMLError: データのシリアライズに失敗した場合。
        OSError: ファイルへの書き込みに失敗した場合。

    Example:
        >>> _save_yaml({"key": "val"}, Path("out.yaml"))
    """
    with file_path.open("w", encoding="utf-8") as f:
        # allow_unicode=True により日本語をネイティブな文字として出力
        # sort_keys=False によりデータ構造の順序を維持
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def _load_yaml(file_path: Path) -> Dict[str, Any]:
    """
    YAML ファイルを読み込み、辞書としてパースする。

    Args:
        file_path (Path): 読み込み対象の Path オブジェクト。

    Returns:
        Dict[str, Any]: パースされた辞書データ。

    Raises:
        yaml.YAMLError: YAML のパースに失敗した場合。
        OSError: ファイルの読み込みに失敗した場合。

    Example:
        >>> data = _load_yaml(Path("config.yaml"))
    """
    with file_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------
# 公開 API: データ保存処理
# ---------------------------------------------------------


def save_data(
    data: Dict[str, Any],
    file_path: str,
    logger: logging.Logger,
    indent: int = 4,
) -> bool:
    """
    拡張子に応じて JSON または YAML 形式でデータを保存する。

    対象ファイルの拡張子（.json, .yaml, .yml）を自動判別し、
    適切な内部保存関数を呼び出す。ディレクトリが存在しない場合は自動生成する。

    Args:
        data (Dict[str, Any]): 保存対象のデータ（辞書形式）。
        file_path (str): 保存先のファイルパス。
        logger (logging.Logger): ログ出力用ロガーインスタンス。
        indent (int): JSON 形式で使用するインデント幅。YAML では無視される。

    Returns:
        bool: 保存に成功した場合は True、バリデーション失敗や I/O エラー時は False。

    Raises:
        None: json.JSONEncodeError / yaml.YAMLError / OSError は内部で捕捉し、
              ERROR ログを出力する。

    Example:
        >>> logger = setup_logger("logs/file_manager.log", logger_name="FileManager")
        >>> success = save_data({"key": "value"}, "output.json", logger)
        >>> print(success)
        True
    """
    # ---------------------------------------------------------
    # 入力バリデーション
    # ---------------------------------------------------------
    if not isinstance(data, dict):
        logger.error(f"保存対象が辞書型ではありません: {type(data)}")
        return False

    path = Path(file_path)

    # ---------------------------------------------------------
    # 保存ディレクトリ準備
    # ---------------------------------------------------------
    try:
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"ディレクトリを作成しました: {path.parent}")
    except OSError as e:
        logger.error(f"ディレクトリの作成に失敗しました: {path.parent} | {e}")
        return False

    # ---------------------------------------------------------
    # 形式判定と実行
    # ---------------------------------------------------------
    try:
        ext = path.suffix.lower()

        if ext in SUPPORTED_JSON_EXTS:
            _save_json(data, path, indent)
        elif ext in SUPPORTED_YAML_EXTS:
            _save_yaml(data, path)
        else:
            logger.error(f"未対応の拡張子です: {ext!r} (path={file_path})")
            return False

        logger.info(f"ファイルを保存しました: {file_path}")
        return True

    except (json.JSONEncodeError, yaml.YAMLError) as e:
        logger.error(f"シリアライズエラーが発生しました: {file_path} | {e}")
        return False
    except Exception as e:
        logger.error(f"データ保存中に予期せぬエラーが発生しました: {file_path} | {e}")
        return False


# ---------------------------------------------------------
# 公開 API: データ読み込み処理
# ---------------------------------------------------------


def load_data(file_path: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    拡張子に応じて JSON または YAML ファイルを読み込む。

    ファイルの存在確認を行い、拡張子に基づいてパースを行う。
    パースエラーやファイル未存在時は None を返し、詳細をログに出力する。

    Args:
        file_path (str): 読み込み対象のファイルパス。
        logger (logging.Logger): ログ出力用ロガーインスタンス。

    Returns:
        Optional[Dict[str, Any]]:
            成功時: 読み込んだデータの辞書。
            ファイル未存在、未対応形式、またはパースエラー時: None。

    Raises:
        None: json.JSONDecodeError / yaml.YAMLError / OSError は内部で捕捉し、
              ERROR ログを出力する。

    Example:
        >>> logger = setup_logger("logs/file_manager.log", logger_name="FileManager")
        >>> config = load_data("config/settings.yaml", logger)
        >>> if config is not None:
        ...     print(config.get("race_id"))
    """
    # ---------------------------------------------------------
    # 読み込み前チェック
    # ---------------------------------------------------------
    path = Path(file_path)
    if not path.exists():
        logger.warning(f"読み込み対象のファイルが存在しません: {file_path}")
        return None

    # ---------------------------------------------------------
    # 形式判定と実行
    # ---------------------------------------------------------
    try:
        ext = path.suffix.lower()

        if ext in SUPPORTED_JSON_EXTS:
            data = _load_json(path)
        elif ext in SUPPORTED_YAML_EXTS:
            data = _load_yaml(path)
        else:
            logger.error(f"未対応の拡張子です: {ext!r} (path={file_path})")
            return None

        logger.info(f"ファイルを正常に読み込みました: {file_path}")
        return data

    except (json.JSONDecodeError, yaml.YAMLError) as e:
        logger.error(f"パースエラーが発生しました: {file_path} | {e}")
        return None
    except Exception as e:
        logger.error(
            f"データ読み取り中に予期せぬエラーが発生しました: {file_path} | {e}"
        )
        return None


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    主要機能の動作確認テストを実行する。

    外部依存（DB・ネットワーク）を持たず、一時ディレクトリ上のファイルのみを使用するため
    単体実行が可能。テスト終了後はファイル・ロガーをすべて解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.utils.file_manager
        _run_tests()
    """
    from src.utils.logger import close_logger_handlers, setup_logger

    TEST_LOG_DIR: Final[str] = "logs/_test_file_manager_tmp"
    TEST_DATA_DIR: Final[str] = "data/_test_file_manager_tmp"
    TEST_LOG_FILE: Final[str] = f"{TEST_LOG_DIR}/test.log"
    TEST_LOGGER_NAME: Final[str] = "test_file_manager"

    print("=" * 60)
    print(" file_manager.py 簡易単体テスト 開始")
    print("=" * 60)

    logger = setup_logger(
        log_filepath=TEST_LOG_FILE,
        log_level="DEBUG",
        logger_name=TEST_LOGGER_NAME,
    )

    try:
        sample_data = {
            "horse_name": "コントレイル",
            "results": [1, 1, 1, 2],
            "metadata": {"retired": True},
        }

        # ---------------------------------------------------------
        # テスト 1: 正常系 (JSON 保存と読み込み)
        # ---------------------------------------------------------
        print("\n[TEST 1] 正常系: JSON 形式の保存と読み込み一致確認")
        json_path = f"{TEST_DATA_DIR}/test.json"
        save_res = save_data(sample_data, json_path, logger)
        assert save_res is True, "JSON 保存が失敗しました"
        load_res = load_data(json_path, logger)
        assert load_res == sample_data, f"JSON 読み込みデータが一致しません: {load_res}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: 正常系 (YAML 保存と読み込み)
        # ---------------------------------------------------------
        print("\n[TEST 2] 正常系: YAML 形式の保存と読み込み一致確認")
        yaml_path = f"{TEST_DATA_DIR}/test.yaml"
        save_res = save_data(sample_data, yaml_path, logger)
        assert save_res is True, "YAML 保存が失敗しました"
        load_res = load_data(yaml_path, logger)
        assert load_res == sample_data, f"YAML 読み込みデータが一致しません: {load_res}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: 異常系 (未対応拡張子の拒絶)
        # ---------------------------------------------------------
        print("\n[TEST 3] 異常系: 未対応拡張子 .txt の保存が拒絶されること")
        invalid_path = f"{TEST_DATA_DIR}/test.txt"
        result = save_data(sample_data, invalid_path, logger)
        assert result is False, f"未対応拡張子が受け入れられてしまいました: {result}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 4: 異常系 (存在しないファイルの読み込み)
        # ---------------------------------------------------------
        print("\n[TEST 4] 異常系: 存在しないファイル読み込みで None 返却")
        result = load_data(f"{TEST_DATA_DIR}/nonexistent.json", logger)
        assert result is None, f"None 以外が返りました: {result}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 5: 異常系 (非辞書型データの保存拒絶)
        # ---------------------------------------------------------
        print("\n[TEST 5] 異常系: 非辞書型データの保存が拒絶されること")
        # 意図: isinstance チェックが機能していることを確認する
        result = save_data(["list", "not", "dict"], json_path, logger)  # type: ignore[arg-type]
        assert result is False, f"非辞書型が受け入れられてしまいました: {result}"
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
        # Windows のファイルロック解放待ち
        time.sleep(0.5)
        for target_dir in [TEST_LOG_DIR, TEST_DATA_DIR]:
            if Path(target_dir).exists():
                try:
                    shutil.rmtree(target_dir)
                    print(f"\nCLEANUP: {target_dir} を削除しました。")
                except Exception as e:
                    print(f"\nCLEANUP WARNING: {target_dir} の削除に失敗: {e}")

    print("\n" + "=" * 60)
    print(" 全テスト完了")
    print("=" * 60)


if __name__ == "__main__":
    # python -m src.utils.file_manager
    _run_tests()
