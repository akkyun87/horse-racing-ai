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

【Usage】
    from src.utils.file_manager import save_data, load_data
    import logging

    logger = logging.getLogger(__name__)

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
SUPPORTED_JSON_EXTS: Final = (".json",)
SUPPORTED_YAML_EXTS: Final = (".yaml", ".yml")


# ---------------------------------------------------------
# 内部補助関数 (JSON)
# ---------------------------------------------------------


def _save_json(
    data: Dict[str, Any], file_path: Path, indent: int = 4, ensure_ascii: bool = False
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
        None
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
        None
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

    Example:
        >>> success = save_data({"key": "value"}, "output.json", logger)
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

    正常系（JSON/YAML）、異常系（未対応拡張子、不在ファイル）を検証し、
    テスト完了後に一時ファイルをクリーンアップする。
    """
    import sys

    # ---- ログ設定 ----
    test_logger = logging.getLogger("test_file_manager")
    test_logger.setLevel(logging.DEBUG)
    if not test_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        test_logger.addHandler(handler)

    # ---- テスト用一時パス ----
    test_dir = Path("tmp_test_file_manager")

    print("\n" + "=" * 60)
    print("   Unit Test: src/utils/file_manager.py")
    print("=" * 60 + "\n")

    errors: list[str] = []

    try:
        sample_data = {
            "horse_name": "コントレイル",
            "results": [1, 1, 1, 2],
            "metadata": {"retired": True},
        }

        # ---------------------------------------------------------
        # [Test 1] JSON 形式 — 保存と読込
        # ---------------------------------------------------------
        print("[Test 1] JSON 形式 — 保存と読込")
        json_path = str(test_dir / "test.json")

        save_res = save_data(sample_data, json_path, test_logger)
        load_res = load_data(json_path, test_logger)

        ok = (save_res is True) and (load_res == sample_data)
        status = "OK" if ok else "FAIL"
        print(f"   {status}: save_data() and load_data()一致確認")
        if status == "FAIL":
            errors.append("JSON 正常系の保存・読込が一致しません。")

        # ---------------------------------------------------------
        # [Test 2] YAML 形式 — 保存と読込
        # ---------------------------------------------------------
        print("\n[Test 2] YAML 形式 — 保存と読込")
        yaml_path = str(test_dir / "test.yaml")

        save_res = save_data(sample_data, yaml_path, test_logger)
        load_res = load_data(yaml_path, test_logger)

        ok = (save_res is True) and (load_res == sample_data)
        status = "OK" if ok else "FAIL"
        print(f"   {status}: save_data() and load_data()一致確認")
        if status == "FAIL":
            errors.append("YAML 正常系の保存・読込が一致しません。")

        # ---------------------------------------------------------
        # [Test 3] 異常系 — 未対応拡張子
        # ---------------------------------------------------------
        print("\n[Test 3] 未対応拡張子の拒否")
        invalid_path = str(test_dir / "test.txt")
        result = save_data(sample_data, invalid_path, test_logger)

        status = "OK" if result is False else "FAIL"
        print(f"   {status}: .txt の保存が拒否されること -> {result}")
        if status == "FAIL":
            errors.append("未対応拡張子 .txt が受け入れられてしまいました。")

        # ---------------------------------------------------------
        # [Test 4] 異常系 — 存在しないファイル読込
        # ---------------------------------------------------------
        print("\n[Test 4] 存在しないファイルの読込")
        result = load_data("nonexistent_file.json", test_logger)

        status = "OK" if result is None else "FAIL"
        print(f"   {status}: 結果が None であること -> {result!r}")
        if status == "FAIL":
            errors.append("存在しないファイルの読込で None が返りませんでした。")

    except Exception as e:
        errors.append(f"テスト中に予期せぬ例外が発生: {e}")
        print(f"   ERROR: {e}")

    finally:
        # クリーンアップ
        if test_dir.exists():
            time.sleep(0.5)  # OSのファイルロック解放待ち
            try:
                shutil.rmtree(test_dir)
                print(f"\n   CLEANUP: 一時ディレクトリを削除しました: {test_dir}")
            except Exception as e:
                print(f"\n   WARNING: クリーンアップに失敗しました: {e}")

    # ---- サマリ ----
    print("\n" + "=" * 60)
    if errors:
        print(f"   FAILED — {len(errors)} error(s):")
        for msg in errors:
            print(f"      ✗ {msg}")
    else:
        print("   ALL TESTS PASSED")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    # python -m src.utils.file_manager
    _run_tests()
