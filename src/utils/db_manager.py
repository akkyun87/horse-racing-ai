# ファイルパス: src/utils/db_manager.py

"""
src/utils/db_manager.py

【概要】
SQLite3 データベースへのデータ保存・読み込み処理を共通化するユーティリティです。
競馬データ収集パイプラインにおけるレース情報・馬情報・血統情報などの
永続化処理を安全に行います。

【外部依存】
- DB: SQLite3 (標準ライブラリ)
- 内部モジュール:
    src.utils.logger (setup_logger, close_logger_handlers)

【Usage】
    from src.utils.db_manager import save_to_db, load_from_db
    from src.utils.logger import setup_logger

    logger = setup_logger("logs/db.log", logger_name="DBManager")
    data = [{"horse_id": "2021100001", "name": "サンプル馬"}]
    save_to_db(data, "data/raw/race/race.db", "horse_info", logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------
import logging
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Final, List, Optional

# ---------------------------------------------------------
# データ保存処理
# ---------------------------------------------------------


def save_to_db(
    data: List[Dict[str, Any]],
    db_path: str,
    table_name: str,
    logger: logging.Logger,
) -> bool:
    """
    辞書形式のリストを SQLite3 テーブルへ保存する。

    テーブルが存在しない場合は、data の最初の要素のキーをカラム名として自動生成する。
    競馬ドメインの制約（カラム名の空白等）を考慮し、安全にクエリを生成する。

    Args:
        data (List[Dict[str, Any]]): 保存対象の辞書リスト。
        db_path (str): SQLite データベースのファイルパス。
        table_name (str): 保存対象テーブル名。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        bool: 保存成功時 True、失敗時（空データ・不正テーブル名・DB接続失敗等）は False。

    Raises:
        None: sqlite3.Error および OSError は内部で捕捉し、ERROR ログを出力する。

    Example:
        >>> logger = setup_logger("logs/db.log", logger_name="DBManager")
        >>> success = save_to_db([{"id": 1}], "data/test.db", "table1", logger)
        >>> print(success)
        True
    """
    # ---------------------------------------------------------
    # 入力バリデーション
    # ---------------------------------------------------------
    if not data:
        logger.warning(f"保存データが空です。処理をスキップします: table={table_name}")
        return False

    if not table_name.isidentifier():
        # 意図: SQLインジェクション防止。ただし競馬DBで特殊な名前が必要な場合は設計見直しが必要
        logger.error(f"不正なテーブル名が指定されました: {table_name!r}")
        return False

    # ---------------------------------------------------------
    # DBディレクトリ準備
    # ---------------------------------------------------------
    db_file = Path(db_path)
    try:
        db_file.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"DBディレクトリ作成失敗: {db_file.parent} | {e}")
        return False

    # ---------------------------------------------------------
    # SQLite 書き込み実行
    # ---------------------------------------------------------
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # カラム名の抽出とエスケープ
            # 意図: [カラム名] 形式にすることで、血統DBなどで発生する「カラム名内の空白」に対応する
            keys = list(data[0].keys())
            escaped_columns = ", ".join([f"[{k}]" for k in keys])
            placeholders = ", ".join(["?"] * len(keys))

            # テーブル作成
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({escaped_columns})"
            cursor.execute(create_sql)

            # データ挿入
            insert_sql = (
                f"INSERT INTO {table_name} ({escaped_columns}) VALUES ({placeholders})"
            )
            rows_to_insert = [tuple(row.get(k) for k in keys) for row in data]

            cursor.executemany(insert_sql, rows_to_insert)
            conn.commit()

        logger.info(f"DB保存完了: {db_path} (table={table_name}, {len(data)}件)")
        return True

    except sqlite3.Error as e:
        logger.error(f"SQLiteエラー (save_to_db): {db_path} | {e}", exc_info=True)
        return False


# ---------------------------------------------------------
# データ読み込み処理
# ---------------------------------------------------------


def load_from_db(
    db_path: str,
    table_name: str,
    logger: logging.Logger,
) -> Optional[List[Dict[str, Any]]]:
    """
    SQLite3 テーブルから全データを読み込み、辞書のリストとして返す。

    Args:
        db_path (str): SQLite データベースのファイルパス。
        table_name (str): 読み込み対象テーブル名。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Optional[List[Dict[str, Any]]]:
            成功時: 辞書リスト。テーブル空時は空リスト []。
            失敗時 (ファイル不在・不正テーブル名・DB接続失敗): None。

    Raises:
        None: sqlite3.Error は内部で捕捉し、ERROR ログを出力する。

    Example:
        >>> logger = setup_logger("logs/db.log", logger_name="DBManager")
        >>> result = load_from_db("data/test.db", "table1", logger)
        >>> if result is not None:
        ...     print(len(result))
    """
    # ---------------------------------------------------------
    # 読み込み前チェック
    # ---------------------------------------------------------
    if not Path(db_path).exists():
        logger.warning(f"DBファイルが存在しません: {db_path}")
        return None

    if not table_name.isidentifier():
        logger.error(f"不正なテーブル名が指定されました: {table_name!r}")
        return None

    # ---------------------------------------------------------
    # SQLite 読み込み実行
    # ---------------------------------------------------------
    try:
        with sqlite3.connect(db_path) as conn:
            # 意図: sqlite3.Row を使い、カラム名をキーとした辞書形式での取得を効率化する
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # 意図: 全カラム取得。将来的に特定の horse_id で絞り込む場合は別関数の検討が必要
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()

            if not rows:
                logger.info(f"テーブルにデータが存在しません: {table_name}")
                return []

            result = [dict(row) for row in rows]
            logger.info(
                f"DB読み込み完了: {db_path} (table={table_name}, {len(result)}件)"
            )
            return result

    except sqlite3.Error as e:
        logger.error(f"SQLiteエラー (load_from_db): {db_path} | {e}", exc_info=True)
        return None


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    主要機能の動作確認テストを実行する。

    外部依存（ネットワーク）を持たず、一時ディレクトリ上のSQLiteのみを使用するため
    単体実行が可能。テスト終了後はファイル・ロガー・DBをすべて解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.utils.db_manager
        _run_tests()
    """
    from src.utils.logger import close_logger_handlers, setup_logger

    TEST_LOG_DIR: Final[str] = "logs/_test_db_tmp"
    TEST_DB_DIR: Final[str] = "data/_test_db_tmp"
    TEST_DB_PATH: Final[str] = f"{TEST_DB_DIR}/test_race.db"
    TEST_LOGGER_NAME: Final[str] = "test_db_manager"

    print("=" * 60)
    print(" db_manager.py 簡易単体テスト 開始")
    print("=" * 60)

    logger = setup_logger(
        log_filepath=f"{TEST_LOG_DIR}/test.log",
        log_level="DEBUG",
        logger_name=TEST_LOGGER_NAME,
    )

    try:
        # ---------------------------------------------------------
        # テスト 1: 正常系 (保存と読み込み)
        # ---------------------------------------------------------
        print("\n[TEST 1] 正常系: データの保存と読み出し")
        dummy_data = [
            {
                "horse_id": 2021100001,
                "name": "テスト馬A",
                "weather ": "晴 ",
            },  # 空白含むキーと値
            {"horse_id": 2021100002, "name": "テスト馬B", "weather ": " 曇"},
        ]

        # 保存実行
        save_res = save_to_db(dummy_data, TEST_DB_PATH, "test_table", logger)
        assert save_res is True, "保存に失敗しました"

        # 読み込み実行
        load_res = load_from_db(TEST_DB_PATH, "test_table", logger)
        assert load_res is not None, "読み込みに失敗しました"
        assert len(load_res) == 2, f"件数不一致: {len(load_res)}"
        # 競馬ドメイン制約: 読み込み後のデータ加工（strip等）は各パーサーの責務だが、DBに保存されていることを確認
        assert str(load_res[0]["horse_id"]) == "2021100001"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: 異常系 (不正なテーブル名)
        # ---------------------------------------------------------
        print("\n[TEST 2] 異常系: 不正なテーブル名による拒絶")
        bad_res = save_to_db(dummy_data, TEST_DB_PATH, "drop table horses;--", logger)
        assert bad_res is False, "不正なテーブル名が受け入れられてしまいました"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: 競馬ドメイン固有 (カラム名の空白)
        # ---------------------------------------------------------
        print("\n[TEST 3] 競馬ドメイン: 空白を含むカラム名の処理")
        special_data = [{"sire_name 母父": "Sunday Silence"}]
        special_res = save_to_db(special_data, TEST_DB_PATH, "lineage_info", logger)
        assert special_res is True, "空白を含むカラム名の保存に失敗しました"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 4: 異常系 (空データの保存拒絶)
        # ---------------------------------------------------------
        print("\n[TEST 4] 異常系: 空リストを渡した際の False 返却")
        # 意図: data が空の場合、警告ログを出力して False を返すことを検証する
        empty_res = save_to_db([], TEST_DB_PATH, "test_table", logger)
        assert empty_res is False, "空データが受け入れられてしまいました"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 5: 異常系 (DBファイル不在時の読み込み)
        # ---------------------------------------------------------
        print("\n[TEST 5] 異常系: 存在しない DB パスへの読み込みで None 返却")
        # 意図: DBファイルが存在しない場合、警告ログを出力して None を返すことを検証する
        missing_res = load_from_db(
            "data/_test_db_tmp/nonexistent.db", "test_table", logger
        )
        assert missing_res is None, f"None 以外が返りました: {missing_res}"
        print("  -> PASS")

    except AssertionError as e:
        print(f"\n[FAIL] アサーション失敗: {e}")
    except Exception as e:
        print(f"\n[FAIL] 予期しないエラー: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # リソース解放
        close_logger_handlers(TEST_LOGGER_NAME)

        # Windowsのファイルロック対策として少し待機してから削除
        time.sleep(0.5)
        for target_dir in [TEST_LOG_DIR, TEST_DB_DIR]:
            if Path(target_dir).exists():
                try:
                    shutil.rmtree(target_dir)
                    print(f"CLEANUP: {target_dir} を削除しました。")
                except Exception as e:
                    print(f"CLEANUP WARNING: {target_dir} の削除に失敗: {e}")

    print("\n" + "=" * 60)
    print(" 全テスト完了")
    print("=" * 60)


if __name__ == "__main__":
    # python -m src.utils.db_manager
    _run_tests()
