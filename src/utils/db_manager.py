# ファイルパス: src/utils/db_manager.py

"""
src/utils/db_manager.py

【概要】
SQLite3 データベースへのデータ保存・読み込み処理を共通化するユーティリティモジュール。

ログ出力・例外処理・ディレクトリ自動生成などの補助機能を提供し、
競馬データ収集パイプラインにおけるレース情報・馬情報・血統情報などの
永続化処理を安全に行う。

【外部依存】
- DB: SQLite3 (標準ライブラリ)
- ファイルシステム: データベースファイルの書き込み権限が必要

【Usage】
    from src.utils.db_manager import save_to_db, load_from_db
    import logging

    logger = logging.getLogger(__name__)

    data = [{"horse_id": "20211001", "name": "ディープインパクト", "wins": 12}]

    save_to_db(data, "data/jbis.db", "horse_results", logger)

    records = load_from_db("data/jbis.db", "horse_results", logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

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

    テーブルが存在しない場合は、data の最初の要素のキーを
    カラム名として自動生成する。
    既存テーブルへのデータ追記にも対応している。

    Args:
        data (List[Dict[str, Any]]): 保存対象の辞書リスト。空リストの場合は即時 False を返す。
        db_path (str): SQLite データベースのファイルパス。
        table_name (str): 保存対象テーブル名。Python の識別子として有効な文字列のみ許容。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        bool: 保存成功時 True、失敗時 False。

    Raises:
        None: sqlite3.Error および OSError は内部でキャッチしログ出力する。

    Example:
        >>> records = [{"horse_id": "H001", "rank": 1}]
        >>> save_to_db(records, "data/race.db", "results", logger)
        True
    """

    # ---------------------------------------------------------
    # 入力バリデーション
    # ---------------------------------------------------------

    # 空データはスキップして呼び出し元に警告する
    if not data:
        logger.warning(
            f"保存するデータが空です。処理をスキップします。 (table={table_name})"
        )
        return False

    # テーブル名が Python 識別子として不正な場合は SQL インジェクションのリスクがあるため拒否する
    # 例: "table; DROP TABLE users;" → isidentifier() = False
    if not table_name.isidentifier():
        logger.error(f"不正なテーブル名が指定されました: {table_name!r}")
        return False

    # ---------------------------------------------------------
    # DB ディレクトリ準備
    # ---------------------------------------------------------

    db_file = Path(db_path)
    try:
        # exist_ok=True: 既存ディレクトリへの競合エラーを抑制する
        db_file.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(
            f"データベース用ディレクトリの作成に失敗しました: {db_file.parent} | {e}"
        )
        return False

    # ---------------------------------------------------------
    # SQLite 書き込み処理
    # ---------------------------------------------------------

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # ---- テーブル定義の生成 ----

            # data の最初の要素のキーをカラム名として使用する
            # 全行が同一スキーマであることを前提とする
            keys = list(data[0].keys())
            columns = ", ".join(keys)
            placeholders = ", ".join(["?"] * len(keys))

            # テーブルが存在しない場合のみ作成する (既存データへの影響なし)
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")

            # ---- INSERT 文の生成と実行 ----

            # カラム名を明示した INSERT 文を使用することで
            # 辞書のキー順序に依存しない安全な挿入を保証する
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # 辞書リストをタプルリストへ一括変換して executemany に渡す
            rows_to_insert = [tuple(row.values()) for row in data]
            cursor.executemany(insert_sql, rows_to_insert)

            # 明示的にコミットし、書き込みの完了を保証する
            conn.commit()

        logger.info(f"DB 保存完了: {db_path} (table={table_name}, {len(data)} 件)")
        return True

    except sqlite3.Error as e:
        logger.error(
            f"データ保存中に SQLite エラーが発生しました: {db_path} "
            f"(table={table_name}) | {e}"
        )
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
        table_name (str): 読み込み対象テーブル名。Python 識別子として有効な文字列のみ許容。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Optional[List[Dict[str, Any]]]:
            成功時: 辞書形式データのリスト (1件以上)。
            テーブルが空の場合: 空リスト ([])。
            DB ファイル未存在またはエラー時: None。

    Raises:
        None: sqlite3.Error は内部でキャッチしログ出力する。

    Example:
        >>> records = load_from_db("data/race.db", "results", logger)
        >>> if records is not None:
        ...     print(len(records))
    """

    # ---------------------------------------------------------
    # 読み込み前チェック
    # ---------------------------------------------------------

    # DB ファイルが存在しない場合は読み込み不能として早期リターンする
    if not Path(db_path).exists():
        logger.warning(f"データベースファイルが存在しません: {db_path}")
        return None

    # テーブル名が不正な場合は SQL インジェクションのリスクがあるため拒否する
    if not table_name.isidentifier():
        logger.error(f"不正なテーブル名が指定されました: {table_name!r}")
        return None

    # ---------------------------------------------------------
    # SQLite 読み込み処理
    # ---------------------------------------------------------

    try:
        with sqlite3.connect(db_path) as conn:
            # sqlite3.Row を使用することでカラム名による辞書変換を効率化する
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()

            # ---------------------------------------------------------
            # 結果の後処理
            # ---------------------------------------------------------

            if not rows:
                logger.warning(f"テーブルにデータが存在しません: {table_name}")
                return []

            # sqlite3.Row オブジェクトを辞書リストへ変換する
            data_list = [dict(row) for row in rows]

            logger.info(
                f"DB 読み込み完了: {db_path} (table={table_name}, {len(data_list)} 件)"
            )
            return data_list

    except sqlite3.Error as e:
        logger.error(
            f"DB 読み込み中に SQLite エラーが発生しました: {db_path} "
            f"(table={table_name}) | {e}"
        )
        return None


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """主要機能の動作確認テストを実行する。

    正常系・異常系・SQL インジェクション対策・境界値を検証する。
    テストで生成した一時ディレクトリは finally ブロックで必ず削除する。
    """
    import sys

    # ---- ログ設定 ----
    test_logger = logging.getLogger("test_db_manager")
    test_logger.setLevel(logging.DEBUG)
    if not test_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        test_logger.addHandler(handler)

    # ---- テスト用一時パス ----
    test_dir = Path("tmp_test_db_manager")
    test_db = str(test_dir / "test.db")
    test_table = "horses"

    print("\n" + "=" * 60)
    print("   Unit Test: src/utils/db_manager.py")
    print("=" * 60 + "\n")

    errors: list[str] = []

    try:
        # サンプルデータ: 実際の競馬データに近いスキーマを使用する
        sample_data = [
            {"horse_id": "H001", "name": "ディープインパクト", "wins": 12},
            {"horse_id": "H002", "name": "ウオッカ", "wins": 10},
            {"horse_id": "H003", "name": "ジェンティルドンナ", "wins": 10},
        ]

        # ---------------------------------------------------------
        # [Test 1] save_to_db: 正常系
        # ---------------------------------------------------------
        print("[Test 1] save_to_db — 正常系")
        result = save_to_db(sample_data, test_db, test_table, test_logger)
        status = "OK" if result is True else "FAIL"
        print(f"   {status}: save_to_db() -> {result} (expected: True)")
        if status == "FAIL":
            errors.append("save_to_db 正常系が False を返しました。")

        # DB ファイルが生成されていることを確認する
        status = "OK" if Path(test_db).exists() else "FAIL"
        print(f"   {status}: DB ファイルが生成されること -> {Path(test_db).exists()}")
        if status == "FAIL":
            errors.append("DB ファイルが生成されていません。")

        # ---------------------------------------------------------
        # [Test 2] load_from_db: 正常系・件数・内容の検証
        # ---------------------------------------------------------
        print("\n[Test 2] load_from_db — 正常系")
        loaded = load_from_db(test_db, test_table, test_logger)
        status = (
            "OK" if (loaded is not None and len(loaded) == len(sample_data)) else "FAIL"
        )
        count = len(loaded) if loaded is not None else "None"
        print(f"   {status}: 件数 -> {count} (expected: {len(sample_data)})")
        if status == "FAIL":
            errors.append(f"load_from_db 件数: {count} (expected: {len(sample_data)})")

        # 1件目の内容を検証する
        if loaded:
            status = "OK" if loaded[0]["horse_id"] == "H001" else "FAIL"
            print(
                f"   {status}: loaded[0]['horse_id'] -> {loaded[0].get('horse_id')!r}"
            )
            if status == "FAIL":
                errors.append(f"1件目の horse_id が不正: {loaded[0].get('horse_id')!r}")

        # ---------------------------------------------------------
        # [Test 3] save_to_db: 異常系 — 空データ → False を返すこと
        # ---------------------------------------------------------
        print("\n[Test 3] save_to_db — 空データ")
        result = save_to_db([], test_db, test_table, test_logger)
        status = "OK" if result is False else "FAIL"
        print(f"   {status}: save_to_db([]) -> {result} (expected: False)")
        if status == "FAIL":
            errors.append(f"空データで False が返りませんでした: {result}")

        # ---------------------------------------------------------
        # [Test 4] save_to_db: 異常系 — SQL インジェクション対策
        # ---------------------------------------------------------
        print("\n[Test 4] save_to_db / load_from_db — 不正テーブル名の拒否")
        malicious_cases = [
            "table; DROP TABLE horses;",
            "1invalid",
            "valid table",
            "",
        ]
        for name in malicious_cases:
            r_save = save_to_db(sample_data, test_db, name, test_logger)
            r_load = load_from_db(test_db, name, test_logger)
            ok = (r_save is False) and (r_load is None)
            status = "OK" if ok else "FAIL"
            print(f"   {status}: テーブル名 {name!r} が拒否されること")
            if status == "FAIL":
                errors.append(f"不正テーブル名 {name!r} が拒否されませんでした。")

        # ---------------------------------------------------------
        # [Test 5] load_from_db: 異常系 — 存在しない DB → None を返すこと
        # ---------------------------------------------------------
        print("\n[Test 5] load_from_db — 存在しない DB")
        result = load_from_db("nonexistent_path/no.db", test_table, test_logger)
        status = "OK" if result is None else "FAIL"
        print(f"   {status}: 存在しない DB -> None (got: {result!r})")
        if status == "FAIL":
            errors.append(f"存在しない DB で None が返りませんでした: {result!r}")

        # ---------------------------------------------------------
        # [Test 6] load_from_db: 空テーブル → 空リストを返すこと
        # ---------------------------------------------------------
        print("\n[Test 6] load_from_db — 空テーブル")
        empty_db = str(test_dir / "empty.db")
        empty_table = "empty_horses"

        # 空テーブルを手動で作成する
        # WinError 32 対策: with句を使用して確実にコネクションを閉じる
        with sqlite3.connect(empty_db) as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {empty_table} (id)")
            conn.commit()

        result = load_from_db(empty_db, empty_table, test_logger)
        status = "OK" if result == [] else "FAIL"
        print(f"   {status}: 空テーブル -> [] (got: {result!r})")
        if status == "FAIL":
            errors.append(f"空テーブルで [] が返りませんでした: {result!r}")

    except Exception as e:
        errors.append(f"テスト中に予期せぬ例外が発生: {e}")
        print(f"   ERROR: {e}")

    finally:
        # テストで生成した一時ディレクトリをすべて削除する
        if test_dir.exists():
            time.sleep(0.5)  # WinError 32 対策: OSのファイルロックが解放されるのを待つ
            try:
                shutil.rmtree(test_dir)
                print(f"\n   CLEANUP: 一時ディレクトリを削除しました: {test_dir}")
            except PermissionError as e:
                # 稀にOS側の解放が間に合わない場合があるためのフォールバック
                print(
                    f"\n   WARNING: 一時ディレクトリの削除に失敗しました (OSロック): {e}"
                )

    # ---------------------------------------------------------
    # テスト結果サマリ
    # ---------------------------------------------------------
    print("\n" + "=" * 60)
    if errors:
        print(f"   FAILED — {len(errors)} error(s):")
        for msg in errors:
            print(f"      ✗ {msg}")
    else:
        print("   ALL TESTS PASSED")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    # python -m src.utils.db_manager
    _run_tests()
