# ファイルパス: src/data_pipeline/data_validator.py

"""
src/data_pipeline/data_validator.py

【概要】
PedigreeInfo (血統情報) および RaceDetail (レース詳細) の
構造的・論理的整合性を検証するモジュール。

フィールドの型チェック・リスト長チェック・ペア長一致チェックを共通
ヘルパー関数として提供し、再利用性を高める。

【外部依存】
- 内部モデル: src.data_pipeline.data_models (PedigreeInfo, RaceDetail)
- 内部モジュール:
    src.utils.logger (setup_logger, close_logger_handlers)

【Usage】
    from src.data_pipeline.data_validator import validate_pedigree_info
    from src.data_pipeline.data_validator import validate_race_detail
    from src.data_pipeline.data_validator import validate_dataset
    from src.utils.logger import setup_logger

    logger = setup_logger(
        log_filepath="logs/data_validator.log",
        log_level="INFO",
        logger_name="DataValidator",
    )

    ok, errors = validate_pedigree_info(pedigree, logger)
    if not ok:
        logger.warning("検証失敗: %s", errors)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import shutil
from pathlib import Path
from typing import Dict, Final, List, Sequence, Tuple, Union

from src.data_pipeline.data_models import PedigreeInfo, RaceDetail

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# 5代血統表における全祖先の想定数 (2^6 - 2 = 62)
EXPECTED_ANCESTOR_COUNT: Final[int] = 62

# 5代血統表における父系祖先の想定数 (2^5 - 1 = 31)
EXPECTED_SIRE_COUNT: Final[int] = 31


# ---------------------------------------------------------
# 共通バリデーションヘルパー
# ---------------------------------------------------------


def _check_type(
    value: object,
    expected_type: type,
    field_name: str,
    errors: List[str],
) -> None:
    """
    値が期待する型であるかを検証し、不一致の場合はエラーリストへ追記する。

    Args:
        value (object): 検証対象の値。
        expected_type (type): 期待する型。
        field_name (str): エラーメッセージに使用するフィールド名。
        errors (List[str]): 検証エラーを追記するリスト (破壊的変更あり)。

    Returns:
        None: 戻り値なし（副作用として errors リストを更新する）。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> errs: List[str] = []
        >>> _check_type("abc", int, "horse_id", errs)
        >>> errs
        ['horse_id が int 型ではありません (実際: str)。']
    """
    # isinstance で型チェックし、不一致の場合のみエラーを追記する
    if not isinstance(value, expected_type):
        errors.append(
            f"{field_name} が {expected_type.__name__} 型ではありません"
            f" (実際: {type(value).__name__})。"
        )


def _check_list_length(
    lst: Sequence,
    expected: int,
    field_name: str,
    errors: List[str],
    allow_zero: bool = False,
) -> None:
    """
    シーケンスの要素数が想定値と一致するかを検証し、不一致の場合はエラーリストへ追記する。

    Args:
        lst (Sequence): 検証対象のシーケンス。
        expected (int): 想定される要素数。
        field_name (str): エラーメッセージに使用するフィールド名。
        errors (List[str]): 検証エラーを追記するリスト (破壊的変更あり)。
        allow_zero (bool): True の場合、要素数 0 を許容する (スクレイピング失敗等を考慮)。
                           デフォルトは False。

    Returns:
        None: 戻り値なし（副作用として errors リストを更新する）。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> errs: List[str] = []
        >>> _check_list_length(["a"] * 10, 62, "ancestors", errs)
        >>> errs
        ['ancestors の要素数が想定外です (想定: 62件 / 実際: 10件)。']
    """
    # allow_zero が True かつ空リストの場合はスクレイピング未取得として許容する
    if allow_zero and len(lst) == 0:
        return

    if len(lst) != expected:
        errors.append(
            f"{field_name} の要素数が想定外です"
            f" (想定: {expected}件 / 実際: {len(lst)}件)。"
        )


def _check_pair_length(
    list_a: Sequence,
    list_b: Sequence,
    name_a: str,
    name_b: str,
    errors: List[str],
) -> None:
    """
    2つのシーケンスの要素数が一致するかを検証し、不一致の場合はエラーリストへ追記する。

    名前リストと ID リストのペア整合性確認など、対応関係があるリスト同士の
    検証に使用する。

    Args:
        list_a (Sequence): 検証対象のシーケンス A。
        list_b (Sequence): 検証対象のシーケンス B。
        name_a (str): シーケンス A のフィールド名 (エラーメッセージ用)。
        name_b (str): シーケンス B のフィールド名 (エラーメッセージ用)。
        errors (List[str]): 検証エラーを追記するリスト (破壊的変更あり)。

    Returns:
        None: 戻り値なし（副作用として errors リストを更新する）。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> errs: List[str] = []
        >>> _check_pair_length(["A", "B"], ["id1"], "names", "ids", errs)
        >>> errs
        ['names と ids の要素数が一致しません (2件 vs 1件)。']
    """
    # 名前リストと ID リストは 1:1 対応であるため必ず同数でなければならない
    if len(list_a) != len(list_b):
        errors.append(
            f"{name_a} と {name_b} の要素数が一致しません"
            f" ({len(list_a)}件 vs {len(list_b)}件)。"
        )


# ---------------------------------------------------------
# PedigreeInfo の検証
# ---------------------------------------------------------


def validate_pedigree_info(
    pedigree: PedigreeInfo,
    logger: logging.Logger,
    ignore_lineage_errors: bool = True,
) -> Tuple[bool, List[str]]:
    """
    PedigreeInfo の構造的・論理的整合性を検証する。

    検証項目:
        - horse_id の型 (str)
        - name の型 (str) および空文字チェック
        - five_gen_ancestor_names と five_gen_ancestor_ids の要素数一致
        - five_gen_sire_names と five_gen_sire_ids の要素数一致
        - five_gen_ancestor_names の要素数 (EXPECTED_ANCESTOR_COUNT、空許容)
        - five_gen_sire_names の要素数 (EXPECTED_SIRE_COUNT、空許容)

    Args:
        pedigree (PedigreeInfo): 検証対象の血統情報オブジェクト。
        logger (logging.Logger): ログ出力用ロガー。
        ignore_lineage_errors (bool): 系統エラーを無視するかどうかのフラグ。
                                      現在の実装では予約済み引数として保持。

    Returns:
        Tuple[bool, List[str]]: (検証合否, エラーメッセージリスト)。
                                 合格時はエラーリストが空。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> logger = setup_logger("logs/validator.log", logger_name="Validator")
        >>> ok, errors = validate_pedigree_info(pedigree, logger)
        >>> if not ok:
        ...     print(errors)
    """
    errors: List[str] = []

    # ---------------------------------------------------------
    # 基本フィールドの型チェック
    # ---------------------------------------------------------

    # horse_id は文字列型でなければ DB キーとして使用できない
    _check_type(pedigree.horse_id, str, "horse_id", errors)

    # name は文字列型かつ空文字でないことを確認する
    _check_type(pedigree.name, str, "name", errors)
    if isinstance(pedigree.name, str) and not pedigree.name.strip():
        errors.append("name が空文字です。")

    # ---------------------------------------------------------
    # 名前リストと ID リストのペア整合性チェック
    # ---------------------------------------------------------

    # 祖先名リストと祖先 ID リストは対応関係があるため同数でなければならない
    _check_pair_length(
        pedigree.five_gen_ancestor_names,
        pedigree.five_gen_ancestor_ids,
        "five_gen_ancestor_names",
        "five_gen_ancestor_ids",
        errors,
    )

    # 父系名リストと父系 ID リストも同様に対応関係を確認する
    _check_pair_length(
        pedigree.five_gen_sire_names,
        pedigree.five_gen_sire_ids,
        "five_gen_sire_names",
        "five_gen_sire_ids",
        errors,
    )

    # ---------------------------------------------------------
    # リスト要素数チェック (空リストは許容)
    # ---------------------------------------------------------

    # スクレイピング失敗等で空リストになる場合があるため allow_zero=True とする
    _check_list_length(
        pedigree.five_gen_ancestor_names,
        EXPECTED_ANCESTOR_COUNT,
        "five_gen_ancestor_names",
        errors,
        allow_zero=True,
    )
    _check_list_length(
        pedigree.five_gen_sire_names,
        EXPECTED_SIRE_COUNT,
        "five_gen_sire_names",
        errors,
        allow_zero=True,
    )

    # ---------------------------------------------------------
    # 検証結果のログ出力
    # ---------------------------------------------------------

    is_valid = len(errors) == 0

    if is_valid:
        logger.info(
            "PedigreeInfo 検証合格: horse_id=%s name=%s",
            pedigree.horse_id,
            pedigree.name,
        )
    else:
        logger.warning(
            "PedigreeInfo 検証失敗: horse_id=%s name=%s errors=%s",
            pedigree.horse_id,
            pedigree.name,
            errors,
        )

    return (is_valid, errors)


# ---------------------------------------------------------
# RaceDetail の検証
# ---------------------------------------------------------


def validate_race_detail(
    race_detail: RaceDetail,
    logger: logging.Logger,
) -> Tuple[bool, List[str]]:
    """
    RaceDetail の基本構造を検証する。

    現在は date フィールドの型チェックのみ実装。
    詳細な各フィールドのチェックは今後の拡張で対応予定。

    Args:
        race_detail (RaceDetail): 検証対象のレース詳細オブジェクト。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Tuple[bool, List[str]]: (検証合否, エラーメッセージリスト)。
                                 合格時はエラーリストが空。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> logger = setup_logger("logs/validator.log", logger_name="Validator")
        >>> ok, errors = validate_race_detail(race_detail, logger)
    """
    errors: List[str] = []

    # ---------------------------------------------------------
    # 基本フィールドのチェック
    # ---------------------------------------------------------

    # date は ISO 8601 形式 (YYYY-MM-DD) の文字列型であることを確認する
    _check_type(race_detail.date, str, "date", errors)

    # ---------------------------------------------------------
    # 検証結果のログ出力
    # ---------------------------------------------------------

    is_valid = len(errors) == 0

    if is_valid:
        logger.info("RaceDetail 検証合格: %s", race_detail.date)

    return (is_valid, errors)


# ---------------------------------------------------------
# 汎用データセット検証
# ---------------------------------------------------------


def validate_dataset(
    dataset: List[Union[PedigreeInfo, RaceDetail]],
    logger: logging.Logger,
) -> Dict[str, List[str]]:
    """
    PedigreeInfo または RaceDetail のリストを一括検証し、結果を辞書で返す。

    未対応の型が含まれていた場合はエラーとして記録しログ出力する。

    Args:
        dataset (List[Union[PedigreeInfo, RaceDetail]]): 検証対象データのリスト。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Dict[str, List[str]]: キーに識別文字列、値にエラーリストを持つ辞書。
                               エラーが0件の場合は空リストが格納される。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> logger = setup_logger("logs/validator.log", logger_name="Validator")
        >>> results = validate_dataset([pedigree, race], logger)
        >>> for key, errs in results.items():
        ...     print(key, errs)
    """
    results: Dict[str, List[str]] = {}

    for data in dataset:

        # ---------------------------------------------------------
        # データ型に応じた検証関数へのディスパッチ
        # ---------------------------------------------------------

        if isinstance(data, PedigreeInfo):
            # horse_id と name を組み合わせた一意キーで識別する
            key = f"Pedigree:{data.horse_id}({data.name})"
            _, errors = validate_pedigree_info(data, logger)

        elif isinstance(data, RaceDetail):
            # 日付とレース名を組み合わせた一意キーで識別する
            key = f"Race:{data.date}_{data.race.name}"
            _, errors = validate_race_detail(data, logger)

        else:
            # 想定外の型はエラーとして記録し、処理は継続する
            key = f"Unknown:{id(data)}"
            errors = [f"未対応のデータ型です: {type(data).__name__}。"]
            logger.error("未対応データ型が渡されました: key=%s", key)

        results[key] = errors

    return results


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    主要機能の動作確認テストを実行する。

    外部依存（DB・ネットワーク）を持たず、データモデルのインスタンス構築のみを使用するため
    単体実行が可能。テスト終了後はログファイルとロガーをすべて解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.data_pipeline.data_validator
        _run_tests()
    """
    from src.utils.logger import close_logger_handlers, setup_logger

    TEST_LOG_DIR: Final[str] = "logs/_test_data_validator_tmp"
    TEST_LOG_FILE: Final[str] = f"{TEST_LOG_DIR}/test.log"
    TEST_LOGGER_NAME: Final[str] = "test_data_validator"

    print("=" * 60)
    print(" data_validator.py 簡易単体テスト 開始")
    print("=" * 60)

    logger = setup_logger(
        log_filepath=TEST_LOG_FILE,
        log_level="DEBUG",
        logger_name=TEST_LOGGER_NAME,
    )

    try:
        # ---------------------------------------------------------
        # テスト 1: _check_type (正常系・異常系)
        # ---------------------------------------------------------
        print("\n[TEST 1] 正常系/異常系: _check_type の型チェック")
        type_cases = [
            (123, int, "horse_id", False),  # 正常: int に int
            ("abc", int, "horse_id", True),  # 異常: int に str
            ("hi", str, "name", False),  # 正常: str に str
            (3.14, str, "name", True),  # 異常: str に float
        ]
        for value, typ, field, expect_error in type_cases:
            errs: List[str] = []
            _check_type(value, typ, field, errs)
            has_error = len(errs) > 0
            assert has_error == expect_error, (
                f"_check_type({value!r}, {typ.__name__}): "
                f"エラー発生={has_error} (期待値: {expect_error})"
            )
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: _check_list_length (正常系・異常系・allow_zero)
        # ---------------------------------------------------------
        print("\n[TEST 2] 正常系/異常系: _check_list_length の要素数チェック")
        len_cases = [
            (["a"] * 62, 62, False, False),  # 正常: 62件
            (["a"] * 10, 62, False, True),  # 異常: 10件
            ([], 62, True, False),  # 正常: 空 + allow_zero=True
            ([], 62, False, True),  # 異常: 空 + allow_zero=False
        ]
        for lst, exp, allow_zero, expect_error in len_cases:
            errs_l: List[str] = []
            _check_list_length(lst, exp, "field", errs_l, allow_zero=allow_zero)
            has_error = len(errs_l) > 0
            assert has_error == expect_error, (
                f"_check_list_length(len={len(lst)}, exp={exp}, allow_zero={allow_zero}): "
                f"エラー発生={has_error} (期待値: {expect_error})"
            )
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: validate_pedigree_info (正常系)
        # ---------------------------------------------------------
        print("\n[TEST 3] 正常系: validate_pedigree_info の合格ケース")
        # 意図: horse_id は str 型でなければ _check_type(str) を通過しないため
        #       str 形式の ID を渡す
        valid_pedigree = PedigreeInfo(
            horse_id="0001234500",
            name="テスト馬",
            five_gen_ancestor_names=["祖先"] * EXPECTED_ANCESTOR_COUNT,
            five_gen_ancestor_ids=[0] * EXPECTED_ANCESTOR_COUNT,
            five_gen_sire_names=["種牡馬"] * EXPECTED_SIRE_COUNT,
            five_gen_sire_ids=[0] * EXPECTED_SIRE_COUNT,
            five_gen_sire_lineage_names=None,
            five_gen_sire_lineage_ids=None,
        )
        ok, errs_v = validate_pedigree_info(valid_pedigree, logger)
        assert ok is True, f"正常系で ok=False が返りました: {errs_v}"
        assert errs_v == [], f"正常系でエラーが返りました: {errs_v}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 4: validate_pedigree_info (異常系・複数エラー)
        # ---------------------------------------------------------
        print("\n[TEST 4] 異常系: validate_pedigree_info の複数エラー検出")
        invalid_pedigree = PedigreeInfo(
            horse_id="INVALID",  # str 型なので型チェックは通過する
            name="",  # 意図: 空文字エラーを発生させる
            five_gen_ancestor_names=["祖先"] * 10,
            five_gen_ancestor_ids=[0] * 20,  # 意図: ペア不一致エラーを発生させる
            five_gen_sire_names=[],
            five_gen_sire_ids=[],
            five_gen_sire_lineage_names=None,
            five_gen_sire_lineage_ids=None,
        )
        ok, errs_i = validate_pedigree_info(invalid_pedigree, logger)
        assert ok is False, f"異常系で ok=True が返りました"
        assert len(errs_i) > 0, "異常系でエラーリストが空です"

        # 意図: horse_id は str 型なので型エラーは出ない。
        #       発生するエラーは name(空文字) と five_gen_ancestor(ペア不一致・件数不一致)
        expected_error_fields = ["name", "five_gen_ancestor"]
        for field in expected_error_fields:
            assert any(
                field in e for e in errs_i
            ), f"'{field}' に関するエラーがエラーリストに含まれていません: {errs_i}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 5: validate_dataset (混合データセット)
        # ---------------------------------------------------------
        print("\n[TEST 5] 正常系/異常系: validate_dataset の混合データセット処理")
        mixed_dataset = [valid_pedigree, invalid_pedigree, "未知の型（文字列）"]
        results = validate_dataset(mixed_dataset, logger)  # type: ignore[arg-type]

        unknown_keys = [k for k in results if k.startswith("Unknown:")]
        pedigree_keys = [k for k in results if k.startswith("Pedigree:")]

        assert len(results) == 3, f"結果件数が一致しません: {len(results)} (期待値: 3)"
        assert (
            len(unknown_keys) == 1
        ), f"Unknown キー数が一致しません: {len(unknown_keys)} (期待値: 1)"
        assert (
            len(pedigree_keys) == 2
        ), f"Pedigree キー数が一致しません: {len(pedigree_keys)} (期待値: 2)"
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
    # python -m src.data_pipeline.data_validator
    _run_tests()
