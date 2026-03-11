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

【Usage】
    from src.data_pipeline.data_validator import validate_pedigree_info
    from src.data_pipeline.data_validator import validate_race_detail
    from src.data_pipeline.data_validator import validate_dataset
    import logging

    logger = logging.getLogger(__name__)

    ok, errors = validate_pedigree_info(pedigree, logger)
    if not ok:
        logger.warning("検証失敗: %s", errors)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
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
        None

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
        None

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
        None

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
        - horse_id の型 (int)
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
        >>> ok, errors = validate_pedigree_info(pedigree, logger)
        >>> if not ok:
        ...     print(errors)
    """
    errors: List[str] = []

    # ---------------------------------------------------------
    # 基本フィールドの型チェック
    # ---------------------------------------------------------

    # horse_id は整数型でなければ DB キーとして使用できない
    _check_type(pedigree.horse_id, int, "horse_id", errors)

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
        >>> ok, errors = validate_race_detail(race_detail, logger)
    """
    errors: List[str] = []

    # ---------------------------------------------------------
    # 基本フィールドのチェック
    # ---------------------------------------------------------

    # date は ISO 8601 形式 (YYYY-MM-DD) の文字列型であることを確認する
    # (ここでは簡略化していますが、実際の各フィールドチェックが入ります)
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
    """主要機能の動作確認テストを実行する。

    正常系・異常系・混合データセットの3軸で検証する。
    外部依存はデータモデルの構築のみであり、ネットワーク・DB は不要。
    """
    import sys

    # ---- ログ設定 ----
    test_logger = logging.getLogger("test_data_validator")
    test_logger.setLevel(logging.DEBUG)
    if not test_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        test_logger.addHandler(handler)

    print("\n" + "=" * 60)
    print("  Unit Test: src/data_pipeline/data_validator.py")
    print("=" * 60 + "\n")

    errors: list[str] = []

    # ---------------------------------------------------------
    # [Test 1] _check_type: 正常系・異常系
    # ---------------------------------------------------------
    print("[Test 1] _check_type")
    type_cases: list[tuple] = [
        (123, int, "horse_id", False),  # 正常: int に int
        ("abc", int, "horse_id", True),  # 異常: int に str
        ("hello", str, "name", False),  # 正常: str に str
        (3.14, str, "name", True),  # 異常: str に float
    ]
    for value, typ, field, expect_error in type_cases:
        errs: list[str] = []
        _check_type(value, typ, field, errs)
        has_error = len(errs) > 0
        status = "OK" if has_error == expect_error else "FAIL"
        print(
            f"  {status}: _check_type({value!r}, {typ.__name__}) -> error={has_error}"
        )
        if status == "FAIL":
            errors.append(
                f"_check_type({value!r}, {typ.__name__}): expected error={expect_error}"
            )

    # ---------------------------------------------------------
    # [Test 2] _check_list_length: 正常系・異常系・allow_zero
    # ---------------------------------------------------------
    print("\n[Test 2] _check_list_length")
    len_cases: list[tuple] = [
        (["a"] * 62, 62, False, False),  # 正常: 62件
        (["a"] * 10, 62, False, True),  # 異常: 10件
        ([], 62, True, False),  # 正常: 空 + allow_zero=True
        ([], 62, False, True),  # 異常: 空 + allow_zero=False
    ]
    for lst, exp, allow_zero, expect_error in len_cases:
        errs_l: list[str] = []
        _check_list_length(lst, exp, "field", errs_l, allow_zero=allow_zero)
        has_error = len(errs_l) > 0
        status = "OK" if has_error == expect_error else "FAIL"
        print(
            f"  {status}: len={len(lst)}, expected={exp}, "
            f"allow_zero={allow_zero} -> error={has_error}"
        )
        if status == "FAIL":
            errors.append(
                f"_check_list_length(len={len(lst)}, exp={exp}, allow_zero={allow_zero})"
            )

    # ---------------------------------------------------------
    # [Test 3] validate_pedigree_info: 正常系
    # ---------------------------------------------------------
    print("\n[Test 3] validate_pedigree_info — 正常系")
    valid_pedigree = PedigreeInfo(
        horse_id=12345,
        name="テスト馬",
        five_gen_ancestor_names=["祖先"] * EXPECTED_ANCESTOR_COUNT,
        five_gen_ancestor_ids=[0] * EXPECTED_ANCESTOR_COUNT,
        five_gen_sire_names=["種牡馬"] * EXPECTED_SIRE_COUNT,
        five_gen_sire_ids=[0] * EXPECTED_SIRE_COUNT,
        five_gen_sire_lineage_names=None,
        five_gen_sire_lineage_ids=None,
    )
    ok, errs_v = validate_pedigree_info(valid_pedigree, test_logger)
    status = "OK" if (ok is True and errs_v == []) else "FAIL"
    print(f"  {status}: ok={ok}, errors={errs_v}")
    if status == "FAIL":
        errors.append(f"validate_pedigree_info 正常系: ok={ok}, errors={errs_v}")

    # ---------------------------------------------------------
    # [Test 4] validate_pedigree_info: 異常系 (複数エラー)
    # ---------------------------------------------------------
    print("\n[Test 4] validate_pedigree_info — 異常系")
    invalid_pedigree = PedigreeInfo(
        horse_id="INVALID",  # type: ignore[arg-type]
        name="",
        five_gen_ancestor_names=["祖先"] * 10,
        five_gen_ancestor_ids=[0] * 20,  # ペア不一致
        five_gen_sire_names=[],
        five_gen_sire_ids=[],
        five_gen_sire_lineage_names=None,
        five_gen_sire_lineage_ids=None,
    )
    ok, errs_i = validate_pedigree_info(invalid_pedigree, test_logger)
    status = "OK" if (ok is False and len(errs_i) > 0) else "FAIL"
    print(f"  {status}: ok={ok}, error_count={len(errs_i)}")
    if status == "FAIL":
        errors.append(f"validate_pedigree_info 異常系: ok={ok}, errors={errs_i}")

    # 具体的なエラー内容の検証
    expected_error_fields = ["horse_id", "name", "five_gen_ancestor"]
    for field in expected_error_fields:
        found = any(field in e for e in errs_i)
        status = "OK" if found else "FAIL"
        print(f"  {status}: '{field}' に関するエラーが含まれること")
        if status == "FAIL":
            errors.append(f"'{field}' のエラーが errs_i に見つかりません: {errs_i}")

    # ---------------------------------------------------------
    # [Test 5] validate_dataset: 混合データセット
    # ---------------------------------------------------------
    print("\n[Test 5] validate_dataset — 混合データセット")
    mixed_dataset = [valid_pedigree, invalid_pedigree, "未知の型（文字列）"]
    results = validate_dataset(mixed_dataset, test_logger)  # type: ignore[arg-type]

    unknown_keys = [k for k in results if k.startswith("Unknown:")]
    pedigree_keys = [k for k in results if k.startswith("Pedigree:")]

    checks = [
        ("結果件数 == 3", len(results) == 3),
        ("Unknown キー == 1", len(unknown_keys) == 1),
        ("Pedigree キー == 2", len(pedigree_keys) == 2),
    ]
    for label, ok_check in checks:
        status = "OK" if ok_check else "FAIL"
        print(f"  {status}: {label}")
        if not ok_check:
            errors.append(f"validate_dataset: {label} が満たされていません。")

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
    # python -m src.data_pipeline.data_validator
    _run_tests()
