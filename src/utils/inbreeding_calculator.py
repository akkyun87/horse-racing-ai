# ファイルパス: src/utils/inbreeding_calculator.py

"""
src/utils/inbreeding_calculator.py

【概要】
競走馬の5代血統データから、馬名クロスおよび系統クロスをバッチで一括計算する
インブリード解析ユーティリティモジュール。

calculate_inbreeding_batch は複数馬IDを受け取り、DBへのアクセスを1回に集約して
全レコードをメモリにロードしたうえでバッチ処理を行うことで高速化を図る。

算出する情報:
    - horse_crosses: 同一祖先が複数ポジションに出現する場合の血量と公式
    - lineage_cross.cross_inter: 父系・母系両側に同一系統が出現するクロス
    - lineage_cross.cross_sire: 父系側のみで同一系統が独立出現するクロス
    - lineage_cross.cross_dam: 母系側のみで同一系統が独立出現するクロス

【外部依存】
- DB: SQLite (data/raw/pedigree/pedigree.db)
- 内部モジュール:
    src.utils.db_manager (load_from_db)
    src.utils.logger     (setup_logger, close_logger_handlers)

【Usage】
    from src.utils.inbreeding_calculator import calculate_inbreeding_batch
    from src.utils.logger import setup_logger

    logger = setup_logger(
        log_filepath="logs/inbreeding.log",
        log_level="INFO",
        logger_name="InbreedingCalculator",
    )
    results = calculate_inbreeding_batch(["0001352760", "0001155349"], logger)
    for horse_id, data in results.items():
        print(horse_id, data["horse_name"])
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import shutil
from pathlib import Path
from typing import Any, Dict, Final, List, Optional

from src.utils import db_manager
from src.utils.logger import setup_logger

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# 血統情報を格納する SQLite ファイルパス
PEDIGREE_DB_PATH: Final[Path] = Path("data/raw/pedigree/pedigree.db")

# 5代血統表の全62ポジションラベル（インデックス順）
# インデックス i が ALL_62_POS[i] のラベルに対応する
ALL_62_POS: Final[List[str]] = [
    # 父系（0〜30）
    "父",
    "父父",
    "父母",
    "父父父",
    "父父母",
    "父母父",
    "父母母",
    "父父父父",
    "父父父母",
    "父父母父",
    "父父母母",
    "父母父父",
    "父母父母",
    "父母母父",
    "父母母母",
    "父父父父父",
    "父父父父母",
    "父父父母父",
    "父父父母母",
    "父父母父父",
    "父父母父母",
    "父父母母父",
    "父父母母母",
    "父母父父父",
    "父母父父母",
    "父母父母父",
    "父母父母母",
    "父母母父父",
    "父母母父母",
    "父母母母父",
    "父母母母母",
    # 母系（31〜61）
    "母",
    "母父",
    "母母",
    "母父父",
    "母父母",
    "母母父",
    "母母母",
    "母父父父",
    "母父父母",
    "母父母父",
    "母父母母",
    "母母父父",
    "母母父母",
    "母母母父",
    "母母母母",
    "母父父父父",
    "母父父父母",
    "母父父母父",
    "母父父母母",
    "母父母父父",
    "母父母父母",
    "母父母母父",
    "母父母母母",
    "母母父父父",
    "母母父父母",
    "母母父母父",
    "母母父母母",
    "母母母父父",
    "母母母父母",
    "母母母母父",
    "母母母母母",
]


# ---------------------------------------------------------
# 内部補助関数
# ---------------------------------------------------------


def _is_independent(label1: str, label2: str) -> bool:
    """
    2つのラベルが血統上独立しているか（祖先・子孫関係でないか）を判定する。

    一方のラベルが他方のプレフィックスになっている場合、同一祖先ラインに属するため
    独立していないとみなす（例: "父父" と "父父父" は独立しない）。

    Args:
        label1 (str): 血統ポジションラベル1（例: "父父"）。
        label2 (str): 血統ポジションラベル2（例: "父父父"）。

    Returns:
        bool: 独立している場合 True、祖先・子孫関係にある場合 False。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> _is_independent("父父", "父母")
        True
        >>> _is_independent("父父", "父父父")
        False
    """
    return not (label1.startswith(label2) or label2.startswith(label1))


def _make_formula(pos_list: List[str]) -> str:
    """
    ポジションラベルのリストから血量公式文字列（例: "5x5"）を生成する。

    各ラベルの文字数（＝代数）を昇順にソートし、"x" 区切りで連結する。
    代数は日本語1文字を1代として数える（例: "父父父父父" = 5代）。

    Args:
        pos_list (List[str]): 血統ポジションラベルのリスト（例: ["父父父父父", "母父父父父"]）。

    Returns:
        str: 代数を "x" 区切りで結合した公式文字列（例: "5x5"）。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> _make_formula(["父父父父父", "母父父父父"])
        '5x5'
        >>> _make_formula(["父父父父", "母父父父父"])
        '4x5'
    """
    # 意図: 代数を昇順に並べて「浅い代 × 深い代」の順で公式を表現する
    gens = sorted([len(p) for p in pos_list])
    return "x".join(map(str, gens))


# ---------------------------------------------------------
# メイン計算ロジック
# ---------------------------------------------------------


def calculate_inbreeding_batch(
    horse_ids: List[str],
    pedigree_records: Optional[List[Dict[str, Any]]] = None,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    指定された馬IDリストに対してインブリード計算を一括実行する。

    DBへのアクセスを1回に集約してメモリにロードしたうえでバッチ処理を行う。
    DB に存在しない馬IDは結果辞書に含まれない（スキップ）。

    Args:
        horse_ids (List[str]): 計算対象の馬IDリスト（10桁ゼロ埋みでなくても可）。
        pedigree_records (List[Dict[str, Any]], optional): 事前にロードされた血統レコードのリスト。
                                                          None の場合は内部で DB からロードする。
        logger (Optional[logging.Logger]): ログ出力用ロガー。
                                            None の場合は内部で setup_logger() を生成する。

    Returns:
        Dict[str, Any]: { horse_id (10桁ゼロ埋め): {"horse_name": str,
                                                      "horse_crosses": dict,
                                                      "lineage_cross": dict} } の辞書。
                         DB 未接続・全馬未登録の場合は空辞書 {} を返す。

    Raises:
        None: DB アクセスエラーは内部で捕捉し ERROR ログを出力する。

    Example:
        >>> logger = setup_logger("logs/inbreeding.log", logger_name="InbreedingCalculator")
        >>> results = calculate_inbreeding_batch(["0001352760"], logger)
        >>> print(results["0001352760"]["horse_name"])
        クロワデュノール
    """
    if logger is None:
        logger = setup_logger(
            log_filepath="logs/inbreeding.log",
            logger_name=__name__,
        )

    # 1. 血統ソースの決定
    # 外部注入があればそれを使用、なければ自前でロード
    source_records = pedigree_records
    if source_records is None:
        source_records = db_manager.load_from_db(
            str(PEDIGREE_DB_PATH), "pedigree_info", logger
        )

    if not source_records:
        logger.warning("血統データが空または取得失敗したため、計算を中断します。")
        return {}

    # 2. 検索効率向上のため ID をキーとしたマップを作成
    # zfill(10) で正規化し、型不一致を解消
    record_map: Dict[str, Dict[str, Any]] = {
        str(r.get("horse_id", "")).zfill(10): r for r in source_records
    }

    final_results: Dict[str, Any] = {}

    # 3. 指定された ID リストに対してループ
    for h_id in horse_ids:
        target_id = str(h_id).zfill(10)
        record = record_map.get(target_id)

        if not record:
            logger.warning(
                "horse_id=%s がデータソースに見つかりません。スキップします。",
                target_id,
            )
            continue

        # 馬名クロスの計算
        ancestors = [n.strip() for n in record.get("five_gen_ancestors", "").split(",")]
        horse_appearances: Dict[str, List[str]] = {}
        for i, name in enumerate(ancestors):
            if not name or name == "Unknown" or i >= len(ALL_62_POS):
                continue
            horse_appearances.setdefault(name, []).append(ALL_62_POS[i])

        horse_crosses_dict: Dict[str, Any] = {}
        for name, labels in horse_appearances.items():
            if len(labels) < 2:
                continue
            indep_labels: List[str] = []
            for lb in sorted(labels, key=len):
                if not any(
                    not _is_independent(lb, existing) for existing in indep_labels
                ):
                    indep_labels.append(lb)
            if len(indep_labels) >= 2:
                horse_crosses_dict[name] = {
                    "formula": _make_formula(indep_labels),
                    "blood_pct": sum([100.0 / (2 ** len(lb)) for lb in indep_labels]),
                }

        # 系統クロスの計算
        sire_side_map: Dict[str, str] = {}
        dam_side_map: Dict[str, str] = {}
        sire_labels = [l for l in ALL_62_POS if l.endswith("父")]
        for pos in sorted(sire_labels, key=len, reverse=True):
            lin = record.get(f"lineage_name_{pos}")
            if not lin or lin == "Unknown":
                continue
            if pos.startswith("母"):
                dam_side_map[lin] = pos
            else:
                sire_side_map[lin] = pos

        lineage_cross: Dict[str, Any] = {
            "cross_inter": {},
            "cross_sire": {},
            "cross_dam": {},
        }

        # cross_inter
        for lin in set(sire_side_map.keys()) & set(dam_side_map.keys()):
            s_pos, d_pos = sire_side_map[lin], dam_side_map[lin]
            lineage_cross["cross_inter"][lin] = {
                "blood_vol": (1.0 / (2 ** len(s_pos))) + (1.0 / (2 ** len(d_pos))),
                "formula": _make_formula([s_pos, d_pos]),
            }

        # cross_sire
        for lin, first_pos in sire_side_map.items():
            all_s_pos = [
                p
                for p in sire_labels
                if not p.startswith("母") and record.get(f"lineage_name_{p}") == lin
            ]
            for op in all_s_pos:
                if _is_independent(first_pos, op):
                    lineage_cross["cross_sire"][lin] = {
                        "blood_vol": (1.0 / (2 ** len(first_pos)))
                        + (1.0 / (2 ** len(op))),
                        "formula": _make_formula([first_pos, op]),
                    }
                    break

        # cross_dam
        for lin, first_pos in dam_side_map.items():
            all_d_pos = [
                p
                for p in sire_labels
                if p.startswith("母") and record.get(f"lineage_name_{p}") == lin
            ]
            for op in all_d_pos:
                if _is_independent(first_pos, op):
                    lineage_cross["cross_dam"][lin] = {
                        "blood_vol": (1.0 / (2 ** len(first_pos)))
                        + (1.0 / (2 ** len(op))),
                        "formula": _make_formula([first_pos, op]),
                    }
                    break

        final_results[target_id] = {
            "horse_name": record.get("name"),
            "horse_crosses": horse_crosses_dict,
            "lineage_cross": lineage_cross,
        }

    return final_results


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """
    主要機能の動作確認テストを実行する。

    DBアクセスは実 PEDIGREE_DB_PATH に接続する（モックなし）。
    DB ファイルが存在しない場合・対象馬が未登録の場合は該当テストを SKIP する。
    純粋ロジック（_is_independent / _make_formula）は DB 不要で常に実行する。
    テスト終了後はログファイルとロガーをすべて解放・削除する。

    Args:
        なし。

    Returns:
        None: 戻り値なし。テスト結果は標準出力に print する。

    Raises:
        None: AssertionError および予期しない例外は内部でキャッチして出力する。

    Example:
        # python -m src.utils.inbreeding_calculator
        _run_tests()
    """
    from src.utils.logger import close_logger_handlers, setup_logger

    TEST_LOG_DIR: Final[str] = "logs/_test_inbreeding_calculator_tmp"
    TEST_LOG_FILE: Final[str] = f"{TEST_LOG_DIR}/test.log"
    TEST_LOGGER_NAME: Final[str] = "test_inbreeding_calculator"
    # 実DB テストで使用する馬ID（クロワデュノール）
    TEST_HORSE_ID: Final[str] = "0001352760"

    print("=" * 60)
    print(" inbreeding_calculator.py 簡易単体テスト 開始")
    print("=" * 60)

    # 意図: DB の有無をテスト開始前に1回だけ確認し、各テストの分岐に再利用する
    db_available: bool = PEDIGREE_DB_PATH.exists()
    if not db_available:
        print(f"\n  [INFO] DB ファイルが存在しません: {PEDIGREE_DB_PATH}")
        print("  DB 依存テストは SKIP されます。")

    logger = setup_logger(
        log_filepath=TEST_LOG_FILE,
        log_level="DEBUG",
        logger_name=TEST_LOGGER_NAME,
    )

    try:
        # ---------------------------------------------------------
        # テスト 1: _is_independent (純粋ロジック・DB 不要)
        # ---------------------------------------------------------
        print("\n[TEST 1] 正常系: _is_independent の独立性判定")
        assert (
            _is_independent("父父", "父母") is True
        ), "'父父' と '父母' は独立しているべきです"
        assert (
            _is_independent("母父", "母母") is True
        ), "'母父' と '母母' は独立しているべきです"
        # 意図: 一方が他方のプレフィックスになっている場合は独立しないことを確認する
        assert (
            _is_independent("父父", "父父父") is False
        ), "'父父' と '父父父' は独立していないべきです"
        assert (
            _is_independent("母", "母父") is False
        ), "'母' と '母父' は独立していないべきです"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 2: _make_formula (純粋ロジック・DB 不要)
        # ---------------------------------------------------------
        print("\n[TEST 2] 正常系: _make_formula の公式文字列生成")
        assert (
            _make_formula(["父父父父父", "母父父父父"]) == "5x5"
        ), "5x5 公式が一致しません"
        assert (
            _make_formula(["父父父父", "母父父父父"]) == "4x5"
        ), "4x5 公式が一致しません"
        assert (
            _make_formula(["父父父", "父父父父", "母父父父父"]) == "3x4x5"
        ), "3x4x5 公式が一致しません"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 3: ALL_62_POS の妥当性検証 (DB 不要)
        # ---------------------------------------------------------
        print("\n[TEST 3] 正常系: ALL_62_POS の件数・構造検証")
        assert (
            len(ALL_62_POS) == 62
        ), f"ALL_62_POS の件数が一致しません: {len(ALL_62_POS)} (期待値: 62)"
        assert (
            ALL_62_POS[0] == "父"
        ), f"インデックス0が '父' ではありません: {ALL_62_POS[0]!r}"
        assert (
            ALL_62_POS[31] == "母"
        ), f"インデックス31が '母' ではありません: {ALL_62_POS[31]!r}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 4: 存在しない ID のみのバッチ → 空辞書 (DB 有無に関わらず)
        # ---------------------------------------------------------
        print("\n[TEST 4] 異常系: 存在しない ID のみのバッチで空辞書が返ること")
        # 意図: DB が存在しない場合は records=None で空辞書、
        #       存在する場合は線形探索が一致しない → いずれも {} が返ることを検証する
        result = calculate_inbreeding_batch(["9999999999"], logger)
        assert result == {}, f"空辞書以外が返りました: {result}"
        print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 5: calculate_inbreeding_batch 実 DB 接続・戻り値構造検証
        # ---------------------------------------------------------
        print("\n[TEST 5] 正常系: calculate_inbreeding_batch の実 DB 接続・構造検証")
        if not db_available:
            print(f"  SKIP: DB ファイルが存在しません ({PEDIGREE_DB_PATH})")
        else:
            results = calculate_inbreeding_batch([TEST_HORSE_ID], logger)
            if TEST_HORSE_ID not in results:
                print(f"  SKIP: horse_id={TEST_HORSE_ID} は DB 未登録のためデータなし")
            else:
                data = results[TEST_HORSE_ID]
                assert "horse_name" in data, "'horse_name' キーが存在しません"
                assert "horse_crosses" in data, "'horse_crosses' キーが存在しません"
                assert "lineage_cross" in data, "'lineage_cross' キーが存在しません"
                lc = data["lineage_cross"]
                assert "cross_inter" in lc, "'cross_inter' キーが存在しません"
                assert "cross_sire" in lc, "'cross_sire' キーが存在しません"
                assert "cross_dam" in lc, "'cross_dam' キーが存在しません"
                print(
                    f"  DB 取得成功: horse_name={data['horse_name']} "
                    f"horse_crosses={len(data['horse_crosses'])}件 "
                    f"cross_inter={len(lc['cross_inter'])}件"
                )
            print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 6: 複数IDの一括処理・存在しないIDがスキップされること
        # ---------------------------------------------------------
        print("\n[TEST 6] 正常系: 複数ID バッチ処理・存在しない ID のスキップ検証")
        if not db_available:
            print(f"  SKIP: DB ファイルが存在しません ({PEDIGREE_DB_PATH})")
        else:
            NONEXISTENT_ID: Final[str] = "9999999999"
            results = calculate_inbreeding_batch(
                [TEST_HORSE_ID, NONEXISTENT_ID], logger
            )
            # 意図: 存在しない ID は結果辞書に含まれないことを確認する
            assert (
                NONEXISTENT_ID not in results
            ), f"存在しない horse_id={NONEXISTENT_ID} が結果に含まれています"
            if TEST_HORSE_ID in results:
                print(
                    f"  バッチ結果: {len(results)}件取得 "
                    f"(存在しないID={NONEXISTENT_ID} は正しくスキップ)"
                )
            else:
                print(
                    f"  SKIP: horse_id={TEST_HORSE_ID} は DB 未登録 "
                    f"(存在しないID={NONEXISTENT_ID} は正しくスキップ)"
                )
            print("  -> PASS")

        # ---------------------------------------------------------
        # テスト 7: horse_crosses の blood_pct / formula 構造検証 (DB 存在時)
        # ---------------------------------------------------------
        print("\n[TEST 7] 正常系: horse_crosses の blood_pct / formula 構造検証")
        if not db_available:
            print(f"  SKIP: DB ファイルが存在しません ({PEDIGREE_DB_PATH})")
        else:
            results = calculate_inbreeding_batch([TEST_HORSE_ID], logger)
            if TEST_HORSE_ID not in results:
                print(f"  SKIP: horse_id={TEST_HORSE_ID} は DB 未登録")
            else:
                for name, cross in results[TEST_HORSE_ID]["horse_crosses"].items():
                    assert (
                        "formula" in cross
                    ), f"'{name}' の horse_crosses に 'formula' キーがありません"
                    assert (
                        "blood_pct" in cross
                    ), f"'{name}' の horse_crosses に 'blood_pct' キーがありません"
                    assert isinstance(cross["blood_pct"], float), (
                        f"'{name}' の blood_pct が float ではありません: "
                        f"{type(cross['blood_pct'])}"
                    )
                    # 意図: "x" 区切りの公式形式であることを確認する
                    assert "x" in cross["formula"], (
                        f"'{name}' の formula に 'x' が含まれていません: "
                        f"{cross['formula']!r}"
                    )
                print(
                    f"  horse_crosses 全"
                    f"{len(results[TEST_HORSE_ID]['horse_crosses'])}件の構造確認完了"
                )
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
    # python -m src.utils.inbreeding_calculator
    _run_tests()
