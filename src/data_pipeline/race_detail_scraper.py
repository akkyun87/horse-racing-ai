# ファイルパス: src/data_pipeline/race_detail_scraper.py

"""
src/data_pipeline/race_detail_scraper.py

【概要】
JBIS レース結果ページ HTML を解析し、開催情報・レース概要・
出走馬詳細・払戻情報を構造化データとして抽出するモジュール。

抽出されたデータは RaceDetail オブジェクトとして階層管理され、
競走馬能力予測 AI の特徴量生成の基礎データとして使用される。

【外部依存】
- ネットワーク: JBIS (https://www.jbis.or.jp) への HTTP リクエスト
- HTML 解析: BeautifulSoup4
- HTTP 通信: src.utils.retry_requests.fetch_html
- データモデル: src.data_pipeline.data_models
  (RaceDetail, RaceInfo, RaceVenue, HorseEntry, Payout)

【Usage】
    from src.data_pipeline.race_detail_scraper import scrape_race_details
    import logging

    logger = logging.getLogger(__name__)
    urls   = ["https://www.jbis.or.jp/race/result/20251130/105/12/"]

    results = scrape_race_details(urls, logger)
"""

# ---------------------------------------------------------
# インポート
# ---------------------------------------------------------

import logging
import re
import unicodedata
from typing import Any, Dict, Final, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from src.data_pipeline.data_models import (
    HorseEntry,
    Payout,
    RaceDetail,
    RaceInfo,
    RaceVenue,
)
from src.utils.retry_requests import fetch_html

# ---------------------------------------------------------
# 定数定義
# ---------------------------------------------------------

# JBIS のベース URL: 相対パスから絶対 URL を生成する際に使用する
BASE_URL: Final[str] = "https://www.jbis.or.jp"

# 馬柱テーブルの最低列数: これを下回る行はヘッダー行またはデータ不足行と判断する
_HORSE_TABLE_MIN_COLS: Final[int] = 15

# JBIS 馬 ID の桁数: URL から ID を抽出する正規表現の基準となる
_HORSE_ID_DIGITS: Final[int] = 10


# ---------------------------------------------------------
# 文字列ユーティリティ
# ---------------------------------------------------------


def to_halfwidth(s: Optional[str]) -> Optional[str]:
    """
    文字列を NFKC 正規化により半角へ変換し、全角スペースを半角化する。

    Args:
        s (Optional[str]): 正規化対象の文字列。None を渡した場合は None を返す。

    Returns:
        Optional[str]: 半角変換後の文字列。入力が None の場合は None。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> to_halfwidth("１２３４　（ＡＢＣ）")
        '1234 (ABC)'
    """
    # 入力が None の場合は後続処理でエラーを起こさないよう早期リターン
    if s is None:
        return None

    # NFKC 正規化: 全角英数字・記号・カタカナを半角に統一する
    normalized = unicodedata.normalize("NFKC", s)

    # 全角スペース (U+3000) を半角スペースへ置換する
    return normalized.replace("\u3000", " ")


def safe_int(text: Optional[str]) -> Optional[int]:
    """
    文字列から数字のみを抽出し、安全に int へ変換する。

    Args:
        text (Optional[str]): 数値を含む文字列 (例: "1,234円")。

    Returns:
        Optional[int]: 抽出された整数値。変換不能な場合は None。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> safe_int("1,234円")
        1234
        >>> safe_int(None)
        None
    """
    try:
        if text is None:
            return None

        # カンマ・通貨記号・単位など数字以外の文字をすべて除去する
        cleaned = re.sub(r"[^\d]", "", text)

        # 数字が1文字も残らない場合 (空文字列) は変換不能と判断する
        return int(cleaned) if cleaned else None

    except (ValueError, TypeError):
        return None


def safe_float(text: Optional[str]) -> Optional[float]:
    """
    文字列から数値 (小数点含む) を抽出し、安全に float へ変換する。

    Args:
        text (Optional[str]): 数値を含む文字列 (例: "34.5kg")。

    Returns:
        Optional[float]: 抽出された浮動小数点数。変換不能な場合は None。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> safe_float("34.5kg")
        34.5
        >>> safe_float("N/A")
        None
    """
    try:
        if text is None:
            return None

        # 数字と小数点以外の文字をすべて除去する
        cleaned = re.sub(r"[^\d\.]", "", text)

        return float(cleaned) if cleaned else None

    except (ValueError, TypeError):
        return None


def extract_id_from_url(url: Optional[str]) -> str:
    """
    JBIS 馬詳細ページ URL から 10 桁の馬 ID を抽出する。

    JBIS の URL 体系では "/horse/XXXXXXXXXX/" 形式で馬 ID が埋め込まれている。

    Args:
        url (Optional[str]): 馬詳細ページ URL (例: "/horse/2020101234/")。

    Returns:
        str: 10 桁の馬 ID 文字列。URL が None またはパターン不一致の場合は空文字。

    Raises:
        None: 本関数は例外を外部へ伝播しない。

    Example:
        >>> extract_id_from_url("/horse/2020101234/")
        '2020101234'
        >>> extract_id_from_url(None)
        ''
    """
    # URL が None または空文字の場合は早期リターン
    if not url:
        return ""

    # JBIS URL 体系: /horse/ の直後に _HORSE_ID_DIGITS 桁の数字が続く
    match = re.search(rf"/horse/(\d{{{_HORSE_ID_DIGITS}}})/", url)
    return match.group(1) if match else ""


# ---------------------------------------------------------
# HTML 解析: コア抽出処理
# ---------------------------------------------------------


def extract_race_data(
    html_content: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    JBIS レース結果 HTML を BeautifulSoup で解析し、生データを辞書形式で返す。

    抽出対象:
        1. 開催情報 (日付・曜日・競馬場・回次)
        2. レース概要 (番号・名称・馬場種別・距離・天候・馬場状態)
        3. タイム情報 (上がりタイム・ラップタイム・コーナー通過順)
        4. 馬柱 (出走馬全頭の詳細データ)
        5. 払戻金情報

    Args:
        html_content (str): JBIS レース結果ページの HTML 文字列。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        Dict[str, Any]: 抽出された生データ辞書。解析失敗した項目は None またはデフォルト値。

    Raises:
        None: 各フェーズの例外は内部でキャッチしログ出力するため外部へ伝播しない。

    Example:
        >>> data = extract_race_data(html_string, logger)
        >>> data["date"]
        '2025-11-30'
    """

    # ---------------------------------------------------------
    # HTML パーサ初期化
    # ---------------------------------------------------------

    # lxml より移植性の高い html.parser を使用する
    soup = BeautifulSoup(html_content, "html.parser")

    # 抽出結果を格納する辞書: 解析失敗時にも参照可能な安全なデフォルト値を設定
    data: Dict[str, Any] = {
        "date": None,
        "weekday": None,
        "venue": {"round": None, "place": None, "day": None},
        "race": {
            "number": None,
            "name": None,
            "surface": None,
            "distance_m": None,
            "weather": None,
            "track_condition": None,
            "final_time": None,
            "lap_time": [],
            "corner_order": {},
        },
        "horses": [],
        "payouts": [],
    }

    # ---------------------------------------------------------
    # 1. 開催情報 (日付・曜日・競馬場・回次・日数)
    # ---------------------------------------------------------

    try:
        header_tag = soup.select_one(".hdg1-search h1")

        if header_tag:
            header_text = header_tag.get_text(strip=True)

            # 「2025年11月30日(日)」形式から年月日・曜日を取得する
            date_match = re.search(
                r"(\d{4})年(\d{1,2})月(\d{1,2})日\((.)\)",
                header_text,
            )
            if date_match:
                y, m, d, weekday = date_match.groups()
                # ISO 8601 形式 (YYYY-MM-DD) で格納する
                data["date"] = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
                data["weekday"] = weekday

            # 「3回 東京 5日」形式から回次・場所・日数を取得する
            venue_match = re.search(r"(\d+)回\s+(\S+)\s+(\d+)日", header_text)
            if venue_match:
                data["venue"]["round"] = safe_int(venue_match.group(1))
                data["venue"]["place"] = venue_match.group(2)
                data["venue"]["day"] = safe_int(venue_match.group(3))
        else:
            logger.warning("開催ヘッダー (.hdg1-search h1) が見つかりません。")

    except Exception as e:
        logger.error(f"開催情報のパース中に例外が発生: {e}")

    # ---------------------------------------------------------
    # 2. レース概要 (番号・名称・馬場・距離・天候・馬場状態)
    # ---------------------------------------------------------

    try:
        race_title_tag = soup.select_one(".hdg2-l-1 h2")

        if race_title_tag:
            race_title = race_title_tag.get_text(strip=True)

            # 「12R ジャパンカップ」形式からレース番号と名称を分離する
            title_match = re.match(r"(\d+)R\s+(.*)", race_title)
            if title_match:
                data["race"]["number"] = safe_int(title_match.group(1))
                data["race"]["name"] = title_match.group(2)

        cond_tag = soup.select_one(".box-race__text")

        if cond_tag:
            cond_text = cond_tag.get_text(strip=True)

            # 「芝2400M」または「ダ1600M」形式から馬場種別と距離を抽出する
            surf_match = re.search(r"(ダ|芝)(\d{3,4})M", cond_text)
            if surf_match:
                # 略称「ダ」を「ダート」に正規化する
                data["race"]["surface"] = (
                    "ダート" if surf_match.group(1) == "ダ" else "芝"
                )
                data["race"]["distance_m"] = safe_int(surf_match.group(2))

            # 「天候：晴」形式から天候を抽出する
            weather_match = re.search(r"天候：([^　 ]+)", cond_text)
            data["race"]["weather"] = weather_match.group(1) if weather_match else None

            # 「芝：良」または「ダ：稍重」形式から馬場状態を抽出する
            track_match = re.search(r"(ダ|芝)：([^　 ]+)", cond_text)
            data["race"]["track_condition"] = (
                track_match.group(2) if track_match else None
            )

    except Exception as e:
        logger.error(f"レース概要のパース中に例外が発生: {e}")

    # ---------------------------------------------------------
    # 3. タイム情報 (上がり3ハロン・ラップ・コーナー通過順)
    # ---------------------------------------------------------

    try:
        # 上がりタイムは <dt>上がり</dt><dd>XX.X</dd> の隣接関係から取得する
        agari_dt = soup.find("dt", string="上がり")
        if agari_dt:
            agari_dd = agari_dt.find_next_sibling("dd")
            if agari_dd:
                data["race"]["final_time"] = agari_dd.get_text(strip=True)

        # ハロンタイム (ラップ) は同様の <dt><dd> 構造から数値リストとして取得する
        h_time_dt = soup.find("dt", string="ハロンタイム")
        if h_time_dt:
            h_time_dd = h_time_dt.find_next_sibling("dd")
            if h_time_dd:
                lap_text = h_time_dd.get_text(strip=True)
                # "XX.X" 形式の数値をすべて抽出し float リスト化する
                data["race"]["lap_time"] = [
                    val
                    for x in re.findall(r"\d+\.\d+", lap_text)
                    if (val := safe_float(x)) is not None
                ]

        # 各コーナーの通過順は ".data-4-1" コンテナ内の <dt><dd> ペアから取得する
        corner_container = soup.select_one(".data-4-1")
        if corner_container:
            for item in corner_container.select(".data-4__item"):
                c_name = item.dt.get_text(strip=True) if item.dt else ""
                order_text = item.dd.get_text(strip=True) if item.dd else ""
                if c_name:
                    # 通過順は空白やハイフンで区切られた数字列として格納される
                    data["race"]["corner_order"][c_name] = [
                        int(n) for n in re.findall(r"\d+", order_text)
                    ]

    except Exception as e:
        logger.warning(f"タイム・コーナー情報のパース中に一部失敗: {e}")

    # ---------------------------------------------------------
    # 4. 馬柱 (全出走馬の詳細データ)
    # ---------------------------------------------------------

    try:
        # ヘッダー行は 1 行目のため nth-child(n+2) で除外する
        horse_rows = soup.select(".data-6-11.sort-1 > div:nth-child(n+2)")

        for row in horse_rows:
            cols = row.find_all("div", recursive=False)

            # _HORSE_TABLE_MIN_COLS 列に満たない行はデータ不整合とみなしスキップする
            if len(cols) < _HORSE_TABLE_MIN_COLS:
                continue

            # ---- 馬名・馬詳細 URL ----
            name_a = cols[3].select_one("a")
            name = name_a.get_text(strip=True) if name_a else "N/A"
            raw_url = name_a.get("href", "") if name_a else ""
            url = urljoin(BASE_URL, raw_url) if raw_url else None

            # ---- 性別・年齢: 「牡3」形式から分離する ----
            sex_age_text = cols[4].get_text(strip=True)
            sex = sex_age_text[0] if sex_age_text else None
            age = safe_int(sex_age_text)

            # ---- 馬体重・前走比増減: 「480(-2)」形式から取得する ----
            bw_text = cols[12].get_text(strip=True)
            bw_match = re.match(r"(\d+)[\(\（]([-\+]?\d+)[\)\）]", bw_text)
            body_weight = safe_int(bw_match.group(1)) if bw_match else None
            diff = safe_int(bw_match.group(2)) if bw_match else None

            # ---- 調教師名・所属: 「国枝 栄(美浦)」形式から分離する ----
            trainer_tag = cols[13].select_one("span.txt-overflow")
            trainer_raw = trainer_tag.get_text(strip=True) if trainer_tag else ""
            trainer_text = to_halfwidth(trainer_raw) or ""
            t_match = re.match(r"(.+)\((.+)\)", trainer_text)
            t_name = t_match.group(1).strip() if t_match else trainer_text
            t_region = t_match.group(2).strip() if t_match else None

            # ---- 馬主・生産者: 同列内の複数リンクから順に取得する ----
            owner_links = cols[14].select("a.txt-link.txt-overflow")
            owner = (
                owner_links[0].get_text(strip=True) if len(owner_links) > 0 else None
            )
            breeder = (
                owner_links[1].get_text(strip=True) if len(owner_links) > 1 else None
            )

            # ---- 騎手名: リンクタグから取得し半角正規化する ----
            jockey_name = "N/A"
            if j_a := cols[5].select_one("a"):
                jockey_name = to_halfwidth(j_a.get_text(strip=True)) or "N/A"

            # ---- 斤量: 同列のテキストから数値を抽出する ----
            jockey_col_text = cols[5].get_text(strip=True)
            weight_match = re.search(r"(\d+\.?\d*)", jockey_col_text)
            j_weight = safe_float(weight_match.group(1)) if weight_match else None

            data["horses"].append(
                {
                    "rank": safe_int(cols[0].get_text()),
                    "frame": safe_int(cols[1].get_text()),
                    "number": safe_int(cols[2].get_text()),
                    "name": name,
                    "url": url,
                    "sex": sex,
                    "age": age,
                    "jockey": jockey_name,
                    "weight": j_weight,
                    "time": safe_float(cols[6].get_text()),
                    "margin": cols[7].get_text(strip=True) or None,
                    "passing_order": [
                        int(n) for n in re.findall(r"\d+", cols[8].get_text())
                    ],
                    "last_3f": safe_float(cols[9].get_text()),
                    "speed_index": safe_float(cols[10].get_text()),
                    "popularity": safe_int(cols[11].get_text()),
                    "body_weight": body_weight,
                    "diff_from_prev": diff,
                    "trainer_name": t_name,
                    "trainer_region": t_region,
                    "owner": owner,
                    "breeder": breeder,
                }
            )

        logger.info(f"馬柱情報抽出完了: {len(data['horses'])} 頭")

    except Exception as e:
        logger.error(f"馬柱情報のパース中に例外が発生: {e}")

    # ---------------------------------------------------------
    # 5. 払戻金情報
    # ---------------------------------------------------------

    try:
        payout_rows = soup.select(".table-1 table tbody tr")

        for row in payout_rows:
            cells = row.find_all(["th", "td"])

            # 券種・的中番号・払戻金額の3列が揃わない行は対象外とする
            if len(cells) < 3:
                continue

            bet_type = cells[0].get_text(strip=True)

            # ワイドなど同一券種で複数的中がある場合に備え div 単位で分割する
            targets = [
                n.get_text(strip=True)
                for n in cells[1].find_all("div", recursive=False)
            ]
            amounts = [
                val
                for a in cells[2].find_all("div", recursive=False)
                if (val := safe_int(a.get_text().replace(",", ""))) is not None
            ]

            for target, amount in zip(targets, amounts):
                data["payouts"].append(
                    {
                        "type": bet_type,
                        "target": target,
                        "amount": amount,
                    }
                )

    except Exception as e:
        logger.warning(f"払戻情報のパース中に一部失敗: {e}")

    return data


# ---------------------------------------------------------
# データモデル変換処理
# ---------------------------------------------------------


def generate_race_objects(
    race_results: Dict[str, Any],
    logger: logging.Logger,
) -> RaceDetail:
    """
    抽出済みレース辞書データを RaceDetail オブジェクトへ変換する。

    Args:
        race_results (Dict[str, Any]): extract_race_data の返り値。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        RaceDetail: 構造化されたレース詳細オブジェクト。

    Raises:
        Exception: オブジェクト生成中に致命的エラーが発生した場合に再送出する。

    Example:
        >>> detail = generate_race_objects(raw_data, logger)
        >>> detail.race.name
        'ジャパンカップ'
    """
    try:
        # ---------------------------------------------------------
        # 1. 開催情報オブジェクト生成
        # ---------------------------------------------------------
        v_raw = race_results.get("venue", {})
        venue = RaceVenue(
            round=v_raw.get("round", 0),
            place=to_halfwidth(v_raw.get("place", "")),
            day=v_raw.get("day", 0),
        )

        # ---------------------------------------------------------
        # 2. レース基本情報オブジェクト生成
        # ---------------------------------------------------------
        r_raw = race_results.get("race", {})
        race = RaceInfo(
            number=r_raw.get("number", 0),
            name=to_halfwidth(r_raw.get("name", "")),
            surface=to_halfwidth(r_raw.get("surface", "")),
            distance_m=r_raw.get("distance_m", 0),
            weather=to_halfwidth(r_raw.get("weather", "")),
            track_condition=to_halfwidth(r_raw.get("track_condition", "")),
            final_time=to_halfwidth(r_raw.get("final_time", "")),
            lap_time=r_raw.get("lap_time", []),
            corner_order={
                to_halfwidth(k): v for k, v in r_raw.get("corner_order", {}).items()
            },
        )

        # ---------------------------------------------------------
        # 3. 出走馬リスト生成
        # ---------------------------------------------------------
        horses = [
            HorseEntry(
                horse_id=extract_id_from_url(h.get("url")),
                # 文字列フィールドのみ半角正規化を適用し、数値・リスト型は維持する
                **{
                    k: (to_halfwidth(v) if isinstance(v, str) else v)
                    for k, v in h.items()
                },
            )
            for h in race_results.get("horses", [])
        ]

        # ---------------------------------------------------------
        # 4. 払戻金リスト生成
        # ---------------------------------------------------------
        payouts = [
            Payout(
                type=to_halfwidth(p.get("type", "")),
                target=to_halfwidth(p.get("target", "")),
                amount=p.get("amount", 0),
            )
            for p in race_results.get("payouts", [])
        ]

        return RaceDetail(
            date=to_halfwidth(race_results.get("date", "")),
            weekday=to_halfwidth(race_results.get("weekday", "")),
            venue=venue,
            race=race,
            horses=horses,
            payouts=payouts,
        )

    except Exception as e:
        logger.error(f"RaceDetail オブジェクト変換中に致命的エラー: {e}")
        raise


# ---------------------------------------------------------
# メインスクレイピング処理
# ---------------------------------------------------------


def scrape_race_details(
    race_urls: List[str],
    logger: logging.Logger,
) -> List[RaceDetail]:
    """
    複数の JBIS レース結果 URL を逐次解析し、RaceDetail リストを返す。

    Args:
        race_urls (List[str]): JBIS レース結果ページの URL リスト。
        logger (logging.Logger): ログ出力用ロガー。

    Returns:
        List[RaceDetail]: 解析成功した RaceDetail オブジェクトのリスト。
                          失敗した URL はスキップされリストに含まれない。

    Raises:
        None: 各 URL の例外は内部でキャッチしログ出力するため外部へ伝播しない。

    Example:
        >>> urls = ["https://www.jbis.or.jp/race/result/20251130/105/12/"]
        >>> details = scrape_race_details(urls, logger)
        >>> len(details)
        1
    """
    results: List[RaceDetail] = []
    total_urls: int = len(race_urls)

    for i, url in enumerate(race_urls, 1):
        try:
            logger.info(f"[{i}/{total_urls}] 解析処理を開始: {url}")

            # ---------------------------------------------------------
            # HTTP リクエスト (共通部品 fetch_html を利用)
            # ---------------------------------------------------------

            # リトライ処理は fetch_html 側で実装されているため直接呼び出す
            response = fetch_html(url, logger)

            if not response or not response.text:
                logger.error(f"HTML の取得に失敗したためスキップします: {url}")
                continue

            # ---------------------------------------------------------
            # データ抽出・オブジェクト変換フェーズ
            # ---------------------------------------------------------

            raw_data = extract_race_data(response.text, logger)
            detail = generate_race_objects(raw_data, logger)

            results.append(detail)
            logger.info(f"解析完了: {detail.race.name} ({len(detail.horses)} 頭)")

        except Exception as e:
            logger.error(f"URL 解析中に未予期のエラーが発生: {url} | {e}")
            continue

    return results


# ---------------------------------------------------------
# 簡易単体テスト
# ---------------------------------------------------------


def _run_tests() -> None:
    """主要機能の動作確認テストを実行する。

    正常系・異常系の双方を検証し、外部依存がある場合はダミーデータで代替する。
    全テストが通過した場合のみ "ALL TESTS PASSED" を表示する。
    """
    import sys

    # ---- ログ設定 ----
    test_logger = logging.getLogger("test_scraper")
    test_logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    test_logger.addHandler(handler)

    print("\n" + "=" * 60)
    print("  Unit Test: src/data_pipeline/race_detail_scraper.py")
    print("=" * 60 + "\n")

    errors: List[str] = []

    # ---------------------------------------------------------
    # [Test 1] to_halfwidth: 全角→半角・全角スペース置換
    # ---------------------------------------------------------
    print("[Test 1] to_halfwidth")
    cases = [
        ("１２３４　（テスト）", "1234 (テスト)"),
        (None, None),
        ("", ""),
        ("ABC", "ABC"),
    ]
    for raw, expected in cases:
        result = to_halfwidth(raw)
        status = "OK" if result == expected else "FAIL"
        print(
            f"  {status}: to_halfwidth({raw!r}) -> {result!r} (expected: {expected!r})"
        )
        if status == "FAIL":
            errors.append(f"to_halfwidth({raw!r}) returned {result!r}")

    # ---------------------------------------------------------
    # [Test 2] safe_int: 正常系・異常系
    # ---------------------------------------------------------
    print("\n[Test 2] safe_int")
    int_cases = [
        ("1,234円", 1234),
        ("0", 0),
        ("", None),
        (None, None),
        ("abc", None),
    ]
    for raw, expected in int_cases:
        result = safe_int(raw)
        status = "OK" if result == expected else "FAIL"
        print(f"  {status}: safe_int({raw!r}) -> {result!r} (expected: {expected!r})")
        if status == "FAIL":
            errors.append(f"safe_int({raw!r}) returned {result!r}")

    # ---------------------------------------------------------
    # [Test 3] safe_float: 正常系・異常系
    # ---------------------------------------------------------
    print("\n[Test 3] safe_float")
    float_cases = [
        ("34.5kg", 34.5),
        ("1.0", 1.0),
        ("", None),
        (None, None),
    ]
    for raw, expected in float_cases:
        result = safe_float(raw)
        status = "OK" if result == expected else "FAIL"
        print(f"  {status}: safe_float({raw!r}) -> {result!r} (expected: {expected!r})")
        if status == "FAIL":
            errors.append(f"safe_float({raw!r}) returned {result!r}")

    # ---------------------------------------------------------
    # [Test 4] extract_id_from_url: URL パースの正常系・異常系
    # ---------------------------------------------------------
    print("\n[Test 4] extract_id_from_url")
    url_cases = [
        ("/horse/2020101234/", "2020101234"),
        ("https://www.jbis.or.jp/horse/1999100001/profile/", "1999100001"),
        ("/horse/123/", ""),  # 桁数不足
        (None, ""),
        ("", ""),
    ]
    for raw, expected in url_cases:
        result = extract_id_from_url(raw)
        status = "OK" if result == expected else "FAIL"
        print(f"  {status}: extract_id_from_url({raw!r}) -> {result!r}")
        if status == "FAIL":
            errors.append(f"extract_id_from_url({raw!r}) returned {result!r}")

    # ---------------------------------------------------------
    # [Test 5] extract_race_data: ダミー HTML による結合テスト
    # ---------------------------------------------------------
    print("\n[Test 5] extract_race_data (dummy HTML)")
    dummy_html = """
    <html><body>
      <div class="hdg1-search"><h1>2025年11月30日(日) 3回 東京 5日</h1></div>
      <div class="hdg2-l-1"><h2>12R ジャパンカップ</h2></div>
      <div class="box-race__text">芝2400M 天候：晴 芝：良</div>
    </body></html>
    """
    raw = extract_race_data(dummy_html, test_logger)

    checks = [
        ("date", raw.get("date"), "2025-11-30"),
        ("weekday", raw.get("weekday"), "日"),
        ("venue.place", raw["venue"]["place"], "東京"),
        ("venue.round", raw["venue"]["round"], 3),
        ("race.number", raw["race"]["number"], 12),
        ("race.name", raw["race"]["name"], "ジャパンカップ"),
        ("race.surface", raw["race"]["surface"], "芝"),
        ("race.distance_m", raw["race"]["distance_m"], 2400),
        ("race.weather", raw["race"]["weather"], "晴"),
        ("race.track_cond", raw["race"]["track_condition"], "良"),
    ]
    for label, actual, expected in checks:
        status = "OK" if actual == expected else "FAIL"
        print(f"  {status}: {label} -> {actual!r} (expected: {expected!r})")
        if status == "FAIL":
            errors.append(
                f"extract_race_data[{label}] = {actual!r}, expected {expected!r}"
            )

    # ---------------------------------------------------------
    # [Test 6] scrape_race_details: 実ネットワーク疎通確認 (オプション)
    # ---------------------------------------------------------
    print("\n[Test 6] scrape_race_details (live network — skip if unavailable)")
    test_url = "https://www.jbis.or.jp/race/result/20251130/105/12/"
    try:
        details = scrape_race_details([test_url], test_logger)
        if details:
            d = details[0]
            print(
                f"  OK: Date={d.date} Venue={d.venue.place} Race={d.race.number}R Horses={len(d.horses)}"
            )
            if d.race.number != 12:
                errors.append(f"race.number expected 12, got {d.race.number}")
        else:
            print("  SKIP: No data returned. Network may be unavailable.")
    except Exception as e:
        print(f"  SKIP: Network test skipped due to exception: {e}")

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
    _run_tests()
