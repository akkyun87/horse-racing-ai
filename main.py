"""
main.py

プログラムの実行を統括するメイン関数.

Usage:
    python main.py
"""

import os
from typing import List

from src.data_pipeline.data_validator import (
    validate_entries,
    validate_pedigree_data,
    validate_races,
)
from src.data_pipeline.pedigree_scraper import scrape_pedigree_info
from src.data_pipeline.race_detail_scraper import scrape_race_details
from src.data_pipeline.race_list_scraper import get_race_list_urls
from src.utils.file_manager import load_config, save_data
from src.utils.logger import setup_logger


def main() -> None:
    """プログラムの実行を統括するメイン関数"""

    # 設定ファイル読み込み
    log_filepath = "logs/data_collection.log"
    logger = setup_logger(
        log_filepath=log_filepath, log_level="INFO", logger_name=__name__
    )
    config = load_config("config/data_loader_config.yaml", logger)
    if not config:
        logger.error("設定ファイルの読み込みに失敗しました。処理を終了します。")
        return

    # レース URL 取得
    start_date = config.get("start_date", "2025-08-01")
    end_date = config.get("end_date", "2025-08-31")
    race_urls: List[str] = get_race_list_urls(start_date, end_date, logger)
    if not race_urls:
        logger.error("取得したレース URL がありません。処理を終了します。")
        return

    # レース詳細と出走馬データ取得
    race_entries_list = scrape_race_details(race_urls, logger)

    races: List = []
    entries: List = []

    for race, race_entries in race_entries_list:
        races.append(race)
        entries.extend(race_entries)

    # データ検証
    if not validate_races(races, logger):
        logger.error("レースデータが無効です。処理を終了します。")
        return

    if not validate_entries(entries, logger):
        logger.error("出走馬データが無効です。処理を終了します。")
        return

    # データ保存
    os.makedirs("data/raw", exist_ok=True)
    save_data([r.__dict__ for r in races], "data/raw/races.json", logger)
    save_data([e.__dict__ for e in entries], "data/raw/entries.json", logger)

    # 血統データ取得
    horse_urls = [e.horse_url for e in entries if getattr(e, "horse_url", None)]
    pedigrees = scrape_pedigree_info(horse_urls, logger)

    if not validate_pedigree_data(pedigrees, logger):
        logger.error("血統データが無効です。処理を終了します。")
        return

    # 血統データ保存
    save_data([p.__dict__ for p in pedigrees], "data/raw/pedigrees.json", logger)

    logger.info("データ収集処理が正常に完了しました。")


if __name__ == "__main__":
    main()
