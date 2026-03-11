# プロジェクト重要メモ

このファイルはAIが覚えておくべき
重要事項を記録する。

---

# DB注意点

race.db

horse_id は INTEGER

pedigree.db

horse_id は TEXT (10桁0埋め)

結合時

型変換が必要

---

# データ問題

weatherに空白あり

例

"晴 "

データクレンジング必要

---

# pedigreeテーブル注意

カラム名に空白あり

SQLでは

[カラム名]

で指定する必要がある

---

# 特徴量設計

血統は

5代祖先

62頭

CSV形式

---

# データ取得

src/data_pipeline

race_list_scraper  
race_detail_scraper  
pedigree_scraper

---

# utils

重要モジュール

db_manager  
retry_requests  
logger

---

# 競馬AI特徴量

重要

スピード指数  
上がり3F  
人気  
距離適性  
血統系統

---

# 今後追加予定

インブリード計算  
血統可視化
