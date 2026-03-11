# プロジェクト概要

このプロジェクトは
競馬データを用いて競走馬の能力を予測するAIを構築する。

---

# システム目的

レース結果  
血統  
レース条件

を統合し

レース予測AIを作成する。

---

# データソース

主データ

JBIS

取得データ

レース結果  
出走馬情報  
血統情報

---

# データ構造

データは3つのSQLite DBに保存される

race.db  
pedigree.db  
sire_lineage.db

---

# AIモデル

主モデル

TabTransformer

理由

表形式データに強い  
カテゴリ特徴量に強い

---

# システム構造

```

data_pipeline
データ取得

features
特徴量生成

model
モデル定義

training
モデル学習

```

---

# 学習データ作成

以下の結合で作成する

```

races
↓
horse_entries
↓
pedigree_info
↓
sire_lineage

```

---

# 主な特徴量

レース条件

距離
芝ダート
馬場状態
天候

馬特徴

年齢
斤量
人気
スピード指数

血統特徴

系統
インブリード
祖先

---

# 想定課題

スクレイピング不安定
HTML変更
DBロック
データ欠損

---

# 将来拡張

Web API
予測WebUI
