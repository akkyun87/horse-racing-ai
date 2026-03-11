# 競走馬能力予測システムプログラム設計書

- **作成日 :** 2025-08-15
- **最終更新日 :** 2025-08-15
- **バージョン :** 1.0
- **作成者 :** Akkyun
- **連絡先 :** ---

## 0. 目次

- [競走馬能力予測システムプログラム設計書](#競走馬能力予測システムプログラム設計書)
  - [0. 目次](#0-目次)
  - [1. 必要なプログラムコンポーネント](#1-必要なプログラムコンポーネント)
    - [1.1. 全体ディレクトリ構成](#11-全体ディレクトリ構成)
    - [1.2. 各プログラムコンポーネントの役割](#12-各プログラムコンポーネントの役割)
    - [1.2. 必要外部モジュール](#12-必要外部モジュール)
  - [2. utilsモジュール](#2-utilsモジュール)
    - [2.1. プログラムファイル構成](#21-プログラムファイル構成)
    - [2.2. 関数仕様詳細](#22-関数仕様詳細)
      - [`src/utils/logger.py`](#srcutilsloggerpy)
      - [`src/utils/retry_requests.py`](#srcutilsretry_requestspy)
      - [`src/utils/db_manager.py`](#srcutilsdb_managerpy)
      - [`src/utils/file_manager.py`](#srcutilsfile_managerpy)
  - [3. data_pipelineモジュール](#3-data_pipelineモジュール)
    - [3.1. プログラムファイル構成](#31-プログラムファイル構成)
    - [3.2. 関数仕様詳細](#32-関数仕様詳細)
      - [`src/data_pipeline/data_loader.py`](#srcdata_pipelinedata_loaderpy)
      - [`src/data_pipeline/data_models.py`](#srcdata_pipelinedata_modelspy)
      - [`src/data_pipeline/data_validator.py`](#srcdata_pipelinedata_validatorpy)
      - [`src/data_pipeline/pedigree_scraper.py`](#srcdata_pipelinepedigree_scraperpy)
      - [`src/data_pipeline/race_detail_scraper.py`](#srcdata_pipelinerace_detail_scraperpy)
      - [`src/data_pipeline/race_list_scraper.py`](#srcdata_pipelinerace_list_scraperpy)

## 1. 必要なプログラムコンポーネント

本章では、競走馬能力予測システムを実装するために必要となる主要なプログラムコンポーネントと、それぞれの役割について定義する。これらは、プロジェクトの論理的な構造と開発の指針となる。

### 1.1. 全体ディレクトリ構成

```text
Folder PATH listing for volume Windows
Volume serial number is 88C9-1F07
C:.
|   .pylintrc
|   main.py
|   REAMDME.md
|   requirements.txt
|
+---.vscode
|       settings.json
|
+---backup
|   +---data
|   |   +---processed
|   |   +---raw
|   |   |   +---pedigree
|   |   |   |       pedigree.db
|   |   |   |       sire_lineage.db
|   |   |   |
|   |   |   \---race
|   |   |           race.db
|   |   |
|   |   +---stats
|   |   \---urls
|   \---data_pipeline
|       |   data_loader.py
|       |   data_models.py
|       |   data_validator.py
|       |   pedigree_scraper.py
|       |   race_detail_scraper.py
|       |   race_list_scraper.py
|       |
|       \---__pycache__
|               data_loader.cpython-312.pyc
|               data_models.cpython-312.pyc
|               data_validator.cpython-312.pyc
|               pedigree_lineage_parser.cpython-312.pyc
|               pedigree_lineage_scraper.cpython-312.pyc
|               pedigree_scraper.cpython-312.pyc
|               race_detail_scraper.cpython-312.pyc
|               race_list_scraper.cpython-312.pyc
|
+---config
|       data_loader_config.yaml
|       lineage.yaml
|
+---data
|   +---processed
|   +---raw
|   |   +---pedigree
|   |   |       pedigree.db
|   |   |       sire_lineage.db
|   |   |
|   |   \---race
|   |           race.db
|   |
|   +---stats
|   \---urls
|       \---2025
|           +---12
|           |       race_list_2025-12.json
|           |
|           \---6
|                   race_list_2025-06.json
|
+---docs
|   |   directory_structure.txt
|   |
|   +---design
|   |       DATABASE_DESIGN.md
|   |       PROGRAM_DESIGN.md
|   |       SYSTEM_DESIGN.md
|   |
|   \---other
|           directory_structure.txt
|           memo.md
|           programmemo.txt
|           PROMPT.md
|
+---images
|   +---drawio
|   |       data_pipeline_architecture.png
|   |       drawio.drawio
|   |
|   \---img
+---logs
|       test_logger.log
|
+---models
+---old
|   \---20251227_data_pipeline_src
|       +---data_pipeline
|       |   |   data_loader.py
|       |   |   data_models.py
|       |   |   data_validator.py
|       |   |   pedigree_lineage_scraper.py
|       |   |   pedigree_scraper.py
|       |   |   race_detail_scraper.py
|       |   |   race_list_scraper.py
|       |   |
|       |   \---__pycache__
|       |           data_loader.cpython-312.pyc
|       |           data_models.cpython-312.pyc
|       |           data_validator.cpython-312.pyc
|       |           pedigree_lineage_parser.cpython-312.pyc
|       |           pedigree_lineage_scraper.cpython-312.pyc
|       |           pedigree_scraper.cpython-312.pyc
|       |           race_detail_scraper.cpython-312.pyc
|       |           race_list_scraper.cpython-312.pyc
|       |
|       +---data_processing
|       |       get_lineage.py
|       |
|       +---model
|       +---training
|       \---utils
|           |   db_manager.py
|           |   file_manager.py
|           |   logger.py
|           |   retry_requests.py
|           |
|           \---__pycache__
|                   db_manager.cpython-312.pyc
|                   file_manager.cpython-312.pyc
|                   logger.cpython-312.pyc
|                   retry_requests.cpython-312.pyc
|
+---results
+---scripts
+---src
|   +---data_pipeline
|   |   |   data_loader.py
|   |   |   data_models.py
|   |   |   data_validator.py
|   |   |   pedigree_scraper.py
|   |   |   race_detail_scraper.py
|   |   |   race_list_scraper.py
|   |   |
|   |   \---__pycache__
|   |           data_loader.cpython-312.pyc
|   |           data_models.cpython-312.pyc
|   |           data_validator.cpython-312.pyc
|   |           pedigree_lineage_parser.cpython-312.pyc
|   |           pedigree_lineage_scraper.cpython-312.pyc
|   |           pedigree_scraper.cpython-312.pyc
|   |           race_detail_scraper.cpython-312.pyc
|   |           race_list_scraper.cpython-312.pyc
|   |
|   +---features
|   |   |   dataset_builder.py
|   |   |   feature_joiner.py
|   |   |   horse_features.py
|   |   |   lineage_features.py
|   |   |   pedigree_features.py
|   |   |   race_features.py
|   |   |
|   |   \---__pycache__
|   |           horse_features.cpython-312.pyc
|   |           lineage_features.cpython-312.pyc
|   |           pedigree_features.cpython-312.pyc
|   |           race_features.cpython-312.pyc
|   |
|   +---model
|   +---training
|   \---utils
|       |   db_manager.py
|       |   file_manager.py
|       |   logger.py
|       |   retry_requests.py
|       |
|       \---__pycache__
|               db_manager.cpython-312.pyc
|               file_manager.cpython-312.pyc
|               logger.cpython-312.pyc
|               retry_requests.cpython-312.pyc
|
\---tests
    |   test.html
    |   test.py
    |   test_data.json
    |   test_data.yaml
    |
    \---__pycache__
            test.cpython-312.pyc


```

### 1.2. 各プログラムコンポーネントの役割

### 1.2. 必要外部モジュール

## 2. utilsモジュール

本章では、システム全体で共通利用されるユーティリティ機能を提供する `src/utils` ディレクトリ配下のプログラムについて定義する。  
本モジュールはログ管理、ファイル管理、データベース操作、HTTPリトライ処理などの共通処理を提供し、他モジュールから横断的に利用される。

### 2.1. プログラムファイル構成

- `src/utils`
  - **`src/utils/logger.py`**: プロジェクト全体で使用するロガーの設定およびログ出力ハンドラの生成を行うユーティリティモジュール。
  - **`src/utils/retry_requests.py`**: HTTP リクエストを送信し、ネットワークエラーやサーバーエラー発生時にリトライ処理を行うユーティリティモジュール。
  - **`db_manager.py`**: SQLite3 データベースに対するデータの保存（I/O）および読み込みを共通化し、テーブルの自動生成機能を備える。
  - **`file_manager.py`**: JSONおよびYAML形式のファイル入出力を共通化し、ディレクトリの自動生成や拡張子判別による安全なデータ操作を提供する。

---

### 2.2. 関数仕様詳細

#### `src/utils/logger.py`

- **`validate_log_level(log_level) -> int`**
  - **役割**: ログレベルの文字列を検証し、対応する `logging` モジュールのレベル定数に変換する。
  - **引数**:
    - `log_level` (`str`): ログレベル文字列。例: `"DEBUG"`, `"INFO"`。
  - **返り値**:
    - `int`: Python `logging` モジュールのログレベル定数。
  - **処理**:
    1. **ログレベル定義**
       - 文字列ログレベルと `logging` レベル定数の対応表を作成する。
       - `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`, `"CRITICAL"` を定義する。
    2. **ログレベル取得**
       - 入力されたログレベル文字列を大文字へ変換する。
       - 対応表から該当するレベル定数を取得する。
    3. **入力値検証**
       - 対応表に存在しない場合は `None` が返る。
       - その場合 `ValueError` を発生させる。
    4. **結果返却**
       - 検証済みのログレベル定数を返す。

---

- **`ensure_log_directory(log_filepath) -> None`**
  - **役割**: ログファイルを保存するディレクトリの存在を確認し、存在しない場合は作成する。
  - **引数**:
    - `log_filepath` (`str`): ログファイルの保存パス。
  - **返り値**:
    - `None`: 戻り値なし。
  - **処理**:
    1. **ディレクトリパス抽出**
       - 指定されたログファイルパスからディレクトリ部分を取得する。
    2. **ディレクトリ存在確認**
       - ディレクトリパスが空でない場合のみ処理を行う。
    3. **ディレクトリ作成**
       - 対象ディレクトリが存在しない場合、ディレクトリを作成する。
       - 既に存在する場合はエラーを発生させず処理を継続する。

---

- **`create_console_handler(log_level, log_format) -> logging.StreamHandler`**
  - **役割**: コンソール出力用のログハンドラを生成し、ログレベルとフォーマットを設定する。
  - **引数**:
    - `log_level` (`int`): コンソール出力のログレベル。
    - `log_format` (`str`): ログ出力フォーマット文字列。
  - **返り値**:
    - `logging.StreamHandler`: 設定済みコンソールハンドラ。
  - **処理**:
    1. **ハンドラ生成**
       - `StreamHandler` を生成する。
    2. **ログレベル設定**
       - ハンドラに指定されたログレベルを設定する。
    3. **フォーマッタ生成**
       - ログフォーマットと日時フォーマット `%Y-%m-%d %H:%M:%S` を指定したフォーマッタを作成する。
    4. **フォーマッタ設定**
       - 作成したフォーマッタをハンドラに設定する。
    5. **結果返却**
       - 設定済みハンドラを返す。

---

- **`create_file_handler(log_filepath, log_level, log_format) -> logging.FileHandler`**
  - **役割**: ファイル出力用ログハンドラを生成し、ログレベルとフォーマットを設定する。
  - **引数**:
    - `log_filepath` (`str`): ログファイルの保存パス。
    - `log_level` (`int`): ファイル出力のログレベル。
    - `log_format` (`str`): ログ出力フォーマット文字列。
  - **返り値**:
    - `logging.FileHandler`: 設定済みファイルハンドラ。
  - **処理**:
    1. **ハンドラ生成**
       - UTF-8 エンコーディング指定でファイルハンドラを生成する。
    2. **ログレベル設定**
       - ハンドラにログレベルを設定する。
    3. **フォーマッタ生成**
       - ログフォーマットと日時フォーマット `%Y-%m-%d %H:%M:%S` を指定したフォーマッタを生成する。
    4. **フォーマッタ設定**
       - 作成したフォーマッタをハンドラに設定する。
    5. **結果返却**
       - 設定済みハンドラを返す。

---

- **`setup_logger(log_filepath, log_level="INFO", logger_name=None, log_format_console="[%(asctime)s][%(levelname)s][%(name)s] %(message)s", log_format_file="[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d] %(message)s") -> logging.Logger`**
  - **役割**: コンソール出力とファイル出力の両方を行うロガーを作成し、ハンドラを設定する。
  - **引数**:
    - `log_filepath` (`str`): ログファイルの保存パス。
    - `log_level` (`str`): 最小ログ出力レベル。
    - `logger_name` (`Optional[str]`): ロガー名。
    - `log_format_console` (`str`): コンソール出力用ログフォーマット。
    - `log_format_file` (`str`): ファイル出力用ログフォーマット。
  - **返り値**:
    - `logging.Logger`: 設定済みロガー。
  - **処理**:
    1. **ロガー取得**
       - ロガー名が指定されていない場合は現在モジュール名を使用する。
       - `logging.getLogger` によりロガーを取得する。
    2. **ログレベル検証**
       - `validate_log_level(log_level) -> int`
       - **役割**: ログレベル文字列を logging レベル定数に変換する。
       - **引数**:
         - `log_level` (`str`): ログレベル文字列。
       - **返り値**:
         - `int`: logging レベル定数。
       - **処理**:
         1. ログレベル対応表から該当する値を取得する。
         2. 存在しない場合は `ValueError` を発生させる。
    3. **ロガーレベル設定**
       - 検証済みログレベルをロガーに設定する。
    4. **ハンドラ重複防止**
       - 既にロガーにハンドラが設定されている場合は新規設定を行わずロガーを返す。
    5. **ログディレクトリ確認**
       - `ensure_log_directory(log_filepath) -> None`
       - ログファイル保存ディレクトリが存在しない場合は作成する。
    6. **コンソールハンドラ生成**
       - `create_console_handler(level, log_format_console)`
    7. **ファイルハンドラ生成**
       - `create_file_handler(log_filepath, level, log_format_file)`
    8. **ハンドラ登録**
       - 生成したハンドラをロガーへ追加する。
    9. **結果返却**
       - 設定済みロガーを返す。

#### `src/utils/retry_requests.py`

- **`fetch_html(url, logger, max_retries=5, request_interval=3.0, retry_interval=2.0, timeout=10.0, headers=None, backoff_strategy=None) -> Optional[requests.Response]`**
  - **役割**: 指定された URL に対して HTTP GET リクエストを送信し、通信エラーやサーバーエラー発生時にリトライ処理を行う。
  - **引数**:
    - `url` (`str`): リクエスト対象の URL。
    - `logger` (`logging.Logger`): ログ出力に使用するロガーインスタンス。
    - `max_retries` (`int`): 最大リトライ回数。（デフォルトは 5）
    - `request_interval` (`float`): 初回リクエスト前の待機時間（秒）。（デフォルトは 3.0）
    - `retry_interval` (`float`): リトライ時のベース待機時間（秒）。（デフォルトは 2.0）
    - `timeout` (`float`): HTTP リクエストのタイムアウト秒数。（デフォルトは 10.0）
    - `headers` (`Optional[Dict[str,str]]`): HTTP リクエストヘッダー。指定がない場合はデフォルト値を使用。
    - `backoff_strategy` (`Optional[Callable[[int],float]]`): 試行回数に応じて待機時間を計算する関数。
  - **返り値**:
    - `Optional[requests.Response]`: リクエスト成功時は `requests.Response` オブジェクトを返す。最大リトライ回数を超過した場合は `None` を返す。
  - **処理**:
    1. **入力値検証**
       - URL が空文字または空白のみの場合はエラーログを出力し `None` を返す。
       - `max_retries` が 0 以下の場合は不正設定としてデバッグログを出力し、値を 5 に変更する。
    2. **HTTPヘッダー設定**
       - `headers` が指定されていない場合はデフォルトヘッダーを設定する。
       - User-Agent、Accept-Language、Referer、Accept を含むヘッダーを使用する。
    3. **バックオフ戦略設定**
       - `backoff_strategy` が指定されていない場合はデフォルト戦略を使用する。
       - デフォルト戦略は `retry_interval × 2^(attempt-1)` により待機時間を計算する。
    4. **HTTP セッション生成**
       - `requests.Session` を生成する。
       - セッションに HTTP ヘッダーを設定する。
       - 試行回数 `attempt` を 0 で初期化する。
    5. **リクエスト実行ループ**
       - 試行回数が `max_retries` 以下の間、処理を繰り返す。
       - **初回リクエスト待機**
         - 試行回数が 0 の場合、`request_interval` 秒待機する。
       - **リトライ待機**
         - 試行回数が 1 以上の場合、バックオフ戦略により待機時間を算出する。
         - 待機時間だけ処理を停止する。
       - **HTTPリクエスト送信**
         - `session.get(url, timeout=timeout)` を実行する。
    6. **レスポンス判定**
       - ステータスコードが 500〜599 の場合はサーバーエラーとして例外を発生させる。
       - ステータスコードが 403 の場合はアクセス拒否として警告ログを出力する。
       - ステータスコードが 400 以上の場合は警告ログを出力する。
       - 正常レスポンスとしてレスポンスオブジェクトを返す。
    7. **通信例外処理**
       - `requests.exceptions.RequestException` を捕捉する。
       - 試行回数を 1 増加させる。
       - 通信エラー詳細をデバッグログとして出力する。
    8. **最大試行回数判定**
       - 試行回数が `max_retries` を超えた場合、エラーログを出力する。
       - `None` を返して処理を終了する。

#### `src/utils/db_manager.py`

- **`save_to_db(data, db_path, table_name, logger) -> bool`**
  - **役割**: 辞書形式のリストデータを、SQLite3 の指定テーブルに保存する。テーブルが存在しない場合は、データのキーに基づいて自動生成する。
  - **引数**:
    - `data` (`List[Dict[str, Any]]`): 保存対象のデータ。
    - `db_path` (`str`): データベースファイルの保存先パス。
    - `table_name` (`str`): 挿入先のテーブル名。
    - `logger` (`logging.Logger`): ログ出力用のロガーインスタンス。
  - **返り値**:
    - `bool`: 保存成功時は `True`、失敗時は `False`。
  - **処理**:
    1. **バリデーションフェーズ**:
       - `data` が空リストまたは `None` の場合、警告ログを出力して `False` を返す。
    2. **環境準備フェーズ**:
       - 指定された `db_path` の親ディレクトリが存在するか確認し、存在しない場合は `os.makedirs` で作成する。作成失敗時はエラーログを出力し `False` を返す。
    3. **SQL構築フェーズ**:
       - データの最初の要素（`data[0]`）のキーを取得し、カラム名リストおよびプレースホルダ（`?`）を生成する。
       - `CREATE TABLE IF NOT EXISTS` 文を構築する。この際、カラムの型指定は行わず SQLite の動的型付けに依存する。
    4. **実行・書き込みフェーズ**:
       - `sqlite3.connect` を用いて接続を確立し、`with` 文によるコンテキスト管理を行う。
       - テーブル作成 SQL を実行する。
       - `cursor.executemany` を使用して、辞書データをタプルに変換した上で一括挿入（バルクインサート）を行う。
       - `conn.commit()` を実行して変更を確定させる。
    5. **例外処理フェーズ**:
       - `sqlite3.Error` が発生した場合はエラー詳細をログ出力し、`False` を返す。

- **`load_from_db(db_path, table_name, logger) -> Optional[List[Dict[str, Any]]]`**
  - **役割**: 指定されたテーブルから全レコードを読み込み、カラム名をキーとした辞書形式のリストとして返す。
  - **引数**:
    - `db_path` (`str`): データベースファイルのパス。
    - `table_name` (`str`): 読み込み対象のテーブル名。
    - `logger` (`logging.Logger`): ログ出力用のロガーインスタンス。
  - **返り値**:
    - `List[Dict[str, Any]]`: 取得したデータのリスト。
    - `[]`: テーブルにデータが存在しない場合。
    - `None`: データベースファイル不在、またはエラー発生時。
  - **処理**:
    1. **ファイルチェックフェーズ**:
       - `db_path` が存在しない場合、警告ログを出力して `None` を返す。
    2. **読み込み設定フェーズ**:
       - 接続確立後、`conn.row_factory = sqlite3.Row` を設定し、レコードを辞書ライクなオブジェクトとして取得可能にする。
    3. **クエリ実行フェーズ**:
       - `SELECT * FROM {table_name}` を実行し、`fetchall()` で全件取得する。
    4. **データ変換フェーズ**:
       - 取得結果が空の場合は警告ログを出力し、空リストを返す。
       - 取得されたレコード群を `dict(row)` で標準的な Python 辞書に変換し、リスト化する。
    5. **例外処理フェーズ**:
       - `sqlite3.Error` が発生した場合はエラー詳細をログ出力し、`None` を返す。

#### `src/utils/file_manager.py`

- **`_save_json(data, file_path, indent, ensure_ascii) -> None`**
  - **役割**: 辞書データをJSON形式でファイルに保存する。本モジュール内でのみ使用される補助関数。
  - **引数**:
    - `data` (`Dict[str, Any]`): 保存対象の辞書データ。
    - `file_path` (`str`): 保存先パス。
    - `indent` (`int`): インデント幅（デフォルトは 4）。
    - `ensure_ascii` (`bool`): `False`を指定し、日本語をUnicodeエスケープせずに出力する。
  - **返り値**:
    - `None`
  - **処理**:
    1. **ファイルオープン**: 指定パスを `utf-8` エンコーディングで書き込みモードとして開く。
    2. **シリアライズ**: `json.dump` を用いて、指定されたインデントと非ASCII許可設定でデータを書き込む。

- **`_load_json(file_path) -> Dict[str, Any]`**
  - **役割**: JSONファイルを読み込み、辞書として返す。
  - **引数**:
    - `file_path` (`str`): 読み込み対象パス。
  - **返り値**:
    - `Dict[str, Any]`: 変換された辞書データ。
  - **処理**:
    1. **デシリアライズ**: `json.load` を用いてファイルを辞書オブジェクトに変換する。

- **`_save_yaml(data, file_path) -> None`**
  - **役割**: 辞書データをYAML形式でファイルに保存する。
  - **引数**:
    - `data` (`Dict[str, Any]`): 保存対象。
    - `file_path` (`str`): 保存先。
  - **返り値**:
    - `None`
  - **処理**:
    1. **シリアライズ**: `yaml.safe_dump` を使用し、`allow_unicode=True` 設定で日本語等のマルチバイト文字を保持したまま書き込む。

- **`_load_yaml(file_path) -> Dict[str, Any]`**
  - **役割**: YAMLファイルを読み込み、辞書として返す。
  - **引数**:
    - `file_path` (`str`): 読み込み対象。
  - **返り値**:
    - `Dict[str, Any]`: 変換された辞書データ。
  - **処理**:
    1. **デシリアライズ**: `yaml.safe_load` を用いてファイルを安全に解析し、辞書化する。

- **`save_data(data, file_path, logger, indent) -> bool`**
  - **役割**: 指定されたパスの拡張子を自動判別（JSON/YAML）し、ディレクトリ作成を含めた保存処理を実行する。
  - **引数**:
    - `data` (`Dict[str, Any]`): 保存対象データ。
    - `file_path` (`str`): 保存先ファイルパス。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
    - `indent` (`int`): JSON保存時のインデント（デフォルトは 4）。
  - **返り値**:
    - `bool`: 成功時は `True`、失敗時は `False`。
  - **処理**:
    1. **環境準備フェーズ**:
       - `pathlib.Path` を用いて親ディレクトリの存在を確認し、存在しない場合は `mkdir(parents=True)` で再帰的に作成する。
    2. **判別・実行フェーズ**:
       - 拡張子（`.json`, `.yaml`, `.yml`）を取得し、小文字化して判定する。
       - 対応する内部補助関数（`_save_json` または `_save_yaml`）を呼び出す。
       - 未対応の拡張子の場合は `NotImplementedError` を送出する。
    3. **終了・例外処理フェーズ**:
       - 成功時は `DEBUG` レベルでログを出力する。
       - 構文エラーやアクセス権限エラー等のあらゆる例外を捕捉し、`ERROR` レベルで詳細を記録した上で `False` を返す。

- **`load_data(file_path, logger) -> Optional[Dict[str, Any]]`**
  - **役割**: 拡張子に基づきJSONまたはYAMLファイルを読み込む。
  - **引数**:
    - `file_path` (`str`): 読み込み対象パス。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `Optional[Dict[str, Any]]`: 成功時はデータ、失敗時またはファイル不在時は `None`。
  - **処理**:
    1. **存在確認フェーズ**:
       - `os.path.exists` でファイルの有無を確認。存在しない場合は `WARNING` ログを出力し `None` を返す。
    2. **判別・実行フェーズ**:
       - 拡張子に応じて `_load_json` または `_load_yaml` を呼び出す。
       - 未対応形式の場合は `NotImplementedError` を送出する。
    3. **例外処理フェーズ**:
       - ファイル破損やフォーマット不正等の例外を捕捉し、`ERROR` ログを出力して `None` を返す。

## 3. data_pipelineモジュール

本章では、競馬データの収集、検証、および構造化を担う `src/data_pipeline` ディレクトリ配下のプログラムについて定義する。  
本モジュールは、外部ソース（JBIS等）からのスクレイピング、取得データのバリデーション、および定義されたデータモデルへのマッピングを行い、後続の解析・学習プロセスへクリーンなデータを提供することを目的とする。

### 3.1. プログラムファイル構成

- `src/data_pipeline`
  - **`src/data_pipeline/data_loader.py`**: ローカルストレージ（JSON/YAML/DB）からのデータ読み込みおよび保存、スクレイピング済みデータの統合管理を行う。
  - **`src/data_pipeline/data_models.py`**: 競走馬、レース結果、血統情報などのデータ構造を定義し、型定義とシリアライズを制御する。
  - **`src/data_pipeline/data_validator.py`**: 取得したデータが期待される型や範囲、競馬ドメインの制約（日付形式、着順の妥当性等）を満たしているかを検証する。
  - **`src/data_pipeline/pedigree_scraper.py`**: JBIS等の血統情報ページを解析し、馬の血統（父、母、母の父等）を再帰的または指定階層まで抽出する。
  - **`src/data_pipeline/race_detail_scraper.py`**: 個別のレース詳細ページを解析し、各馬の走破タイム、通過順位、馬体重、配当金等の詳細情報を抽出する。
  - **`src/data_pipeline/race_list_scraper.py`**: 開催日や競馬場別のレース一覧ページから、各レースのID、名称、距離、発走時刻などの基本情報を抽出する。

---

### 3.2. 関数仕様詳細

#### `src/data_pipeline/data_loader.py`

- **`_load_config(config_path, logger) -> Dict[str, Any]`**
  - **役割**: YAML 設定ファイルを読み込み、データ収集パイプラインに必要な設定値を抽出する。
  - **引数**:
    - `config_path` (`str`): 設定ファイルのパス（YAML形式）。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `Dict[str, Any]`: 設定値辞書。ロード失敗時は空辞書 `{}`。
  - **処理**:
    1. **ファイル存在確認**
       - `pathlib.Path` を用いて設定ファイルパスを取得する。
    2. **設定ロード**
       - `utils.file_manager.load_data()` を使用して YAML ファイルを読み込む。
    3. **ロード失敗判定**
       - 読み込み結果が `None` または空の場合は `ERROR` ログを出力し `{}` を返す。
    4. **必須項目検証**
       - `data.start_date` および `data.end_date` が存在するか確認する。
       - 未定義の場合は `ERROR` ログを出力し `{}` を返す。
    5. **設定値整理**
       - 保存先ディレクトリ、スクレイピング設定、ログ設定を辞書として構築する。
       - 保存先ディレクトリは未指定時にデフォルト値を設定する。
    6. **結果返却**
       - 整形済み設定辞書を返す。

---

- **`_build_race_key(rd) -> str`**
  - **役割**: `RaceDetail` オブジェクトからレースを一意に識別するキー文字列を生成する。
  - **引数**:
    - `rd` (`RaceDetail`): レース詳細データ。
  - **返り値**:
    - `str`: `"date_place_raceNumber"` 形式の識別キー。
  - **処理**:
    1. **識別要素取得**
       - `rd.date`
       - `rd.venue.place`
       - `rd.race.number`
    2. **識別キー生成**
       - 上記値を `"_"` 区切り文字列として結合する。
    3. **結果返却**
       - 生成したレース識別キーを返す。

---

- **`_split_date_range(start_date, end_date) -> List[Tuple[str, str]]`**
  - **役割**: 指定された日付範囲を月単位の期間リストへ分割する。
  - **引数**:
    - `start_date` (`str`): 収集開始日（`YYYY-MM-DD`）。
    - `end_date` (`str`): 収集終了日（`YYYY-MM-DD`）。
  - **返り値**:
    - `List[Tuple[str,str]]`: `(月開始日, 月終了日)` のリスト。
  - **処理**:
    1. **日付パース**
       - `datetime.strptime` により文字列を日付型へ変換する。
    2. **月次ループ**
       - 開始月から終了月までループする。
    3. **月末日取得**
       - `calendar.monthrange` を用いて月末日を取得する。
    4. **実際の終了日決定**
       - 月末日と `end_date` の小さい方を採用する。
    5. **タプル追加**
       - `(開始日, 終了日)` をリストへ追加する。
    6. **翌月移動**
       - 月を1つ進めてループを継続する。
    7. **結果返却**
       - 月次分割リストを返す。

---

- **`_extract_year_month_from_url(url) -> Tuple[str, str]`**
  - **役割**: JBIS レース結果 URL から開催年および月を抽出する。
  - **引数**:
    - `url` (`str`): レース結果ページの URL。
  - **返り値**:
    - `Tuple[str,str]`: `(year, month)`。
  - **処理**:
    1. **正規表現検索**
       - `RACE_DATE_PATTERN` を用いて `YYYYMMDD` を抽出する。
    2. **抽出失敗判定**
       - マッチしない場合は `ValueError` を送出する。
    3. **年月分割**
       - 年 (`YYYY`) と月 (`MM`) を取得する。
    4. **月ゼロ埋め除去**
       - 月を整数化して文字列化する。
    5. **結果返却**
       - `(year, month)` を返す。

---

- **`_extract_year_month_from_date(date_str) -> Tuple[str,str]`**
  - **役割**: `YYYY-MM-DD` 形式の日付文字列から年と月を取得する。
  - **引数**:
    - `date_str` (`str`): 日付文字列。
  - **返り値**:
    - `Tuple[str,str]`: `(year, month)`。
  - **処理**:
    1. **文字列分割**
       - `split("-")` により年・月・日を取得する。
    2. **月ゼロ埋め除去**
       - 月を整数化して文字列化する。
    3. **例外処理**
       - 不正フォーマットの場合は `ValueError` を送出する。
    4. **結果返却**
       - `(year, month)` を返す。

---

- **`_extract_race_records(race_details, logger) -> List[Dict[str,Any]]`**
  - **役割**: `RaceDetail` オブジェクト群から `races` テーブル用レコードを生成する。
  - **引数**:
    - `race_details` (`List[RaceDetail]`): レース詳細データ。
    - `logger` (`logging.Logger`): ロガー。
  - **返り値**:
    - `List[Dict[str,Any]]`: `races` テーブル挿入用レコード。
  - **処理**:
    1. **レース名取得**
       - `rd.race.name` を取得する。
    2. **グレード判定**
       - `GRADE_PATTERN` により G1〜G3 等を判定する。
    3. **条件戦判定**
       - 未該当の場合 `_CLASS_KEYWORDS` を順次検索する。
    4. **レコード構築**
       - `race_key`, `date`, `venue`, `race_number` 等の項目を辞書化する。
    5. **リスト追加**
       - レコードをリストへ追加する。
    6. **ログ出力**
       - 生成件数を `INFO` ログ出力する。
    7. **結果返却**
       - レコードリストを返す。

---

- **`_extract_horse_entry_records(race_details, logger) -> List[Dict[str,Any]]`**
  - **役割**: `RaceDetail` から `horse_entries` テーブル用レコードを生成する。
  - **引数**:
    - `race_details` (`List[RaceDetail]`)
    - `logger` (`logging.Logger`)
  - **返り値**:
    - `List[Dict[str,Any]]`
  - **処理**:
    1. **race_key生成**
       - `_build_race_key()` を使用する。
    2. **馬ループ**
       - `rd.horses` を走査する。
    3. **horse_id抽出**
       - `HORSE_ID_PATTERN` により URL から ID を取得する。
    4. **passing_order変換**
       - リストをカンマ区切り文字列へ変換する。
    5. **レコード構築**
       - 出走結果情報を辞書へ格納する。
    6. **結果追加**
       - レコードリストへ追加する。
    7. **ログ出力**
       - 生成件数を `INFO` ログ出力する。

---

- **`_flatten_pedigree_info(pedigree_infos, logger) -> List[Dict[str,Any]]`**
  - **役割**: `PedigreeInfo` の階層データを DB 保存用のフラット構造へ変換する。
  - **引数**:
    - `pedigree_infos` (`List[PedigreeInfo]`)
    - `logger` (`logging.Logger`)
  - **返り値**:
    - `List[Dict[str,Any]]`
  - **処理**:
    1. **ラベル取得**
       - `LABELS_SIRE_INDEX` のキー順を取得する。
    2. **祖先名連結**
       - `five_gen_ancestor_names` をカンマ区切りへ変換する。
    3. **種牡馬名展開**
       - 各世代の種牡馬名を `sire_name_{label}` へ格納する。
    4. **系統名展開**
       - `lineage_name_{label}` カラムへ格納する。
    5. **デフォルト値設定**
       - 未取得データは `"未登録"` または `"不明"` を設定する。
    6. **結果追加**
       - レコードリストへ追加する。
    7. **ログ出力**
       - 生成件数を `INFO` ログ出力する。

---

- **`_extract_unique_horses(race_details, pedigree_db_path, logger) -> List[Dict[str,Any]]`**
  - **役割**: レース結果から未登録の馬のみを抽出する。
  - **引数**:
    - `race_details` (`List[RaceDetail]`)
    - `pedigree_db_path` (`str`)
    - `logger` (`logging.Logger`)
  - **返り値**:
    - `List[Dict[str,Any]]`
  - **処理**:
    1. **既存 DB 確認**
       - `pedigree_info` テーブルの `horse_id` を取得する。
    2. **既存 ID 集合生成**
       - DB 内馬 ID を `Set[int]` へ格納する。
    3. **レース走査**
       - `RaceDetail.horses` を走査する。
    4. **ID 抽出**
       - URL から `horse_id` を取得する。
    5. **新規馬判定**
       - 既存 DB 未登録かつ重複なしの馬を抽出する。
    6. **結果返却**
       - 新規馬情報 `{id,url}` のリストを返す。

---

- **`_filter_unique_entries(entries, db_file_path, table_name, key_columns, logger) -> List[Dict[str,Any]]`**
  - **役割**: DB 内既存レコードと照合し未登録データのみを抽出する。
  - **引数**:
    - `entries` (`List[Dict[str,Any]]`)
    - `db_file_path` (`str`)
    - `table_name` (`str`)
    - `key_columns` (`Union[str,List[str]]`)
    - `logger` (`logging.Logger`)
  - **返り値**:
    - `List[Dict[str,Any]]`
  - **処理**:
    1. **DB存在確認**
       - DB 未存在時は全件を返す。
    2. **既存キー取得**
       - 対象テーブルからキー列を取得する。
    3. **キー集合構築**
       - 既存キーを `Set` として保存する。
    4. **新規レコード抽出**
       - entries 内キーが集合に存在しないものを抽出する。
    5. **結果返却**
       - 新規レコードのみのリストを返す。

---

- **`_save_race_urls(all_race_urls, start_date, logger, base_dir=None) -> None`**
  - **役割**: レース URL を年月単位で JSON ファイルとして保存する。
  - **引数**:
    - `all_race_urls` (`List[str]`)
    - `start_date` (`str`)
    - `logger` (`logging.Logger`)
    - `base_dir` (`Optional[Path]`)
  - **返り値**:
    - `None`
  - **処理**:
    1. **保存対象確認**
       - URL リストが空の場合は処理を終了する。
    2. **フォールバック年月取得**
       - `start_date` から年月を取得する。
    3. **URL解析**
       - URL から年と月を抽出する。
    4. **グループ化**
       - `year/month` キーで URL を分類する。
    5. **ディレクトリ作成**
       - 保存ディレクトリを `mkdir(parents=True)` で作成する。
    6. **JSON保存**
       - `json.dump` を使用して URL リストを保存する。
    7. **ログ出力**
       - 保存完了ログを出力する。

---

- **`fetch_and_store_race_and_pedigree_data(logger, tests=False) -> None`**
  - **役割**: レースデータ収集・血統取得・データ検証・DB保存までを実行するメインパイプライン。
  - **引数**:
    - `logger` (`logging.Logger`)
    - `tests` (`bool`)
  - **返り値**:
    - `None`
  - **処理**:
    1. **設定ロード**
       - `_load_config()` を実行する。
    2. **URL収集**
       - `_split_date_range()` により月次分割する。
       - `race_list_scraper.get_race_list_urls()` を実行する。
    3. **URL保存**
       - `_save_race_urls()` を実行する。
    4. **レース詳細取得**
       - `scrape_race_details()` を実行する。
    5. **血統対象抽出**
       - `_extract_unique_horses()` を実行する。
    6. **血統スクレイピング**
       - `scrape_pedigree_data()` を実行する。
    7. **データ検証**
       - `validate_dataset()` を実行する。
    8. **レース保存**
       - `_extract_race_records()` → `_filter_unique_entries()` → `save_to_db()`。
    9. **出走馬保存**
       - `_extract_horse_entry_records()` → `_filter_unique_entries()` → `save_to_db()`。
    10. **血統保存**
        - `_flatten_pedigree_info()` → `save_to_db()`。
    11. **処理終了**
        - 正常終了ログを出力する。

#### `src/data_pipeline/data_models.py`

- **`EXPECTED_ANCESTOR_COUNT: int`**
  - **役割**: 5代血統表に含まれる祖先ノード数の期待値を定義する定数。
  - **値**:
    - `62`
  - **説明**:
    - 父母(2) + 祖父母(4) + 曾祖父母(8) + 高祖父母(16) + 5代祖(32) の合計ノード数。
    - `PedigreeInfo` における `five_gen_ancestor_names` および `five_gen_ancestor_ids` の要素数として想定される。

---

- **`EXPECTED_SIRE_COUNT: int`**
  - **役割**: 5代種牡馬ラインのノード数の期待値を定義する定数。
  - **値**:
    - `31`
  - **説明**:
    - 父(1) + 祖父(2) + 曾祖父(4) + 高祖父(8) + 5代祖父(16) の合計ノード数。
    - `PedigreeInfo` における `five_gen_sire_names` および `five_gen_sire_ids` の要素数として想定される。

---

- **`PedigreeInfo`**
  - **役割**: 競走馬の5代血統表情報を保持する不変データ構造（Raw Data）。
  - **属性**:
    - `horse_id` (`int`): 馬固有ID（JBIS採番の主キー）。
    - `name` (`str`): 馬名。
    - `five_gen_ancestor_names` (`Sequence[str]`): 5代祖先の馬名リスト（62要素を想定）。
    - `five_gen_ancestor_ids` (`Sequence[int]`): 5代祖先の馬IDリスト（62要素を想定）。
    - `five_gen_sire_names` (`Sequence[str]`): 5代種牡馬ラインの馬名リスト（31要素を想定）。
    - `five_gen_sire_ids` (`Sequence[int]`): 5代種牡馬ラインの馬IDリスト（31要素を想定）。
    - `five_gen_sire_lineage_names` (`Optional[Sequence[str]]`): 種牡馬ラインの系統名リスト（31要素、取得失敗時は `None`）。
    - `five_gen_sire_lineage_ids` (`Optional[Sequence[int]]`): 種牡馬ラインの系統IDリスト（31要素、取得失敗時は `None`）。
  - **特徴**:
    - `@dataclass(slots=True, frozen=True)` により以下を保証する。
      - **slots=True**: メモリ効率向上および属性追加防止。
      - **frozen=True**: 不変オブジェクトとして動作し、生成後の書き換えを禁止する。
  - **構造検証**:
    - 要素数検証などの構造チェックは `src/data_pipeline/data_validator.py` に委譲される。

---

- **`RaceVenue`**
  - **役割**: 競馬場および開催回次情報を保持する不変データ構造。
  - **属性**:
    - `round` (`int`): 開催回次（第○回）。
    - `place` (`str`): 競馬場名。
    - `day` (`int`): 開催日次（第○日）。
  - **特徴**:
    - `@dataclass(slots=True, frozen=True)` により不変オブジェクトとして扱われる。

---

- **`RaceInfo`**
  - **役割**: レースの環境条件および基本情報を保持するデータ構造。
  - **属性**:
    - `number` (`int`): レース番号。
    - `name` (`str`): レース名。
    - `surface` (`str`): 馬場種別（芝 / ダート等）。
    - `distance_m` (`int`): 距離（メートル）。
    - `weather` (`str`): 天候。
    - `track_condition` (`str`): 馬場状態。
    - `final_time` (`str`): 勝ち馬走破タイム。
    - `lap_time` (`Sequence[float]`): ラップタイム配列。
    - `corner_order` (`Mapping[str, Sequence[Union[str, Sequence[str]]]]`): コーナー通過順情報。
  - **特徴**:
    - ラップタイムは `Sequence` 型で保持し、NumPy配列などへの変換を容易にする。
    - コーナー通過順はコーナー番号をキーとする辞書構造で保持する。

---

- **`HorseEntry`**
  - **役割**: 出走馬ごとの詳細な競走成績情報を保持するデータ構造。
  - **必須属性**:
    - `rank` (`int`): 着順。
  - **識別情報属性**:
    - `frame` (`Optional[int]`): 枠番。
    - `number` (`Optional[int]`): 馬番。
    - `horse_id` (`int`): 馬固有ID。
    - `name` (`str`): 馬名。
    - `url` (`str`): 馬詳細ページURL。
    - `sex` (`str`): 性別。
    - `age` (`int`): 馬齢。
    - `jockey` (`str`): 騎手名。
    - `weight` (`float`): 斤量。
  - **競走成績属性**:
    - `time` (`Optional[float]`): 走破タイム（秒）。
    - `margin` (`Optional[str]`): 着差。
    - `passing_order` (`Optional[Sequence[int]]`): コーナー通過順位。
    - `last_3f` (`Optional[float]`): 上がり3F。
    - `speed_index` (`Optional[float]`): スピード指数。
    - `popularity` (`Optional[int]`): 人気順位。
    - `body_weight` (`Optional[int]`): 馬体重。
    - `diff_from_prev` (`Optional[int]`): 前走比体重差。
  - **関係者情報属性**:
    - `trainer_name` (`Optional[str]`): 調教師名。
    - `trainer_region` (`Optional[str]`): 調教師所属地区。
    - `owner` (`Optional[str]`): 馬主名。
    - `breeder` (`Optional[str]`): 生産者名。
  - **特徴**:
    - 欠損が多いデータのため、多くのフィールドが `Optional` 型で定義されている。
    - デフォルト値により最小引数でインスタンス生成可能。

---

- **`Payout`**
  - **役割**: 払戻金情報を保持するデータ構造。
  - **属性**:
    - `type` (`str`): 払戻種別。
    - `target` (`str`): 対象馬番または組み合わせ。
    - `amount` (`int`): 払戻金額（円）。

---

- **`RaceDetail`**
  - **役割**: 1レース分の全体情報を集約するデータコンテナ。
  - **属性**:
    - `date` (`str`): 開催日。
    - `weekday` (`str`): 曜日。
    - `venue` (`RaceVenue`): 開催情報。
    - `race` (`RaceInfo`): レース基本情報。
    - `horses` (`Sequence[HorseEntry]`): 出走馬リスト。
    - `payouts` (`Sequence[Payout]`): 払戻情報リスト。
  - **特徴**:
    - `horses` と `payouts` は `field(default_factory=list)` により安全に初期化される。
    - 複数インスタンス間でリストが共有されない設計。

---

- **`PedigreeFeature`**
  - **役割**: 血統情報をベクトル化した ML モデル入力用特徴量データ構造。
  - **属性**:
    - `horse_id` (`int`): 馬固有ID。
    - `sire_vector` (`Sequence[float]`): 種牡馬Embeddingベクトル。
    - `lineage_vector` (`Sequence[float]`): 系統Embeddingベクトル。
    - `cross_vector` (`Sequence[int]`): クロス配合One-Hotベクトル。
    - `inbreeding_vector` (`Sequence[float]`): インブリード血量ベクトル。
    - `nick_vector` (`Sequence[float]`): ニックス評価ベクトル。
  - **特徴**:
    - Embeddingベクトルおよび血統特徴量を統合したML入力データ。

---

- **`RacePerformanceFeature`**
  - **役割**: 競走成績をベクトル化した ML モデル入力用特徴量データ構造。
  - **識別属性**:
    - `race_id` (`int`): レースID。
    - `horse_id` (`int`): 馬ID。
  - **カテゴリ特徴量ベクトル**:
    - `distance_vector` (`Sequence[int]`)
    - `surface_vector` (`Sequence[int]`)
    - `course_vector` (`Sequence[int]`)
    - `draw_vector` (`Sequence[int]`)
    - `ground_vector` (`Sequence[int]`)
    - `running_style_vector` (`Sequence[int]`)
    - `age_vector` (`Sequence[int]`)
    - `race_level_vector` (`Sequence[int]`)
    - `season_vector` (`Sequence[int]`)
  - **連続値特徴量**:
    - `time_continuous` (`float`)
    - `body_weight_continuous` (`float`)
    - `final_time_continuous` (`float`)
    - `finish_position_continuous` (`float`)
  - **カテゴリ補助ベクトル**:
    - `time_category_vector` (`Sequence[int]`)
    - `body_weight_category_vector` (`Sequence[int]`)
    - `final_time_category_vector` (`Sequence[int]`)
  - **その他特徴量**:
    - `sex_vector` (`Sequence[int]`)
  - **特徴**:
    - 連続値は z-score 正規化済み。
    - カテゴリ変数は One-Hot または Embedding 形式でエンコードされている。

---

#### `src/data_pipeline/data_validator.py`

- **`_check_type(value, expected_type, field_name, errors) -> None`**
  - **役割**: 値が期待する型であるかを検証し、不一致の場合はエラーリストへ追記する。
  - **引数**:
    - `value` (`object`): 検証対象の値。
    - `expected_type` (`type`): 期待する型。
    - `field_name` (`str`): エラーメッセージに使用するフィールド名。
    - `errors` (`List[str]`): 検証エラーを追記するリスト (破壊的変更あり)。
  - **返り値**:
    - `None`: 戻り値なし。
  - **処理**:
    1. **型判定**:
       - `isinstance` を用いて `value` が `expected_type` と一致するかを判定する。
    2. **エラー追記**:
       - 一致しない場合、期待する型名と実際の型名を含むエラーメッセージを `errors` に追加する。

- **`_check_list_length(lst, expected, field_name, errors, allow_zero) -> None`**
  - **役割**: シーケンスの要素数が想定値と一致するかを検証し、不一致の場合はエラーリストへ追記する。
  - **引数**:
    - `lst` (`Sequence`): 検証対象のシーケンス。
    - `expected` (`int`): 想定される要素数。
    - `field_name` (`str`): エラーメッセージに使用するフィールド名。
    - `errors` (`List[str]`): 検証エラーを追記するリスト (破壊的変更あり)。
    - `allow_zero` (`bool`): 要素数 0 を許容するかどうかのフラグ (規定値: `False`)。
  - **返り値**:
    - `None`: 戻り値なし。
  - **処理**:
    1. **空リスト許容判定**:
       - `allow_zero` が `True` かつリスト長が 0 の場合は、正常として処理を終了する。
    2. **要素数比較**:
       - リスト長が `expected` と異なる場合、想定件数と実際件数を含むエラーメッセージを `errors` に追加する。

- **`_check_pair_length(list_a, list_b, name_a, name_b, errors) -> None`**
  - **役割**: 2つのシーケンスの要素数が一致するかを検証し、不一致の場合はエラーリストへ追記する。
  - **引数**:
    - `list_a` (`Sequence`): 検証対象のシーケンス A。
    - `list_b` (`Sequence`): 検証対象のシーケンス B。
    - `name_a` (`str`): シーケンス A のフィールド名。
    - `name_b` (`str`): シーケンス B のフィールド名。
    - `errors` (`List[str]`): 検証エラーを追記するリスト (破壊的変更あり)。
  - **返り値**:
    - `None`: 戻り値なし。
  - **処理**:
    1. **整合性比較**:
       - `list_a` と `list_b` の長さを比較する。
    2. **エラー追記**:
       - 長さが異なる場合、それぞれの件数を明記したエラーメッセージを `errors` に追加する。

- **`validate_pedigree_info(pedigree, logger, ignore_lineage_errors) -> Tuple[bool, List[str]]`**
  - **役割**: PedigreeInfo の構造的・論理的整合性を検証する。
  - **引数**:
    - `pedigree` (`PedigreeInfo`): 検証対象の血統情報オブジェクト。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
    - `ignore_lineage_errors` (`bool`): 系統エラーを無視するかどうかのフラグ (規定値: `True`)。
  - **返り値**:
    - `Tuple[bool, List[str]]`: (検証合否, エラーメッセージリスト)。
  - **処理**:
    1. **基本フィールド検証**:
       - `horse_id` の型 (int) および `name` の型 (str) と空文字チェックを行う。
    2. **ペア整合性検証**:
       - 祖先リスト (名前/ID) および父系リスト (名前/ID) の要素数が一致しているかを確認する。
    3. **ドメイン論理検証**:
       - 祖先リストおよび父系リストが、それぞれ規定の要素数 (62件/31件) を満たしているかを確認する (空リストは許容)。
    4. **結果記録とログ出力**:
       - エラーの有無を判定し、合格または失敗の内容を詳細メッセージとともにログへ出力する。

- **`validate_race_detail(race_detail, logger) -> Tuple[bool, List[str]]`**
  - **役割**: RaceDetail の基本構造を検証する。
  - **引数**:
    - `race_detail` (`RaceDetail`): 検証対象のレース詳細オブジェクト。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `Tuple[bool, List[str]]`: (検証合否, エラーメッセージリスト)。
  - **処理**:
    1. **基本フィールド検証**:
       - `date` フィールドが文字列型 (str) であることを確認する。
    2. **結果記録とログ出力**:
       - 検証が合格した場合は、日付情報をログに出力する。

- **`validate_dataset(dataset, logger) -> Dict[str, List[str]]`**
  - **役割**: PedigreeInfo または RaceDetail のリストを一括検証し、結果を辞書で返す。
  - **引数**:
    - `dataset` (`List[Union[PedigreeInfo, RaceDetail]]`): 検証対象データのリスト。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `Dict[str, List[str]]`: 識別文字列をキー、エラーリストを値とする辞書。
  - **処理**:
    1. **データ巡回**:
       - `dataset` 内の各要素に対して、型に応じた検証処理を振り分ける。
    2. **血統情報検証**:
       - `PedigreeInfo` の場合、`Pedigree:馬ID(馬名)` 形式のキーを生成し、`validate_pedigree_info` を実行する。
    3. **レース詳細検証**:
       - `RaceDetail` の場合、`Race:日付_レース名` 形式のキーを生成し、`validate_race_detail` を実行する。
    4. **未知型処理**:
       - 想定外の型の場合、`Unknown:オブジェクトID` 形式のキーを生成し、未対応エラーを記録してログにエラーを出力する。

#### `src/data_pipeline/pedigree_scraper.py`

- **`to_halfwidth(text) -> str`**
  - **役割**: 全角文字を半角に変換し、国名表記や不要な空白を除去する。
  - **引数**:
    - `text` (`str`): 変換対象の文字列。
  - **返り値**:
    - `str`: 正規化済み文字列。
  - **処理**:
    1. NFKC正規化により全角英数字・記号を半角に統一する。
    2. 正規表現を用いて `(USA)` や `(IRE)` などの国名括弧表記を除去する。
    3. 日本語（漢字・かな）が含まれる場合は、馬名内の空白を削除する。

- **`normalize_name(name) -> str`**
  - **役割**: 馬名を系統照合用（YAMLキー用）の統一形式に変換する。
  - **引数**:
    - `name` (`str`): 正規化対象の馬名。
  - **返り値**:
    - `str`: 大文字・空白なしの正規化済み文字列。
  - **処理**:
    1. `to_halfwidth` を呼び出し、全角処理と国名除去を行う。
    2. すべての空白を除去し、英字を大文字に変換する。

- **`format_horse_id(raw_id) -> str`**
  - **役割**: 任意の形式の馬IDを10桁のゼロ埋め文字列に統一する。
  - **引数**:
    - `raw_id` (`Any`): 変換元の馬ID。
  - **返り値**:
    - `str`: 10桁のゼロ埋め文字列。
  - **処理**:
    1. 入力値を文字列化し、数字以外の文字を除去する。
    2. 抽出された数字を `zfill(10)` でゼロ埋めする。

- **`load_lineage_config(logger) -> Dict[str, Dict[str, str]]`**
  - **役割**: `config/lineage.yaml` から系統創始者の定義を読み込み、展開する。
  - **引数**:
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `Dict`: 正規化済み馬名をキー、系統名とIDを値に持つ辞書。
  - **処理**:
    1. `load_data` を用いてYAMLファイルをロードする。
    2. 内部関数 `_traverse` を用い、ネストされた `sub_lineages` を再帰的に走査する。
    3. 各ノードの `founder` 名を `normalize_name` で変換し、一平坦な辞書構造として構築する。

- **`load_existing_lineages(logger) -> Dict[str, Dict[str, str]]`**
  - **役割**: キャッシュDBから既知の系統判定結果を読み込む。
  - **引数**:
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `Dict`: 馬IDをキー、系統情報レコードを値に持つ辞書。
  - **処理**:
    1. `SIRE_LINEAGE_DB_PATH` の存在を確認し、未存在時は空辞書を返す。
    2. `load_from_db` を実行し、`sire_lineage` テーブルから全レコードを取得する。
    3. 各行の `horse_id` をキーとした辞書形式でインデックスを構築する。

- **`_parse_ancestor_items(soup) -> List[Ancestor]`**
  - **役割**: JBIS血統ページのHTMLから祖先ノード（最大62箇所）を抽出する。
  - **引数**:
    - `soup` (`BeautifulSoup`): パース済みのHTML。
  - **返り値**:
    - `List[Ancestor]`: 抽出された `Ancestor` オブジェクトのリスト。
  - **処理**:
    1. セレクタ `div.data-3__male` および `div.data-3__female` を用いてノードを取得する。
    2. リンクの `href` 属性から正規表現で馬IDを抽出し、`format_horse_id` で正規化する。
    3. 馬名を `to_halfwidth` で整形し、不変（frozen）な `Ancestor` オブジェクトとして順次追加する。

- **`extract_pedigree_data(html, horse_id) -> Optional[Dict[str, Any]]`**
  - **役割**: HTML文字列から対象馬名および全祖先情報を構造化データとして抽出する。
  - **引数**:
    - `html` (`str`): HTMLソース。
    - `horse_id` (`str`): 対象馬のID。
  - **返り値**:
    - `Optional[Dict]`: 成功時は馬名と祖先リストを含む辞書、失敗時は `None`。
  - **処理**:
    1. `h1` タグから正規化された対象馬名を取得する。
    2. `_parse_ancestor_items` を呼び出し、血統表の全ノードを取得する。
    3. ノードが1件も存在しない場合は解析失敗とみなす。

- **`trace_sire_lineage(start_id, founders, existing, logger) -> Optional[LineageResult]`**
  - **役割**: 系統が即時特定できない種牡馬に対し、JBISを遡上して系統を特定する。
  - **引数**:
    - `start_id` (`str`): 遡上開始ID。
    - `founders`, `existing`: 創始者定義およびキャッシュ辞書。
  - **処理**:
    1. 最大 `MAX_TRACE_CYCLES` (5サイクル) の反復ループを実行する。
    2. 各サイクルで当該馬の血統ページを取得し、直系父系ライン（5代分）を `_DIRECT_SIRE_INDICES` に基づきスキャンする。
    3. スキャン中、YAML創始者またはキャッシュDBに一致する馬が見つかれば、その系統で確定し `LineageResult` を返す。
    4. 見つからない場合、5代前の父（インデックス15）を次のサイクルの起点として再設定する。

- **`determine_lineages_for_sires(sire_list, founders, existing, logger) -> List[LineageResult]`**
  - **役割**: リスト内の種牡馬に対し、優先順位に基づき系統を確定させる。
  - **引数**:
    - `sire_list`: 判定対象の種牡馬リスト（31件）。
  - **処理**:
    1. 各種牡馬について「1.YAML創始者照合」「2.キャッシュDB照合」「3.遡上スクレイピング」の順で判定を行う。
    2. 新たに判定された系統は `existing` 辞書（オンメモリ）に追加し、同一バッチ内での重複計算を避ける。
    3. 全判定終了後、新規分を `save_to_db` を用いてSQLiteへバルクインサートする。

- **`scrape_pedigree_data(horse_list, logger) -> List[PedigreeInfo]`**
  - **役割**: 外部から呼び出されるメインエントリポイント。血統収集と系統判定を統合実行する。
  - **引数**:
    - `horse_list`: 収集対象の馬情報リスト。
  - **返り値**:
    - `List[PedigreeInfo]`: 系統判定済みの血統データオブジェクト。
  - **処理**:
    1. 創始者設定と既存キャッシュを一括ロードする。
    2. 各馬の血統ページ（`.../pedigree/`）を取得し、祖先62箇所を抽出する。
    3. 血統表内の種牡馬ポジション（31箇所）を特定し、`determine_lineages_for_sires` で系統を確定させる。
    4. 最終的に `PedigreeInfo` モデルへ情報を詰め込み、リストとして返却する。

#### `src/data_pipeline/race_detail_scraper.py`

- **`to_halfwidth(s) -> Optional[str]`**
  - **役割**: 文字列を NFKC 正規化により半角へ変換し、全角スペースを半角化する。
  - **引数**:
    - `s` (`Optional[str]`): 正規化対象の文字列。None を渡した場合は None を返す。
  - **返り値**:
    - `Optional[str]`: 半角変換後の文字列。入力が None の場合は None。
  - **処理**:
    1. `unicodedata.normalize("NFKC", s)` を実行し、全角英数字・記号・カタカナを半角に統一する。
    2. 全角スペース (`\u3000`) を半角スペース (` `) へ置換する。

- **`safe_int(text) -> Optional[int]`**
  - **役割**: 文字列から数字のみを抽出し、安全に `int` へ変換する。
  - **引数**:
    - `text` (`Optional[str]`): 数値を含む文字列 (例: "1,234円")。
  - **返り値**:
    - `Optional[int]`: 抽出された整数値。変換不能な場合は None。
  - **処理**:
    1. 入力が None の場合は早期リターンする。
    2. 正規表現 `r"[^\d]"` を用いて、カンマや単位など数字以外の文字をすべて除去する。
    3. 除去後の文字列が空でない場合のみ `int()` 変換を行い、失敗時は None を返す。

- **`safe_float(text) -> Optional[float]`**
  - **役割**: 文字列から数値（小数点含む）を抽出し、安全に `float` へ変換する。
  - **引数**:
    - `text` (`Optional[str]`): 数値を含む文字列 (例: "34.5kg")。
  - **返り値**:
    - `Optional[float]`: 抽出された浮動小数点数。変換不能な場合は None。
  - **処理**:
    1. 入力が None の場合は早期リターンする。
    2. 正規表現 `r"[^\d\.]"` を用いて、数字と小数点以外の文字をすべて除去する。
    3. 抽出された文字列を `float()` に変換する。

- **`extract_id_from_url(url) -> str`**
  - **役割**: JBIS 馬詳細ページ URL から 10 桁の馬 ID を抽出する。
  - **引数**:
    - `url` (`Optional[str]`): 馬詳細ページ URL (例: "/horse/2020101234/")。
  - **返り値**:
    - `str`: 10 桁の馬 ID 文字列。URL が None またはパターン不一致の場合は空文字。
  - **処理**:
    1. 正規表現 `rf"/horse/(\d{10})/"` を用いてマッチングを行う。
    2. キャプチャグループから ID 部分を取得する。

- **`extract_race_data(html_content, logger) -> Dict[str, Any]`**
  - **役割**: JBIS レース結果 HTML を BeautifulSoup で解析し、生データを辞書形式で抽出する。
  - **引数**:
    - `html_content` (`str`): レース結果ページの HTML 文字列。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `Dict[str, Any]`: 抽出された生データ辞書（date, venue, race, horses, payouts を含む）。
  - **処理**:
    1. **開催情報**: セレクタ `.hdg1-search h1` から日付（YYYY-MM-DD）、曜日、回次、場所、日数を正規表現で抽出。
    2. **レース概要**: `.hdg2-l-1 h2` からレース番号と名称を、`.box-race__text` から馬場種別（芝/ダート）、距離、天候、馬場状態を抽出。
    3. **タイム情報**: `dt` タグの「上がり」「ハロンタイム」をキーに、隣接する `dd` タグからタイムとラップリストを取得。`.data-4-1` からコーナー通過順を抽出。
    4. **馬柱**: `.data-6-11.sort-1` の各行から着順、枠番、馬番、馬名、馬ID（URL経由）、性齢、斤量、タイム、着差、通過順、上がり3F、指数、人気、体重（増減）、調教師（所属）、馬主、生産者を抽出。
    5. **払戻金**: `.table-1 table` から券種、的中番号、金額を抽出し、ワイド等の複数的中にも対応する。

- **`generate_race_objects(race_results, logger) -> RaceDetail`**
  - **役割**: 抽出済みレース辞書データを `RaceDetail` オブジェクトおよび関連サブモデルへ変換する。
  - **引数**:
    - `race_results` (`Dict[str, Any]`): `extract_race_data` の返り値。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `RaceDetail`: 構造化されたレース詳細オブジェクト。
  - **処理**:
    1. `RaceVenue` オブジェクトを生成（round, place, day）。
    2. `RaceInfo` オブジェクトを生成（number, name, surface, distance_m, weather, track_condition, final_time, lap_time, corner_order）。
    3. `HorseEntry` オブジェクトのリストを生成。各フィールドに対し、文字列型であれば `to_halfwidth` を適用し、馬 ID を URL から再抽出する。
    4. `Payout` オブジェクトのリストを生成。
    5. 上記を統合し、`RaceDetail` インスタンスを構築する。

- **`scrape_race_details(race_urls, logger) -> List[RaceDetail]`**
  - **役割**: 複数の JBIS レース結果 URL を逐次解析し、RaceDetail リストを返すメインエントリ。
  - **引数**:
    - `race_urls` (`List[str]`): 解析対象 URL リスト。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `List[RaceDetail]`: 解析に成功した RaceDetail オブジェクトのリスト。
  - **処理**:
    1. リスト内の各 URL に対し、`src.utils.retry_requests.fetch_html` を呼び出して HTML を取得。
    2. 取得失敗時はエラーログを出力し、次の URL へスキップ。
    3. 正常取得時は `extract_race_data` および `generate_race_objects` を連続実行し、結果をリストに蓄積する。

#### `src/data_pipeline/race_list_scraper.py`

- **`get_race_list_urls(start_date, end_date, logger) -> List[str]`**
  - **役割**: 指定された期間内の JBIS レース結果検索ページを巡回し、個別レース結果ページの絶対 URL リストを抽出する。
  - **引数**:
    - `start_date` (`str`): データ収集開始日 (YYYY-MM-DD 形式)。
    - `end_date` (`str`): データ収集終了日 (YYYY-MM-DD 形式)。
    - `logger` (`logging.Logger`): ログ出力用ロガー。
  - **返り値**:
    - `List[str]`: 個別レース結果ページの絶対 URL リスト。重複が除去され、昇順にソートされている。
  - **処理**:
    1. **日付バリデーション**: 入力された日付文字列を `-` で分割し、年・月を取得する。形式不正時は空リストを返す。
    2. **検索ページ巡回ループ**:
       - `MAX_PAGES` (500) を上限として、ページ番号をインクリメントしながら `while` ループを実行。
       - JBIS の検索エンドポイントに対し、中央競馬 (`hold_1`)、平地競走 (`racetype_1`)、100件表示 (`items=100`)、日付降順 (`order=D`) のパラメータを付与した検索 URL を生成する。
       - `src.utils.retry_requests.fetch_html` を用いて検索結果ページの HTML を取得する。
    3. **URL 抽出と変換**:
       - `BeautifulSoup` で HTML を解析し、`href` 属性が `RACE_URL_PATTERN` (`/race/result/YYYYMMDD/場/R/`) に合致するリンクを抽出。
       - `dict.fromkeys()` を用いて、順序を維持したままページ内の重複 URL を除去する。
       - 相対 URL を `urljoin` を用いて絶対 URL (`https://www.jbis.or.jp/...`) へ変換し、蓄積リストに追加する。
    4. **終了判定**: ページ内に該当する URL が 1 件も存在しない場合、全件取得完了とみなしループを抜ける。
    5. **後処理**: 全ページ分が蓄積されたリストに対し `set` による全体重複除去と `sorted()` による昇順ソートを行い、最終的なリストとして返却する。
