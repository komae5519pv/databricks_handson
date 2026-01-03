# Lakeflow SDP ハンズオン：メダリオン・アーキテクチャ

このプロジェクトは、Databricks Lakeflow SDP（Serverless Data Pipeline）を用いたデータパイプラインの構築を体験するためのリポジトリです。ECサイトのデータを模した3層構造（Bronze, Silver, Gold）をステップバイステップで実装します。

## 📂 フォルダ構成とファイル詳細

### 📜 transformations/

パイプラインのメインロジック（Delta Live Tables / Streaming Table）です。

* **bronze/**: ソースCSVをDelta形式へ変換（Auto Loader使用）
  * `ingest_users.py`: 顧客マスタ（CDC/変更データキャプチャ対応）
  * `ingest_transactions.py`: 取引履歴（Append専用）
  * `ingest_products.py`: 商品マスタ

* **silver/**: クレンジングとマスタ結合
  * `enrich_transactions.py`: 基本的な結合と加工（初級用）
  * `enrich_transactions_expectation.py`: **[中級]** データ品質チェック（Expectations）実装版

* **gold/**: ビジネス分析用ビュー（マテリアライズド・ビュー）
  * `gold_users.py`: 顧客ごとの購買プロファイリング集計
  * `revenue_by_user_segment.py`: 性別・地域・年代別の売上分析
  * `revenue_by_subcategory.py`: カテゴリ・サブカテゴリ別の売上分析

  [構文の詳細はこちらをご覧ください](https://docs.databricks.com/ldp/developer/python-ref)<br>
  [チュートリアルや参考資料はこちらをご覧ください](https://docs.databricks.com/ldp)<br>
  [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html#flows)<br>

### 📝 explorations/

ハンズオンの操作用ノートブックです。<br>

* `00_config`: パス設定やディレクトリの初期化
* `01_初期データ生成`: 顧客マスタ・注文データ一式（CSV）と・商品マスタ（外部テーブル想定）を作成
* `02_正常データ追加`: **[初級・中級]** 田中さんの住所変更・メール許諾OFF（CDC）や追加注文データの投入
* `03_不正データ追加`: **[中級]** 異常値（メアド形式不正・単価0・ユーザーIDがnullなど）の投入による品質管理の挙動確認
* `99_サンプルデータ全削除`: 学習環境のクリーンアップ（フォルダ・テーブル削除）

### 🛠️ utilities/

* `expectation_rules.py`: Silver層で使用する共通の品質管理ルール（辞書定義）

---

## 🟢 初級：メダリオン・クイックスタート

**シナリオ**: 3つのソース（注文・製品・顧客）を結合し、基本的な売上ビューを作成します。

* **Auto Loader**: フォルダに置かれたファイルを自動検知して取り込む。
* **Join**: 取引データに製品名や顧客情報を紐付ける。
* **Streaming**: 新しいファイルが届くと、即座にGoldテーブルの合計値が更新される様子を確認。

## 🟡 中級：データ品質管理と更新モデル

**シナリオ**: 不正データへの対処や、マスタの更新（CDC）を正しく反映する手法を学びます。

* **Expectations**: `amount > 0` や `user_id IS NOT NULL` などのルールで不正レコードをフィルタリング。
* **ST vs MV**: 履歴として残すべきデータ（Streaming Table）と、最新状態を反映すべきデータ（Materialized View）の使い分け。
* **CDC (Apply Changes)**: 引越し（地域変更）などのマスタ更新を、過去の集計に正しく反映・追従させる。

---

## 🚀 ハンズオンの進め方

1. `explorations/00_config` で、カタログ名・スキーマ名を編集して実行
2. `explorations/01_初期データ生成` を実行し、ソースとなるCSVを配置
3. DLTパイプラインを作成し、`transformations` フォルダをソースに指定して実行
4. 実行中に `02_正常データ追加` や `03_不正データ追加` を実行し、パイプラインがどう反応するかをUIで観察