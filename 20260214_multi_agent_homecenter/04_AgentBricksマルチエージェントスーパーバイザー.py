# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Bricks - マルチエージェントスーパーバイザー
# MAGIC
# MAGIC [Agent Bricksの使用: Multi-Agent Supervisor](https://docs.databricks.com/aws/ja/generative-ai/agent-bricks/multi-agent-supervisor)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. マルチエージェントスーパーバイザーを作る
# MAGIC
# MAGIC - Agent Bricks -> マルチエージェントスーパーバイザー -> ビルド
# MAGIC - 名前: `ma-homecenter-assistant`
# MAGIC - 説明:`ホームセンター店舗スタッフ向けの意思決定支援アシスタントです。在庫・商品データと社内ナレッジを統合し、在庫確認・取り置き判断・DIY提案・接客対応を支援します。`
# MAGIC - エージェントを設定
# MAGIC
# MAGIC | タイプ | エージェントエンドポイント/Genieスペース | エージェント名 | コンテンツを説明 ※人が理解しやすい説明 |
# MAGIC | ------------- | ---------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------ |
# MAGIC | Genieスペース | ホームセンター在庫分析 | agent | inventory / product / reservation テーブルを参照し、在庫数・入荷予定・予約状況・商品スペック・価格などのStructuredデータを横断的に分析するエージェント |
# MAGIC | ナレッジアシスタント | kw-store-knowledge-bot | agent | 店舗運営マニュアル、接客研修資料、CS対応FAQ、商品知識ハンドブックを参照し、原則・例外・判断基準・接客トークを整理するUnstructuredナレッジ参照エージェント |
# MAGIC
# MAGIC - オプション
# MAGIC   - 手順設定（オプション）:
# MAGIC     ```
# MAGIC     * ホームセンターの店舗スタッフが使う意思決定支援アシスタントです
# MAGIC     * 必ず日本語で回答してください
# MAGIC     * 最初に結論を簡潔に述べ、その後に詳細を説明してください
# MAGIC
# MAGIC     * 在庫数・数量・入荷日・価格・スペックなどのStructuredデータ確認が必要な場合は「ホームセンター在庫分析（Genie）」を使用してください
# MAGIC     * ルール・例外・判断基準・接客トーク・商品知識が必要な場合は「kw-store-knowledge-bot（RAG）」を使用してください
# MAGIC     * 両方が必要な場合は、必ず両エージェントを使用してください
# MAGIC
# MAGIC     * 最終回答は以下の順で整理してください
# MAGIC
# MAGIC       1. 事実（在庫・数量・スペックなど）
# MAGIC       2. 原則
# MAGIC       3. 例外条件
# MAGIC       4. 推奨アクション
# MAGIC       5. 接客トーク例（必要な場合）
# MAGIC
# MAGIC     * 取り置き予約可能か？と聞かれた場合は、
# MAGIC       - Genieで販売可能在庫数と予約数を確認し、
# MAGIC       - RAGで安全在庫基準・例外条件を参照して判断してください
# MAGIC
# MAGIC     * DIYプロジェクトの相談を受けた場合は、
# MAGIC       - RAGで必要な資材・工具・注意点を整理し、
# MAGIC       - Genieで各商品の在庫状況を確認してください
# MAGIC
# MAGIC     * 店長判断・本部判断が必要な場合は明示してください
# MAGIC     ```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![マルチエージェントスーパーバイザー作成.gif](./_image/マルチエージェントスーパーバイザー作成.gif "マルチエージェントスーパーバイザー作成.gif")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. マルチエージェントスーパーバイザーを使ってみる
# MAGIC
# MAGIC |質問番号|質問内容|主な参照元（エージェント）|狙い・意図|
# MAGIC |----|----------------------------------------------|------------------------------|----------------------------------------------|
# MAGIC |Q1|多摩店でCRAFT BASEのインパクトドライバー(10.8V)の在庫ある？なければ最短入荷も教えて|Genie主導|Structuredデータのみで完結する質問。在庫・店舗横断・入荷予定を正確に返せるか確認する。|
# MAGIC |Q2|その商品、多摩店で取り置きできる？プロ会員なんだけど|Genie + RAG|Genieで販売可能在庫と予約状況を確認し、RAGで安全在庫基準・例外条件を参照する。数値＋ルール統合の確認。販売可能0台→安全在庫割れ→プロ会員例外→店長判断、が出るか。|
# MAGIC |Q3|その商品について詳しく教えて|Genie + RAG|商品マスタ情報（スペック・価格）に加え、商品知識ハンドブック・接客研修資料を参照してトーンを整えた説明ができるか確認する。|
# MAGIC |Q4|このお客様、ウッドデッキも作りたいらしい。多摩店で資材揃う？|RAG → Genie|RAGがDIY相談フローから必要資材を判断→Genieが多摩店の在庫を一括チェック→不足分（2×4材・塗料）の代替案（他店舗取り寄せ）まで出せるか。最大のWowポイント。|
# MAGIC |Q5|この店で人気の商品、在庫あるやつ教えて|Genie + RAG|Genieでレビュー上位×在庫ありを抽出し、RAGで「なぜ人気か」の訴求ポイントを添えた提案ができるか確認する。|
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![マルチエージェントスーパーバイザーを使う.gif](./_image/マルチエージェントスーパーバイザーを使う.gif "マルチエージェントスーパーバイザーを使う.gif")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Playgroundでマルチエージェントスーパーバイザーを使ってみる
# MAGIC
# MAGIC ![Playgroundで使ってみる.gif](./_image/Playgroundで使ってみる.gif "Playgroundで使ってみる.gif")
