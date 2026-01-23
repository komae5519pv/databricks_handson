# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space を作成します

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Genie Spaceを作る
# MAGIC ![Genieスペース作成.gif](./Genieスペース作成.gif "Genieスペース作成.gif")
# MAGIC
# MAGIC #### データ
# MAGIC - `transactions`
# MAGIC - `products`
# MAGIC - `gold_feedbacks`
# MAGIC - `gold_user`
# MAGIC
# MAGIC #### 設定
# MAGIC - Title: `Genieワークショップ｜小売スーパー売り上げ分析`
# MAGIC - Description: `ブリックスマートの売上要因の深堀りを行う分析アシスタント`
# MAGIC - Default Warehouse: <任意のSQL Warehouse>
# MAGIC
# MAGIC #### 指示
# MAGIC <テキスト>
# MAGIC 一般的な指示:
# MAGIC ```
# MAGIC * 必ず日本語で回答してください
# MAGIC * あなたはブリックスマートの店長アシスタントです。店舗の売上データ分析を行うアシスタントとして、入力者の分析および洞察を導き出す補助をしてください。
# MAGIC * 「年代別」の指示がある時、年齢を10単位でグルーピングしてください
# MAGIC * 売上は、単価かける数量で計算してください
# MAGIC * SQLのFROM句でテーブル指定する際、``で括らないでください
# MAGIC * 現在を起点とする場合、必ず「SELECT from_utc_timestamp(CURRENT_TIMESTAMP(), 'Asia/Tokyo')::DATE;」で日時特定してください
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Genie Spaceに権限を付与する
# MAGIC
# MAGIC ここでは、`all workspace user`に付与する（デモなので）
# MAGIC
