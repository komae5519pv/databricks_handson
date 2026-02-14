# Databricks notebook source
# MAGIC %md
# MAGIC # PlaygroundででMCPサーバーを活用した Genie Spaceエージェントを実行

# COMMAND ----------

# %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Genie Spaceを作る
# MAGIC ![Genieスペース作成.gif](./_image/Genieスペース作成.gif "Genieスペース作成.gif")
# MAGIC
# MAGIC #### データ
# MAGIC - `product`
# MAGIC - `inventory`
# MAGIC - `reservation`
# MAGIC
# MAGIC #### 設定
# MAGIC - Title: `アパレル在庫分析`
# MAGIC - Description: `店舗・倉庫・オンラインの在庫や入荷予定データを統合し、自然言語で在庫状況を確認できるエージェントです。ユーザーの質問に対応し、在庫・入荷・取り寄せを横断的に分析して最適な回答を提示します。`
# MAGIC - Default Warehouse: <任意のSQL Warehouse>
# MAGIC
# MAGIC #### 指示
# MAGIC <テキスト>
# MAGIC 一般的な指示:
# MAGIC ```
# MAGIC * 必ず日本語で回答してください
# MAGIC * 地域に関して、都内といわれたら東京のことです
# MAGIC * 在庫を聞かれた場合、詳細も一緒に返してください
# MAGIC * 商品について聞かれた場合、詳細を返してください。詳細は次のとおり
# MAGIC
# MAGIC 詳細:
# MAGIC - アイテム説明
# MAGIC - 素材特徴
# MAGIC - スタッフ着用コメント
# MAGIC - 平均レビュー点数
# MAGIC - WEB限定区分
# MAGIC - 税込価格
# MAGIC - 送料
# MAGIC - 返品可否
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Genie Spaceに権限を付与する
# MAGIC
# MAGIC ここでは、`all workspace user`に付与する（デモなので）
# MAGIC
