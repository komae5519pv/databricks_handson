# Databricks notebook source
# MAGIC %md
# MAGIC サーバレスコンピュートで実行してください

# COMMAND ----------

# DBTITLE 1,変数設定
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. CSVからDeltaテーブルを作成

# COMMAND ----------

# DBTITLE 1,line_items_poutine_sales テーブル作成
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.line_items_poutine_sales")
spark.sql(f"""
    CREATE TABLE {catalog}.{schema}.line_items_poutine_sales
    AS SELECT * EXCEPT (_rescued_data)
    FROM read_files('/Volumes/{catalog}/{schema}/{volume}/full_data/line_items_poutine_sales_expanded_multi_basket.csv')
""")

# COMMAND ----------

# DBTITLE 1,products_poutine テーブル作成
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.products_poutine")
spark.sql(f"""
    CREATE TABLE {catalog}.{schema}.products_poutine
    AS SELECT * EXCEPT (_rescued_data)
    FROM read_files('/Volumes/{catalog}/{schema}/{volume}/full_data/products_poutine_expanded.csv')
""")

# COMMAND ----------

# DBTITLE 1,stores_canada テーブル作成
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.stores_canada")
spark.sql(f"""
    CREATE TABLE {catalog}.{schema}.stores_canada
    AS SELECT * EXCEPT (_rescued_data)
    FROM read_files('/Volumes/{catalog}/{schema}/{volume}/full_data/stores_canada_with_latlong.csv')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 主キー設定

# COMMAND ----------

# DBTITLE 1,line_items_poutine_sales: 主キー
spark.sql(f"ALTER TABLE {catalog}.{schema}.line_items_poutine_sales ALTER COLUMN transaction_id SET NOT NULL;")
spark.sql(f"ALTER TABLE {catalog}.{schema}.line_items_poutine_sales ALTER COLUMN line_id SET NOT NULL;")

spark.sql(f"ALTER TABLE {catalog}.{schema}.line_items_poutine_sales DROP CONSTRAINT IF EXISTS pk_line_items_poutine_sales;")
spark.sql(f"ALTER TABLE {catalog}.{schema}.line_items_poutine_sales ADD CONSTRAINT pk_line_items_poutine_sales PRIMARY KEY (transaction_id, line_id);")

# COMMAND ----------

# DBTITLE 1,products_poutine: 主キー
spark.sql(f"ALTER TABLE {catalog}.{schema}.products_poutine ALTER COLUMN product_id SET NOT NULL;")

spark.sql(f"ALTER TABLE {catalog}.{schema}.products_poutine DROP CONSTRAINT IF EXISTS pk_products_poutine;")
spark.sql(f"ALTER TABLE {catalog}.{schema}.products_poutine ADD CONSTRAINT pk_products_poutine PRIMARY KEY (product_id);")

# COMMAND ----------

# DBTITLE 1,stores_canada: 主キー
spark.sql(f"ALTER TABLE {catalog}.{schema}.stores_canada ALTER COLUMN store_id SET NOT NULL;")

spark.sql(f"ALTER TABLE {catalog}.{schema}.stores_canada DROP CONSTRAINT IF EXISTS pk_stores_canada;")
spark.sql(f"ALTER TABLE {catalog}.{schema}.stores_canada ADD CONSTRAINT pk_stores_canada PRIMARY KEY (store_id);")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. テーブル・カラムコメント追加

# COMMAND ----------

# DBTITLE 1,line_items_poutine_sales: テーブルコメント
spark.sql(f"""
COMMENT ON TABLE {catalog}.{schema}.line_items_poutine_sales IS '
テーブル名：line_items_poutine_sales / プーティン販売明細
説明：各取引の明細行を格納するファクトテーブルです。1つのトランザクションに複数の商品が含まれる場合、行が複数になります。
補足：
* 主キー：transaction_id, line_id（複合キー）
* products_poutine.product_id および stores_canada.store_id と結合可能
'
""")

# COMMAND ----------

# DBTITLE 1,line_items_poutine_sales: カラムコメント
column_comments = {
    "transaction_id": "取引 ID（主キーの一部）",
    "line_id":        "明細行 ID（主キーの一部）",
    "store_id":       "店舗 ID（stores_canada への外部キー）",
    "product_id":     "商品 ID（products_poutine への外部キー）",
    "transaction_ts": "取引日時（YYYY-MM-DD HH:MM:SS）",
}

for column, col_comment in column_comments.items():
    escaped_comment = col_comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {catalog}.{schema}.line_items_poutine_sales ALTER COLUMN {column} COMMENT '{escaped_comment}'")

# COMMAND ----------

# DBTITLE 1,products_poutine: テーブルコメント
spark.sql(f"""
COMMENT ON TABLE {catalog}.{schema}.products_poutine IS '
テーブル名：products_poutine / プーティン商品マスタ
説明：プーティン商品のカテゴリ・価格・原価を管理するディメンションテーブルです。
補足：
* 主キー：product_id
'
""")

# COMMAND ----------

# DBTITLE 1,products_poutine: カラムコメント
column_comments = {
    "product_id":   "商品 ID（主キー）",
    "category":     "カテゴリ（例：Poutine）",
    "subcategory":  "サブカテゴリ（例：Classic, Premium）",
    "product_name": "商品名",
    "unit_price":   "単価（数値）",
    "unit_cost":    "原価（数値）",
}

for column, col_comment in column_comments.items():
    escaped_comment = col_comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {catalog}.{schema}.products_poutine ALTER COLUMN {column} COMMENT '{escaped_comment}'")

# COMMAND ----------

# DBTITLE 1,stores_canada: テーブルコメント
spark.sql(f"""
COMMENT ON TABLE {catalog}.{schema}.stores_canada IS '
テーブル名：stores_canada / カナダ店舗マスタ
説明：カナダ国内の店舗所在地と緯度経度を管理するディメンションテーブルです。
補足：
* 主キー：store_id
'
""")

# COMMAND ----------

# DBTITLE 1,stores_canada: カラムコメント
column_comments = {
    "store_id":  "店舗 ID（主キー）",
    "location":  "所在地（例：Toronto, ON）",
    "latitude":  "緯度（数値：度）",
    "longitude": "経度（数値：度）",
}

for column, col_comment in column_comments.items():
    escaped_comment = col_comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {catalog}.{schema}.stores_canada ALTER COLUMN {column} COMMENT '{escaped_comment}'")

# COMMAND ----------

print("全テーブル作成・主キー設定・コメント追加 完了")
