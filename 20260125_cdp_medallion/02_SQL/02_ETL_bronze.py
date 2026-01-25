# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze層 ETL
# MAGIC CSVデータをBronzeテーブルとして取り込む

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,既存bronzeテーブルの削除
# スキーマ内のすべてのテーブル名を取得
tables = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}").collect()

# テーブル名が "bz_" で始まるテーブルのみ削除
for t in tables:
    if t["tableName"].startswith("bz_"):
        spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{t['tableName']}")
        print(f"削除: {t['tableName']}")

print("\n既存のBronzeテーブルを全て削除しました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bz_users

# COMMAND ----------

# DBTITLE 1,bz_usersの作成
# テーブル作成
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_users
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# CSVデータを取り込み
spark.sql(f"""
    COPY INTO {MY_CATALOG}.{MY_SCHEMA}.bz_users
    FROM '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/users'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

# 監査カラムを追加
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_users
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS
    SELECT
        *,
        current_timestamp() AS _ingested_at,
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/users' AS _source_file
    FROM {MY_CATALOG}.{MY_SCHEMA}.bz_users
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_users").collect()[0]["cnt"]
print(f"bz_users: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bz_items

# COMMAND ----------

# DBTITLE 1,bz_itemsの作成
# テーブル作成
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_items
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# CSVデータを取り込み
spark.sql(f"""
    COPY INTO {MY_CATALOG}.{MY_SCHEMA}.bz_items
    FROM '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/items'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

# 監査カラムを追加
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_items
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS
    SELECT
        *,
        current_timestamp() AS _ingested_at,
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/items' AS _source_file
    FROM {MY_CATALOG}.{MY_SCHEMA}.bz_items
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_items").collect()[0]["cnt"]
print(f"bz_items: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bz_stores

# COMMAND ----------

# DBTITLE 1,bz_storesの作成
# テーブル作成
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_stores
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# CSVデータを取り込み
spark.sql(f"""
    COPY INTO {MY_CATALOG}.{MY_SCHEMA}.bz_stores
    FROM '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/stores'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

# 監査カラムを追加
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_stores
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS
    SELECT
        *,
        current_timestamp() AS _ingested_at,
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/stores' AS _source_file
    FROM {MY_CATALOG}.{MY_SCHEMA}.bz_stores
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_stores").collect()[0]["cnt"]
print(f"bz_stores: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bz_orders

# COMMAND ----------

# DBTITLE 1,bz_ordersの作成
# テーブル作成
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_orders
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# CSVデータを取り込み
spark.sql(f"""
    COPY INTO {MY_CATALOG}.{MY_SCHEMA}.bz_orders
    FROM '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/orders'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

# 監査カラムを追加
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_orders
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS
    SELECT
        *,
        current_timestamp() AS _ingested_at,
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/orders' AS _source_file
    FROM {MY_CATALOG}.{MY_SCHEMA}.bz_orders
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_orders").collect()[0]["cnt"]
print(f"bz_orders: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bz_order_items

# COMMAND ----------

# DBTITLE 1,bz_order_itemsの作成
# テーブル作成
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_order_items
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# CSVデータを取り込み
spark.sql(f"""
    COPY INTO {MY_CATALOG}.{MY_SCHEMA}.bz_order_items
    FROM '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/order_items'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

# 監査カラムを追加
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_order_items
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS
    SELECT
        *,
        current_timestamp() AS _ingested_at,
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/order_items' AS _source_file
    FROM {MY_CATALOG}.{MY_SCHEMA}.bz_order_items
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_order_items").collect()[0]["cnt"]
print(f"bz_order_items: {cnt}件")
