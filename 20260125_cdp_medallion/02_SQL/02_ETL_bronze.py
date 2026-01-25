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
# マスタデータ: read_filesで一括取り込み + CDF有効化
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_users
    USING DELTA
    -- テーブルの変更データフィード（CDF）を有効化
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS
    SELECT
        *,
        current_timestamp() AS _ingested_at,                                        -- 取り込み日時
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/users' AS _source_file       -- ソースファイルパス
    FROM read_files(
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/users',
        format => 'csv',
        header => 'true'
    )
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_users").collect()[0]["cnt"]
print(f"bz_users: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bz_items

# COMMAND ----------

# DBTITLE 1,bz_itemsの作成
# マスタデータ: read_filesで一括取り込み + CDF有効化
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_items
    USING DELTA
    -- テーブルの変更データフィード（CDF）を有効化
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS
    SELECT
        *,
        current_timestamp() AS _ingested_at,                                        -- 取り込み日時
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/items' AS _source_file       -- ソースファイルパス
    FROM read_files(
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/items',
        format => 'csv',
        header => 'true'
    )
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_items").collect()[0]["cnt"]
print(f"bz_items: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bz_stores

# COMMAND ----------

# DBTITLE 1,bz_storesの作成
# マスタデータ: read_filesで一括取り込み + CDF有効化
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_stores
    USING DELTA
    -- テーブルの変更データフィード（CDF）を有効化
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS
    SELECT
        *,
        current_timestamp() AS _ingested_at,                                        -- 取り込み日時
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/stores' AS _source_file      -- ソースファイルパス
    FROM read_files(
        '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/stores',
        format => 'csv',
        header => 'true'
    )
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_stores").collect()[0]["cnt"]
print(f"bz_stores: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bz_orders

# COMMAND ----------

# DBTITLE 1,bz_ordersの作成
# トランザクションデータ: COPY INTOで増分取り込み
# 空テーブル作成（COPY INTOのため）
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_orders
    USING DELTA
""")

# CSVデータを増分取り込み（監査列はSELECTで付与）
spark.sql(f"""
    COPY INTO {MY_CATALOG}.{MY_SCHEMA}.bz_orders
    FROM (
        SELECT
            *,
            current_timestamp() AS _ingested_at,                                        -- 取り込み日時
            '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/orders' AS _source_file      -- ソースファイルパス
        FROM '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/orders'
    )
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_orders").collect()[0]["cnt"]
print(f"bz_orders: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bz_order_items

# COMMAND ----------

# DBTITLE 1,bz_order_itemsの作成
# トランザクションデータ: COPY INTOで増分取り込み
# 空テーブル作成（COPY INTOのため）
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_order_items
    USING DELTA
""")

# CSVデータを増分取り込み（監査列はSELECTで付与）
spark.sql(f"""
    COPY INTO {MY_CATALOG}.{MY_SCHEMA}.bz_order_items
    FROM (
        SELECT
            *,
            current_timestamp() AS _ingested_at,                                            -- 取り込み日時
            '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/order_items' AS _source_file     -- ソースファイルパス
        FROM '/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/order_items'
    )
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_order_items").collect()[0]["cnt"]
print(f"bz_order_items: {cnt}件")
