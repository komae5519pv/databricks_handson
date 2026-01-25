# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze層 ETL
# MAGIC CSVデータをBronzeテーブルとして取り込む

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,ライブラリのインポート
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# DBTITLE 1,既存bronzeテーブルの削除
# スキーマ内のすべてのテーブル名を取得
tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")

# テーブル名が "bz_" で始まるテーブルのみ削除
for table in tables_df.collect():
    table_name = table["tableName"]
    if table_name.startswith("bz_"):
        spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
        print(f"削除: {table_name}")

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
df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_users")
df_with_audit = df \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/users"))
df_with_audit.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_users")

print(f"bz_users: {df_with_audit.count()}件")

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
df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_items")
df_with_audit = df \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/items"))
df_with_audit.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_items")

print(f"bz_items: {df_with_audit.count()}件")

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
df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_stores")
df_with_audit = df \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/stores"))
df_with_audit.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_stores")

print(f"bz_stores: {df_with_audit.count()}件")

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
df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_orders")
df_with_audit = df \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/orders"))
df_with_audit.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_orders")

print(f"bz_orders: {df_with_audit.count()}件")

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
df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_order_items")
df_with_audit = df \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/order_items"))
df_with_audit.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_order_items")

print(f"bz_order_items: {df_with_audit.count()}件")
