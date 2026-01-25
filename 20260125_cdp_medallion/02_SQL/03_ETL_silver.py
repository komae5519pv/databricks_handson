# Databricks notebook source
# MAGIC %md
# MAGIC # Silver層 ETL
# MAGIC Bronzeテーブルをクレンジング・型変換してSilverテーブルを作成

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,既存silverテーブルの削除
# スキーマ内のすべてのテーブル名を取得
tables = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}").collect()

# テーブル名が "sl_" で始まるテーブルのみ削除
for t in tables:
    if t["tableName"].startswith("sl_"):
        spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{t['tableName']}")
        print(f"削除: {t['tableName']}")

print("\n既存のSilverテーブルを全て削除しました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_users

# COMMAND ----------

# DBTITLE 1,sl_usersの作成
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.sl_users
    USING DELTA
    AS
    SELECT
        user_id,
        name,
        gender,
        TO_DATE(birth_date) AS birth_date,
        CASE
            WHEN pref IS NULL OR pref = '' THEN '不明'
            ELSE pref
        END AS pref,
        phone_number,
        email,
        CAST(email_permission_flg AS BOOLEAN) AS email_permission_flg,
        TO_DATE(registration_date) AS registration_date,
        status,
        TO_TIMESTAMP(last_login_at) AS last_login_at
    FROM {MY_CATALOG}.{MY_SCHEMA}.bz_users
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.sl_users").collect()[0]["cnt"]
print(f"sl_users: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_items

# COMMAND ----------

# DBTITLE 1,sl_itemsの作成
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.sl_items
    USING DELTA
    AS
    SELECT
        CAST(item_id AS BIGINT) AS item_id,
        CAST(category_id AS BIGINT) AS category_id,
        item_name,
        category_name,
        CAST(price AS BIGINT) AS price,
        TO_DATE(expiration_date) AS expiration_date,
        TO_DATE(arrival_date) AS arrival_date,
        CAST(is_organic AS BOOLEAN) AS is_organic,
        CAST(is_discount_target AS BOOLEAN) AS is_discount_target,
        CAST(is_ready_to_eat AS BOOLEAN) AS is_ready_to_eat,
        CAST(is_premium AS BOOLEAN) AS is_premium
    FROM {MY_CATALOG}.{MY_SCHEMA}.bz_items
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.sl_items").collect()[0]["cnt"]
print(f"sl_items: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_stores

# COMMAND ----------

# DBTITLE 1,sl_storesの作成
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.sl_stores
    USING DELTA
    AS
    SELECT
        CAST(store_id AS BIGINT) AS store_id,
        store_name,
        store_area,
        address,
        postal_code,
        prefecture,
        city,
        phone_number,
        email,
        business_hours,
        closed_days,
        CAST(manager_id AS BIGINT) AS manager_id,
        TO_DATE(opening_date) AS opening_date,
        status
    FROM {MY_CATALOG}.{MY_SCHEMA}.bz_stores
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.sl_stores").collect()[0]["cnt"]
print(f"sl_stores: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_orders

# COMMAND ----------

# DBTITLE 1,sl_ordersの作成（店舗情報を結合）
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.sl_orders
    USING DELTA
    AS
    SELECT
        CAST(o.order_id AS BIGINT) AS order_id,
        o.user_id,
        CAST(o.store_id AS BIGINT) AS store_id,
        s.store_name,
        s.store_area,
        TO_TIMESTAMP(o.order_datetime) AS order_datetime,
        CAST(o.total_amount AS BIGINT) AS total_amount,
        CAST(o.total_quantity AS INT) AS total_quantity,
        o.order_status
    FROM {MY_CATALOG}.{MY_SCHEMA}.bz_orders o
    LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.sl_stores s
        ON CAST(o.store_id AS BIGINT) = s.store_id
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.sl_orders").collect()[0]["cnt"]
print(f"sl_orders: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_order_items

# COMMAND ----------

# DBTITLE 1,重複排除処理
# 重複排除の結果を確認
before_cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.bz_order_items").collect()[0]["cnt"]

# 重複排除した一時ビューを作成
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW tmp_order_items_dedup AS
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY _ingested_at DESC) AS row_num
        FROM {MY_CATALOG}.{MY_SCHEMA}.bz_order_items
    )
    WHERE row_num = 1
""")

after_cnt = spark.sql("SELECT COUNT(*) AS cnt FROM tmp_order_items_dedup").collect()[0]["cnt"]

print(f"重複排除前: {before_cnt}件")
print(f"重複排除後: {after_cnt}件")
print(f"削除された重複: {before_cnt - after_cnt}件")

# COMMAND ----------

# DBTITLE 1,sl_order_itemsの作成（型変換 + 非正規化）
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.sl_order_items
    USING DELTA
    AS
    SELECT
        CAST(oi.order_item_id AS BIGINT) AS order_item_id,
        oi.user_id,
        CAST(oi.order_id AS BIGINT) AS order_id,
        CAST(oi.store_id AS BIGINT) AS store_id,
        s.store_name,
        s.store_area,
        CAST(oi.item_id AS BIGINT) AS item_id,
        CAST(oi.category_id AS BIGINT) AS category_id,
        i.item_name,
        i.category_name,
        i.is_organic,
        i.is_discount_target,
        i.is_ready_to_eat,
        i.is_premium,
        CAST(oi.quantity AS INT) AS quantity,
        CAST(oi.unit_price AS BIGINT) AS unit_price,
        CAST(oi.subtotal AS BIGINT) AS subtotal,
        CAST(oi.cancel_flg AS BOOLEAN) AS cancel_flg,
        TO_TIMESTAMP(oi.order_datetime) AS order_datetime
    FROM tmp_order_items_dedup oi
    LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.sl_items i
        ON CAST(oi.item_id AS BIGINT) = i.item_id
    LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.sl_stores s
        ON CAST(oi.store_id AS BIGINT) = s.store_id
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.sl_order_items").collect()[0]["cnt"]
print(f"sl_order_items: {cnt}件")
