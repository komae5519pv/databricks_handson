# Databricks notebook source
# MAGIC %md
# MAGIC # Silver層 ETL
# MAGIC Bronzeテーブルをクレンジング・型変換してSilverテーブルを作成

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,ライブラリのインポート
from pyspark.sql.functions import col, when, lit, to_date, to_timestamp, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, LongType, BooleanType

# COMMAND ----------

# DBTITLE 1,既存silverテーブルの削除
# スキーマ内のすべてのテーブル名を取得
tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")

# テーブル名が "sl_" で始まるテーブルのみ削除
for table in tables_df.collect():
    table_name = table["tableName"]
    if table_name.startswith("sl_"):
        spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
        print(f"削除: {table_name}")

print("\n既存のSilverテーブルを全て削除しました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_users

# COMMAND ----------

# DBTITLE 1,sl_usersの作成
bz_users = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_users")

# クレンジング処理
sl_users = (
    bz_users
    # 型変換
    .withColumn("birth_date", to_date(col("birth_date")))
    .withColumn("registration_date", to_date(col("registration_date")))
    .withColumn("email_permission_flg", col("email_permission_flg").cast(BooleanType()))
    .withColumn("last_login_at", to_timestamp(col("last_login_at")))
    # NULL処理: prefがNULLまたは空文字の場合は"不明"に
    .withColumn("pref",
        when((col("pref").isNull()) | (col("pref") == ""), lit("不明"))
        .otherwise(col("pref"))
    )
    # カラム選択（behavior_clusterはGold層で購買履歴から算出）
    .select(
        "user_id", "name", "gender", "birth_date", "pref",
        "phone_number", "email", "email_permission_flg",
        "registration_date", "status", "last_login_at"
    )
)

# Slテーブルとして保存
sl_users.write.mode("overwrite").format("delta").saveAsTable(
    f"{MY_CATALOG}.{MY_SCHEMA}.sl_users"
)

print(f"sl_users: {sl_users.count()}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_items

# COMMAND ----------

# DBTITLE 1,sl_itemsの作成
bz_items = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_items")

sl_items = (
    bz_items
    # 型変換
    .withColumn("item_id", col("item_id").cast(LongType()))
    .withColumn("category_id", col("category_id").cast(LongType()))
    .withColumn("price", col("price").cast(LongType()))
    .withColumn("expiration_date", to_date(col("expiration_date")))
    .withColumn("arrival_date", to_date(col("arrival_date")))
    .withColumn("is_organic", col("is_organic").cast(BooleanType()))
    .withColumn("is_discount_target", col("is_discount_target").cast(BooleanType()))
    .withColumn("is_ready_to_eat", col("is_ready_to_eat").cast(BooleanType()))
    .withColumn("is_premium", col("is_premium").cast(BooleanType()))
    # カラム選択
    .select(
        "item_id", "category_id", "item_name", "category_name", "price",
        "expiration_date", "arrival_date",
        "is_organic", "is_discount_target", "is_ready_to_eat", "is_premium"
    )
)

sl_items.write.mode("overwrite").format("delta").saveAsTable(
    f"{MY_CATALOG}.{MY_SCHEMA}.sl_items"
)

print(f"sl_items: {sl_items.count()}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_stores

# COMMAND ----------

# DBTITLE 1,sl_storesの作成
bz_stores = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_stores")

sl_stores = (
    bz_stores
    # 型変換
    .withColumn("store_id", col("store_id").cast(LongType()))
    .withColumn("manager_id", col("manager_id").cast(LongType()))
    .withColumn("opening_date", to_date(col("opening_date")))
    # カラム選択
    .select(
        "store_id", "store_name", "store_area", "address", "postal_code",
        "prefecture", "city", "phone_number", "email",
        "business_hours", "closed_days", "manager_id", "opening_date", "status"
    )
)

sl_stores.write.mode("overwrite").format("delta").saveAsTable(
    f"{MY_CATALOG}.{MY_SCHEMA}.sl_stores"
)

print(f"sl_stores: {sl_stores.count()}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_orders

# COMMAND ----------

# DBTITLE 1,sl_ordersの作成（店舗情報を結合）
bz_orders = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_orders")
sl_stores = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.sl_stores")

# 店舗情報の結合用
stores_ref = sl_stores.select(
    col("store_id").alias("ref_store_id"),
    "store_name",
    "store_area"
)

# 型変換 + 非正規化
sl_orders = (
    bz_orders
    # 型変換
    .withColumn("order_id", col("order_id").cast(LongType()))
    .withColumn("store_id", col("store_id").cast(LongType()))
    .withColumn("total_amount", col("total_amount").cast(LongType()))
    .withColumn("total_quantity", col("total_quantity").cast(IntegerType()))
    .withColumn("order_datetime", to_timestamp(col("order_datetime")))
)

# 店舗情報を結合
sl_orders = (
    sl_orders
    .join(stores_ref, sl_orders["store_id"] == stores_ref["ref_store_id"], "left")
    .select(
        "order_id", "user_id", "store_id", "store_name", "store_area",
        "order_datetime", "total_amount", "total_quantity", "order_status"
    )
)

sl_orders.write.mode("overwrite").format("delta").saveAsTable(
    f"{MY_CATALOG}.{MY_SCHEMA}.sl_orders"
)

print(f"sl_orders: {sl_orders.count()}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sl_order_items

# COMMAND ----------

# DBTITLE 1,重複排除処理
# 重複排除用のウィンドウ関数
# order_item_id でパーティション、_ingested_at の降順でランク付け
window_spec = Window.partitionBy("order_item_id").orderBy(col("_ingested_at").desc())

bz_order_items = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bz_order_items")

# 重複排除
sl_order_items_dedup = (
    bz_order_items
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)  # 各グループの最初の行（最新）のみ保持
    .drop("row_num")
)

before_count = bz_order_items.count()
after_count = sl_order_items_dedup.count()

print(f"重複排除前: {before_count}件")
print(f"重複排除後: {after_count}件")
print(f"削除された重複: {before_count - after_count}件")

# COMMAND ----------

# DBTITLE 1,sl_order_itemsの作成（型変換 + 非正規化）
# マスタテーブル読み込み
sl_items = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.sl_items")
sl_stores = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.sl_stores")

# 商品情報の結合用（商品名、カテゴリ名、属性フラグ）
items_ref = sl_items.select(
    col("item_id").alias("ref_item_id"),
    "item_name",
    "category_name",
    "is_organic",
    "is_discount_target",
    "is_ready_to_eat",
    "is_premium"
)

# 店舗情報の結合用
stores_ref = sl_stores.select(
    col("store_id").alias("ref_store_id"),
    "store_name",
    "store_area"
)

# 型変換
sl_order_items_typed = (
    sl_order_items_dedup
    .withColumn("order_item_id", col("order_item_id").cast(LongType()))
    .withColumn("order_id", col("order_id").cast(LongType()))
    .withColumn("store_id", col("store_id").cast(LongType()))
    .withColumn("item_id", col("item_id").cast(LongType()))
    .withColumn("category_id", col("category_id").cast(LongType()))
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .withColumn("unit_price", col("unit_price").cast(LongType()))
    .withColumn("subtotal", col("subtotal").cast(LongType()))
    .withColumn("cancel_flg", col("cancel_flg").cast(BooleanType()))
    .withColumn("order_datetime", to_timestamp(col("order_datetime")))
)

# 商品情報・店舗情報を結合（非正規化）
sl_order_items = (
    sl_order_items_typed
    .join(items_ref, sl_order_items_typed["item_id"] == items_ref["ref_item_id"], "left")
    .join(stores_ref, sl_order_items_typed["store_id"] == stores_ref["ref_store_id"], "left")
    .select(
        "order_item_id", "user_id", "order_id",
        "store_id", "store_name", "store_area",
        "item_id", "category_id", "item_name", "category_name",
        "is_organic", "is_discount_target", "is_ready_to_eat", "is_premium",
        "quantity", "unit_price", "subtotal", "cancel_flg",
        "order_datetime"
    )
)

sl_order_items.write.mode("overwrite").format("delta").saveAsTable(
    f"{MY_CATALOG}.{MY_SCHEMA}.sl_order_items"
)

print(f"sl_order_items: {sl_order_items.count()}件")
