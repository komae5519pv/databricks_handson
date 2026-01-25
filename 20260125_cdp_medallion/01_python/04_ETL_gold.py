# Databricks notebook source
# MAGIC %md
# MAGIC # Gold層 ETL
# MAGIC Silverテーブルに分析セグメントを付与してGoldテーブルを作成

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,ライブラリのインポート
from pyspark.sql.functions import (
    col, sum as _sum, count, countDistinct, round as _round,
    to_date, datediff, current_date, when, lit, max as _max,
    row_number, hour, dayofweek
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,既存goldテーブルの削除
# スキーマ内のすべてのテーブル名を取得
tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")

# テーブル名が "gd_" で始まるテーブルのみ削除
for table in tables_df.collect():
    table_name = table["tableName"]
    if table_name.startswith("gd_"):
        spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
        print(f"削除: {table_name}")

print("\n既存のGoldテーブルを全て削除しました。")

# COMMAND ----------

# DBTITLE 1,Silverテーブルの読み込み
sl_users = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.sl_users")
sl_orders = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.sl_orders")
sl_order_items = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.sl_order_items")

# COMMAND ----------

# DBTITLE 1,RFM指標の計算
# ユーザーごとのRFM指標を計算（sl_ordersから会計単位で集計）
user_rfm = (
    sl_orders
    .withColumn("order_date", to_date(col("order_datetime")))
    .groupBy("user_id")
    .agg(
        _max("order_date").alias("last_purchase_date"),
        countDistinct("order_id").alias("frequency"),
        _sum("total_amount").alias("monetary")
    )
    .withColumn("recency", datediff(current_date(), col("last_purchase_date")))
)

# COMMAND ----------

# DBTITLE 1,RFMスコアとセグメントの付与
# RFMスコアを付与（1-5のスコア）
user_rfm_scored = (
    user_rfm
    # Recencyスコア（小さいほど良い → 高スコア）
    .withColumn("r_score",
        when(col("recency") <= 7, 5)
        .when(col("recency") <= 14, 4)
        .when(col("recency") <= 30, 3)
        .when(col("recency") <= 60, 2)
        .otherwise(1)
    )
    # Frequencyスコア（大きいほど良い → 高スコア）
    .withColumn("f_score",
        when(col("frequency") >= 10, 5)
        .when(col("frequency") >= 5, 4)
        .when(col("frequency") >= 3, 3)
        .when(col("frequency") >= 2, 2)
        .otherwise(1)
    )
    # Monetaryスコア（大きいほど良い → 高スコア）
    .withColumn("m_score",
        when(col("monetary") >= 50000, 5)
        .when(col("monetary") >= 20000, 4)
        .when(col("monetary") >= 10000, 3)
        .when(col("monetary") >= 5000, 2)
        .otherwise(1)
    )
    # RFMセグメント分類
    .withColumn("rfm_segment",
        when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), "ロイヤル顧客")
        .when((col("r_score") >= 4) & (col("f_score") >= 3), "常連顧客")
        .when((col("r_score") >= 4) & (col("f_score") <= 2), "新規顧客")
        .when((col("r_score") <= 2) & (col("f_score") >= 3), "離脱リスク")
        .when((col("r_score") <= 2) & (col("f_score") <= 2), "休眠顧客")
        .otherwise("一般顧客")
    )
)

# 新規・既存フラグを追加
user_rfm_with_type = (
    user_rfm_scored
    .withColumn("customer_type",
        when(col("frequency") == 1, "新規")
        .otherwise("既存")
    )
)

# 結合用にカラム名を調整
user_rfm_ref = user_rfm_with_type.select(
    col("user_id").alias("rfm_user_id"),
    "last_purchase_date",
    "recency",
    "frequency",
    "monetary",
    "r_score",
    "f_score",
    "m_score",
    "rfm_segment",
    "customer_type"
)

# COMMAND ----------

# DBTITLE 1,初回注文フラグの計算
# 各顧客の注文に順番を付与（order_datetimeの昇順で1, 2, 3...）
# ※「新規顧客」= 登録から30日以内の初回注文（データ開始前に登録済みの既存顧客は除外）
user_order_window = Window.partitionBy("user_id").orderBy("order_datetime")

# ユーザーの登録日を取得
users_reg_date = sl_users.select(
    col("user_id").alias("reg_user_id"),
    col("registration_date")
)

first_order_ref = (
    sl_orders
    .withColumn("order_rank", row_number().over(user_order_window))
    .withColumn("order_date", to_date(col("order_datetime")))
    # ユーザーの登録日を結合
    .join(users_reg_date, sl_orders["user_id"] == users_reg_date["reg_user_id"], "left")
    # 初回注文フラグ（登録から30日以内の場合のみTrue = 真の新規顧客）
    .withColumn("is_first_order",
        when(
            (col("order_rank") == 1) &
            (datediff(col("order_date"), col("registration_date")) <= 30),
            lit(True)
        ).otherwise(lit(False))
    )
    .select(
        col("order_id").alias("fo_order_id"),
        "is_first_order"
    )
)

# COMMAND ----------

# DBTITLE 1,行動クラスター（behavior_cluster）の算出
# 購買履歴からユーザーの行動パターンを分析し、クラスターを判定
# ※ クラスターはRawデータには含まれず、Gold層で算出

# --- Step 1: 注文レベルの統計（曜日、時間帯） ---
order_stats = (
    sl_orders
    .withColumn("order_hour", hour(col("order_datetime")))
    .withColumn("order_dayofweek", dayofweek(col("order_datetime")))  # 1=日曜, 7=土曜
    .withColumn("is_weekend", when(dayofweek(col("order_datetime")).isin([1, 7]), 1).otherwise(0))
    .withColumn("is_weekday_evening",  # 平日16-18時
        when(
            (~dayofweek(col("order_datetime")).isin([1, 7])) &
            (hour(col("order_datetime")).between(16, 18)),
            1
        ).otherwise(0)
    )
    .withColumn("is_night",  # 19時以降
        when(hour(col("order_datetime")) >= 19, 1).otherwise(0)
    )
    .groupBy("user_id")
    .agg(
        count("*").alias("total_orders"),
        _sum("is_weekend").alias("weekend_orders"),
        _sum("is_weekday_evening").alias("weekday_evening_orders"),
        _sum("is_night").alias("night_orders"),
        _sum("total_quantity").alias("total_items")
    )
    .withColumn("weekend_rate", col("weekend_orders") / col("total_orders"))
    .withColumn("weekday_evening_rate", col("weekday_evening_orders") / col("total_orders"))
    .withColumn("night_rate", col("night_orders") / col("total_orders"))
    .withColumn("avg_basket_size", col("total_items") / col("total_orders"))
)

# --- Step 2: 商品レベルの統計（カテゴリ、属性） ---
item_stats = (
    sl_order_items
    .withColumn("is_category_junkie",  # 酒類(1100)、お菓子(600)、スイーツ(1200)
        when(col("category_id").isin([1100, 600, 1200]), 1).otherwise(0)
    )
    .withColumn("is_organic_premium",  # オーガニック or プレミアム
        when((col("is_organic") == True) | (col("is_premium") == True), 1).otherwise(0)
    )
    .withColumn("is_discount",  # 値引き対象
        when(col("is_discount_target") == True, 1).otherwise(0)
    )
    .withColumn("is_ready_to_eat_item",  # 即食系
        when(col("is_ready_to_eat") == True, 1).otherwise(0)
    )
    .groupBy("user_id")
    .agg(
        count("*").alias("total_items_purchased"),
        _sum("is_category_junkie").alias("category_junkie_items"),
        _sum("is_organic_premium").alias("organic_premium_items"),
        _sum("is_discount").alias("discount_items"),
        _sum("is_ready_to_eat_item").alias("ready_to_eat_items")
    )
    .withColumn("category_junkie_rate", col("category_junkie_items") / col("total_items_purchased"))
    .withColumn("organic_premium_rate", col("organic_premium_items") / col("total_items_purchased"))
    .withColumn("discount_rate", col("discount_items") / col("total_items_purchased"))
    .withColumn("ready_to_eat_rate", col("ready_to_eat_items") / col("total_items_purchased"))
)

# --- Step 3: クラスター判定 ---
# 優先順位: 5(時短族) > 1(週末族) > 2(夜型) > 3(健康) > 4(カテゴリ) > NULL(ランダム)
user_cluster = (
    order_stats
    .join(item_stats, "user_id", "left")
    .withColumn("behavior_cluster",
        when(
            # クラスター5: 平日夕方の時短族
            # 平日16-18時購入率 >= 70% AND 即食系購入率 >= 60%
            (col("weekday_evening_rate") >= 0.70) & (col("ready_to_eat_rate") >= 0.60),
            lit("5")
        ).when(
            # クラスター1: 週末まとめ買い族
            # 土日購入率 >= 80% AND 平均バスケットサイズ >= 10点
            (col("weekend_rate") >= 0.80) & (col("avg_basket_size") >= 10),
            lit("1")
        ).when(
            # クラスター2: 夜型値引きハンター
            # 19時以降購入率 >= 70% AND 値引き商品購入率 >= 50%
            (col("night_rate") >= 0.70) & (col("discount_rate") >= 0.50),
            lit("2")
        ).when(
            # クラスター3: 健康・こだわり層
            # オーガニック/プレミアム商品購入率 >= 60%
            (col("organic_premium_rate") >= 0.60),
            lit("3")
        ).when(
            # クラスター4: カテゴリジャンキー
            # 特定カテゴリ（酒/菓子/スイーツ）購入率 >= 70%
            (col("category_junkie_rate") >= 0.70),
            lit("4")
        ).otherwise(lit(None))  # ランダム層
    )
    .select(
        col("user_id").alias("cluster_user_id"),
        "behavior_cluster"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## gd_users

# COMMAND ----------

# DBTITLE 1,gd_usersの作成
# クラスター情報（購買履歴から算出）とRFM情報を結合
gd_users = (
    sl_users
    # 行動クラスター情報を結合（購買履歴から算出）
    .join(user_cluster, sl_users["user_id"] == user_cluster["cluster_user_id"], "left")
    # RFM情報を結合
    .join(user_rfm_ref, sl_users["user_id"] == user_rfm_ref["rfm_user_id"], "left")
    # クラスター名を付与
    .withColumn("behavior_cluster_name",
        when(col("behavior_cluster") == "1", "週末まとめ買い族")
        .when(col("behavior_cluster") == "2", "夜型値引きハンター")
        .when(col("behavior_cluster") == "3", "健康・こだわり層")
        .when(col("behavior_cluster") == "4", "カテゴリジャンキー")
        .when(col("behavior_cluster") == "5", "平日夕方の時短族")
        .otherwise("ランダム")
    )
    # カラム選択
    .select(
        # 会員基本情報
        "user_id", "name", "gender", "birth_date", "pref",
        "phone_number", "email", "email_permission_flg",
        "registration_date", "status", "last_login_at",
        # クラスター情報（購買履歴から算出）
        "behavior_cluster", "behavior_cluster_name",
        # RFM情報
        "last_purchase_date", "recency", "frequency", "monetary",
        "r_score", "f_score", "m_score", "rfm_segment",
        # 新規・既存フラグ
        "customer_type"
    )
)

gd_users.write.mode("overwrite").format("delta").saveAsTable(
    f"{MY_CATALOG}.{MY_SCHEMA}.gd_users"
)

print(f"gd_users: {gd_users.count()}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gd_orders

# COMMAND ----------

# DBTITLE 1,gd_ordersの作成
# gd_ordersを作成（クラスター情報は購買履歴から算出済みのuser_clusterを使用）
gd_orders = (
    sl_orders
    # 日時関連カラムを追加
    .withColumn("order_date", to_date(col("order_datetime")))
    .withColumn("order_hour", hour(col("order_datetime")))
    .withColumn("order_dayofweek", dayofweek(col("order_datetime")))  # 1=日曜, 7=土曜
    .withColumn("is_weekend",
        when(dayofweek(col("order_datetime")).isin([1, 7]), lit(True))
        .otherwise(lit(False))
    )
    # クラスター情報を結合（購買履歴から算出）
    .join(user_cluster, sl_orders["user_id"] == user_cluster["cluster_user_id"], "left")
    # RFM情報を結合
    .join(user_rfm_ref, sl_orders["user_id"] == user_rfm_ref["rfm_user_id"], "left")
    # 初回注文フラグを結合
    .join(first_order_ref, sl_orders["order_id"] == first_order_ref["fo_order_id"], "left")
    # クラスター名を付与
    .withColumn("behavior_cluster_name",
        when(col("behavior_cluster") == "1", "週末まとめ買い族")
        .when(col("behavior_cluster") == "2", "夜型値引きハンター")
        .when(col("behavior_cluster") == "3", "健康・こだわり層")
        .when(col("behavior_cluster") == "4", "カテゴリジャンキー")
        .when(col("behavior_cluster") == "5", "平日夕方の時短族")
        .otherwise("ランダム")
    )
    # 注文単位の新規・既存フラグ（初回注文 = 新規）
    .withColumn("customer_type",
        when(col("is_first_order") == True, "新規")
        .otherwise("既存")
    )
    # カラム選択
    .select(
        # 注文基本情報
        "order_id", "user_id",
        "store_id", "store_name", "store_area",
        "order_datetime", "order_date", "order_hour", "order_dayofweek", "is_weekend",
        "total_amount", "total_quantity", "order_status",
        # 分析セグメント
        "behavior_cluster", "behavior_cluster_name",
        "r_score", "f_score", "m_score", "rfm_segment",
        # 新規・既存フラグ（注文単位）
        "is_first_order", "customer_type"
    )
)

gd_orders.write.mode("overwrite").format("delta").saveAsTable(
    f"{MY_CATALOG}.{MY_SCHEMA}.gd_orders"
)

print(f"gd_orders: {gd_orders.count()}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gd_order_items

# COMMAND ----------

# DBTITLE 1,gd_order_itemsの作成
# gd_order_itemsを作成（クラスター情報は購買履歴から算出済みのuser_clusterを使用）
gd_order_items = (
    sl_order_items
    # 日時関連カラムを追加
    .withColumn("order_date", to_date(col("order_datetime")))
    .withColumn("order_hour", hour(col("order_datetime")))
    .withColumn("order_dayofweek", dayofweek(col("order_datetime")))  # 1=日曜, 7=土曜
    .withColumn("is_weekend",
        when(dayofweek(col("order_datetime")).isin([1, 7]), lit(True))
        .otherwise(lit(False))
    )
    # クラスター情報を結合（購買履歴から算出）
    .join(user_cluster, sl_order_items["user_id"] == user_cluster["cluster_user_id"], "left")
    # RFM情報を結合
    .join(user_rfm_ref, sl_order_items["user_id"] == user_rfm_ref["rfm_user_id"], "left")
    # 初回注文フラグを結合（order_id経由）
    .join(first_order_ref, sl_order_items["order_id"] == first_order_ref["fo_order_id"], "left")
    # クラスター名を付与
    .withColumn("behavior_cluster_name",
        when(col("behavior_cluster") == "1", "週末まとめ買い族")
        .when(col("behavior_cluster") == "2", "夜型値引きハンター")
        .when(col("behavior_cluster") == "3", "健康・こだわり層")
        .when(col("behavior_cluster") == "4", "カテゴリジャンキー")
        .when(col("behavior_cluster") == "5", "平日夕方の時短族")
        .otherwise("ランダム")
    )
    # 注文単位の新規・既存フラグ（初回注文 = 新規）
    .withColumn("customer_type",
        when(col("is_first_order") == True, "新規")
        .otherwise("既存")
    )
    # カラム選択
    .select(
        # 注文明細基本情報
        "order_item_id", "order_id", "user_id",
        "store_id", "store_name", "store_area",
        "item_id", "item_name", "category_id", "category_name",
        "is_organic", "is_discount_target", "is_ready_to_eat", "is_premium",
        "quantity", "unit_price", "subtotal", "cancel_flg",
        "order_datetime", "order_date", "order_hour", "order_dayofweek", "is_weekend",
        # 分析セグメント
        "behavior_cluster", "behavior_cluster_name",
        "r_score", "f_score", "m_score", "rfm_segment",
        # 新規・既存フラグ（注文単位）
        "is_first_order", "customer_type"
    )
)

gd_order_items.write.mode("overwrite").format("delta").saveAsTable(
    f"{MY_CATALOG}.{MY_SCHEMA}.gd_order_items"
)

print(f"gd_order_items: {gd_order_items.count()}件")
