# Databricks notebook source
# MAGIC %md
# MAGIC # Gold層 ETL
# MAGIC Silverテーブルに分析セグメントを付与してGoldテーブルを作成

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,既存goldテーブルの削除
# スキーマ内のすべてのテーブル名を取得
tables = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}").collect()

# テーブル名が "gd_" で始まるテーブルのみ削除
for t in tables:
    if t["tableName"].startswith("gd_"):
        spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{t['tableName']}")
        print(f"削除: {t['tableName']}")

print("\n既存のGoldテーブルを全て削除しました。")

# COMMAND ----------

# DBTITLE 1,RFM指標の計算
# ユーザーごとのRFM指標を計算（sl_ordersから会計単位で集計）
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW tmp_user_rfm AS
    SELECT
        user_id,
        MAX(TO_DATE(order_datetime)) AS last_purchase_date,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(total_amount) AS monetary,
        DATEDIFF(CURRENT_DATE(), MAX(TO_DATE(order_datetime))) AS recency
    FROM {MY_CATALOG}.{MY_SCHEMA}.sl_orders
    GROUP BY user_id
""")

# COMMAND ----------

# DBTITLE 1,RFMスコアとセグメントの付与
# RFMスコアを付与（1-5のスコア）
spark.sql("""
    CREATE OR REPLACE TEMP VIEW tmp_user_rfm_scored AS
    SELECT
        user_id,
        last_purchase_date,
        frequency,
        monetary,
        recency,
        -- Recencyスコア（小さいほど良い → 高スコア）
        CASE
            WHEN recency <= 7 THEN 5
            WHEN recency <= 14 THEN 4
            WHEN recency <= 30 THEN 3
            WHEN recency <= 60 THEN 2
            ELSE 1
        END AS r_score,
        -- Frequencyスコア（大きいほど良い → 高スコア）
        CASE
            WHEN frequency >= 10 THEN 5
            WHEN frequency >= 5 THEN 4
            WHEN frequency >= 3 THEN 3
            WHEN frequency >= 2 THEN 2
            ELSE 1
        END AS f_score,
        -- Monetaryスコア（大きいほど良い → 高スコア）
        CASE
            WHEN monetary >= 50000 THEN 5
            WHEN monetary >= 20000 THEN 4
            WHEN monetary >= 10000 THEN 3
            WHEN monetary >= 5000 THEN 2
            ELSE 1
        END AS m_score,
        -- 新規・既存フラグ
        CASE
            WHEN frequency = 1 THEN '新規'
            ELSE '既存'
        END AS customer_type
    FROM tmp_user_rfm
""")

# RFMセグメント分類を追加
spark.sql("""
    CREATE OR REPLACE TEMP VIEW tmp_user_rfm_ref AS
    SELECT
        user_id AS rfm_user_id,
        last_purchase_date,
        recency,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'ロイヤル顧客'
            WHEN r_score >= 4 AND f_score >= 3 THEN '常連顧客'
            WHEN r_score >= 4 AND f_score <= 2 THEN '新規顧客'
            WHEN r_score <= 2 AND f_score >= 3 THEN '離脱リスク'
            WHEN r_score <= 2 AND f_score <= 2 THEN '休眠顧客'
            ELSE '一般顧客'
        END AS rfm_segment,
        customer_type
    FROM tmp_user_rfm_scored
""")

# COMMAND ----------

# DBTITLE 1,初回注文フラグの計算
# 各顧客の注文に順番を付与（order_datetimeの昇順で1, 2, 3...）
# ※「新規顧客」= 登録から30日以内の初回注文
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW tmp_first_order_ref AS
    SELECT
        order_id AS fo_order_id,
        CASE
            WHEN order_rank = 1 AND DATEDIFF(order_date, registration_date) <= 30 THEN TRUE
            ELSE FALSE
        END AS is_first_order
    FROM (
        SELECT
            o.order_id,
            o.user_id,
            TO_DATE(o.order_datetime) AS order_date,
            u.registration_date,
            ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.order_datetime) AS order_rank
        FROM {MY_CATALOG}.{MY_SCHEMA}.sl_orders o
        LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.sl_users u
            ON o.user_id = u.user_id
    )
""")

# COMMAND ----------

# DBTITLE 1,行動クラスター - 注文レベルの統計
# 曜日、時間帯の統計を計算
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW tmp_order_stats AS
    SELECT
        user_id,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN DAYOFWEEK(order_datetime) IN (1, 7) THEN 1 ELSE 0 END) AS weekend_orders,
        SUM(CASE WHEN DAYOFWEEK(order_datetime) NOT IN (1, 7) AND HOUR(order_datetime) BETWEEN 16 AND 18 THEN 1 ELSE 0 END) AS weekday_evening_orders,
        SUM(CASE WHEN HOUR(order_datetime) >= 19 THEN 1 ELSE 0 END) AS night_orders,
        SUM(total_quantity) AS total_items,
        -- 各割合を計算
        SUM(CASE WHEN DAYOFWEEK(order_datetime) IN (1, 7) THEN 1 ELSE 0 END) / COUNT(*) AS weekend_rate,
        SUM(CASE WHEN DAYOFWEEK(order_datetime) NOT IN (1, 7) AND HOUR(order_datetime) BETWEEN 16 AND 18 THEN 1 ELSE 0 END) / COUNT(*) AS weekday_evening_rate,
        SUM(CASE WHEN HOUR(order_datetime) >= 19 THEN 1 ELSE 0 END) / COUNT(*) AS night_rate,
        SUM(total_quantity) / COUNT(*) AS avg_basket_size
    FROM {MY_CATALOG}.{MY_SCHEMA}.sl_orders
    GROUP BY user_id
""")

# COMMAND ----------

# DBTITLE 1,行動クラスター - 商品レベルの統計
# カテゴリ、属性の統計を計算
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW tmp_item_stats AS
    SELECT
        user_id,
        COUNT(*) AS total_items_purchased,
        SUM(CASE WHEN category_id IN (1100, 600, 1200) THEN 1 ELSE 0 END) AS category_junkie_items,
        SUM(CASE WHEN is_organic = TRUE OR is_premium = TRUE THEN 1 ELSE 0 END) AS organic_premium_items,
        SUM(CASE WHEN is_discount_target = TRUE THEN 1 ELSE 0 END) AS discount_items,
        SUM(CASE WHEN is_ready_to_eat = TRUE THEN 1 ELSE 0 END) AS ready_to_eat_items,
        -- 各割合を計算
        SUM(CASE WHEN category_id IN (1100, 600, 1200) THEN 1 ELSE 0 END) / COUNT(*) AS category_junkie_rate,
        SUM(CASE WHEN is_organic = TRUE OR is_premium = TRUE THEN 1 ELSE 0 END) / COUNT(*) AS organic_premium_rate,
        SUM(CASE WHEN is_discount_target = TRUE THEN 1 ELSE 0 END) / COUNT(*) AS discount_rate,
        SUM(CASE WHEN is_ready_to_eat = TRUE THEN 1 ELSE 0 END) / COUNT(*) AS ready_to_eat_rate
    FROM {MY_CATALOG}.{MY_SCHEMA}.sl_order_items
    GROUP BY user_id
""")

# COMMAND ----------

# DBTITLE 1,行動クラスター判定
# 優先順位: 5(時短族) > 1(週末族) > 2(夜型) > 3(健康) > 4(カテゴリ) > NULL(ランダム)
spark.sql("""
    CREATE OR REPLACE TEMP VIEW tmp_user_cluster AS
    SELECT
        o.user_id AS cluster_user_id,
        CASE
            -- クラスター5: 平日夕方の時短族
            WHEN o.weekday_evening_rate >= 0.70 AND i.ready_to_eat_rate >= 0.60 THEN '5'
            -- クラスター1: 週末まとめ買い族
            WHEN o.weekend_rate >= 0.80 AND o.avg_basket_size >= 10 THEN '1'
            -- クラスター2: 夜型値引きハンター
            WHEN o.night_rate >= 0.70 AND i.discount_rate >= 0.50 THEN '2'
            -- クラスター3: 健康・こだわり層
            WHEN i.organic_premium_rate >= 0.60 THEN '3'
            -- クラスター4: カテゴリジャンキー
            WHEN i.category_junkie_rate >= 0.70 THEN '4'
            ELSE NULL
        END AS behavior_cluster
    FROM tmp_order_stats o
    LEFT JOIN tmp_item_stats i
        ON o.user_id = i.user_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gd_users

# COMMAND ----------

# DBTITLE 1,gd_usersの作成
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_users
    USING DELTA
    AS
    SELECT
        -- 会員基本情報
        u.user_id,
        u.name,
        u.gender,
        u.birth_date,
        u.pref,
        u.phone_number,
        u.email,
        u.email_permission_flg,
        u.registration_date,
        u.status,
        u.last_login_at,
        -- クラスター情報（購買履歴から算出）
        c.behavior_cluster,
        CASE
            WHEN c.behavior_cluster = '1' THEN '週末まとめ買い族'
            WHEN c.behavior_cluster = '2' THEN '夜型値引きハンター'
            WHEN c.behavior_cluster = '3' THEN '健康・こだわり層'
            WHEN c.behavior_cluster = '4' THEN 'カテゴリジャンキー'
            WHEN c.behavior_cluster = '5' THEN '平日夕方の時短族'
            ELSE 'ランダム'
        END AS behavior_cluster_name,
        -- RFM情報
        r.last_purchase_date,
        r.recency,
        r.frequency,
        r.monetary,
        r.r_score,
        r.f_score,
        r.m_score,
        r.rfm_segment,
        -- 新規・既存フラグ
        r.customer_type
    FROM {MY_CATALOG}.{MY_SCHEMA}.sl_users u
    LEFT JOIN tmp_user_cluster c
        ON u.user_id = c.cluster_user_id
    LEFT JOIN tmp_user_rfm_ref r
        ON u.user_id = r.rfm_user_id
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.gd_users").collect()[0]["cnt"]
print(f"gd_users: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gd_orders

# COMMAND ----------

# DBTITLE 1,gd_ordersの作成
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_orders
    USING DELTA
    AS
    SELECT
        -- 注文基本情報
        o.order_id,
        o.user_id,
        o.store_id,
        o.store_name,
        o.store_area,
        o.order_datetime,
        TO_DATE(o.order_datetime) AS order_date,
        HOUR(o.order_datetime) AS order_hour,
        DAYOFWEEK(o.order_datetime) AS order_dayofweek,
        CASE WHEN DAYOFWEEK(o.order_datetime) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        o.total_amount,
        o.total_quantity,
        o.order_status,
        -- 分析セグメント
        c.behavior_cluster,
        CASE
            WHEN c.behavior_cluster = '1' THEN '週末まとめ買い族'
            WHEN c.behavior_cluster = '2' THEN '夜型値引きハンター'
            WHEN c.behavior_cluster = '3' THEN '健康・こだわり層'
            WHEN c.behavior_cluster = '4' THEN 'カテゴリジャンキー'
            WHEN c.behavior_cluster = '5' THEN '平日夕方の時短族'
            ELSE 'ランダム'
        END AS behavior_cluster_name,
        r.r_score,
        r.f_score,
        r.m_score,
        r.rfm_segment,
        -- 新規・既存フラグ（注文単位）
        f.is_first_order,
        CASE WHEN f.is_first_order = TRUE THEN '新規' ELSE '既存' END AS customer_type
    FROM {MY_CATALOG}.{MY_SCHEMA}.sl_orders o
    LEFT JOIN tmp_user_cluster c
        ON o.user_id = c.cluster_user_id
    LEFT JOIN tmp_user_rfm_ref r
        ON o.user_id = r.rfm_user_id
    LEFT JOIN tmp_first_order_ref f
        ON o.order_id = f.fo_order_id
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.gd_orders").collect()[0]["cnt"]
print(f"gd_orders: {cnt}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gd_order_items

# COMMAND ----------

# DBTITLE 1,gd_order_itemsの作成
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_order_items
    USING DELTA
    AS
    SELECT
        -- 注文明細基本情報
        oi.order_item_id,
        oi.order_id,
        oi.user_id,
        oi.store_id,
        oi.store_name,
        oi.store_area,
        oi.item_id,
        oi.item_name,
        oi.category_id,
        oi.category_name,
        oi.is_organic,
        oi.is_discount_target,
        oi.is_ready_to_eat,
        oi.is_premium,
        oi.quantity,
        oi.unit_price,
        oi.subtotal,
        oi.cancel_flg,
        oi.order_datetime,
        TO_DATE(oi.order_datetime) AS order_date,
        HOUR(oi.order_datetime) AS order_hour,
        DAYOFWEEK(oi.order_datetime) AS order_dayofweek,
        CASE WHEN DAYOFWEEK(oi.order_datetime) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        -- 分析セグメント
        c.behavior_cluster,
        CASE
            WHEN c.behavior_cluster = '1' THEN '週末まとめ買い族'
            WHEN c.behavior_cluster = '2' THEN '夜型値引きハンター'
            WHEN c.behavior_cluster = '3' THEN '健康・こだわり層'
            WHEN c.behavior_cluster = '4' THEN 'カテゴリジャンキー'
            WHEN c.behavior_cluster = '5' THEN '平日夕方の時短族'
            ELSE 'ランダム'
        END AS behavior_cluster_name,
        r.r_score,
        r.f_score,
        r.m_score,
        r.rfm_segment,
        -- 新規・既存フラグ（注文単位）
        f.is_first_order,
        CASE WHEN f.is_first_order = TRUE THEN '新規' ELSE '既存' END AS customer_type
    FROM {MY_CATALOG}.{MY_SCHEMA}.sl_order_items oi
    LEFT JOIN tmp_user_cluster c
        ON oi.user_id = c.cluster_user_id
    LEFT JOIN tmp_user_rfm_ref r
        ON oi.user_id = r.rfm_user_id
    LEFT JOIN tmp_first_order_ref f
        ON oi.order_id = f.fo_order_id
""")

cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {MY_CATALOG}.{MY_SCHEMA}.gd_order_items").collect()[0]["cnt"]
print(f"gd_order_items: {cnt}件")
