# Databricks notebook source
# MAGIC %md
# MAGIC # Gold レイヤー: 横断分析テーブル
# MAGIC サーバレスコンピュートで実行してください。
# MAGIC
# MAGIC 全テーブルを結合し、分析用のGoldテーブルを作成します：
# MAGIC - **gd_dashboard_fact** — ダッシュボード用の単一データソース（注文粒度・全ディメンション結合・緯度経度付き）
# MAGIC - **gd_customer_journey** — 顧客軸の横断分析（Genie用）
# MAGIC - **gd_channel_performance** — 媒体×商品の効果分析（Genie用）

# COMMAND ----------

# DBTITLE 1,変数設定
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. gd_dashboard_fact（ダッシュボード用・単一データソース）
# MAGIC
# MAGIC 注文粒度で全ディメンションを結合。クロスフィルタリング対応。
# MAGIC MAPチャート用の緯度経度を含む。

# COMMAND ----------

# DBTITLE 1,gd_dashboard_fact テーブル作成
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gd_dashboard_fact AS

WITH pref_geo AS (
    SELECT * FROM (VALUES
        ('北海道',   43.0642, 141.3469),
        ('青森県',   40.8244, 140.7400),
        ('岩手県',   39.7036, 141.1527),
        ('宮城県',   38.2688, 140.8721),
        ('秋田県',   39.7186, 140.1024),
        ('山形県',   38.2405, 140.3634),
        ('福島県',   37.7503, 140.4676),
        ('茨城県',   36.3418, 140.4468),
        ('栃木県',   36.5657, 139.8836),
        ('群馬県',   36.3912, 139.0608),
        ('埼玉県',   35.8569, 139.6489),
        ('千葉県',   35.6047, 140.1233),
        ('東京都',   35.6895, 139.6917),
        ('神奈川県', 35.4478, 139.6425),
        ('新潟県',   37.9026, 139.0236),
        ('富山県',   36.6953, 137.2114),
        ('石川県',   36.5946, 136.6256),
        ('福井県',   36.0652, 136.2219),
        ('山梨県',   35.6642, 138.5684),
        ('長野県',   36.2332, 138.1810),
        ('岐阜県',   35.3912, 136.7223),
        ('静岡県',   34.9769, 138.3831),
        ('愛知県',   35.1802, 136.9066),
        ('三重県',   34.7303, 136.5086),
        ('滋賀県',   35.0045, 135.8686),
        ('京都府',   35.0214, 135.7556),
        ('大阪府',   34.6864, 135.5200),
        ('兵庫県',   34.6913, 135.1830),
        ('奈良県',   34.6851, 135.8328),
        ('和歌山県', 34.2260, 135.1675),
        ('鳥取県',   35.5039, 134.2381),
        ('島根県',   35.4723, 133.0505),
        ('岡山県',   34.6618, 133.9344),
        ('広島県',   34.3966, 132.4596),
        ('山口県',   34.1861, 131.4705),
        ('徳島県',   34.0658, 134.5593),
        ('香川県',   34.3401, 134.0434),
        ('愛媛県',   33.8416, 132.7661),
        ('高知県',   33.5597, 133.5311),
        ('福岡県',   33.6064, 130.4183),
        ('佐賀県',   33.2494, 130.2998),
        ('長崎県',   32.7503, 129.8779),
        ('熊本県',   32.7898, 130.7417),
        ('大分県',   33.2382, 131.6126),
        ('宮崎県',   31.9111, 131.4239),
        ('鹿児島県', 31.5602, 130.5581),
        ('沖縄県',   26.3358, 127.8011)
    ) AS t(prefecture, latitude, longitude)
),

customer_primary_channel AS (
    SELECT
        customer_id,
        channel AS primary_channel,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM {catalog}.{schema}.bz_campaign_exposures
    GROUP BY customer_id, channel
),

customer_call_summary AS (
    SELECT
        customer_id,
        call_id,
        purpose AS call_purpose,
        urgency_level,
        trouble_type,
        competitor_mention,
        operator_action,
        next_intent,
        resolution_status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY call_id) AS rn
    FROM {catalog}.{schema}.sv_call_analysis
)

SELECT
    -- 注文ディメンション
    o.order_id,
    o.order_date,
    DATE_TRUNC('month', o.order_date) AS order_month,
    DATE_TRUNC('week', o.order_date)  AS order_week,
    DAYOFWEEK(o.order_date)           AS order_dow,
    o.product_category,
    o.product_name,
    o.size,
    o.order_channel,

    -- 注文メジャー
    o.quantity,
    o.unit_price,
    o.discount_applied,
    o.total_amount,
    o.returned,
    o.return_reason,

    -- 顧客ディメンション
    c.customer_id,
    c.customer_name,
    c.age,
    CASE
        WHEN c.age < 25 THEN '20-24歳'
        WHEN c.age < 30 THEN '25-29歳'
        WHEN c.age < 35 THEN '30-34歳'
        WHEN c.age < 40 THEN '35-39歳'
        WHEN c.age < 45 THEN '40-44歳'
        WHEN c.age < 50 THEN '45-49歳'
        WHEN c.age < 55 THEN '50-54歳'
        WHEN c.age < 60 THEN '55-59歳'
        ELSE '60歳以上'
    END AS age_group,
    c.gender,
    c.prefecture,
    c.customer_segment,
    c.preferred_channel,

    -- 地理情報（MAP用）
    g.latitude,
    g.longitude,

    -- 媒体チャネル
    COALESCE(cpc.primary_channel, '直接') AS campaign_channel,

    -- 通話ログ情報（顧客に紐づく最初のコール）
    ca.call_id,
    ca.call_purpose,
    ca.urgency_level,
    ca.trouble_type,
    ca.competitor_mention,
    ca.operator_action,
    ca.next_intent,
    ca.resolution_status,
    CASE WHEN ca.trouble_type = '割引条件' THEN 1 ELSE 0 END AS has_discount_trouble,
    CASE WHEN ca.competitor_mention THEN 1 ELSE 0 END AS has_competitor_mention

FROM {catalog}.{schema}.bz_orders o
INNER JOIN {catalog}.{schema}.bz_customers c
    ON o.customer_id = c.customer_id
LEFT JOIN pref_geo g
    ON c.prefecture = g.prefecture
LEFT JOIN customer_primary_channel cpc
    ON o.customer_id = cpc.customer_id AND cpc.rn = 1
LEFT JOIN customer_call_summary ca
    ON o.customer_id = ca.customer_id AND ca.rn = 1
""")

df_fact = spark.table(f"{catalog}.{schema}.gd_dashboard_fact")
print(f"gd_dashboard_fact: {df_fact.count():,} 件")
df_fact.display()

# COMMAND ----------

# DBTITLE 1,gd_dashboard_fact コメント設定
spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.gd_dashboard_fact IS
'ダッシュボード用ファクトテーブル。注文粒度で顧客情報、地理情報（緯度経度）、キャンペーンチャネル、通話ログ分析結果を全て結合。クロスフィルタリングに対応した単一データソース。'
""")

fact_comments = {
    "order_id": "注文ID",
    "order_date": "注文日",
    "order_month": "注文月（月次集計用）",
    "order_week": "注文週（週次集計用）",
    "order_dow": "注文曜日（1=日〜7=土）",
    "product_category": "商品カテゴリ（スーツ/シャツ/ネクタイ/ベルト/シューズ/コート）",
    "product_name": "商品名",
    "size": "サイズ（S/M/L/LL/3L）",
    "order_channel": "注文チャネル（EC/店舗）",
    "quantity": "数量",
    "unit_price": "単価（税込）",
    "discount_applied": "適用割引率",
    "total_amount": "合計金額（税込・割引後）",
    "returned": "返品フラグ",
    "return_reason": "返品理由",
    "customer_id": "顧客ID",
    "customer_name": "顧客氏名",
    "age": "年齢",
    "age_group": "年齢層（5歳刻み）",
    "gender": "性別",
    "prefecture": "都道府県",
    "customer_segment": "顧客セグメント（新規/リピート/VIP）",
    "preferred_channel": "優先チャネル（EC/店舗/両方）",
    "latitude": "都道府県の緯度（MAP用）",
    "longitude": "都道府県の経度（MAP用）",
    "campaign_channel": "主要キャンペーン接触チャネル（Web広告/LINE/DM/店舗チラシ/アプリ/直接）",
    "call_id": "通話ID（通話がある場合）",
    "call_purpose": "問い合わせ目的",
    "urgency_level": "緊急度（高/中/低）",
    "trouble_type": "トラブル要因（割引条件/サイズ/納期/品質/請求/なし）",
    "competitor_mention": "他社言及の有無",
    "operator_action": "オペレーターの対応内容",
    "next_intent": "顧客の次の意向",
    "resolution_status": "解決状況（解決/未解決/エスカレーション）",
    "has_discount_trouble": "割引条件トラブルフラグ（1/0）",
    "has_competitor_mention": "他社言及フラグ（1/0）",
}
for col, comment in fact_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.gd_dashboard_fact ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. gd_customer_journey（顧客ジャーニー・Genie用）

# COMMAND ----------

# DBTITLE 1,顧客軸の横断テーブル作成
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gd_customer_journey AS

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(total_amount) AS total_spend,
        AVG(total_amount) AS avg_order_amount,
        AVG(discount_applied) AS avg_discount,
        SUM(CASE WHEN returned THEN 1 ELSE 0 END) AS return_count,
        ROUND(AVG(CASE WHEN returned THEN 1.0 ELSE 0.0 END), 4) AS return_rate,
        FIRST_VALUE(return_reason) IGNORE NULLS AS primary_return_reason,
        FIRST_VALUE(product_category) AS top_category
    FROM {catalog}.{schema}.bz_orders
    GROUP BY customer_id
),

customer_exposures AS (
    SELECT
        customer_id,
        COUNT(DISTINCT exposure_id) AS exposure_count,
        SUM(CASE WHEN clicked THEN 1 ELSE 0 END) AS click_count,
        SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS conversion_count,
        ROUND(AVG(CASE WHEN clicked THEN 1.0 ELSE 0.0 END), 4) AS click_rate,
        FIRST_VALUE(channel) AS primary_exposure_channel
    FROM {catalog}.{schema}.bz_campaign_exposures
    GROUP BY customer_id
),

customer_calls AS (
    SELECT
        customer_id,
        COUNT(DISTINCT call_id) AS call_count,
        AVG(call_duration_sec) AS avg_call_duration,
        FIRST_VALUE(purpose) AS primary_call_purpose,
        FIRST_VALUE(trouble_type) AS primary_trouble_type,
        MAX(CASE WHEN competitor_mention THEN 1 ELSE 0 END) AS had_competitor_mention,
        MAX(CASE WHEN trouble_type = '割引条件' THEN 1 ELSE 0 END) AS had_discount_trouble,
        MAX(CASE WHEN urgency_level = '高' THEN 1 ELSE 0 END) AS had_urgency_call,
        FIRST_VALUE(urgency_level) AS primary_urgency_level,
        FIRST_VALUE(next_intent) AS latest_intent,
        ROUND(AVG(CASE WHEN resolution_status = '解決' THEN 1.0 ELSE 0.0 END), 4) AS resolution_rate
    FROM {catalog}.{schema}.sv_call_analysis
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.customer_name,
    c.age,
    c.gender,
    c.prefecture,
    c.registration_date,
    c.customer_segment,
    c.preferred_channel,
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(o.total_spend, 0) AS total_spend,
    o.avg_order_amount,
    o.avg_discount,
    COALESCE(o.return_count, 0) AS return_count,
    COALESCE(o.return_rate, 0) AS return_rate,
    o.primary_return_reason,
    o.top_category,
    COALESCE(e.exposure_count, 0) AS exposure_count,
    COALESCE(e.click_count, 0) AS click_count,
    COALESCE(e.conversion_count, 0) AS conversion_count,
    COALESCE(e.click_rate, 0) AS click_rate,
    e.primary_exposure_channel,
    COALESCE(ca.call_count, 0) AS call_count,
    ca.avg_call_duration,
    ca.primary_call_purpose,
    ca.primary_trouble_type,
    COALESCE(ca.had_competitor_mention, 0) AS had_competitor_mention,
    COALESCE(ca.had_discount_trouble, 0) AS had_discount_trouble,
    COALESCE(ca.had_urgency_call, 0) AS had_urgency_call,
    ca.primary_urgency_level,
    ca.latest_intent,
    ca.resolution_rate
FROM {catalog}.{schema}.bz_customers c
LEFT JOIN customer_orders o ON c.customer_id = o.customer_id
LEFT JOIN customer_exposures e ON c.customer_id = e.customer_id
LEFT JOIN customer_calls ca ON c.customer_id = ca.customer_id
""")

print(f"gd_customer_journey: {spark.table(f'{catalog}.{schema}.gd_customer_journey').count():,} 件")

# COMMAND ----------

# DBTITLE 1,gd_customer_journey コメント設定
spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.gd_customer_journey IS
'顧客ジャーニーGoldテーブル。顧客マスタに対して注文サマリ、キャンペーン接触サマリ、通話ログ分析サマリを結合した横断分析テーブル。30,000顧客分。Genie分析のメインテーブル。'
""")

journey_comments = {
    "customer_id": "顧客ID（主キー）", "customer_name": "顧客氏名",
    "age": "年齢", "gender": "性別", "prefecture": "都道府県",
    "registration_date": "会員登録日",
    "customer_segment": "顧客セグメント（新規/リピート/VIP）",
    "preferred_channel": "優先チャネル（EC/店舗/両方）",
    "order_count": "注文件数", "total_spend": "合計購入金額",
    "avg_order_amount": "平均注文金額", "avg_discount": "平均適用割引率",
    "return_count": "返品件数", "return_rate": "返品率",
    "primary_return_reason": "主な返品理由",
    "top_category": "最も購入の多い商品カテゴリ",
    "exposure_count": "キャンペーン接触回数",
    "click_count": "クリック回数", "conversion_count": "コンバージョン回数",
    "click_rate": "クリック率", "primary_exposure_channel": "主要接触チャネル",
    "call_count": "通話回数", "avg_call_duration": "平均通話時間（秒）",
    "primary_call_purpose": "主な問い合わせ目的",
    "primary_trouble_type": "主なトラブル要因",
    "had_competitor_mention": "他社言及の有無（1=あり/0=なし）",
    "had_discount_trouble": "割引条件のトラブル有無（1=あり/0=なし）",
    "had_urgency_call": "緊急通話の有無（1=あり/0=なし）",
    "primary_urgency_level": "主な問い合わせの緊急度（高/中/低）",
    "latest_intent": "最新の意向", "resolution_rate": "問い合わせ解決率",
}
for col, comment in journey_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.gd_customer_journey ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. gd_channel_performance（媒体パフォーマンス・Genie用）

# COMMAND ----------

# DBTITLE 1,媒体×商品カテゴリのパフォーマンステーブル作成
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gd_channel_performance AS

WITH customer_primary_channel AS (
    SELECT customer_id, channel AS primary_channel,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
    FROM {catalog}.{schema}.bz_campaign_exposures
    GROUP BY customer_id, channel
),

-- 注文データを媒体×商品カテゴリで集計（コールとの多対多を回避）
order_summary AS (
    SELECT
        COALESCE(cpc.primary_channel, '直接') AS channel,
        o.product_category,
        COUNT(DISTINCT o.order_id) AS order_count,
        COUNT(DISTINCT o.customer_id) AS customer_count,
        SUM(o.total_amount) AS total_revenue,
        ROUND(AVG(o.total_amount), 0) AS avg_order_amount,
        ROUND(AVG(o.discount_applied), 4) AS avg_discount,
        SUM(CASE WHEN o.returned THEN 1 ELSE 0 END) AS return_count,
        ROUND(AVG(CASE WHEN o.returned THEN 1.0 ELSE 0.0 END), 4) AS return_rate,
        SUM(CASE WHEN o.return_reason = '割引条件違い' THEN 1 ELSE 0 END) AS discount_trouble_returns,
        SUM(CASE WHEN o.return_reason = 'サイズ不一致' THEN 1 ELSE 0 END) AS size_mismatch_returns,
        SUM(CASE WHEN o.return_reason = 'イメージ違い' THEN 1 ELSE 0 END) AS image_diff_returns
    FROM {catalog}.{schema}.bz_orders o
    LEFT JOIN customer_primary_channel cpc
        ON o.customer_id = cpc.customer_id AND cpc.rn = 1
    GROUP BY COALESCE(cpc.primary_channel, '直接'), o.product_category
),

-- 通話データを媒体で集計（注文粒度と独立して集計）
call_summary AS (
    SELECT
        COALESCE(cpc.primary_channel, '直接') AS channel,
        COUNT(DISTINCT ca.call_id) AS call_count,
        SUM(CASE WHEN ca.trouble_type = '割引条件' THEN 1 ELSE 0 END) AS discount_trouble_calls,
        SUM(CASE WHEN ca.competitor_mention THEN 1 ELSE 0 END) AS competitor_mention_calls
    FROM {catalog}.{schema}.sv_call_analysis ca
    LEFT JOIN customer_primary_channel cpc
        ON ca.customer_id = cpc.customer_id AND cpc.rn = 1
    GROUP BY COALESCE(cpc.primary_channel, '直接')
)

SELECT
    os.channel,
    os.product_category,
    os.order_count,
    os.customer_count,
    os.total_revenue,
    os.avg_order_amount,
    os.avg_discount,
    os.return_count,
    os.return_rate,
    os.discount_trouble_returns,
    os.size_mismatch_returns,
    os.image_diff_returns,
    COALESCE(cs.call_count, 0) AS call_count,
    COALESCE(cs.discount_trouble_calls, 0) AS discount_trouble_calls,
    COALESCE(cs.competitor_mention_calls, 0) AS competitor_mention_calls
FROM order_summary os
LEFT JOIN call_summary cs ON os.channel = cs.channel
""")

print(f"gd_channel_performance: {spark.table(f'{catalog}.{schema}.gd_channel_performance').count():,} 件")

# COMMAND ----------

# DBTITLE 1,gd_channel_performance コメント設定
spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.gd_channel_performance IS
'媒体パフォーマンスGoldテーブル。キャンペーン接触チャネル × 商品カテゴリごとの注文・返品・通話指標を集計。'
""")

channel_comments = {
    "channel": "主要接触チャネル（Web広告/LINE/DM/店舗チラシ/アプリ/直接）",
    "product_category": "商品カテゴリ", "order_count": "注文件数",
    "customer_count": "ユニーク顧客数", "total_revenue": "合計売上金額",
    "avg_order_amount": "平均注文金額", "avg_discount": "平均適用割引率",
    "return_count": "返品件数", "return_rate": "返品率",
    "discount_trouble_returns": "割引条件違いによる返品件数",
    "size_mismatch_returns": "サイズ不一致による返品件数",
    "image_diff_returns": "イメージ違いによる返品件数",
    "call_count": "通話件数",
    "discount_trouble_calls": "割引条件に関する問い合わせ件数",
    "competitor_mention_calls": "他社言及のある通話件数",
}
for col, comment in channel_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.gd_channel_performance ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 分析結果のプレビュー

# COMMAND ----------

# DBTITLE 1,gd_dashboard_fact: 媒体別サマリ（デモの核心データ）
spark.sql(f"""
SELECT
    campaign_channel,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(CASE WHEN returned THEN 1.0 ELSE 0.0 END) * 100, 2) AS return_rate_pct,
    SUM(CASE WHEN return_reason = '割引条件違い' THEN 1 ELSE 0 END) AS discount_trouble_returns,
    SUM(has_discount_trouble) AS discount_trouble_calls
FROM {catalog}.{schema}.gd_dashboard_fact
GROUP BY campaign_channel
ORDER BY return_rate_pct DESC
""").display()

# COMMAND ----------

# DBTITLE 1,gd_dashboard_fact: 都道府県別売上（MAP確認用）
spark.sql(f"""
SELECT
    prefecture,
    latitude,
    longitude,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue
FROM {catalog}.{schema}.gd_dashboard_fact
GROUP BY prefecture, latitude, longitude
ORDER BY total_revenue DESC
LIMIT 15
""").display()

# COMMAND ----------

# DBTITLE 1,★Wow: 解決率 × リピート購入率（解決した顧客はリピートする）
spark.sql(f"""
SELECT
    CASE
        WHEN resolution_rate >= 0.8 THEN '解決率80%以上'
        WHEN resolution_rate >= 0.5 THEN '解決率50-79%'
        WHEN resolution_rate > 0    THEN '解決率50%未満'
        ELSE '通話なし'
    END AS resolution_group,
    COUNT(*) AS customer_count,
    ROUND(AVG(order_count), 2) AS avg_order_count,
    ROUND(AVG(total_spend), 0) AS avg_total_spend,
    ROUND(AVG(return_rate), 4) AS avg_return_rate
FROM {catalog}.{schema}.gd_customer_journey
WHERE call_count > 0
GROUP BY 1
ORDER BY 1
""").display()

# COMMAND ----------

# DBTITLE 1,★Wow: 緊急度 × 購買単価（急ぎ顧客は高単価）
spark.sql(f"""
SELECT
    s.urgency_level,
    COUNT(DISTINCT s.customer_id) AS customer_count,
    ROUND(AVG(j.avg_order_amount), 0) AS avg_order_amount,
    ROUND(AVG(j.total_spend), 0) AS avg_total_spend,
    ROUND(AVG(j.return_rate), 4) AS avg_return_rate
FROM {catalog}.{schema}.sv_call_analysis s
JOIN {catalog}.{schema}.gd_customer_journey j
    ON s.customer_id = j.customer_id
GROUP BY s.urgency_level
ORDER BY
    CASE s.urgency_level WHEN '高' THEN 1 WHEN '中' THEN 2 ELSE 3 END
""").display()

# COMMAND ----------

# DBTITLE 1,全テーブルの件数サマリ
for table in [
    "bz_customers", "bz_offers", "bz_campaign_exposures", "bz_orders",
    "bz_call_logs", "sv_call_analysis",
    "gd_dashboard_fact", "gd_customer_journey", "gd_channel_performance"
]:
    cnt = spark.table(f"{catalog}.{schema}.{table}").count()
    print(f"{table}: {cnt:,} 件")
