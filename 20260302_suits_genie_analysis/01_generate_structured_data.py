# Databricks notebook source
# MAGIC %md
# MAGIC # 構造化データ生成 → Bronzeテーブル作成
# MAGIC サーバレスコンピュートで実行してください。
# MAGIC
# MAGIC **処理フロー**: データ生成 → CSV（Volume保存）→ CSV読み込み → Bronzeテーブル
# MAGIC
# MAGIC | # | テーブル | 件数 | 内容 |
# MAGIC |---|---------|------|------|
# MAGIC | 1 | bz_customers | 30,000 | 顧客マスタ |
# MAGIC | 2 | bz_offers | 30 | キャンペーン・割引オファー |
# MAGIC | 3 | bz_campaign_exposures | 150,000 | キャンペーン接触ログ |
# MAGIC | 4 | bz_orders | 50,000 | 注文データ |

# COMMAND ----------

# DBTITLE 1,変数設定
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,共通インポート
import random
import hashlib
from datetime import datetime, timedelta, date
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, DateType
)
from pyspark.sql import functions as F

random.seed(42)

# 基準日（直近1年の範囲）
TODAY = date(2026, 3, 1)
DATE_START = TODAY - timedelta(days=365)

# CSV出力先
CSV_PATH = f"/Volumes/{catalog}/{schema}/{volume}/structured_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: CSVデータ生成 → Volume保存
# MAGIC
# MAGIC 実際の業務では、基幹システムやSaaSからCSVでエクスポートされたデータを想定しています。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 customers（30,000件）

# COMMAND ----------

# DBTITLE 1,顧客データ生成 → CSV保存
# --- 姓名リスト ---
LAST_NAMES = [
    "佐藤", "鈴木", "高橋", "田中", "伊藤", "渡辺", "山本", "中村", "小林", "加藤",
    "吉田", "山田", "佐々木", "松本", "井上", "木村", "林", "斎藤", "清水", "山口",
    "森", "池田", "橋本", "阿部", "石川", "山崎", "中島", "藤田", "小川", "後藤",
    "岡田", "長谷川", "村上", "近藤", "石井", "遠藤", "青木", "坂本", "斉藤", "福田",
    "太田", "西村", "藤井", "金子", "三浦", "岡本", "松田", "中野", "原田", "小野",
]

FIRST_NAMES_M = [
    "太郎", "一郎", "健太", "大輔", "翔太", "拓也", "直樹", "和也", "達也", "誠",
    "浩二", "修", "亮", "隆", "剛", "学", "豊", "哲也", "雄太", "裕介",
    "慎一", "勇気", "大地", "蓮", "陸", "悠真", "颯太", "奏太", "陽翔", "樹",
]

FIRST_NAMES_F = [
    "花子", "美咲", "陽子", "恵子", "裕子", "由美", "真由美", "久美子", "智子", "直美",
    "明美", "京子", "洋子", "幸子", "和子", "愛", "さくら", "結衣", "凛", "葵",
    "美月", "彩花", "優奈", "莉子", "心春", "紗良", "美桜", "芽依", "杏", "楓",
]

PREFECTURES = [
    ("東京都", 20), ("神奈川県", 12), ("大阪府", 10), ("埼玉県", 8), ("千葉県", 7),
    ("愛知県", 7), ("福岡県", 5), ("北海道", 4), ("兵庫県", 4), ("京都府", 3),
    ("広島県", 2), ("宮城県", 2), ("静岡県", 2), ("新潟県", 2), ("茨城県", 2),
    ("岐阜県", 1), ("栃木県", 1), ("群馬県", 1), ("長野県", 1), ("岡山県", 1),
    ("三重県", 1), ("鹿児島県", 1), ("石川県", 1), ("奈良県", 1),
]

SEGMENTS = ["新規", "リピート", "VIP"]
SEGMENT_WEIGHTS = [0.50, 0.35, 0.15]
CHANNELS = ["EC", "店舗", "両方"]
CHANNEL_WEIGHTS = [0.35, 0.40, 0.25]

# 都道府県の重み付きリスト
pref_list = []
for pref, weight in PREFECTURES:
    pref_list.extend([pref] * weight)

NUM_CUSTOMERS = 30000

customers_data = []
for i in range(1, NUM_CUSTOMERS + 1):
    cid = f"CUST-{i:05d}"
    gender = random.choice(["男性", "女性"])
    last = random.choice(LAST_NAMES)
    first = random.choice(FIRST_NAMES_M if gender == "男性" else FIRST_NAMES_F)
    name = f"{last} {first}"
    age = random.randint(20, 65)
    pref = random.choice(pref_list)
    reg_date = TODAY - timedelta(days=random.randint(1, 730))
    segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]

    customers_data.append((
        cid, name, age, gender, pref,
        reg_date, segment, channel
    ))

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("prefecture", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("preferred_channel", StringType(), True),
])

df_customers = spark.createDataFrame(customers_data, schema=customers_schema)
df_customers.toPandas().to_csv(f"{CSV_PATH}/customers.csv", index=False)

print(f"customers.csv: {df_customers.count():,} 件 → {CSV_PATH}/customers.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 offers（30件）

# COMMAND ----------

# DBTITLE 1,オファーデータ生成 → CSV保存
OFFER_CHANNELS = ["Web広告", "LINE", "DM", "店舗チラシ", "アプリ"]

# キャンペーン名テンプレート（同一キャンペーンが複数チャネルに展開される）
CAMPAIGN_TEMPLATES = [
    {
        "name": "春の新生活スーツフェア",
        "channels": {
            "Web広告": 0.20, "LINE": 0.15, "DM": 0.20, "店舗チラシ": 0.20, "アプリ": 0.18
        },
        "target": "新規", "min_amount": 20000,
        "desc": "新生活を始める方向けのスーツセットキャンペーン",
    },
    {
        "name": "ビジネスリニューアルキャンペーン",
        "channels": {
            "Web広告": 0.15, "LINE": 0.25, "アプリ": 0.20
        },
        "target": "リピート", "min_amount": 30000,
        "desc": "買い替え需要を狙ったリピーター向けキャンペーン",
    },
    {
        "name": "VIP会員限定シークレットセール",
        "channels": {
            "DM": 0.30, "アプリ": 0.30
        },
        "target": "VIP", "min_amount": 0,
        "desc": "VIP会員向けの特別割引セール",
    },
    {
        "name": "就活応援フェア",
        "channels": {
            "Web広告": 0.25, "LINE": 0.20, "店舗チラシ": 0.25
        },
        "target": "新規", "min_amount": 10000,
        "desc": "就職活動中の学生向けスーツキャンペーン",
    },
    {
        "name": "まとめ買い割引フェア",
        "channels": {
            "Web広告": 0.10, "LINE": 0.15, "DM": 0.10, "店舗チラシ": 0.10, "アプリ": 0.12
        },
        "target": "全員", "min_amount": 30000,
        "desc": "スーツ＋シャツ＋ネクタイのまとめ買いで割引",
    },
    {
        "name": "週末限定タイムセール",
        "channels": {
            "LINE": 0.20, "アプリ": 0.25
        },
        "target": "全員", "min_amount": 0,
        "desc": "毎週末のタイムセールキャンペーン",
    },
]

offers_data = []
offer_idx = 1
for tmpl in CAMPAIGN_TEMPLATES:
    start = DATE_START + timedelta(days=random.randint(0, 335))
    duration = random.randint(7, 30)
    end = start + timedelta(days=duration)

    for ch, rate in tmpl["channels"].items():
        oid = f"OFR-{offer_idx:03d}"
        offers_data.append((
            oid, tmpl["name"], ch, rate,
            start, end,
            tmpl["target"], tmpl["min_amount"], tmpl["desc"]
        ))
        offer_idx += 1

offers_schema = StructType([
    StructField("offer_id", StringType(), False),
    StructField("offer_name", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("discount_rate", DoubleType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("target_segment", StringType(), True),
    StructField("min_purchase_amount", IntegerType(), True),
    StructField("description", StringType(), True),
])

df_offers = spark.createDataFrame(offers_data, schema=offers_schema)
df_offers.toPandas().to_csv(f"{CSV_PATH}/offers.csv", index=False)

print(f"offers.csv: {df_offers.count():,} 件 → {CSV_PATH}/offers.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 campaign_exposures（150,000件）

# COMMAND ----------

# DBTITLE 1,キャンペーン接触データ生成 → CSV保存
customer_ids = [row.customer_id for row in df_customers.select("customer_id").collect()]
offer_rows = [(row.offer_id, row.channel, row.target_segment) for row in df_offers.select("offer_id", "channel", "target_segment").collect()]
customer_segments = {row.customer_id: row.customer_segment for row in df_customers.select("customer_id", "customer_segment").collect()}

NUM_EXPOSURES = 150000

exposures_data = []
for i in range(1, NUM_EXPOSURES + 1):
    eid = f"EXP-{i:06d}"
    cid = random.choice(customer_ids)
    offer = random.choice(offer_rows)
    oid, channel, target_seg = offer

    # 日付
    exp_date = DATE_START + timedelta(days=random.randint(0, 364))

    # クリック率はチャネルによって変動
    click_rates = {"Web広告": 0.08, "LINE": 0.15, "DM": 0.05, "店舗チラシ": 0.03, "アプリ": 0.12}
    clicked = random.random() < click_rates.get(channel, 0.05)

    # コンバージョン率（クリックした人のうち）
    conv_rates = {"Web広告": 0.12, "LINE": 0.18, "DM": 0.15, "店舗チラシ": 0.20, "アプリ": 0.14}
    converted = clicked and (random.random() < conv_rates.get(channel, 0.10))

    exposures_data.append((
        eid, cid, oid, exp_date, channel, clicked, converted
    ))

exposures_schema = StructType([
    StructField("exposure_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("offer_id", StringType(), True),
    StructField("exposure_date", DateType(), True),
    StructField("channel", StringType(), True),
    StructField("clicked", BooleanType(), True),
    StructField("converted", BooleanType(), True),
])

df_exposures = spark.createDataFrame(exposures_data, schema=exposures_schema)
df_exposures.toPandas().to_csv(f"{CSV_PATH}/campaign_exposures.csv", index=False)

print(f"campaign_exposures.csv: {df_exposures.count():,} 件 → {CSV_PATH}/campaign_exposures.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 orders（50,000件）

# COMMAND ----------

# DBTITLE 1,注文データ生成 → CSV保存
# 商品マスタ（カテゴリ・商品名・価格帯）
PRODUCTS = {
    "スーツ": [
        ("ビジネススーツ（ネイビー）", 39800),
        ("ビジネススーツ（チャコール）", 39800),
        ("ビジネススーツ（ブラック）", 42800),
        ("リクルートスーツ（ブラック）", 29800),
        ("ストレッチスーツ（グレー）", 34800),
        ("ウォッシャブルスーツ（ネイビー）", 32800),
        ("フォーマルスーツ（ブラック）", 49800),
        ("スリムフィットスーツ（ネイビー）", 36800),
    ],
    "シャツ": [
        ("ワイシャツ（白・レギュラー）", 3980),
        ("ワイシャツ（白・ボタンダウン）", 4280),
        ("ワイシャツ（サックスブルー）", 4280),
        ("ワイシャツ（ストライプ）", 4580),
        ("ノンアイロンシャツ（白）", 5280),
        ("形態安定シャツ（白）", 4980),
    ],
    "ネクタイ": [
        ("シルクネクタイ（ネイビー）", 5800),
        ("シルクネクタイ（レッド）", 5800),
        ("シルクネクタイ（ストライプ）", 6200),
        ("ニットタイ（ネイビー）", 4800),
    ],
    "ベルト": [
        ("ビジネスベルト（ブラック）", 4980),
        ("ビジネスベルト（ブラウン）", 4980),
        ("リバーシブルベルト", 5980),
    ],
    "シューズ": [
        ("ビジネスシューズ（ストレートチップ）", 15800),
        ("ビジネスシューズ（プレーントゥ）", 14800),
        ("ビジネスシューズ（ローファー）", 12800),
    ],
    "コート": [
        ("ステンカラーコート（ネイビー）", 29800),
        ("トレンチコート（ベージュ）", 34800),
        ("チェスターコート（チャコール）", 39800),
    ],
}

SIZES = ["S", "M", "L", "LL", "3L"]
SIZE_WEIGHTS = [0.10, 0.35, 0.30, 0.18, 0.07]
ORDER_CHANNELS = ["EC", "店舗"]

# 返品率をチャネル（キャンペーン元）別に制御
# LINE経由の顧客は返品率が高い（割引条件トラブル）
RETURN_RATES = {
    "LINE": 0.18,       # 意図的に高い
    "Web広告": 0.12,    # やや高い
    "DM": 0.06,
    "店舗チラシ": 0.04,
    "アプリ": 0.07,
    "なし": 0.05,        # キャンペーン非経由
}

RETURN_REASONS = ["サイズ不一致", "イメージ違い", "割引条件違い", "不良品", "その他"]

# 返品理由の分布（チャネル別）
RETURN_REASON_WEIGHTS = {
    "LINE":     [0.15, 0.10, 0.55, 0.05, 0.15],  # 割引条件違いが突出
    "Web広告":  [0.20, 0.15, 0.40, 0.05, 0.20],  # 割引条件違いが多い
    "DM":       [0.30, 0.25, 0.10, 0.10, 0.25],
    "店舗チラシ": [0.35, 0.20, 0.05, 0.15, 0.25],
    "アプリ":   [0.25, 0.20, 0.25, 0.05, 0.25],
    "なし":     [0.35, 0.25, 0.05, 0.10, 0.25],
}

# 顧客のキャンペーン接触チャネルを事前に集計（最頻チャネル）
from collections import Counter
customer_channels = {}
for row in df_exposures.select("customer_id", "channel").collect():
    if row.customer_id not in customer_channels:
        customer_channels[row.customer_id] = []
    customer_channels[row.customer_id].append(row.channel)

def get_primary_channel(cid):
    """顧客の主要接触チャネルを返す"""
    if cid in customer_channels and customer_channels[cid]:
        return Counter(customer_channels[cid]).most_common(1)[0][0]
    return "なし"

NUM_ORDERS = 50000

orders_data = []
for i in range(1, NUM_ORDERS + 1):
    oid = f"ORD-{i:06d}"
    cid = random.choice(customer_ids)
    order_date = DATE_START + timedelta(days=random.randint(0, 364))

    # 商品選択
    category = random.choices(
        list(PRODUCTS.keys()),
        weights=[0.35, 0.25, 0.10, 0.08, 0.12, 0.10],
        k=1
    )[0]
    product_name, unit_price = random.choice(PRODUCTS[category])

    # サイズ（シューズ以外）
    size = random.choices(SIZES, weights=SIZE_WEIGHTS, k=1)[0]

    quantity = random.choices([1, 2, 3], weights=[0.75, 0.20, 0.05], k=1)[0]

    # 割引率（顧客の接触チャネルに紐づくオファーから）
    primary_ch = get_primary_channel(cid)
    channel_offers = [o for o in offers_data if o[2] == primary_ch]
    if channel_offers:
        discount = random.choice(channel_offers)[3]  # discount_rate
    else:
        discount = random.choice([0.0, 0.05, 0.10])

    total = int(unit_price * quantity * (1.0 - discount))

    # 注文チャネル
    order_ch = random.choices(ORDER_CHANNELS, weights=[0.55, 0.45], k=1)[0]

    # 返品判定（チャネル別の返品率）
    return_rate = RETURN_RATES.get(primary_ch, 0.05)
    returned = random.random() < return_rate

    # 返品理由
    if returned:
        reason_weights = RETURN_REASON_WEIGHTS.get(primary_ch, RETURN_REASON_WEIGHTS["なし"])
        return_reason = random.choices(RETURN_REASONS, weights=reason_weights, k=1)[0]
    else:
        return_reason = None

    orders_data.append((
        oid, cid, order_date, category, product_name, size,
        quantity, unit_price, discount, total,
        order_ch, returned, return_reason
    ))

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("size", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", IntegerType(), True),
    StructField("discount_applied", DoubleType(), True),
    StructField("total_amount", IntegerType(), True),
    StructField("order_channel", StringType(), True),
    StructField("returned", BooleanType(), True),
    StructField("return_reason", StringType(), True),
])

df_orders = spark.createDataFrame(orders_data, schema=orders_schema)
df_orders.toPandas().to_csv(f"{CSV_PATH}/orders.csv", index=False)

print(f"orders.csv: {df_orders.count():,} 件 → {CSV_PATH}/orders.csv")

# COMMAND ----------

# DBTITLE 1,Volume内のCSVファイル確認
display(dbutils.fs.ls(CSV_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: CSV → Bronzeテーブル作成
# MAGIC
# MAGIC VolumeにあるCSVファイルを読み込み、Bronzeテーブルとして登録します。

# COMMAND ----------

# DBTITLE 1,bz_customers テーブル作成
df_bz_customers = spark.read.option("header", "true").schema(customers_schema).csv(f"{CSV_PATH}/customers.csv")
df_bz_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.bz_customers")

print(f"bz_customers: {spark.table(f'{catalog}.{schema}.bz_customers').count():,} 件")
spark.table(f"{catalog}.{schema}.bz_customers").display()

# COMMAND ----------

# DBTITLE 1,bz_customers 主キー・コメント設定
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_customers ALTER COLUMN customer_id SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_customers DROP CONSTRAINT IF EXISTS pk_customers")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_customers ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id)")

spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.bz_customers IS
'顧客マスタテーブル。30,000名の顧客情報を格納。セグメント（新規/リピート/VIP）、優先チャネル（EC/店舗/両方）等を含む。'
""")

customer_comments = {
    "customer_id": "顧客ID（主キー）CUST-00001形式",
    "customer_name": "顧客氏名",
    "age": "年齢（20〜65歳）",
    "gender": "性別（男性/女性）",
    "prefecture": "都道府県",
    "registration_date": "会員登録日",
    "customer_segment": "顧客セグメント（新規/リピート/VIP）",
    "preferred_channel": "優先チャネル（EC/店舗/両方）",
}
for col, comment in customer_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_customers ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# DBTITLE 1,bz_offers テーブル作成
df_bz_offers = spark.read.option("header", "true").schema(offers_schema).csv(f"{CSV_PATH}/offers.csv")
df_bz_offers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.bz_offers")

print(f"bz_offers: {spark.table(f'{catalog}.{schema}.bz_offers').count():,} 件")
spark.table(f"{catalog}.{schema}.bz_offers").display()

# COMMAND ----------

# DBTITLE 1,bz_offers 主キー・コメント設定
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_offers ALTER COLUMN offer_id SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_offers DROP CONSTRAINT IF EXISTS pk_offers")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_offers ADD CONSTRAINT pk_offers PRIMARY KEY (offer_id)")

spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.bz_offers IS
'キャンペーン・割引オファーマスタ。同一キャンペーンが複数チャネル（Web広告/LINE/DM/店舗チラシ/アプリ）に展開され、チャネルごとに割引率が異なる場合がある。'
""")

offer_comments = {
    "offer_id": "オファーID（主キー）OFR-001形式",
    "offer_name": "キャンペーン名",
    "channel": "配信チャネル（Web広告/LINE/DM/店舗チラシ/アプリ）",
    "discount_rate": "割引率（0.05〜0.30）",
    "start_date": "キャンペーン開始日",
    "end_date": "キャンペーン終了日",
    "target_segment": "対象セグメント（新規/リピート/VIP/全員）",
    "min_purchase_amount": "最低購入金額",
    "description": "キャンペーン説明",
}
for col, comment in offer_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_offers ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# DBTITLE 1,bz_campaign_exposures テーブル作成
df_bz_exposures = spark.read.option("header", "true").schema(exposures_schema).csv(f"{CSV_PATH}/campaign_exposures.csv")
df_bz_exposures.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.bz_campaign_exposures")

print(f"bz_campaign_exposures: {spark.table(f'{catalog}.{schema}.bz_campaign_exposures').count():,} 件")
spark.table(f"{catalog}.{schema}.bz_campaign_exposures").display()

# COMMAND ----------

# DBTITLE 1,bz_campaign_exposures 主キー・コメント設定
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_campaign_exposures ALTER COLUMN exposure_id SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_campaign_exposures DROP CONSTRAINT IF EXISTS pk_exposures")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_campaign_exposures ADD CONSTRAINT pk_exposures PRIMARY KEY (exposure_id)")

spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.bz_campaign_exposures IS
'キャンペーン接触ログ。顧客が各チャネルのキャンペーンに接触した記録。クリック・コンバージョン（購入転換）のフラグを含む。150,000件。'
""")

exposure_comments = {
    "exposure_id": "接触ID（主キー）EXP-000001形式",
    "customer_id": "顧客ID（FK → customers）",
    "offer_id": "オファーID（FK → offers）",
    "exposure_date": "接触日",
    "channel": "接触チャネル（Web広告/LINE/DM/店舗チラシ/アプリ）",
    "clicked": "クリックフラグ",
    "converted": "コンバージョンフラグ（購入に至ったか）",
}
for col, comment in exposure_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_campaign_exposures ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# DBTITLE 1,bz_orders テーブル作成
df_bz_orders = spark.read.option("header", "true").schema(orders_schema).csv(f"{CSV_PATH}/orders.csv")
df_bz_orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.bz_orders")

print(f"bz_orders: {spark.table(f'{catalog}.{schema}.bz_orders').count():,} 件")
spark.table(f"{catalog}.{schema}.bz_orders").display()

# COMMAND ----------

# DBTITLE 1,bz_orders 主キー・コメント設定
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_orders ALTER COLUMN order_id SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_orders DROP CONSTRAINT IF EXISTS pk_orders")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_orders ADD CONSTRAINT pk_orders PRIMARY KEY (order_id)")

spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.bz_orders IS
'注文テーブル。50,000件の注文データ。商品カテゴリ（スーツ/シャツ/ネクタイ/ベルト/シューズ/コート）、割引適用率、返品フラグ・理由を含む。LINEチャネル経由の顧客は割引条件の不一致による返品率が高い傾向がある。'
""")

order_comments = {
    "order_id": "注文ID（主キー）ORD-000001形式",
    "customer_id": "顧客ID（FK → customers）",
    "order_date": "注文日",
    "product_category": "商品カテゴリ（スーツ/シャツ/ネクタイ/ベルト/シューズ/コート）",
    "product_name": "商品名",
    "size": "サイズ（S/M/L/LL/3L）",
    "quantity": "数量",
    "unit_price": "単価（税込）",
    "discount_applied": "適用割引率",
    "total_amount": "合計金額（税込・割引後）",
    "order_channel": "注文チャネル（EC/店舗）",
    "returned": "返品フラグ",
    "return_reason": "返品理由（サイズ不一致/イメージ違い/割引条件違い/不良品/その他）",
}
for col, comment in order_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_orders ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: データ整合性チェック

# COMMAND ----------

# DBTITLE 1,件数確認
for table in ["bz_customers", "bz_offers", "bz_campaign_exposures", "bz_orders"]:
    cnt = spark.table(f"{catalog}.{schema}.{table}").count()
    print(f"{table}: {cnt:,} 件")

# COMMAND ----------

# DBTITLE 1,FK整合性チェック：orphan customer_id がないことを確認
orphan_exposures = spark.sql(f"""
SELECT COUNT(*) AS orphan_count
FROM {catalog}.{schema}.bz_campaign_exposures e
LEFT JOIN {catalog}.{schema}.bz_customers c ON e.customer_id = c.customer_id
WHERE c.customer_id IS NULL
""")
orphan_orders = spark.sql(f"""
SELECT COUNT(*) AS orphan_count
FROM {catalog}.{schema}.bz_orders o
LEFT JOIN {catalog}.{schema}.bz_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
""")
print("orphan customer_id in exposures:", orphan_exposures.collect()[0][0])
print("orphan customer_id in orders:", orphan_orders.collect()[0][0])

# COMMAND ----------

# DBTITLE 1,返品率のチャネル別分布確認（デモ用データの偏りが意図通りか）
spark.sql(f"""
WITH customer_primary_channel AS (
    SELECT customer_id, channel,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
    FROM {catalog}.{schema}.bz_campaign_exposures
    GROUP BY customer_id, channel
)
SELECT
    COALESCE(cpc.channel, '直接') AS primary_channel,
    COUNT(*) AS order_count,
    SUM(CASE WHEN o.returned THEN 1 ELSE 0 END) AS return_count,
    ROUND(AVG(CASE WHEN o.returned THEN 1.0 ELSE 0.0 END), 4) AS return_rate,
    -- 返品理由の内訳
    SUM(CASE WHEN o.return_reason = '割引条件違い' THEN 1 ELSE 0 END) AS discount_trouble_returns
FROM {catalog}.{schema}.bz_orders o
LEFT JOIN customer_primary_channel cpc
    ON o.customer_id = cpc.customer_id AND cpc.rn = 1
GROUP BY COALESCE(cpc.channel, '直接')
ORDER BY return_rate DESC
""").display()
