# Databricks notebook source
# MAGIC %md
# MAGIC # サンプルデータ作成
# MAGIC
# MAGIC 商圏分析デモ用のサンプルデータを生成します。
# MAGIC
# MAGIC ## 作成するテーブル
# MAGIC | テーブル名 | 説明 | 件数目安 |
# MAGIC |-----------|------|---------|
# MAGIC | `stores` | 店舗マスタ | 50店舗 |
# MAGIC | `sales_monthly` | 月別売上 | 50店舗 × 36ヶ月 |
# MAGIC | `trade_area` | 商圏情報（人口統計） | 50店舗 |
# MAGIC | `trade_area_expenditure` | 商圏消費支出（ゼンリン形式） | 50店舗 |
# MAGIC | `competitors` | 競合店舗 | 約200件 |
# MAGIC | `categories` | カテゴリマスタ | 10カテゴリ |
# MAGIC | `sales_by_category` | カテゴリ別売上 | 50店舗 × 10カテゴリ × 36ヶ月 |
# MAGIC | `similar_stores` | 類似店舗マスタ | 50店舗 × 5類似店舗 |
# MAGIC | `store_measures` | 施策管理 | 約80件 |
# MAGIC | `nearby_facilities` | 近隣施設 | 50店舗 × 約10施設 |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, rand, floor, ceil, date_add, to_date, add_months,
    concat, lpad, expr, when, round as spark_round, array, element_at,
    monotonically_increasing_id, explode, sequence, abs as spark_abs
)
from pyspark.sql.types import *
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 店舗マスタ（stores）

# COMMAND ----------

# 地域データ
regions = [
    ("北海道", "札幌市", 43.06, 141.35),
    ("北海道", "旭川市", 43.77, 142.37),
    ("北海道", "函館市", 41.77, 140.73),
    ("宮城県", "仙台市", 38.27, 140.87),
    ("宮城県", "石巻市", 38.43, 141.30),
    ("福島県", "郡山市", 37.40, 140.38),
    ("茨城県", "水戸市", 36.34, 140.45),
    ("茨城県", "つくば市", 36.08, 140.11),
    ("埼玉県", "さいたま市", 35.86, 139.65),
    ("埼玉県", "川越市", 35.93, 139.49),
    ("埼玉県", "越谷市", 35.89, 139.79),
    ("埼玉県", "熊谷市", 36.15, 139.39),
    ("千葉県", "千葉市", 35.61, 140.11),
    ("千葉県", "船橋市", 35.69, 139.98),
    ("千葉県", "柏市", 35.87, 139.97),
    ("東京都", "八王子市", 35.67, 139.32),
    ("東京都", "町田市", 35.55, 139.45),
    ("東京都", "立川市", 35.71, 139.41),
    ("神奈川県", "横浜市", 35.44, 139.64),
    ("神奈川県", "相模原市", 35.57, 139.37),
    ("神奈川県", "藤沢市", 35.34, 139.49),
    ("新潟県", "新潟市", 37.90, 139.02),
    ("新潟県", "長岡市", 37.45, 138.85),
    ("長野県", "長野市", 36.65, 138.19),
    ("長野県", "松本市", 36.24, 137.97),
    ("静岡県", "静岡市", 34.98, 138.38),
    ("静岡県", "浜松市", 34.71, 137.73),
    ("愛知県", "名古屋市", 35.18, 136.91),
    ("愛知県", "豊田市", 35.08, 137.16),
    ("愛知県", "岡崎市", 34.95, 137.17),
    ("岐阜県", "岐阜市", 35.42, 136.76),
    ("三重県", "四日市市", 34.97, 136.62),
    ("京都府", "京都市", 35.01, 135.77),
    ("大阪府", "大阪市", 34.69, 135.50),
    ("大阪府", "堺市", 34.57, 135.48),
    ("大阪府", "東大阪市", 34.68, 135.60),
    ("兵庫県", "神戸市", 34.69, 135.20),
    ("兵庫県", "姫路市", 34.83, 134.69),
    ("兵庫県", "西宮市", 34.74, 135.34),
    ("奈良県", "奈良市", 34.69, 135.80),
    ("岡山県", "岡山市", 34.66, 133.92),
    ("広島県", "広島市", 34.40, 132.46),
    ("広島県", "福山市", 34.49, 133.36),
    ("福岡県", "福岡市", 33.59, 130.40),
    ("福岡県", "北九州市", 33.88, 130.88),
    ("福岡県", "久留米市", 33.32, 130.51),
    ("熊本県", "熊本市", 32.79, 130.74),
    ("鹿児島県", "鹿児島市", 31.60, 130.56),
    ("沖縄県", "那覇市", 26.21, 127.68),
    ("沖縄県", "浦添市", 26.25, 127.72),
]

# 店舗マスタ作成
stores_data = []
for i, (pref, city, lat, lon) in enumerate(regions):
    store_id = f"S{str(i+1).zfill(3)}"
    store_name = f"店舗_{city}"
    size_sqm = random.randint(10, 50) * 100
    open_year = random.randint(2010, 2023)
    open_month = random.randint(1, 12)
    open_date = f"{open_year}-{str(open_month).zfill(2)}-01"
    store_type = random.choice(["郊外型", "都市型", "ロードサイド型"])
    parking = size_sqm // 10 + random.randint(-20, 50)

    stores_data.append((store_id, store_name, pref, city, lat + random.uniform(-0.05, 0.05),
                        lon + random.uniform(-0.05, 0.05), size_sqm, open_date, store_type, max(0, parking)))

stores_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), False),
    StructField("prefecture", StringType(), False),
    StructField("city", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("size_sqm", IntegerType(), False),
    StructField("open_date", StringType(), False),
    StructField("store_type", StringType(), False),
    StructField("parking_capacity", IntegerType(), False),
])

df_stores = spark.createDataFrame(stores_data, stores_schema)
df_stores = df_stores.withColumn("open_date", to_date(col("open_date")))

df_stores.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("stores")
print(f"stores テーブル作成完了: {df_stores.count()} 件")

# COMMAND ----------

display(spark.table("stores"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 商圏情報 - 人口統計（trade_area）

# COMMAND ----------

df_stores_pd = df_stores.toPandas()

trade_area_data = []
for _, row in df_stores_pd.iterrows():
    store_id = row['store_id']
    is_urban = row['store_type'] == "都市型"

    population_5km = random.randint(30000, 300000) if is_urban else random.randint(20000, 150000)
    households = int(population_5km / random.uniform(2.2, 2.5))
    avg_income = random.randint(350, 650) * 10000
    elderly_rate = random.uniform(0.15, 0.40)
    young_adult_rate = random.uniform(0.15, 0.35)
    detached_house_rate = random.uniform(0.30, 0.80) if not is_urban else random.uniform(0.15, 0.50)
    day_night_ratio = random.uniform(0.9, 1.5) if is_urban else random.uniform(0.7, 1.1)
    num_businesses = random.randint(500, 5000) if is_urban else random.randint(100, 1500)

    trade_area_data.append((
        store_id, population_5km, households, avg_income,
        round(elderly_rate, 3), round(young_adult_rate, 3),
        round(detached_house_rate, 3), round(day_night_ratio, 2), num_businesses
    ))

trade_area_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("population_5km", IntegerType(), False),
    StructField("households", IntegerType(), False),
    StructField("avg_income", IntegerType(), False),
    StructField("elderly_rate", DoubleType(), False),
    StructField("young_adult_rate", DoubleType(), False),
    StructField("detached_house_rate", DoubleType(), False),
    StructField("day_night_ratio", DoubleType(), False),
    StructField("num_businesses", IntegerType(), False),
])

df_trade_area = spark.createDataFrame(trade_area_data, trade_area_schema)
df_trade_area.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("trade_area")
print(f"trade_area テーブル作成完了: {df_trade_area.count()} 件")

# COMMAND ----------

display(spark.table("trade_area"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 商圏消費支出（trade_area_expenditure）
# MAGIC
# MAGIC ゼンリン「消費支出データ」形式の商圏消費ポテンシャル。
# MAGIC 単位は千円/年/世帯あたり。
# MAGIC
# MAGIC **参考**: [ゼンリン 消費支出データ項目一覧（2023年版）](https://www.giken.co.jp/static/2025/02/expenditure2023.pdf)

# COMMAND ----------

# ゼンリン消費支出データ形式
# DCM（ホームセンター）に関連するカテゴリを抽出

expenditure_data = []

for _, row in df_stores_pd.iterrows():
    store_id = row['store_id']
    is_urban = row['store_type'] == "都市型"

    # 基準値（全国平均的な値）に地域変動を加える
    urban_factor = 1.1 if is_urban else 0.95
    region_factor = random.uniform(0.85, 1.15)

    # === 消費支出（千円/年/世帯） ===

    # 消費支出合計
    total_expenditure = int(3200 * urban_factor * region_factor)

    # --- 家具・家事用品（ホームセンター主力） ---
    furniture_household_total = int(120 * urban_factor * region_factor * random.uniform(0.9, 1.1))
    # 家庭用耐久財
    household_durables = int(45 * urban_factor * region_factor * random.uniform(0.8, 1.2))
    # 家事雑貨（食器、鍋、タオル等）
    household_miscellaneous = int(25 * urban_factor * region_factor * random.uniform(0.9, 1.1))
    # 家事用消耗品（洗剤、ティッシュ等）
    household_consumables = int(35 * urban_factor * region_factor * random.uniform(0.9, 1.1))

    # --- 住居関連（DIY・リフォーム） ---
    housing_repair_total = int(85 * region_factor * random.uniform(0.7, 1.3))
    # 設備材料（設備器具、修繕材料）
    equipment_materials = int(15 * region_factor * random.uniform(0.6, 1.4))
    # 工事その他サービス（畳替え、給排水、外壁等）
    construction_services = int(55 * region_factor * random.uniform(0.5, 1.5))
    # 植木・庭手入れ代
    garden_maintenance = int(8 * region_factor * random.uniform(0.5, 1.5))

    # --- 教養娯楽（園芸・ペット） ---
    # 園芸用植物
    garden_plants = int(4 * region_factor * random.uniform(0.5, 1.8))
    # 園芸用品
    garden_supplies = int(3 * region_factor * random.uniform(0.5, 1.8))
    # ペットフード
    pet_food = int(8 * region_factor * random.uniform(0.6, 1.4))
    # ペット・ペット用品
    pet_supplies = int(5 * region_factor * random.uniform(0.6, 1.4))

    # --- 光熱・水道 ---
    # 灯油
    kerosene = int(12 * region_factor * random.uniform(0.3, 1.7))  # 地域差大

    # --- その他関連 ---
    # 自動車等関連用品
    auto_supplies = int(8 * region_factor * random.uniform(0.7, 1.3))

    # 商圏の世帯数を取得（trade_areaと結合用）
    households = int(row['size_sqm'] * random.uniform(50, 150))  # 仮の世帯数

    expenditure_data.append((
        store_id,
        total_expenditure,
        furniture_household_total,
        household_durables,
        household_miscellaneous,
        household_consumables,
        housing_repair_total,
        equipment_materials,
        construction_services,
        garden_maintenance,
        garden_plants,
        garden_supplies,
        pet_food,
        pet_supplies,
        kerosene,
        auto_supplies
    ))

expenditure_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("expenditure_total", IntegerType(), False),           # 消費支出合計
    StructField("furniture_household_total", IntegerType(), False),   # 家具・家事用品合計
    StructField("household_durables", IntegerType(), False),          # 家庭用耐久財
    StructField("household_miscellaneous", IntegerType(), False),     # 家事雑貨
    StructField("household_consumables", IntegerType(), False),       # 家事用消耗品
    StructField("housing_repair_total", IntegerType(), False),        # 住居_設備修繕・維持合計
    StructField("equipment_materials", IntegerType(), False),         # 設備材料
    StructField("construction_services", IntegerType(), False),       # 工事その他サービス
    StructField("garden_maintenance", IntegerType(), False),          # 植木・庭手入れ代
    StructField("garden_plants", IntegerType(), False),               # 園芸用植物
    StructField("garden_supplies", IntegerType(), False),             # 園芸用品
    StructField("pet_food", IntegerType(), False),                    # ペットフード
    StructField("pet_supplies", IntegerType(), False),                # ペット用品
    StructField("kerosene", IntegerType(), False),                    # 灯油
    StructField("auto_supplies", IntegerType(), False),               # 自動車等関連用品
])

df_expenditure = spark.createDataFrame(expenditure_data, expenditure_schema)
df_expenditure.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("trade_area_expenditure")
print(f"trade_area_expenditure テーブル作成完了: {df_expenditure.count()} 件")

# COMMAND ----------

display(spark.table("trade_area_expenditure"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 競合店舗（competitors）

# COMMAND ----------

competitor_types = ["カインズ", "コメリ", "ジョイフル本田", "ビバホーム", "コーナン", "ナフコ", "ケーヨーD2", "島忠", "ロイヤルホームセンター"]

competitors_data = []
comp_id = 1

for _, row in df_stores_pd.iterrows():
    store_id = row['store_id']
    num_competitors = random.randint(1, 6)

    for _ in range(num_competitors):
        competitor_name = random.choice(competitor_types)
        distance_km = round(random.uniform(0.5, 10.0), 1)
        competitor_size = random.randint(10, 80) * 100
        open_year = random.randint(2000, 2025)
        is_new = open_year >= 2024

        competitors_data.append((
            f"C{str(comp_id).zfill(4)}", store_id, competitor_name,
            distance_km, competitor_size, open_year, is_new
        ))
        comp_id += 1

competitors_schema = StructType([
    StructField("competitor_id", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("competitor_name", StringType(), False),
    StructField("distance_km", DoubleType(), False),
    StructField("size_sqm", IntegerType(), False),
    StructField("open_year", IntegerType(), False),
    StructField("is_new_entry", BooleanType(), False),
])

df_competitors = spark.createDataFrame(competitors_data, competitors_schema)
df_competitors.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("competitors")
print(f"competitors テーブル作成完了: {df_competitors.count()} 件")

# COMMAND ----------

display(spark.table("competitors"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. カテゴリマスタ（categories）
# MAGIC
# MAGIC ゼンリン消費支出データとの対応付け

# COMMAND ----------

# ホームセンターの主要カテゴリ（消費支出データとの対応）
categories_data = [
    ("CAT01", "園芸用品", 0.15, "garden_plants + garden_supplies", "季節変動大、春夏に需要増"),
    ("CAT02", "DIY工具", 0.12, "equipment_materials", "プロ・DIY層向け、安定需要"),
    ("CAT03", "塗料・接着剤", 0.08, "housing_repair_total", "リフォーム需要連動"),
    ("CAT04", "木材・建材", 0.10, "construction_services", "プロ向け比率高い"),
    ("CAT05", "金物・作業用品", 0.08, "equipment_materials", "安定需要"),
    ("CAT06", "電材・照明", 0.09, "household_durables", "LED化需要"),
    ("CAT07", "水道・配管用品", 0.07, "construction_services", "修繕需要"),
    ("CAT08", "収納・インテリア", 0.11, "furniture_household_total", "生活密着型"),
    ("CAT09", "日用品・清掃用品", 0.12, "household_consumables", "リピート購入多い"),
    ("CAT10", "ペット用品", 0.08, "pet_food + pet_supplies", "成長カテゴリ"),
]

categories_schema = StructType([
    StructField("category_id", StringType(), False),
    StructField("category_name", StringType(), False),
    StructField("potential_coefficient", DoubleType(), False),
    StructField("expenditure_mapping", StringType(), False),
    StructField("description", StringType(), False),
])

df_categories = spark.createDataFrame(categories_data, categories_schema)
df_categories.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("categories")
print(f"categories テーブル作成完了: {df_categories.count()} 件")

# COMMAND ----------

display(spark.table("categories"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 月別売上（sales_monthly）

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta

months = []
start_date = datetime(2022, 1, 1)
for i in range(36):
    months.append((start_date + relativedelta(months=i)).strftime("%Y-%m-01"))

sales_data = []

for _, store_row in df_stores_pd.iterrows():
    store_id = store_row['store_id']
    size_sqm = store_row['size_sqm']

    tsubo = size_sqm / 3.3
    base_monthly_sales = tsubo * random.randint(30000, 80000)
    annual_trend = random.uniform(-0.15, 0.10)

    is_struggling = random.random() < 0.20
    if is_struggling:
        annual_trend = random.uniform(-0.20, -0.08)

    for month_idx, month in enumerate(months):
        trend_factor = 1 + (annual_trend * month_idx / 12)
        month_num = int(month.split("-")[1])

        if month_num in [3, 4, 5]:
            seasonal_factor = random.uniform(1.10, 1.20)
        elif month_num in [6, 7, 8]:
            seasonal_factor = random.uniform(1.05, 1.15)
        elif month_num in [11, 12, 1, 2]:
            seasonal_factor = random.uniform(0.85, 0.95)
        else:
            seasonal_factor = random.uniform(0.95, 1.05)

        random_factor = random.uniform(0.90, 1.10)
        sales_amount = int(base_monthly_sales * trend_factor * seasonal_factor * random_factor)
        avg_basket = random.randint(2000, 4000)
        customer_count = int(sales_amount / avg_basket)

        if month_idx >= 12:
            yoy_change = trend_factor / (1 + annual_trend * (month_idx - 12) / 12) - 1
        else:
            yoy_change = 0.0

        sales_data.append((store_id, month, sales_amount, customer_count, avg_basket, round(yoy_change, 3)))

sales_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("month", StringType(), False),
    StructField("sales_amount", IntegerType(), False),
    StructField("customer_count", IntegerType(), False),
    StructField("avg_basket", IntegerType(), False),
    StructField("yoy_change", DoubleType(), False),
])

df_sales = spark.createDataFrame(sales_data, sales_schema)
df_sales = df_sales.withColumn("month", to_date(col("month")))

df_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_monthly")
print(f"sales_monthly テーブル作成完了: {df_sales.count()} 件")

# COMMAND ----------

display(spark.table("sales_monthly").orderBy("store_id", "month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. カテゴリ別売上（sales_by_category）

# COMMAND ----------

sales_by_cat_data = []
df_sales_pd = df_sales.toPandas()
categories_list = [c[0] for c in categories_data]
category_weights = [c[2] for c in categories_data]

for _, sales_row in df_sales_pd.iterrows():
    store_id = sales_row['store_id']
    month = sales_row['month'].strftime("%Y-%m-%d")
    total_sales = sales_row['sales_amount']

    store_hash = hash(store_id) % 100
    adjusted_weights = []
    for i, w in enumerate(category_weights):
        adjustment = 1 + ((store_hash + i * 10) % 60 - 30) / 100
        adjusted_weights.append(w * adjustment)

    total_weight = sum(adjusted_weights)
    normalized_weights = [w / total_weight for w in adjusted_weights]

    for cat_id, weight in zip(categories_list, normalized_weights):
        cat_sales = int(total_sales * weight * random.uniform(0.9, 1.1))
        sales_by_cat_data.append((store_id, cat_id, month, cat_sales))

sales_by_cat_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("category_id", StringType(), False),
    StructField("month", StringType(), False),
    StructField("sales_amount", IntegerType(), False),
])

df_sales_by_cat = spark.createDataFrame(sales_by_cat_data, sales_by_cat_schema)
df_sales_by_cat = df_sales_by_cat.withColumn("month", to_date(col("month")))

df_sales_by_cat.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_by_category")
print(f"sales_by_category テーブル作成完了: {df_sales_by_cat.count()} 件")

# COMMAND ----------

display(spark.table("sales_by_category").orderBy("store_id", "category_id", "month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. データ確認

# COMMAND ----------

tables = ["stores", "trade_area", "trade_area_expenditure", "competitors", "categories", "sales_monthly", "sales_by_category"]

print("=" * 60)
print("作成したテーブル一覧")
print("=" * 60)

for table in tables:
    count = spark.table(table).count()
    print(f"{table:30} : {count:,} 件")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. 商圏消費ポテンシャルと売上の関係確認

# COMMAND ----------

# 商圏消費支出データと売上の相関を確認
spark.sql("""
    SELECT
        s.store_id,
        s.store_name,
        s.store_type,
        e.furniture_household_total as `家具家事用品_千円`,
        e.housing_repair_total as `住居修繕_千円`,
        e.garden_plants + e.garden_supplies as `園芸_千円`,
        e.pet_food + e.pet_supplies as `ペット_千円`,
        ROUND(AVG(sm.sales_amount) / 10000, 0) as `月平均売上_万円`,
        ROUND(AVG(sm.yoy_change) * 100, 1) as `平均前年比_pct`
    FROM stores s
    JOIN trade_area_expenditure e ON s.store_id = e.store_id
    JOIN sales_monthly sm ON s.store_id = sm.store_id
    WHERE sm.month >= '2024-01-01'
    GROUP BY s.store_id, s.store_name, s.store_type,
             e.furniture_household_total, e.housing_repair_total,
             e.garden_plants, e.garden_supplies, e.pet_food, e.pet_supplies
    ORDER BY `平均前年比_pct`
    LIMIT 15
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # メタデータ付与
# MAGIC
# MAGIC テーブル説明、カラムコメント、PK/FK制約を追加します。
# MAGIC Genie Codeがデータを理解しやすくなります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. テーブルコメント

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE stores IS '店舗マスタ。全店舗の基本情報（所在地、売場面積、開店日など）を管理';
# MAGIC COMMENT ON TABLE trade_area IS '商圏情報（人口統計）。各店舗の商圏5km圏内の人口・世帯・年収などの統計データ';
# MAGIC COMMENT ON TABLE trade_area_expenditure IS '商圏消費支出データ（ゼンリン形式）。商圏内世帯の年間消費支出を品目別に集計。単位は千円/年/世帯';
# MAGIC COMMENT ON TABLE competitors IS '競合店舗。各店舗周辺の競合ホームセンター情報（距離、売場面積、出店年など）';
# MAGIC COMMENT ON TABLE categories IS 'カテゴリマスタ。ホームセンターの商品カテゴリと消費支出データとの対応関係を定義';
# MAGIC COMMENT ON TABLE sales_monthly IS '月別売上。店舗ごとの月次売上実績（売上金額、客数、客単価、前年比）';
# MAGIC COMMENT ON TABLE sales_by_category IS 'カテゴリ別売上。店舗×カテゴリ×月ごとの売上金額';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. カラムコメント

# COMMAND ----------

# MAGIC %sql
# MAGIC -- stores
# MAGIC ALTER TABLE stores ALTER COLUMN store_id COMMENT '店舗ID（主キー）。形式: S001, S002, ...';
# MAGIC ALTER TABLE stores ALTER COLUMN store_name COMMENT '店舗名';
# MAGIC ALTER TABLE stores ALTER COLUMN prefecture COMMENT '都道府県';
# MAGIC ALTER TABLE stores ALTER COLUMN city COMMENT '市区町村';
# MAGIC ALTER TABLE stores ALTER COLUMN latitude COMMENT '緯度';
# MAGIC ALTER TABLE stores ALTER COLUMN longitude COMMENT '経度';
# MAGIC ALTER TABLE stores ALTER COLUMN size_sqm COMMENT '売場面積（平方メートル）';
# MAGIC ALTER TABLE stores ALTER COLUMN open_date COMMENT '開店日';
# MAGIC ALTER TABLE stores ALTER COLUMN store_type COMMENT '店舗タイプ（郊外型/都市型/ロードサイド型）';
# MAGIC ALTER TABLE stores ALTER COLUMN parking_capacity COMMENT '駐車場収容台数';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- trade_area
# MAGIC ALTER TABLE trade_area ALTER COLUMN store_id COMMENT '店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE trade_area ALTER COLUMN population_5km COMMENT '商圏人口（5km圏内）';
# MAGIC ALTER TABLE trade_area ALTER COLUMN households COMMENT '世帯数';
# MAGIC ALTER TABLE trade_area ALTER COLUMN avg_income COMMENT '平均世帯年収（円）';
# MAGIC ALTER TABLE trade_area ALTER COLUMN elderly_rate COMMENT '高齢化率（65歳以上人口比率）。0.0〜1.0';
# MAGIC ALTER TABLE trade_area ALTER COLUMN young_adult_rate COMMENT '若年層比率（20-39歳人口比率）。0.0〜1.0';
# MAGIC ALTER TABLE trade_area ALTER COLUMN detached_house_rate COMMENT '戸建て比率。0.0〜1.0';
# MAGIC ALTER TABLE trade_area ALTER COLUMN day_night_ratio COMMENT '昼夜間人口比率。1.0以上は昼間人口が多い';
# MAGIC ALTER TABLE trade_area ALTER COLUMN num_businesses COMMENT '商圏内事業所数';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- trade_area_expenditure
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN store_id COMMENT '店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN expenditure_total COMMENT '消費支出合計（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN furniture_household_total COMMENT '家具・家事用品合計（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN household_durables COMMENT '家庭用耐久財（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN household_miscellaneous COMMENT '家事雑貨（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN household_consumables COMMENT '家事用消耗品（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN housing_repair_total COMMENT '住居_設備修繕・維持合計（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN equipment_materials COMMENT '設備材料（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN construction_services COMMENT '工事その他サービス（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN garden_maintenance COMMENT '植木・庭手入れ代（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN garden_plants COMMENT '園芸用植物（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN garden_supplies COMMENT '園芸用品（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN pet_food COMMENT 'ペットフード（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN pet_supplies COMMENT 'ペット用品（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN kerosene COMMENT '灯油（千円/年/世帯）';
# MAGIC ALTER TABLE trade_area_expenditure ALTER COLUMN auto_supplies COMMENT '自動車等関連用品（千円/年/世帯）';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- competitors
# MAGIC ALTER TABLE competitors ALTER COLUMN competitor_id COMMENT '競合店舗ID（主キー）。形式: C0001, C0002, ...';
# MAGIC ALTER TABLE competitors ALTER COLUMN store_id COMMENT '店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE competitors ALTER COLUMN competitor_name COMMENT '競合店舗名（チェーン名）';
# MAGIC ALTER TABLE competitors ALTER COLUMN distance_km COMMENT '自店舗からの距離（km）';
# MAGIC ALTER TABLE competitors ALTER COLUMN size_sqm COMMENT '競合店舗の売場面積（平方メートル）';
# MAGIC ALTER TABLE competitors ALTER COLUMN open_year COMMENT '競合店舗の出店年';
# MAGIC ALTER TABLE competitors ALTER COLUMN is_new_entry COMMENT '新規出店フラグ。true=2024年以降の出店';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- categories
# MAGIC ALTER TABLE categories ALTER COLUMN category_id COMMENT 'カテゴリID（主キー）。形式: CAT01, CAT02, ...';
# MAGIC ALTER TABLE categories ALTER COLUMN category_name COMMENT 'カテゴリ名（園芸用品、DIY工具など）';
# MAGIC ALTER TABLE categories ALTER COLUMN potential_coefficient COMMENT 'ポテンシャル係数';
# MAGIC ALTER TABLE categories ALTER COLUMN expenditure_mapping COMMENT '消費支出マッピング';
# MAGIC ALTER TABLE categories ALTER COLUMN description COMMENT 'カテゴリの説明・特性';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sales_monthly
# MAGIC ALTER TABLE sales_monthly ALTER COLUMN store_id COMMENT '店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE sales_monthly ALTER COLUMN month COMMENT '対象月（月初日）';
# MAGIC ALTER TABLE sales_monthly ALTER COLUMN sales_amount COMMENT '売上金額（円）';
# MAGIC ALTER TABLE sales_monthly ALTER COLUMN customer_count COMMENT '来店客数';
# MAGIC ALTER TABLE sales_monthly ALTER COLUMN avg_basket COMMENT '客単価（円）';
# MAGIC ALTER TABLE sales_monthly ALTER COLUMN yoy_change COMMENT '前年同月比。0.1 = +10%, -0.1 = -10%';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sales_by_category
# MAGIC ALTER TABLE sales_by_category ALTER COLUMN store_id COMMENT '店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE sales_by_category ALTER COLUMN category_id COMMENT 'カテゴリID（外部キー → categories.category_id）';
# MAGIC ALTER TABLE sales_by_category ALTER COLUMN month COMMENT '対象月（月初日）';
# MAGIC ALTER TABLE sales_by_category ALTER COLUMN sales_amount COMMENT 'カテゴリ別売上金額（円）';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. 主キー・外部キー制約

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 主キーカラムをNOT NULLに設定
# MAGIC ALTER TABLE stores ALTER COLUMN store_id SET NOT NULL;
# MAGIC ALTER TABLE competitors ALTER COLUMN competitor_id SET NOT NULL;
# MAGIC ALTER TABLE categories ALTER COLUMN category_id SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 主キー制約
# MAGIC ALTER TABLE stores ADD CONSTRAINT pk_stores PRIMARY KEY (store_id);
# MAGIC ALTER TABLE competitors ADD CONSTRAINT pk_competitors PRIMARY KEY (competitor_id);
# MAGIC ALTER TABLE categories ADD CONSTRAINT pk_categories PRIMARY KEY (category_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 外部キー制約
# MAGIC ALTER TABLE trade_area ADD CONSTRAINT fk_trade_area_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE trade_area_expenditure ADD CONSTRAINT fk_expenditure_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE competitors ADD CONSTRAINT fk_competitors_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE sales_monthly ADD CONSTRAINT fk_sales_monthly_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE sales_by_category ADD CONSTRAINT fk_sales_by_category_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE sales_by_category ADD CONSTRAINT fk_sales_by_category_category FOREIGN KEY (category_id) REFERENCES categories(category_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. メタデータ確認

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED stores;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. 類似店舗マスタ（similar_stores）
# MAGIC
# MAGIC 各店舗に対して、商圏特性・店舗規模・業績傾向が類似した店舗を定義。
# MAGIC 施策効果の比較分析に使用。

# COMMAND ----------

# 店舗の特徴量を計算
df_store_features = spark.sql("""
    SELECT
        s.store_id,
        s.store_type,
        s.size_sqm,
        s.prefecture,
        ta.population_5km,
        ta.avg_income,
        ta.elderly_rate,
        ta.detached_house_rate,
        AVG(sm.sales_amount) as avg_monthly_sales,
        AVG(sm.yoy_change) as avg_yoy_change
    FROM stores s
    JOIN trade_area ta ON s.store_id = ta.store_id
    JOIN sales_monthly sm ON s.store_id = sm.store_id
    WHERE sm.month >= '2024-01-01'
    GROUP BY s.store_id, s.store_type, s.size_sqm, s.prefecture,
             ta.population_5km, ta.avg_income, ta.elderly_rate, ta.detached_house_rate
""")

store_features_pd = df_store_features.toPandas()

# 類似店舗を計算（特徴量の類似度に基づく）
similar_stores_data = []

for idx, row in store_features_pd.iterrows():
    store_id = row['store_id']

    # 各店舗との類似度スコアを計算
    similarities = []
    for idx2, row2 in store_features_pd.iterrows():
        if row['store_id'] == row2['store_id']:
            continue

        # 類似度計算（各特徴量の差分を正規化して合計）
        size_sim = 1 - abs(row['size_sqm'] - row2['size_sqm']) / 5000
        pop_sim = 1 - abs(row['population_5km'] - row2['population_5km']) / 300000
        income_sim = 1 - abs(row['avg_income'] - row2['avg_income']) / 3000000
        elderly_sim = 1 - abs(row['elderly_rate'] - row2['elderly_rate'])
        type_sim = 1.0 if row['store_type'] == row2['store_type'] else 0.5

        # 総合類似度スコア（0-100）
        similarity_score = (size_sim * 0.25 + pop_sim * 0.25 + income_sim * 0.2 +
                          elderly_sim * 0.15 + type_sim * 0.15) * 100

        similarities.append((row2['store_id'], similarity_score))

    # 上位5店舗を類似店舗として選択
    similarities.sort(key=lambda x: x[1], reverse=True)
    for rank, (similar_id, score) in enumerate(similarities[:5], 1):
        similar_stores_data.append((
            store_id,
            similar_id,
            rank,
            round(score, 1)
        ))

similar_stores_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("similar_store_id", StringType(), False),
    StructField("similarity_rank", IntegerType(), False),
    StructField("similarity_score", DoubleType(), False),
])

df_similar_stores = spark.createDataFrame(similar_stores_data, similar_stores_schema)
df_similar_stores.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("similar_stores")
print(f"similar_stores テーブル作成完了: {df_similar_stores.count()} 件")

# COMMAND ----------

display(spark.table("similar_stores").orderBy("store_id", "similarity_rank"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. 施策管理（store_measures）
# MAGIC
# MAGIC 各店舗に実施した施策（リニューアル、棚割り変更、価格施策など）を管理。
# MAGIC 施策前後の効果比較に使用。

# COMMAND ----------

from datetime import datetime, timedelta

# 施策タイプ定義
measure_types = [
    ("リニューアル", "大規模改装・店舗リニューアル", 0.15),
    ("棚割り最適化", "商品配置・棚割りの見直し", 0.08),
    ("品揃え強化", "特定カテゴリの品揃え拡充", 0.10),
    ("価格施策", "競合対抗価格・ポイント還元", 0.05),
    ("販促強化", "チラシ・デジタル広告の強化", 0.06),
    ("接客改善", "スタッフ教育・接客品質向上", 0.04),
    ("営業時間延長", "営業時間の延長", 0.03),
    ("駐車場拡張", "駐車場の拡張・改善", 0.07),
]

# 施策ステータス
statuses = ["計画中", "実施中", "完了", "効果測定中"]

# 関連カテゴリ
categories_list = ["CAT01", "CAT02", "CAT03", "CAT04", "CAT05", "CAT06", "CAT07", "CAT08", "CAT09", "CAT10"]

store_measures_data = []
measure_id = 1

# 各店舗に0-3件の施策を割り当て
for _, store_row in df_stores.toPandas().iterrows():
    store_id = store_row['store_id']
    num_measures = random.randint(0, 3)

    # 業績が悪い店舗には施策を多めに
    if random.random() < 0.3:  # 30%の確率で業績悪化店舗として扱う
        num_measures = random.randint(2, 4)

    used_types = set()
    for _ in range(num_measures):
        # 未使用の施策タイプを選択
        available_types = [t for t in measure_types if t[0] not in used_types]
        if not available_types:
            break

        measure_type, description, expected_effect = random.choice(available_types)
        used_types.add(measure_type)

        # 施策日程
        start_date = datetime(2024, random.randint(1, 12), random.randint(1, 28))
        duration_days = random.randint(30, 180)
        end_date = start_date + timedelta(days=duration_days)

        # ステータス決定
        today = datetime(2024, 12, 31)
        if start_date > today:
            status = "計画中"
        elif end_date > today:
            status = "実施中"
        elif (today - end_date).days < 90:
            status = "効果測定中"
        else:
            status = "完了"

        # 実績効果（完了または効果測定中の場合のみ）
        if status in ["完了", "効果測定中"]:
            actual_effect = expected_effect * random.uniform(0.5, 1.5)
        else:
            actual_effect = None

        # 関連カテゴリ
        related_categories = random.sample(categories_list, random.randint(1, 3))

        # 投資額（万円）
        investment_amount = random.randint(50, 5000) * 10000

        store_measures_data.append((
            f"M{str(measure_id).zfill(4)}",
            store_id,
            measure_type,
            description,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            status,
            round(expected_effect, 3),
            round(actual_effect, 3) if actual_effect else None,
            ",".join(related_categories),
            investment_amount
        ))
        measure_id += 1

store_measures_schema = StructType([
    StructField("measure_id", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("measure_type", StringType(), False),
    StructField("description", StringType(), False),
    StructField("start_date", StringType(), False),
    StructField("end_date", StringType(), False),
    StructField("status", StringType(), False),
    StructField("expected_effect", DoubleType(), False),
    StructField("actual_effect", DoubleType(), True),
    StructField("related_categories", StringType(), False),
    StructField("investment_amount", IntegerType(), False),
])

df_measures = spark.createDataFrame(store_measures_data, store_measures_schema)
df_measures = df_measures.withColumn("start_date", to_date(col("start_date")))
df_measures = df_measures.withColumn("end_date", to_date(col("end_date")))

df_measures.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("store_measures")
print(f"store_measures テーブル作成完了: {df_measures.count()} 件")

# COMMAND ----------

display(spark.table("store_measures").orderBy("store_id", "start_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. 近隣施設（nearby_facilities）
# MAGIC
# MAGIC 各店舗周辺の集客施設・生活施設を管理。
# MAGIC 商圏環境分析に使用。

# COMMAND ----------

# 施設タイプ定義
facility_types = [
    ("大型商業施設", "ショッピングモール・大型スーパー", 0.8),
    ("スーパーマーケット", "食品スーパー", 0.6),
    ("ドラッグストア", "ドラッグストアチェーン", 0.3),
    ("家電量販店", "家電量販店", 0.5),
    ("ガソリンスタンド", "ガソリンスタンド", 0.2),
    ("飲食店街", "飲食店集積エリア", 0.4),
    ("病院", "総合病院・クリニック", 0.3),
    ("学校", "小中高校・大学", 0.3),
    ("公園・レジャー", "公園・レジャー施設", 0.4),
    ("駅", "鉄道駅", 0.7),
    ("役所・公共施設", "市役所・図書館等", 0.2),
    ("工業団地", "工業団地・物流倉庫", 0.3),
]

facility_names = {
    "大型商業施設": ["イオンモール", "ららぽーと", "アリオ", "イトーヨーカドー", "ゆめタウン"],
    "スーパーマーケット": ["マルエツ", "ライフ", "サミット", "ヨークベニマル", "マックスバリュ", "オーケー"],
    "ドラッグストア": ["マツモトキヨシ", "ウエルシア", "スギ薬局", "ツルハドラッグ", "サンドラッグ"],
    "家電量販店": ["ヤマダ電機", "ビックカメラ", "ケーズデンキ", "エディオン", "ノジマ"],
    "ガソリンスタンド": ["ENEOS", "出光", "コスモ石油", "昭和シェル"],
    "飲食店街": ["駅前商店街", "ロードサイド飲食エリア", "ショッピングセンター内飲食街"],
    "病院": ["市民病院", "総合病院", "大学病院", "医療センター"],
    "学校": ["小学校", "中学校", "高校", "大学"],
    "公園・レジャー": ["市民公園", "運動公園", "レジャー施設", "スポーツクラブ"],
    "駅": ["JR駅", "私鉄駅", "地下鉄駅"],
    "役所・公共施設": ["市役所", "区役所", "図書館", "公民館"],
    "工業団地": ["工業団地", "物流センター", "配送センター"],
}

nearby_facilities_data = []
facility_id = 1

for _, store_row in df_stores.toPandas().iterrows():
    store_id = store_row['store_id']
    is_urban = store_row['store_type'] == "都市型"

    # 店舗タイプに応じて施設数を調整
    if is_urban:
        num_facilities = random.randint(8, 15)
    else:
        num_facilities = random.randint(4, 10)

    # ランダムに施設を配置
    selected_types = random.sample(facility_types, min(num_facilities, len(facility_types)))

    for ftype, fdesc, base_traffic in selected_types:
        facility_name = random.choice(facility_names[ftype])
        distance_km = round(random.uniform(0.2, 5.0), 1)

        # 距離に応じて集客への影響を調整
        if distance_km <= 1.0:
            traffic_impact = base_traffic * random.uniform(0.9, 1.1)
        elif distance_km <= 2.0:
            traffic_impact = base_traffic * random.uniform(0.6, 0.8)
        else:
            traffic_impact = base_traffic * random.uniform(0.3, 0.5)

        # 営業時間（施設タイプに応じて）
        if ftype in ["大型商業施設", "スーパーマーケット", "ドラッグストア", "家電量販店"]:
            opening_hours = random.choice(["9:00-21:00", "10:00-20:00", "9:00-22:00"])
        elif ftype == "駅":
            opening_hours = "5:00-24:00"
        elif ftype in ["病院", "学校", "役所・公共施設"]:
            opening_hours = "8:30-17:30"
        else:
            opening_hours = None

        nearby_facilities_data.append((
            f"F{str(facility_id).zfill(5)}",
            store_id,
            ftype,
            facility_name,
            distance_km,
            round(traffic_impact, 2),
            opening_hours
        ))
        facility_id += 1

nearby_facilities_schema = StructType([
    StructField("facility_id", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("facility_type", StringType(), False),
    StructField("facility_name", StringType(), False),
    StructField("distance_km", DoubleType(), False),
    StructField("traffic_impact", DoubleType(), False),
    StructField("opening_hours", StringType(), True),
])

df_facilities = spark.createDataFrame(nearby_facilities_data, nearby_facilities_schema)
df_facilities.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("nearby_facilities")
print(f"nearby_facilities テーブル作成完了: {df_facilities.count()} 件")

# COMMAND ----------

display(spark.table("nearby_facilities").orderBy("store_id", "distance_km"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. 追加テーブルのメタデータ

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルコメント
# MAGIC COMMENT ON TABLE similar_stores IS '類似店舗マスタ。商圏特性・店舗規模が類似した店舗のペアを定義。施策効果の比較分析に使用';
# MAGIC COMMENT ON TABLE store_measures IS '施策管理。各店舗に実施した施策（リニューアル、棚割り変更、価格施策など）を管理。施策前後の効果比較に使用';
# MAGIC COMMENT ON TABLE nearby_facilities IS '近隣施設。各店舗周辺の集客施設・生活施設を管理。商圏環境分析に使用';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- similar_stores カラムコメント
# MAGIC ALTER TABLE similar_stores ALTER COLUMN store_id COMMENT '店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE similar_stores ALTER COLUMN similar_store_id COMMENT '類似店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE similar_stores ALTER COLUMN similarity_rank COMMENT '類似度ランク（1が最も類似）';
# MAGIC ALTER TABLE similar_stores ALTER COLUMN similarity_score COMMENT '類似度スコア（0-100）';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- store_measures カラムコメント
# MAGIC ALTER TABLE store_measures ALTER COLUMN measure_id COMMENT '施策ID（主キー）。形式: M0001, M0002, ...';
# MAGIC ALTER TABLE store_measures ALTER COLUMN store_id COMMENT '店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE store_measures ALTER COLUMN measure_type COMMENT '施策タイプ（リニューアル/棚割り最適化/品揃え強化/価格施策/販促強化/接客改善/営業時間延長/駐車場拡張）';
# MAGIC ALTER TABLE store_measures ALTER COLUMN description COMMENT '施策の詳細説明';
# MAGIC ALTER TABLE store_measures ALTER COLUMN start_date COMMENT '施策開始日';
# MAGIC ALTER TABLE store_measures ALTER COLUMN end_date COMMENT '施策終了日';
# MAGIC ALTER TABLE store_measures ALTER COLUMN status COMMENT '施策ステータス（計画中/実施中/完了/効果測定中）';
# MAGIC ALTER TABLE store_measures ALTER COLUMN expected_effect COMMENT '期待効果（前年比改善率）。0.1 = +10%';
# MAGIC ALTER TABLE store_measures ALTER COLUMN actual_effect COMMENT '実績効果（前年比改善率）。完了・効果測定中の場合のみ';
# MAGIC ALTER TABLE store_measures ALTER COLUMN related_categories COMMENT '関連カテゴリ（カンマ区切り）';
# MAGIC ALTER TABLE store_measures ALTER COLUMN investment_amount COMMENT '投資額（円）';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- nearby_facilities カラムコメント
# MAGIC ALTER TABLE nearby_facilities ALTER COLUMN facility_id COMMENT '施設ID（主キー）。形式: F00001, F00002, ...';
# MAGIC ALTER TABLE nearby_facilities ALTER COLUMN store_id COMMENT '店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE nearby_facilities ALTER COLUMN facility_type COMMENT '施設タイプ（大型商業施設/スーパーマーケット/ドラッグストア/家電量販店/ガソリンスタンド/飲食店街/病院/学校/公園・レジャー/駅/役所・公共施設/工業団地）';
# MAGIC ALTER TABLE nearby_facilities ALTER COLUMN facility_name COMMENT '施設名';
# MAGIC ALTER TABLE nearby_facilities ALTER COLUMN distance_km COMMENT '店舗からの距離（km）';
# MAGIC ALTER TABLE nearby_facilities ALTER COLUMN traffic_impact COMMENT '集客への影響度（0-1）。1が最も影響大';
# MAGIC ALTER TABLE nearby_facilities ALTER COLUMN opening_hours COMMENT '営業時間';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 主キーカラムをNOT NULLに設定
# MAGIC ALTER TABLE store_measures ALTER COLUMN measure_id SET NOT NULL;
# MAGIC ALTER TABLE nearby_facilities ALTER COLUMN facility_id SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 主キー制約
# MAGIC ALTER TABLE store_measures ADD CONSTRAINT pk_store_measures PRIMARY KEY (measure_id);
# MAGIC ALTER TABLE nearby_facilities ADD CONSTRAINT pk_nearby_facilities PRIMARY KEY (facility_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 外部キー制約
# MAGIC ALTER TABLE similar_stores ADD CONSTRAINT fk_similar_stores_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE similar_stores ADD CONSTRAINT fk_similar_stores_similar FOREIGN KEY (similar_store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE store_measures ADD CONSTRAINT fk_store_measures_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE nearby_facilities ADD CONSTRAINT fk_nearby_facilities_store FOREIGN KEY (store_id) REFERENCES stores(store_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. 全テーブル確認

# COMMAND ----------

all_tables = ["stores", "trade_area", "trade_area_expenditure", "competitors", "categories", "sales_monthly", "sales_by_category", "similar_stores", "store_measures", "nearby_facilities"]

print("=" * 60)
print("作成したテーブル一覧")
print("=" * 60)

for table in all_tables:
    count = spark.table(table).count()
    print(f"{table:30} : {count:,} 件")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 完了
# MAGIC
# MAGIC 以下のテーブルが作成されました：
# MAGIC
# MAGIC | テーブル | 件数 | 説明 |
# MAGIC |---------|------|------|
# MAGIC | `stores` | 50 | 店舗マスタ |
# MAGIC | `trade_area` | 50 | 商圏情報（人口統計） |
# MAGIC | `trade_area_expenditure` | 50 | **商圏消費支出（ゼンリン形式）** |
# MAGIC | `competitors` | 約170 | 競合店舗 |
# MAGIC | `categories` | 10 | カテゴリマスタ（消費支出マッピング付き） |
# MAGIC | `sales_monthly` | 1,800 | 月別売上 |
# MAGIC | `sales_by_category` | 18,000 | カテゴリ別売上 |
# MAGIC | `similar_stores` | 250 | **類似店舗マスタ**（各店舗5類似店舗） |
# MAGIC | `store_measures` | 約80 | **施策管理**（リニューアル、棚割り等） |
# MAGIC | `nearby_facilities` | 約500 | **近隣施設**（商圏環境分析用） |
# MAGIC
# MAGIC ### 消費支出データ項目（ゼンリン準拠）
# MAGIC
# MAGIC | カラム名 | 説明 | 単位 |
# MAGIC |---------|------|------|
# MAGIC | expenditure_total | 消費支出合計 | 千円/年/世帯 |
# MAGIC | furniture_household_total | 家具・家事用品合計 | 千円/年/世帯 |
# MAGIC | household_durables | 家庭用耐久財 | 千円/年/世帯 |
# MAGIC | housing_repair_total | 住居_設備修繕・維持合計 | 千円/年/世帯 |
# MAGIC | garden_plants | 園芸用植物 | 千円/年/世帯 |
# MAGIC | garden_supplies | 園芸用品 | 千円/年/世帯 |
# MAGIC | pet_food | ペットフード | 千円/年/世帯 |
# MAGIC | pet_supplies | ペット用品 | 千円/年/世帯 |
# MAGIC
# MAGIC ### メタデータ
# MAGIC
# MAGIC | 種別 | 件数 |
# MAGIC |------|------|
# MAGIC | テーブルコメント | 10テーブル |
# MAGIC | カラムコメント | 全カラム |
# MAGIC | 主キー制約 | 5テーブル（stores, competitors, categories, store_measures, nearby_facilities） |
# MAGIC | 外部キー制約 | 10制約 |
# MAGIC
# MAGIC ### ER図
# MAGIC
# MAGIC ```
# MAGIC stores (PK: store_id)
# MAGIC   │
# MAGIC   ├── trade_area (FK: store_id)
# MAGIC   ├── trade_area_expenditure (FK: store_id)
# MAGIC   ├── competitors (PK: competitor_id, FK: store_id)
# MAGIC   ├── sales_monthly (FK: store_id)
# MAGIC   ├── sales_by_category (FK: store_id, category_id)
# MAGIC   │                          │
# MAGIC   │   categories (PK: category_id) ─┘
# MAGIC   │
# MAGIC   ├── similar_stores (FK: store_id, similar_store_id)
# MAGIC   ├── store_measures (PK: measure_id, FK: store_id)
# MAGIC   └── nearby_facilities (PK: facility_id, FK: store_id)
# MAGIC ```
# MAGIC
# MAGIC ### 参考資料
# MAGIC - [商圏分析とは？](https://www.zenrin-ms.co.jp/blog/2024/013/) - ゼンリンマーケティングソリューションズ
# MAGIC - [ゼンリン 消費支出データ項目一覧（2023年版）](https://www.giken.co.jp/static/2025/02/expenditure2023.pdf)
