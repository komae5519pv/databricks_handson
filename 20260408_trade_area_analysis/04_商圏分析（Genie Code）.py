# Databricks notebook source
# MAGIC %md
# MAGIC # 商圏分析（Genie Code）
# MAGIC
# MAGIC このノートブックでは、Genie Code の Agent mode と Skills を使って商圏分析を行います。
# MAGIC
# MAGIC ## 前提条件
# MAGIC 1. `01_サンプルデータ作成` を実行済み
# MAGIC 2. Genie Code Agent mode を有効化
# MAGIC 3. `trade-area-analysis` Skill が設定済み（`/.assistant/skills/trade-area-analysis/SKILL.md`）
# MAGIC
# MAGIC ## 使い方
# MAGIC 1. 右側の Genie Code パネルを開く
# MAGIC 2. Agent mode をオンにする
# MAGIC 3. 下記のサンプルプロンプトをコピーして実行

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## データ確認

# COMMAND ----------

# 利用可能なテーブル一覧
spark.sql("SHOW TABLES").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # サンプルプロンプト集
# MAGIC
# MAGIC 以下のプロンプトを Genie Code にコピー＆ペーストして試してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lv1: 基礎分析
# MAGIC
# MAGIC ### プロンプト 1-1: 低迷店舗の特定
# MAGIC ```
# MAGIC 売上が前年比で10%以上落ちている店舗を特定して、
# MAGIC 店舗名、所在地、前年比をテーブルで表示してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 1-2: 商圏特性の確認
# MAGIC ```
# MAGIC 最も売上が低迷している店舗の商圏特性を教えてください。
# MAGIC 人口、世帯年収、高齢化率、競合店舗数を含めて分析してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 1-3: 競合状況の可視化
# MAGIC ```
# MAGIC 低迷店舗の周辺にある競合店舗を一覧表示し、
# MAGIC 距離と売場面積でソートしてください。
# MAGIC 特に2024年以降に出店した新規競合をハイライトしてください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lv2: 深掘分析
# MAGIC
# MAGIC ### プロンプト 2-1: 売上低迷要因の分析
# MAGIC ```
# MAGIC 低迷店舗と好調店舗を比較して、
# MAGIC 商圏特性（人口、年収、高齢化率）と売上の相関を分析してください。
# MAGIC 散布図で可視化してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 2-2: カテゴリ別ギャップ分析
# MAGIC ```
# MAGIC 低迷店舗のカテゴリ別売上構成を、
# MAGIC 全店平均と比較してください。
# MAGIC どのカテゴリで差が大きいか特定してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 2-3: 店舗クラスタリング
# MAGIC ```
# MAGIC 商圏特性（人口、年収、高齢化率、戸建て比率）を使って
# MAGIC 店舗をクラスタリングしてください。
# MAGIC 各クラスタの特徴と、低迷店舗がどのクラスタに属するか教えてください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lv3: 施策立案
# MAGIC
# MAGIC ### プロンプト 3-1: ベンチマーク店舗の発見
# MAGIC ```
# MAGIC 最も低迷している店舗と商圏特性が似ていて、
# MAGIC かつ売上が好調な店舗を見つけてください。
# MAGIC その店舗との違い（カテゴリ構成など）を分析してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 3-2: 改善施策の提案
# MAGIC ```
# MAGIC 低迷店舗の分析結果に基づいて、
# MAGIC 具体的な改善施策を3つ提案してください。
# MAGIC 各施策の期待効果と優先度も教えてください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 3-3: 総合レポート作成
# MAGIC ```
# MAGIC 低迷店舗について、以下の内容を含む分析レポートを作成してください：
# MAGIC 1. 現状サマリー（売上推移、前年比）
# MAGIC 2. 商圏特性の特徴
# MAGIC 3. 競合環境
# MAGIC 4. 低迷要因の仮説
# MAGIC 5. 改善施策の提案
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 応用プロンプト
# MAGIC
# MAGIC ### 複合的な分析
# MAGIC ```
# MAGIC 商圏分析スキルを使って、
# MAGIC 売上低迷店舗の要因を特定し、
# MAGIC 類似成功店舗との比較から改善策を提案してください。
# MAGIC 分析は基礎分析→深掘分析→施策立案の順で進めてください。
# MAGIC ```
# MAGIC
# MAGIC ### 可視化重視
# MAGIC ```
# MAGIC 全店舗の売上と商圏人口の関係を散布図で可視化し、
# MAGIC 低迷店舗を赤色でハイライトしてください。
# MAGIC 回帰直線も追加してください。
# MAGIC ```
# MAGIC
# MAGIC ### 競合影響の定量化
# MAGIC ```
# MAGIC 新規競合（2024年以降出店）がある店舗とない店舗で、
# MAGIC 売上前年比の平均を比較してください。
# MAGIC 競合の出店が売上に与える影響を定量化してください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## メモ欄
# MAGIC
# MAGIC Genie Code の回答や発見をここにメモしてください。

# COMMAND ----------

# DBTITLE 1,Lv1: 基礎分析
# MAGIC %md
# MAGIC ---
# MAGIC # Lv1: 基礎分析 — 「どの店舗が、どれくらい苦しいのか？」
# MAGIC
# MAGIC まずは全体50店舗の売上データを俯瞰し、**「低迷店舗」を客観的に特定**します。
# MAGIC
# MAGIC | 分析ステップ | 内容 |
# MAGIC |---|---|
# MAGIC | 1-1 | 低迷店舗の特定（前年比-10%以下） |
# MAGIC | 1-2 | 最低迷店舗の商圏プロフィール（人口統計 + 消費支出） |
# MAGIC | 1-3 | 低迷店舗周辺の競合状況 |

# COMMAND ----------

# DBTITLE 1,Lv1-1: 低迷店舗の特定
from pyspark.sql import functions as F

# ============================================================
# Lv1-1: 低迷店舗の特定
# ============================================================
# 分析概要:
#   使用データ: sales_monthly (yoy_change), stores (store_name, 所在地)
#   分析手法: 2024年1月～12月の前年同月比(yoy_change)を店舗ごとに平均
#   低迷の定義: 平均前年比が -10% 以下
# ============================================================

sales = spark.table("sales_monthly")
stores = spark.table("stores")

# --- 全店舗の2024年平均前年比を算出 ---
yoy_by_store = (
    sales
    .filter(F.year("month") == 2024)
    .groupBy("store_id")
    .agg(
        F.round(F.avg("yoy_change"), 3).alias("avg_yoy_change"),
        F.round(F.sum("sales_amount") / 10000, 0).alias("annual_sales_man")
    )
)

# --- 低迷店舗（前年比 -10% 以下）を特定 ---
declining = (
    yoy_by_store
    .filter(F.col("avg_yoy_change") <= -0.1)
    .join(stores.select("store_id", "store_name", "prefecture", "city", "store_type"), "store_id")
    .select(
        "store_id", "store_name",
        F.concat_ws(" ", "prefecture", "city").alias("所在地"),
        "store_type",
        F.format_string("%,.0f万円", "annual_sales_man").alias("2024年売上"),
        F.format_string("%.1f%%", F.col("avg_yoy_change") * 100).alias("前年比")
    )
    .orderBy("avg_yoy_change")
)

total = yoy_by_store.count()
declining_count = declining.count()

print("="*60)
print("■ Lv1-1: 低迷店舗の特定")
print("="*60)
print(f"定義: 2024年の平均前年比が -10% 以下")
print(f"結果: 全{total}店中 【{declining_count}店舗】（{declining_count/total:.0%}）が低迷")
print("="*60)
declining.display()

# COMMAND ----------

# DBTITLE 1,Lv1-2: 最低迷店舗の商圏プロフィール
from pyspark.sql import functions as F

# ============================================================
# Lv1-2: 最低迷店舗の商圏プロフィール
# ============================================================
# 使用データ: stores, trade_area, trade_area_expenditure, competitors
# 分析手法: 対象店舗の商圏特性を「全店平均」と比較して特徴を浮き彫りにする
# 対象: 店舗_川越市（S010）—— 前年比 -25.5% で全店ワースト
# ============================================================

trade_area = spark.table("trade_area")
expenditure = spark.table("trade_area_expenditure")
competitors = spark.table("competitors")
worst_id = "S010"  # 川越市

# --- 1. 人口統計 ---
print("="*60)
print("■ Lv1-2: 店舗_川越市（S010）の商圏プロフィール")
print("="*60)
print("\n【A】 人口統計（trade_area）")

profile = (
    stores.filter(F.col("store_id") == worst_id)
    .join(trade_area, "store_id")
    .select(
        "store_name",
        F.concat_ws(" ", "prefecture", "city").alias("所在地"),
        "store_type",
        F.format_number("population_5km", 0).alias("商圏人口_5km"),
        F.format_number("households", 0).alias("世帯数"),
        F.format_string("%,.0f万円", F.col("avg_income") / 10000).alias("平均世帯年収"),
        F.format_string("%.1f%%", F.col("elderly_rate") * 100).alias("高齢化率"),
        F.format_string("%.1f%%", F.col("young_adult_rate") * 100).alias("若年層比率"),
        F.format_string("%.1f%%", F.col("detached_house_rate") * 100).alias("戸建て比率"),
        F.round("day_night_ratio", 2).alias("昼夜間人口比"),
    )
)

# 全店平均も併記
avg_profile = (
    trade_area.agg(
        F.lit("【全店平均】").alias("store_name"),
        F.lit("-").alias("所在地"),
        F.lit("-").alias("store_type"),
        F.format_number(F.avg("population_5km"), 0).alias("商圏人口_5km"),
        F.format_number(F.avg("households"), 0).alias("世帯数"),
        F.format_string("%,.0f万円", F.avg("avg_income") / 10000).alias("平均世帯年収"),
        F.format_string("%.1f%%", F.avg("elderly_rate") * 100).alias("高齢化率"),
        F.format_string("%.1f%%", F.avg("young_adult_rate") * 100).alias("若年層比率"),
        F.format_string("%.1f%%", F.avg("detached_house_rate") * 100).alias("戸建て比率"),
        F.round(F.avg("day_night_ratio"), 2).alias("昼夜間人口比"),
    )
)
profile.unionByName(avg_profile).display()

# --- 2. 消費支出ポテンシャル ---
print("\n【B】 消費支出ポテンシャル（trade_area_expenditure）単位: 千円/年/世帯")
print("※ 商圏内の世帯が年間で各カテゴリにいくら使うかの　推定値")

exp_worst = expenditure.filter(F.col("store_id") == worst_id)
exp_avg = expenditure.agg(
    F.lit("【全店平均】").alias("store_id"),
    *[F.round(F.avg(c), 1).alias(c) for c in expenditure.columns if c != "store_id"]
)
exp_worst.select(
    "store_id", "expenditure_total",
    (F.col("garden_plants") + F.col("garden_supplies")).alias("園芸計"),
    "equipment_materials", "housing_repair_total", "construction_services",
    "furniture_household_total", "household_consumables",
    (F.col("pet_food") + F.col("pet_supplies")).alias("ペット計"),
    "household_durables"
).unionByName(
    exp_avg.select(
        "store_id", "expenditure_total",
        (F.col("garden_plants") + F.col("garden_supplies")).alias("園芸計"),
        "equipment_materials", "housing_repair_total", "construction_services",
        "furniture_household_total", "household_consumables",
        (F.col("pet_food") + F.col("pet_supplies")).alias("ペット計"),
        "household_durables"
    )
).display()

# --- 3. 競合状況（簡易） ---
print("\n【C】 競合店舗")
competitors.filter(F.col("store_id") == worst_id).orderBy("distance_km").display()

# COMMAND ----------

# DBTITLE 1,Lv1-3: 競合状況の分析
from pyspark.sql import functions as F

# ============================================================
# Lv1-3: 低迷店舗周辺の競合状況
# ============================================================
# 使用データ: competitors, sales_monthly, stores
# 分析手法: (1) 低迷ワースト5店の競合一覧 (2) 新規競合の影響定量化
# ============================================================

competitors = spark.table("competitors")

print("="*60)
print("■ Lv1-3: 低迷店舗周辺の競合状況")
print("="*60)

# --- (1) 低迷ワースト5店の競合一覧 ---
worst5_ids = ["S010", "S009", "S014", "S022", "S024"]  # Lv1-1の結果

print("\n【A】 低迷ワースト5店の競合店舗一覧（★ = 2024年以降の新規競合）")
(
    competitors
    .filter(F.col("store_id").isin(worst5_ids))
    .join(stores.select("store_id", "store_name"), "store_id")
    .join(
        yoy_by_store.select(
            "store_id",
            F.format_string("%.1f%%", F.col("avg_yoy_change") * 100).alias("前年比")
        ), "store_id"
    )
    .select(
        "store_name", "前年比",
        "competitor_name",
        F.round("distance_km", 1).alias("距離_km"),
        F.format_number("size_sqm", 0).alias("売場面積_㎡"),
        "open_year",
        F.when(F.col("is_new_entry"), "★新規").otherwise("").alias("新規")
    )
    .orderBy("前年比", "距離_km")
    .display()
)

# --- (2) 新規競合の影響定量化 ---
print("\n【B】 新規競合（2024年以降出店）の影響")
print("全店を「新規競合あり」「なし」に分けて前年比を比較\n")

has_new_competitor = (
    competitors
    .filter(F.col("is_new_entry") == True)
    .select("store_id").distinct()
    .withColumn("has_new", F.lit(True))
)

(
    yoy_by_store
    .join(has_new_competitor, "store_id", "left")
    .withColumn("区分", F.when(F.col("has_new"), "新規競合あり").otherwise("新規競合なし"))
    .groupBy("区分")
    .agg(
        F.count("*").alias("店舗数"),
        F.format_string("%.1f%%", F.avg("avg_yoy_change") * 100).alias("平均前年比"),
        F.format_string("%.1f%%", F.min("avg_yoy_change") * 100).alias("最低前年比"),
        F.sum(F.when(F.col("avg_yoy_change") <= -0.1, 1).otherwise(0)).alias("低迷店数")
    )
    .display()
)

# COMMAND ----------

# DBTITLE 1,Lv1 基礎分析: 発見と仮説
# MAGIC %md
# MAGIC ## Lv1 基礎分析: 発見と仮説
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 分析概要
# MAGIC - **使用データ**: `sales_monthly`(2024年), `stores`, `trade_area`, `trade_area_expenditure`, `competitors`
# MAGIC - **分析手法**: 前年同月比平均、全店平均比較、新規競合有無別集計
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 主な発見
# MAGIC
# MAGIC | # | 発見 | ポイント |
# MAGIC |---|--------|----------|
# MAGIC | 1 | 全店50店の**半数（25店）**が前年比-10%以下 | 個別の問題ではなく、構造的な問題の可能性 |
# MAGIC | 2 | 川越市がワースト（**-25.5%**） | 商圏人口は全店平均の39%と非常に小さい |
# MAGIC | 3 | 川越市の消費支出ポテンシャルは全店平均の**122%** | 「需要はあるのに取りこぼしている」状態 |
# MAGIC | 4 | 川越市の特徴: 若年層少ない / 戸建て少ない / 年収高い | 駅前タワマンの新住民（共働きDINKs）と古い住宅街が混在 |
# MAGIC | 5 | 川越市の`construction_services`が全店平均より**-26%低い** | タワマン住民は「外注もDIYもしない」可能性 |
# MAGIC | 6 | 2024年にコメリが1.1km地点に新規出店 | プロ向け資材に強いコメリに職人層を奪われた可能性 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ここまでの仮説（Lv2で検証）
# MAGIC
# MAGIC > **仮説①: 「タワマン×高齢住宅街」の二極化**
# MAGIC > 川越市の商圏は、新しいタワマン住民（DINKs・都内通勤）と古くからの高齢住民が混在。従来の「郊外型HC」の品揃えがどちらにもフィットしていないのでは？
# MAGIC > → **検証**: カテゴリ別ギャップ分析で「どのカテゴリで取りこぼしが大きいか」を確認
# MAGIC
# MAGIC > **仮説②: 「コメリ出店」による商圏侵食**
# MAGIC > 2024年のコメリ新規出店が、小さい商圏のパイを奪った可能性。ただし全体では「新規競合なし」の方が前年比が悪いという意外な結果も。
# MAGIC > → **検証**: 商圏規模別に競合影響を分析（小商圏では致命的かも）
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 次のステップ → Lv2 深掘分析
# MAGIC - カテゴリ別の「ポテンシャル vs 実績」ギャップ分析
# MAGIC - 商圏特性による店舗クラスタリング

# COMMAND ----------

# DBTITLE 1,Lv2: 深掘分析
# MAGIC %md
# MAGIC ---
# MAGIC # Lv2: 深掘分析 — 「なぜ苦しいのか？」
# MAGIC
# MAGIC Lv1で立てた仮説を、データで検証します。
# MAGIC
# MAGIC | 分析ステップ | 内容 | 検証する仮説 |
# MAGIC |---|---|---|
# MAGIC | 2-1 | カテゴリ別ギャップ分析 | 仮説①「品揃えが住民に合っていない」 |
# MAGIC | 2-2 | 店舗クラスタリング | 商圏タイプ別の低迷パターンを発見 |
# MAGIC
# MAGIC **ギャップ分析の考え方**（専門用語なしで説明）:
# MAGIC - `trade_area_expenditure` = 「商圏内の住民が、園芸やペットなどに年間いくら使うか」の推定値
# MAGIC - `sales_by_category` = 「実際にその店舗でいくら売れたか」
# MAGIC - **ギャップ = 推定値 − 実績** → これが大きいカテゴリ = 「取りこぼし」が大きい改善機会

# COMMAND ----------

# DBTITLE 1,Lv2-1: カテゴリ別ギャップ分析（仮説①の検証）
from pyspark.sql import functions as F

# ============================================================
# Lv2-1: カテゴリ別ギャップ分析（仮説①の検証）
# ============================================================
# 使用データ:
#   - sales_by_category (2024年): カテゴリ別実績売上
#   - trade_area_expenditure: 商圏内消費支出ポテンシャル（千円/年/世帯）
#   - trade_area: 世帯数（households）
#   - categories: expenditure_mapping, potential_coefficient
# 分析手法:
#   ポテンシャル = 支出単価(千円/年/世帯) × 世帯数 × 1000 × potential_coefficient
#   捕捉率 = 実績売上 / ポテンシャル × 100
# ============================================================

categories = spark.table("categories")
sales_cat = spark.table("sales_by_category")
expenditure = spark.table("trade_area_expenditure")
trade_area = spark.table("trade_area")
worst_id = "S010"

print("="*60)
print("■ Lv2-1: カテゴリ別ギャップ分析")
print("="*60)
print("「商圏内の住民が使うはずの金額」と「実際の売上」のギャップをカテゴリ別に算出\n")

# --- ステップ1: カテゴリ別の支出単価を取得 ---
# expenditure_mapping に基づいてカテゴリごとの支出額を計算
worst_exp = expenditure.filter(F.col("store_id") == worst_id)
worst_hh = trade_area.filter(F.col("store_id") == worst_id).select("households").first()[0]

# マッピング定義（categories.expenditure_mapping をプログラムで解釈）
mapping = {
    "CAT01": ("garden_plants", "garden_supplies"),   # 園芸用品
    "CAT02": ("equipment_materials",),                # DIY工具
    "CAT03": ("housing_repair_total",),               # 塗料・接着剤
    "CAT04": ("construction_services",),              # 木材・建材
    "CAT05": ("equipment_materials",),                # 金物・作業用品
    "CAT06": ("household_durables",),                 # 電材・照明
    "CAT07": ("construction_services",),              # 水道・配管用品
    "CAT08": ("furniture_household_total",),          # 収納・インテリア
    "CAT09": ("household_consumables",),              # 日用品・清掃用品
    "CAT10": ("pet_food", "pet_supplies"),            # ペット用品
}
coefficients = dict(categories.select("category_id", "potential_coefficient").collect())
cat_names = dict(categories.select("category_id", "category_name").collect())

# 支出単価を取得
exp_row = worst_exp.first()

import pandas as pd
results = []
for cat_id, cols in mapping.items():
    exp_value = sum(exp_row[c] for c in cols)  # 千円/年/世帯
    coeff = coefficients[cat_id]
    potential = exp_value * worst_hh * 1000 * coeff  # 円
    results.append({"category_id": cat_id, "category_name": cat_names[cat_id],
                    "expenditure_千円": exp_value, "coefficient": coeff,
                    "potential_円": potential})

df_potential = spark.createDataFrame(pd.DataFrame(results))

# --- ステップ2: 実績売上を取得 ---
actual_worst = (
    sales_cat
    .filter((F.year("month") == 2024) & (F.col("store_id") == worst_id))
    .groupBy("category_id")
    .agg(F.sum(F.col("sales_amount").cast("double")).alias("actual_円"))
)

# --- ステップ3: 全店平均の捕捉率も算出 ---
all_potential = []
for sid_row in trade_area.select("store_id", "households").collect():
    sid, hh = sid_row[0], sid_row[1]
    e_row = expenditure.filter(F.col("store_id") == sid).first()
    for cat_id, cols in mapping.items():
        ev = sum(e_row[c] for c in cols)
        p = ev * hh * 1000 * coefficients[cat_id]
        all_potential.append({"store_id": sid, "category_id": cat_id, "potential_円": p})

df_all_pot = spark.createDataFrame(pd.DataFrame(all_potential))
actual_all = (
    sales_cat.filter(F.year("month") == 2024)
    .groupBy("store_id", "category_id")
    .agg(F.sum(F.col("sales_amount").cast("double")).alias("actual_円"))
)
avg_capture = (
    actual_all.join(df_all_pot, ["store_id", "category_id"])
    .withColumn("capture", F.col("actual_円") / F.col("potential_円") * 100)
    .groupBy("category_id")
    .agg(F.round(F.avg("capture"), 1).alias("全店平均捕捉率_%"))
)

# --- 結合して表示 ---
print("【A】 川越市のカテゴリ別ギャップ（捕捉率 = 実績 ÷ ポテンシャル）")
gap = (
    df_potential.alias("p")
    .join(actual_worst.alias("a"), "category_id")
    .join(avg_capture.alias("avg"), "category_id")
    .select(
        F.col("p.category_name"),
        F.round(F.col("p.potential_円") / 10000, 0).alias("ポテンシャル_万円"),
        F.round(F.col("a.actual_円") / 10000, 0).alias("実績売上_万円"),
        F.round(F.col("a.actual_円") / F.col("p.potential_円") * 100, 1).alias("捕捉率_%"),
        F.col("avg.全店平均捕捉率_%"),
        F.round(
            F.col("a.actual_円") / F.col("p.potential_円") * 100 - F.col("avg.全店平均捕捉率_%"), 1
        ).alias("差分_pt")
    )
    .orderBy("差分_pt")
)
gap.display()

print("\n↑ 差分_ptがマイナス = 全店平均より取りこぼしが大きいカテゴリ")
print("  差分_ptがプラス = 全店平均より強いカテゴリ（強み）")

# COMMAND ----------

# DBTITLE 1,Lv2-2: 店舗クラスタリング
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# ============================================================
# Lv2-2: 店舗クラスタリング
# ============================================================
# 使用データ: trade_area, sales_monthly(2024), stores
# 分析手法: K-meansクラスタリング（k=4）
# 特徴量: 商圏人口、平均年収、高齢化率、戸建て比率、昼夜間人口比
# ============================================================

print("="*60)
print("■ Lv2-2: 店舗クラスタリング")
print("="*60)
print("商圏特性が似ている店舗をグループ化し、低迷パターンを発見\n")

# データ準備（store_idでソートして再現性確保）
df_cluster = (
    trade_area.join(yoy_by_store, "store_id")
    .join(stores.select("store_id", "store_name", "store_type"), "store_id")
    .orderBy("store_id")  # 再現性のためソート
    .toPandas()
)
df_cluster["is_declining"] = df_cluster["avg_yoy_change"] <= -0.1

# 特徴量の標準化とクラスタリング
features = ["population_5km", "avg_income", "elderly_rate", "detached_house_rate", "day_night_ratio"]
scaler = StandardScaler()
X = scaler.fit_transform(df_cluster[features])

kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
df_cluster["cluster"] = kmeans.fit_predict(X)

# クラスタ別の特徴を算出
cluster_summary = df_cluster.groupby("cluster").agg(
    店舗数=("store_id", "count"),
    低迷店数=("is_declining", "sum"),
    平均前年比=("avg_yoy_change", lambda x: round(x.mean() * 100, 1)),
    平均人口=("population_5km", lambda x: round(x.mean())),
    平均年収=("avg_income", lambda x: round(x.mean())),
    平均高齢化率=("elderly_rate", lambda x: round(x.mean(), 3)),
    平均戸建て比率=("detached_house_rate", lambda x: round(x.mean(), 3)),
)
cluster_summary["低迷率"] = (cluster_summary["低迷店数"] / cluster_summary["店舗数"] * 100).round(1)

# クラスタに名前を付ける（特徴量から自動判定）
def name_cluster(row):
    if row["平均人口"] > 150000: return "都市型大商圏"
    if row["平均高齢化率"] > 0.30: return "高齢化型"
    if row["平均戸建て比率"] > 0.55: return "郊外戸建て型"
    return "小規模混合型"

cluster_summary["タイプ"] = cluster_summary.apply(name_cluster, axis=1)
cluster_summary.index = [f"C{i}: {cluster_summary.loc[i, 'タイプ']}" for i in cluster_summary.index]

print("【クラスタ別特徴】")
print(cluster_summary[["店舗数", "低迷店数", "低迷率", "平均前年比", "平均人口", "平均年収", "平均高齢化率", "平均戸建て比率"]].to_string())

# 川越市のクラスタ確認
worst_cluster = df_cluster[df_cluster["store_id"] == "S010"]["cluster"].values[0]
worst_type = cluster_summary.index[worst_cluster].split(": ")[1] if worst_cluster < len(cluster_summary) else "?"
print(f"\n→ 川越市の所属クラスタ: C{worst_cluster}: {worst_type}")

# COMMAND ----------

# DBTITLE 1,Lv2 深掘分析: 仮説検証結果
# MAGIC %md
# MAGIC ## Lv2 深掘分析: 仮説検証結果
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 分析概要
# MAGIC - **使用データ**: `sales_by_category`(2024年), `trade_area_expenditure`, `trade_area`, `categories`, `stores`
# MAGIC - **分析手法**: ギャップ分析（ポテンシャル vs 実績）、K-meansクラスタリング(k=4)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 仮説①の検証結果: 「品揃えが住民に合っていない」 → ✅ **裏付けられた**
# MAGIC
# MAGIC 川越市は以下のカテゴリで、全店平均より大幅に**取りこぼし**ています：
# MAGIC
# MAGIC | 取りこぼしが大きい | 差分 | 考えられる原因 |
# MAGIC |---|---|---|
# MAGIC | 園芸用品 | **-97.7pt** | ポテンシャルは高いのに捕捉できていない。マンション住民向けのベランダ園芸等が弱い？ |
# MAGIC | ペット用品 | **-49.1pt** | タワマンDINKsはペット飼育率が高いはず。品揃えが不足？ |
# MAGIC | 日用品・清掃 | **-21.9pt** | 「ついで買い」の日用品が弱いと来店頻度が下がる |
# MAGIC
# MAGIC 一方、川越市の**強み**は:
# MAGIC - **DIY工具**（+14.6pt）と**水道・配管**（+17.7pt）
# MAGIC - → これは古くからの持ち家住民や職人層の需要を捕捉できている
# MAGIC
# MAGIC ### 仮説②の検証結果: 「コメリ出店」の影響
# MAGIC
# MAGIC - 全体では新規競合ありの方が好調（-7.1% vs -9.7%）
# MAGIC - ただし**小商圏の川越市では致命的**：コメリはプロ向け資材に強く、川越市の強み（DIY・配管）と直接競合
# MAGIC
# MAGIC ### クラスタリングからの発見
# MAGIC
# MAGIC 川越市が属するクラスタの特徴を上記のテーブルで確認。「小規模×高年収」の商圏では従来型のHCの品揃えが合わないことが確認されました。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Lv2の結論
# MAGIC
# MAGIC > 川越市の低迷は「強み（DIY・配管）に寄りすぎ、日常使い（日用品・ペット・園芸）を取りこぼしている」構造。
# MAGIC > 商圏にはタワマン新住民（DINKs）がいるが、彼ら向けの品揃えがない。
# MAGIC > さらにコメリの新規出店でDIY・配管の強みも奪われるリスクがある。
# MAGIC
# MAGIC ### 次のステップ → Lv3 施策立案
# MAGIC - 類似商圏で好調な「お手本店舗」を見つける
# MAGIC - 具体的なカテゴリ改善施策と期待効果を算出

# COMMAND ----------

# DBTITLE 1,Lv3: 施策立案
# MAGIC %md
# MAGIC ---
# MAGIC # Lv3: 施策立案 — 「どうすれば改善できるか？」
# MAGIC
# MAGIC Lv1・Lv2の分析結果を踏まえ、具体的な「打ち手」を導き出します。
# MAGIC
# MAGIC | 分析ステップ | 内容 |
# MAGIC |---|---|
# MAGIC | 3-1 | 「お手本店舗」の発見（商圏が似ていて好調な店） |
# MAGIC | 3-2 | 改善施策の提案と期待効果 |
# MAGIC
# MAGIC **ベンチマークの考え方**: 川越市と商圏特性が似ているのに売上好調な店舗を見つけ、「何が違うのか」を特定します。

# COMMAND ----------

# DBTITLE 1,Lv3-1: ベンチマーク店舗の発見
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

# ============================================================
# Lv3-1: ベンチマーク店舗の発見
# ============================================================
# 使用データ: trade_area, sales_monthly(2024), sales_by_category(2024), stores, categories
# 分析手法: ユークリッド距離で商圏類似度を算出、前年比>-5%の好調店から上位3店を選定
# ============================================================

print("="*60)
print("■ Lv3-1: ベンチマーク店舗の発見")
print("="*60)
print("川越市と商圏が似ていて、かつ売上好調な「お手本」を探す\n")

# 全店データを準備（Lv2-2のdf_clusterを再利用）
worst_id = "S010"
features = ["population_5km", "avg_income", "elderly_rate", "detached_house_rate", "day_night_ratio"]
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df_cluster[features])
worst_idx = df_cluster[df_cluster["store_id"] == worst_id].index[0]

# ユークリッド距離で類似度算出
distances = np.sqrt(((X_scaled - X_scaled[worst_idx]) ** 2).sum(axis=1))
df_cluster["similarity_dist"] = distances

# 好調店（前年比 > -5%）の中で商圏が類似する上位3店
benchmarks = (
    df_cluster[(df_cluster["avg_yoy_change"] > -0.05) & (df_cluster["store_id"] != worst_id)]
    .nsmallest(3, "similarity_dist")
)

print("【A】 川越市 vs ベンチマーク3店の比較")
compare_cols = ["store_name", "avg_yoy_change", "population_5km", "avg_income", "elderly_rate", "detached_house_rate"]
compare_df = pd.concat([
    df_cluster[df_cluster["store_id"] == worst_id][compare_cols],
    benchmarks[compare_cols]
]).copy()
compare_df["avg_yoy_change"] = (compare_df["avg_yoy_change"] * 100).round(1).astype(str) + "%"
compare_df["elderly_rate"] = (compare_df["elderly_rate"] * 100).round(1).astype(str) + "%"
compare_df["detached_house_rate"] = (compare_df["detached_house_rate"] * 100).round(1).astype(str) + "%"
compare_df.columns = ["店舗名", "前年比", "商圏人口", "平均年収", "高齢化率", "戸建て比率"]
display(spark.createDataFrame(compare_df))

# --- カテゴリ構成比の比較 ---
print("\n【B】 カテゴリ構成比の比較（川越市 vs BM平均）")
print("各店舗の売上全体に対して、各カテゴリが何%を占めるか\n")

categories = spark.table("categories")
sales_cat = spark.table("sales_by_category")
bm_ids = benchmarks["store_id"].tolist()

# カテゴリ構成比の算出
def get_cat_share(store_ids, label):
    return (
        sales_cat
        .filter((F.year("month") == 2024) & F.col("store_id").isin(store_ids))
        .join(categories.select("category_id", "category_name"), "category_id")
        .groupBy("category_name")
        .agg(F.sum(F.col("sales_amount").cast("double")).alias("cat_sales"))
        .withColumn("total", F.lit(
            sales_cat
            .filter((F.year("month") == 2024) & F.col("store_id").isin(store_ids))
            .agg(F.sum(F.col("sales_amount").cast("double"))).first()[0]
        ))
        .withColumn(f"{label}_%", F.round(F.col("cat_sales") / F.col("total") * 100, 1))
        .select("category_name", f"{label}_%")
    )

worst_share = get_cat_share([worst_id], "川越市")
bm_share = get_cat_share(bm_ids, "BM平均")

(
    worst_share.join(bm_share, "category_name")
    .withColumn("差分_pt", F.round(F.col("川越市_%") - F.col("BM平均_%"), 1))
    .orderBy("差分_pt")
    .display()
)
print("↑ 差分がマイナス = 川越市がベンチマークより弱いカテゴリ")
print("  差分がプラス = 川越市がベンチマークより強いカテゴリ")

# COMMAND ----------

# DBTITLE 1,Lv3-2: 改善施策の提案と期待効果
from pyspark.sql import functions as F

# ============================================================
# Lv3-2: 改善施策の提案と期待効果
# ============================================================
# 使用データ: Lv1～Lv3-1の全分析結果
# 分析手法: 全店平均まで引き上げた場合の改善余地をカテゴリ別に算出
# ============================================================

print("="*60)
print("■ Lv3-2: 改善施策の提案")
print("="*60)
print("川越市の各カテゴリを全店平均まで引き上げた場合の「改善余地」を算出\n")

categories = spark.table("categories")
sales_cat = spark.table("sales_by_category")
worst_id = "S010"

# 川越市のカテゴリ別実績
actual_worst = (
    sales_cat
    .filter((F.year("month") == 2024) & (F.col("store_id") == worst_id))
    .groupBy("category_id")
    .agg(F.sum(F.col("sales_amount").cast("double")).alias("actual"))
    .join(categories.select("category_id", "category_name"), "category_id")
)

# 全店平均のカテゴリ別売上
actual_all_avg = (
    sales_cat
    .filter(F.year("month") == 2024)
    .groupBy("category_id")
    .agg((F.sum(F.col("sales_amount").cast("double")) / F.countDistinct("store_id")).alias("avg_sales"))
    .join(categories.select("category_id", "category_name"), "category_id")
)

# 改善余地の算出
print("【カテゴリ別改善ポテンシャル】")
improvement = (
    actual_worst.alias("w")
    .join(actual_all_avg.alias("a"), "category_id")
    .select(
        F.col("w.category_name"),
        F.round(F.col("w.actual") / 10000, 0).alias("現状売上_万円"),
        F.round(F.col("a.avg_sales") / 10000, 0).alias("全店平均_万円"),
        F.round((F.col("a.avg_sales") - F.col("w.actual")) / 10000, 0).alias("改善余地_万円"),
        F.round((F.col("a.avg_sales") - F.col("w.actual")) / F.col("w.actual") * 100, 1).alias("改善率_%")
    )
    .orderBy(F.col("改善余地_万円").desc())
)
improvement.display()

total_gap = improvement.select(F.sum("改善余地_万円")).first()[0]
print(f"\n全カテゴリ合計の改善ポテンシャル: {total_gap:,.0f}万円（約{total_gap/10000:.1f}億円）")
print("※ 全カテゴリを全店平均まで引き上げた場合の理論値")

# COMMAND ----------

# DBTITLE 1,Lv3 総合レポート
# MAGIC %md
# MAGIC ## Lv3 総合レポート: 店舗\_川越市（S010）の改善提案
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. 現状サマリー
# MAGIC
# MAGIC | 項目 | 店舗\_川越市 | 全店平均との比較 |
# MAGIC |---|---|---|
# MAGIC | 2024年前年比 | **-25.5%**（全店ワースト） | 全店平均: -8.8% |
# MAGIC | 商圏人口 | 43,165人 | 全店平均の**39%** |
# MAGIC | 消費支出ポテンシャル | 3,920千円/世帯/年 | 全店平均の**122%** |
# MAGIC | クラスタ | C2: 高齢化型（低迷率63.6%） | 最も低迷率が高い |
# MAGIC | 競合 | コメリ 1.1km（2024年新規） | 小商圏×新規競合でダメージ大 |
# MAGIC
# MAGIC > **一言で言うと**: 「お金を使う住民はいるのに、そのお金を取りこぼしている」状態
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. 商圏の特徴（なぜ難しいのか）
# MAGIC
# MAGIC 川越市の商圏は、駅から徒歩15分のエリアに位置し、2023年に駅前タワーマンションが竣工。その結果、**2つの全く違う住民層**が混在しています：
# MAGIC
# MAGIC | 属性 | 新住民（タワマン） | 旧住民（古い住宅街） |
# MAGIC |---|---|---|
# MAGIC | 世帯構成 | 共働きDINKs・都内通勤 | 高齢者・持ち家 |
# MAGIC | DIY需要 | 低い（新築マンションで修繕不要） | あるが体力的に難しい |
# MAGIC | ペット | 飼育率高い（マンションで小型犬） | 少ない |
# MAGIC | 日用品 | 利便性重視（コンビニ/ネット購入） | 近所で購入したい |
# MAGIC
# MAGIC 従来の「郊外型HC」の品揃えは、**どちらの層にもフィットしていません**。
# MAGIC
# MAGIC また、2024年にコメリが1.1km地点に出店。コメリはプロ向け資材に強く、川越市の強み（DIY・配管）と直接競合するリスクがあります。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. データから見えた「取りこぼし」
# MAGIC
# MAGIC 全店平均と比べて川越市が弱いカテゴリ（捕捉率の差分）：
# MAGIC
# MAGIC | カテゴリ | 全店平均との差 | 改善余地 | なぜ弱いのか？ |
# MAGIC |---|---|---|---|
# MAGIC | 園芸用品 | **-97.7pt** | 1,788万 | マンション住民向けベランダ園芸が弱い |
# MAGIC | ペット用品 | **-49.1pt** | 1,910万 | DINKsのペット需要を取れていない |
# MAGIC | 日用品・清掃 | **-21.9pt** | 3,611万 | 「ついで買い」の来店動機を作れていない |
# MAGIC
# MAGIC 一方、強み: **DIY工具**（+14.6pt）, **水道・配管**（+17.7pt）
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. ベンチマークからの学び
# MAGIC
# MAGIC 神戸市が最も参考になります（年収540万・戸建て33.1%と川越市に非常に似ているが、前年比+5.3%と好調）。
# MAGIC
# MAGIC 神戸市との主な違い：
# MAGIC - 日用品の構成比が**川越8.7% vs BM平坈11.6%**（-2.9pt）
# MAGIC - ペットの構成比が**6.8% vs 8.6%**（-1.8pt）
# MAGIC - 園芸の構成比が**17.2% vs 14.6%**（+2.6pt）→ 園芸に偏りすぎ
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. 改善施策（3つ）
# MAGIC
# MAGIC #### 施策①：日用品・ペット売場の拡充（優先度: ★★★）
# MAGIC - **背景**: 日用品が最大の改善余地（3,611万円）、ペットも捕捉率が全店平均より-49pt低い
# MAGIC - **アクション**:
# MAGIC   - 日用消耗品の品揃え拡大で「ついで買い」の来店動機を作る
# MAGIC   - タワマンDINKs向けにペットコーナーを拡充（小型犬・猫用品）
# MAGIC - **期待効果**: 約**5,500万円**の売上改善余地（日用品+ペット合計）
# MAGIC
# MAGIC #### 施策②：タワマン住民向けの品揃え転換（優先度: ★★）
# MAGIC - **背景**: タワマン住民は「DIYより外注」志向。塗料・木材の改善余地が合計約4,800万円
# MAGIC - **アクション**:
# MAGIC   - DIY初心者向けワークショップの開催（「賃貸でもできる原状回復DIY」）
# MAGIC   - 時短商品・小型リフォーム用品の充実
# MAGIC - **期待効果**: 約**4,800万円**の売上改善余地（塗料+木材）
# MAGIC
# MAGIC #### 施策③：コメリとの差別化（DIY・配管の強みを守る）（優先度: ★★）
# MAGIC - **背景**: DIY工具・水道配管は全店平均を上回る強みだが、コメリに奪われるリスク
# MAGIC - **アクション**:
# MAGIC   - プロ向け工具・配管材料の品揃えをコメリ以上に充実
# MAGIC   - 「相談できるスタッフ」で接客力を差別化
# MAGIC - **期待効果**: 既存顧客の固定化、プロ層の新規獲得
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. 施策の優先度マトリクス
# MAGIC
# MAGIC | 施策 | インパクト | 実現容易性 | 期待効果 | 優先度 |
# MAGIC |---|---|---|---|---|
# MAGIC | ① 日用品・ペット拡充 | ★★★ | ★★★ | 約5,500万円 | **最優先** |
# MAGIC | ② タワマン向け品揃え | ★★ | ★★ | 約4,800万円 | 高 |
# MAGIC | ③ コメリとの差別化 | ★★ | ★★★ | 顧客固定化 | 高 |
# MAGIC | **合計** | | | **約1.9億円** | |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. まとめ
# MAGIC
# MAGIC 店舗\_川越市の低迷は、**「強み（DIY・配管）に寄りすぎ、日常使いのカテゴリを取りこぼしている」**ことが主因です。
# MAGIC
# MAGIC タワーマンション竣工で新しい住民層（DINKs）が流入しているのに、そのニーズ（ペット・日用品・ベランダ園芸）に応えられていません。またコメリの新規出店にDIY・配管の強みも奪われるリスクがあります。
# MAGIC
# MAGIC 改善ポテンシャルは**約1.9億円**。まずは施策①（日用品・ペット拡充）から始め、施策②（タワマン向け品揃え）と施策③（コメリ差別化）を並行して進めることを推奨します。
