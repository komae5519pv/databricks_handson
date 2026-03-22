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

# DBTITLE 1,Lv1 基礎分析 ヘッダー
# MAGIC %md
# MAGIC ---
# MAGIC # 🔍 Lv1: 基礎分析 — 現状把握
# MAGIC
# MAGIC まず「どの店舗が低迷しているか」を特定し、その店舗がどんな商圏にあるのか、周辺にどんな競合がいるのかを把握します。
# MAGIC
# MAGIC **分析ステップ:**
# MAGIC 1. 売上前年比で10%以上落ちている店舗を特定
# MAGIC 2. 最も低迷している店舗の商圏特性を深掘り
# MAGIC 3. 低迷店舗周辺の競合状況を可視化

# COMMAND ----------

# DBTITLE 1,Lv1-1: 低迷店舗の特定
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# 日本語フォント設定
try:
    import japanize_matplotlib
except ImportError:
    import subprocess
    subprocess.check_call(['pip', 'install', 'japanize-matplotlib', '-q'])
    import japanize_matplotlib

matplotlib.rcParams['axes.unicode_minus'] = False

# ========================================
# Lv1-1: 売上前年比10%以上低下の店舗を特定
# ========================================
# 使用データ: sales_monthly, stores
# 分析手法: 2024年度12ヶ月の前年比平均を算出し、-10%以下の店舗を抽出
# 期間: 2024年1月～12月

# データの期間を確認
date_info = spark.sql("""
    SELECT MIN(month) as min_month, MAX(month) as max_month,
           COUNT(DISTINCT store_id) as num_stores
    FROM komae_demo_v4.trade_area.sales_monthly
    WHERE yoy_change != 0
""").collect()[0]
print(f"📅 前年比データ期間: {date_info.min_month} ～ {date_info.max_month}")
print(f"📊 対象店舗数: {date_info.num_stores}")
print("=" * 60)

# 2024年度の年間パフォーマンスを集計
store_performance = spark.sql("""
    SELECT 
        s.store_id,
        s.store_name,
        s.prefecture,
        s.city,
        s.store_type,
        s.size_sqm,
        s.parking_capacity,
        ROUND(AVG(sm.yoy_change) * 100, 1) AS avg_yoy_pct,
        ROUND(SUM(sm.sales_amount) / 1e8, 2) AS annual_sales_oku,
        ROUND(AVG(sm.customer_count), 0) AS avg_monthly_customers,
        ROUND(AVG(sm.avg_basket), 0) AS avg_basket
    FROM komae_demo_v4.trade_area.sales_monthly sm
    JOIN komae_demo_v4.trade_area.stores s ON sm.store_id = s.store_id
    WHERE sm.month >= '2024-01-01' AND sm.month < '2025-01-01'
    GROUP BY s.store_id, s.store_name, s.prefecture, s.city, 
             s.store_type, s.size_sqm, s.parking_capacity
    ORDER BY avg_yoy_pct ASC
""").toPandas()

# 低迷店舗（前年比-10%以下）の判定
store_performance['status'] = store_performance['avg_yoy_pct'].apply(
    lambda x: '⚠️ 低迷' if x <= -10 else ('📉 やや減少' if x < 0 else '✅ 好調')
)

struggling = store_performance[store_performance['avg_yoy_pct'] <= -10]
print(f"\n🚨 前年比-10%以下の低迷店舗: {len(struggling)}店舗 / 全{len(store_performance)}店舗")
print("=" * 60)

# 全店舗のテーブル表示
display_cols = ['store_id', 'store_name', 'prefecture', 'city', 'store_type',
                'avg_yoy_pct', 'annual_sales_oku', 'avg_monthly_customers', 'avg_basket', 'status']
result_df = spark.createDataFrame(
    store_performance[display_cols].rename(columns={
        'store_id': '店舗ID', 'store_name': '店舗名', 'prefecture': '都道府県',
        'city': '市区町村', 'store_type': '店舗タイプ',
        'avg_yoy_pct': '前年比(%)', 'annual_sales_oku': '年間売上(億円)',
        'avg_monthly_customers': '月平均客数', 'avg_basket': '客単価(円)', 'status': '判定'
    })
)
display(result_df)

# COMMAND ----------

# DBTITLE 1,Lv1-1: 低迷店舗の可視化
# 全店舗の前年比を棒グラフで可視化（低迷店舗をハイライト）
fig, ax = plt.subplots(figsize=(16, 6))

colors = ['#d32f2f' if x <= -10 else ('#ff9800' if x < 0 else '#4caf50') 
          for x in store_performance['avg_yoy_pct']]

bars = ax.bar(range(len(store_performance)), store_performance['avg_yoy_pct'], color=colors)
ax.axhline(y=-10, color='red', linestyle='--', alpha=0.7, label='低迷ライン (-10%)')
ax.axhline(y=0, color='gray', linestyle='-', alpha=0.3)

ax.set_xticks(range(len(store_performance)))
ax.set_xticklabels(store_performance['store_name'].str.replace('店舗_', ''), 
                   rotation=45, ha='right', fontsize=8)
ax.set_ylabel('前年比 (%)')
ax.set_title('【Lv1-1】全店舗の売上前年比（2024年度平均）', fontsize=14, fontweight='bold')
ax.legend(fontsize=10)

# 低迷店舗にラベル
for idx_pos, (i, row) in enumerate(store_performance.iterrows()):
    if row['avg_yoy_pct'] <= -10:
        ax.annotate(f"{row['avg_yoy_pct']}%", 
                   (idx_pos, row['avg_yoy_pct']),
                   textcoords='offset points', xytext=(0, -15),
                   ha='center', fontsize=7, fontweight='bold', color='darkred')

plt.tight_layout()
plt.show()

print("\n📋 凡例: 🔴赤 = 前年比-10%以下（低迷）、🟠橙 = 微減、🟢緑 = 成長")

# COMMAND ----------

# DBTITLE 1,Lv1-1: 解釈と示唆
# MAGIC %md
# MAGIC ### 📝 Lv1-1 解釈と示唆
# MAGIC
# MAGIC **発見事項:**
# MAGIC - 全50店舗中 **15店舗（30%）** が前年比-10%以下の低迷状態
# MAGIC - ワースト3: **岐阜市**（-20.7%）、**松本市**（-20.6%）、**鹿児島市**（-18.7%）
# MAGIC - 店舗タイプ別では「都市型」が目立つ（6/15店舗）
# MAGIC - 地域的には北関東・中部・関西に分散しており、特定地域に集中しているわけではない
# MAGIC
# MAGIC **仮説:**
# MAGIC > 「低迷の原因は地域要因ではなく、各店舗の商圏特性や競合環境に起因するのではないか」
# MAGIC
# MAGIC 次のステップでこの仮説を検証します。

# COMMAND ----------

# DBTITLE 1,Lv1-2: 低迷店舗の商圏特性分析
# ========================================
# Lv1-2: 低迷店舗の商圏特性を分析
# ========================================
# 使用データ: trade_area, competitors, stores, sales_monthly
# 分析手法: 低迷店舗 vs 全店平均で商圏特性を比較

# 全店舗の商圏特性 + 売上パフォーマンスを結合
trade_profile = spark.sql("""
    SELECT 
        s.store_id, s.store_name, s.prefecture, s.city, s.store_type,
        ta.population_5km, ta.households, ta.avg_income,
        ROUND(ta.elderly_rate * 100, 1) AS elderly_rate_pct,
        ROUND(ta.young_adult_rate * 100, 1) AS young_adult_pct,
        ROUND(ta.detached_house_rate * 100, 1) AS detached_house_pct,
        ta.day_night_ratio, ta.num_businesses,
        -- 競合店舗数
        comp.competitor_count,
        comp.nearest_competitor_km,
        -- 新規競合数
        COALESCE(comp.new_entry_count, 0) AS new_entry_count
    FROM komae_demo_v4.trade_area.stores s
    JOIN komae_demo_v4.trade_area.trade_area ta ON s.store_id = ta.store_id
    LEFT JOIN (
        SELECT store_id, 
               COUNT(*) AS competitor_count,
               ROUND(MIN(distance_km), 1) AS nearest_competitor_km,
               SUM(CASE WHEN is_new_entry THEN 1 ELSE 0 END) AS new_entry_count
        FROM komae_demo_v4.trade_area.competitors
        GROUP BY store_id
    ) comp ON s.store_id = comp.store_id
""").toPandas()

# store_performanceと結合して低迷/好調のラベルを付与
trade_profile = trade_profile.merge(
    store_performance[['store_id', 'avg_yoy_pct', 'annual_sales_oku', 'status']],
    on='store_id'
)

# --- 低迷店舗 vs 全店平均の比較テーブル ---
struggling_ids = struggling['store_id'].tolist()
struggling_profile = trade_profile[trade_profile['store_id'].isin(struggling_ids)]
normal_profile = trade_profile[~trade_profile['store_id'].isin(struggling_ids)]

comparison = pd.DataFrame({
    '指標': ['商圏人口(5km)', '世帯数', '平均年収(万円)', '高齢化率(%)',
            '若年層比率(%)', '戸建て比率(%)', '昼夜間人口比',
            '事業所数', '競合店数', '最寄り競合(km)', '新規競合数'],
    '低迷店舗(15店)平均': [
        f"{struggling_profile['population_5km'].mean():,.0f}",
        f"{struggling_profile['households'].mean():,.0f}",
        f"{struggling_profile['avg_income'].mean()/10000:.0f}",
        f"{struggling_profile['elderly_rate_pct'].mean():.1f}",
        f"{struggling_profile['young_adult_pct'].mean():.1f}",
        f"{struggling_profile['detached_house_pct'].mean():.1f}",
        f"{struggling_profile['day_night_ratio'].mean():.2f}",
        f"{struggling_profile['num_businesses'].mean():,.0f}",
        f"{struggling_profile['competitor_count'].mean():.1f}",
        f"{struggling_profile['nearest_competitor_km'].mean():.1f}",
        f"{struggling_profile['new_entry_count'].mean():.1f}",
    ],
    '好調店舗(35店)平均': [
        f"{normal_profile['population_5km'].mean():,.0f}",
        f"{normal_profile['households'].mean():,.0f}",
        f"{normal_profile['avg_income'].mean()/10000:.0f}",
        f"{normal_profile['elderly_rate_pct'].mean():.1f}",
        f"{normal_profile['young_adult_pct'].mean():.1f}",
        f"{normal_profile['detached_house_pct'].mean():.1f}",
        f"{normal_profile['day_night_ratio'].mean():.2f}",
        f"{normal_profile['num_businesses'].mean():,.0f}",
        f"{normal_profile['competitor_count'].mean():.1f}",
        f"{normal_profile['nearest_competitor_km'].mean():.1f}",
        f"{normal_profile['new_entry_count'].mean():.1f}",
    ]
})

print("📊 低迷店舗 vs その他店舗の商圏特性比較")
print("=" * 60)
display(spark.createDataFrame(comparison))

# --- ワースト3店舗の詳細プロファイル ---
print("\n🚨 ワースト3店舗の詳細プロファイル")
print("=" * 60)
worst3 = struggling_profile.nsmallest(3, 'avg_yoy_pct')
for _, row in worst3.iterrows():
    print(f"\n■ {row['store_name']}({row['store_id']}) | 前年比: {row['avg_yoy_pct']}%")
    print(f"  商圏人口: {row['population_5km']:,}人 / 平均年収: {row['avg_income']/10000:.0f}万円")
    print(f"  高齢化率: {row['elderly_rate_pct']}% / 若年層: {row['young_adult_pct']}%")
    print(f"  戸建て比率: {row['detached_house_pct']}% / 競合: {row['competitor_count']}店(最寄り{row['nearest_competitor_km']}km)")
    print(f"  新規競合: {row['new_entry_count']}店（2024年以降出店）")

# COMMAND ----------

# DBTITLE 1,Lv1-3: 競合状況の可視化
# ========================================
# Lv1-3: 低迷店舗周辺の競合店舗一覧
# ========================================
# 使用データ: competitors, stores
# 分析手法: 低迷店舗の競合を距離順に一覧化、新規出店をハイライト

competitor_detail = spark.sql(f"""
    SELECT 
        s.store_name AS `自店舗`,
        s.store_id,
        c.competitor_name AS `競合名`,
        c.distance_km AS `距離_km`,
        c.size_sqm AS `競合売場面積_sqm`,
        c.open_year AS `出店年`,
        CASE WHEN c.is_new_entry THEN '🆕 新規' ELSE '' END AS `新規フラグ`
    FROM komae_demo_v4.trade_area.competitors c
    JOIN komae_demo_v4.trade_area.stores s ON c.store_id = s.store_id
    WHERE c.store_id IN ({','.join([f"'{sid}'" for sid in struggling_ids])})
    ORDER BY s.store_id, c.distance_km
""")
display(competitor_detail)

print(f"\n📊 低迷{len(struggling_ids)}店舗周辺の競合: 合計{competitor_detail.count()}店舗")

# --- 新規競合の影響分析 ---
new_entry_impact = spark.sql("""
    WITH store_new_entry AS (
        SELECT s.store_id, s.store_name,
               MAX(CASE WHEN c.is_new_entry THEN 1 ELSE 0 END) AS has_new_entry,
               SUM(CASE WHEN c.is_new_entry THEN 1 ELSE 0 END) AS new_entry_count
        FROM komae_demo_v4.trade_area.stores s
        LEFT JOIN komae_demo_v4.trade_area.competitors c ON s.store_id = c.store_id
        GROUP BY s.store_id, s.store_name
    )
    SELECT 
        CASE WHEN has_new_entry = 1 THEN '新規競合あり' ELSE '新規競合なし' END AS `競合状況`,
        COUNT(*) AS `店舗数`,
        ROUND(AVG(sm.avg_yoy), 1) AS `平均前年比_pct`
    FROM store_new_entry sne
    JOIN (
        SELECT store_id, AVG(yoy_change) * 100 AS avg_yoy
        FROM komae_demo_v4.trade_area.sales_monthly
        WHERE month >= '2024-01-01' AND month < '2025-01-01'
        GROUP BY store_id
    ) sm ON sne.store_id = sm.store_id
    GROUP BY has_new_entry
    ORDER BY has_new_entry DESC
""")
print("\n🆕 新規競合の有無と売上前年比の関係:")
display(new_entry_impact)

# --- 競合状況の可視化 ---
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# 左: 低迷店舗別競合数
comp_count = struggling_profile[['store_name', 'competitor_count', 'new_entry_count']].copy()
comp_count['store_name'] = comp_count['store_name'].str.replace('店舗_', '')
comp_count = comp_count.sort_values('competitor_count', ascending=True)

y_pos = range(len(comp_count))
axes[0].barh(y_pos, comp_count['competitor_count'], color='#1976d2', alpha=0.7, label='既存競合')
axes[0].barh(y_pos, comp_count['new_entry_count'], color='#d32f2f', alpha=0.9, label='新規競合(2024~)')
axes[0].set_yticks(y_pos)
axes[0].set_yticklabels(comp_count['store_name'], fontsize=9)
axes[0].set_xlabel('競合店舗数')
axes[0].set_title('低迷店舗別: 競合店舗数', fontweight='bold')
axes[0].legend(fontsize=9)

# 右: 前年比 vs 最寄り競合距離
sc = axes[1].scatter(trade_profile['nearest_competitor_km'], 
                     trade_profile['avg_yoy_pct'],
                     c=['#d32f2f' if sid in struggling_ids else '#4caf50' 
                        for sid in trade_profile['store_id']],
                     s=80, alpha=0.7, edgecolors='white')
axes[1].axhline(y=-10, color='red', linestyle='--', alpha=0.5)
axes[1].set_xlabel('最寄り競合までの距離 (km)')
axes[1].set_ylabel('前年比 (%)')
axes[1].set_title('競合距離 vs 売上前年比', fontweight='bold')

plt.tight_layout()
plt.show()
print("🟢緑=好調店舗 / 🔴赤=低迷店舗")

# COMMAND ----------

# DBTITLE 1,Lv1 まとめ
# MAGIC %md
# MAGIC ### 📝 Lv1 分析結果まとめ
# MAGIC
# MAGIC **① 低迷店舗の全体像:** 50店舗中**15店舗（30%）**が前年比-10%以下。地域は全国に分散。
# MAGIC
# MAGIC **② 商圏特性の意外な発見:**
# MAGIC - 低迷店舗は商圏人口が「大きい」（平均 13.9万人 vs 10.0万人）→ 人が多いのに売れない = **「取りこぼし」の可能性**
# MAGIC - 年収や高齢化率はほぼ同等 → 単純な人口構造では説明できない
# MAGIC - 戸建て比率は低迷店舗が高い（51.8% vs 44.0%）→ DIY・園芸のポテンシャルがあるはず
# MAGIC
# MAGIC **③ 競合の影響:**
# MAGIC - 「新規競合あり」店舗の前年比-2.8% vs 「なし」の-5.3% → **新規競合の影響は限定的**
# MAGIC - 低迷の主因は競合よりも「商圏ポテンシャルを活かしきれていない」ことにある可能性
# MAGIC
# MAGIC **⚡ Lv2で検証する仮説:**
# MAGIC > 「低迷店舗は商圏ポテンシャル（特にDIY・園芸）を取りこぼしており、カテゴリ構成のミスマッチが低迷の原因ではないか」

# COMMAND ----------

# DBTITLE 1,Lv2 深掘分析 ヘッダー
# MAGIC %md
# MAGIC ---
# MAGIC # 🔬 Lv2: 深掘分析 — 要因特定
# MAGIC
# MAGIC Lv1で「商圏ポテンシャルの取りこぼし」という仮説を立てました。ここではそれをデータで検証します。
# MAGIC
# MAGIC **分析ステップ:**
# MAGIC 1. 商圏特性と売上の相関分析（何が売上に最も影響するか）
# MAGIC 2. カテゴリ別ギャップ分析（どの商品で取りこぼしが大きいか）
# MAGIC 3. 店舗クラスタリング（低迷店舗がどのグループに属するか）

# COMMAND ----------

# DBTITLE 1,Lv2-1: 商圏特性×売上の相関分析
# ========================================
# Lv2-1: 商圏特性 × 売上の相関分析
# ========================================
# 使用データ: trade_profile (Lv1-2で作成済み)
# 分析手法: ピアソン相関係数 + 散布図マトリクス
from scipy import stats

# 相関分析の対象変数
features = ['population_5km', 'avg_income', 'elderly_rate_pct', 
            'young_adult_pct', 'detached_house_pct', 'competitor_count', 
            'nearest_competitor_km', 'day_night_ratio', 'num_businesses']
feature_names_jp = ['商圏人口', '平均年収', '高齢化率', 
                    '若年層比率', '戸建て比率', '競合店数', 
                    '最寄り競合距離', '昼夜間人口比', '事業所数']

# 前年比との相関係数を計算
corr_results = []
for feat, feat_jp in zip(features, feature_names_jp):
    r, p = stats.pearsonr(trade_profile[feat].dropna(), 
                          trade_profile.loc[trade_profile[feat].notna(), 'avg_yoy_pct'])
    corr_results.append({'feature': feat_jp, 'correlation': round(r, 3), 'p_value': round(p, 4)})

corr_df = pd.DataFrame(corr_results).sort_values('correlation')
print("📈 商圏特性と売上前年比の相関係数")
print("(正 = 売上にプラス / 負 = 売上にマイナス)")
print("=" * 50)
for _, row in corr_df.iterrows():
    sig = '★' if row['p_value'] < 0.05 else ''
    bar = '█' * int(abs(row['correlation']) * 30)
    direction = '🟢+' if row['correlation'] > 0 else '🔴-'
    print(f"  {direction} {row['feature']:<12} r={row['correlation']:>6.3f} {sig} |{bar}")

print("\n★ = p<0.05（統計的に有意）")

# --- 主要4変数の散布図 ---
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
top_features = [('population_5km', '商圏人口 (5km)'),
                ('avg_income', '平均世帯年収 (円)'),
                ('elderly_rate_pct', '高齢化率 (%)'),
                ('detached_house_pct', '戸建て比率 (%)')]

for ax, (feat, label) in zip(axes.flat, top_features):
    colors = ['#d32f2f' if sid in struggling_ids else '#4caf50' 
              for sid in trade_profile['store_id']]
    ax.scatter(trade_profile[feat], trade_profile['avg_yoy_pct'], 
              c=colors, s=80, alpha=0.7, edgecolors='white')
    ax.axhline(y=-10, color='red', linestyle='--', alpha=0.3)
    ax.axhline(y=0, color='gray', linestyle='-', alpha=0.2)
    ax.set_xlabel(label)
    ax.set_ylabel('前年比 (%)')
    # 回帰直線
    x_vals = trade_profile[feat].values
    y_vals = trade_profile['avg_yoy_pct'].values
    mask = ~np.isnan(x_vals) & ~np.isnan(y_vals)
    if mask.sum() > 2:
        z = np.polyfit(x_vals[mask], y_vals[mask], 1)
        p = np.poly1d(z)
        x_line = np.linspace(x_vals[mask].min(), x_vals[mask].max(), 100)
        ax.plot(x_line, p(x_line), 'b--', alpha=0.5)
    r = stats.pearsonr(x_vals[mask], y_vals[mask])[0]
    ax.set_title(f'{label} vs 前年比 (r={r:.2f})', fontweight='bold')

plt.suptitle('【Lv2-1】商圏特性と売上前年比の散布図', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.show()
print("🟢緑=好調店舗 / 🔴赤=低迷店舗 / 青破線=回帰直線")

# COMMAND ----------

# DBTITLE 1,Lv2-2: カテゴリ別ギャップ分析
# ========================================
# Lv2-2: カテゴリ別ギャップ分析
# ========================================
# 使用データ: sales_by_category, categories, trade_area_expenditure
# 分析手法: 低迷店舗のカテゴリ構成を全店平均と比較
#          + 商圏消費支出ポテンシャルとの乖離を計算

# カテゴリ別売上構成比（低迷店舗 vs 全店平均）
category_comp = spark.sql("""
    WITH cat_sales AS (
        SELECT 
            sc.store_id,
            c.category_name,
            c.category_id,
            SUM(sc.sales_amount) AS annual_sales
        FROM komae_demo_v4.trade_area.sales_by_category sc
        JOIN komae_demo_v4.trade_area.categories c ON sc.category_id = c.category_id
        WHERE sc.month >= '2024-01-01' AND sc.month < '2025-01-01'
        GROUP BY sc.store_id, c.category_name, c.category_id
    ),
    store_total AS (
        SELECT store_id, SUM(annual_sales) AS total_sales
        FROM cat_sales
        GROUP BY store_id
    )
    SELECT 
        cs.category_name,
        cs.category_id,
        -- 全店平均構成比
        ROUND(AVG(cs.annual_sales / st.total_sales * 100), 1) AS avg_all_pct,
        -- 低迷店舗構成比
        ROUND(AVG(CASE WHEN cs.store_id IN ({struggling_sql}) THEN cs.annual_sales / st.total_sales * 100 END), 1) AS avg_struggling_pct,
        -- 好調店舗構成比
        ROUND(AVG(CASE WHEN cs.store_id NOT IN ({struggling_sql}) THEN cs.annual_sales / st.total_sales * 100 END), 1) AS avg_good_pct,
        -- 低迷店舗の平均売上
        ROUND(AVG(CASE WHEN cs.store_id IN ({struggling_sql}) THEN cs.annual_sales END) / 1e6, 1) AS struggling_sales_m,
        -- 好調店舗の平均売上
        ROUND(AVG(CASE WHEN cs.store_id NOT IN ({struggling_sql}) THEN cs.annual_sales END) / 1e6, 1) AS good_sales_m
    FROM cat_sales cs
    JOIN store_total st ON cs.store_id = st.store_id
    GROUP BY cs.category_name, cs.category_id
    ORDER BY cs.category_id
""".format(struggling_sql=','.join([f"'{sid}'" for sid in struggling_ids]))).toPandas()

print("📊 カテゴリ別売上構成比比較")
print("=" * 60)

# ギャップ計算
category_comp['gap_pct'] = category_comp['avg_struggling_pct'] - category_comp['avg_good_pct']
category_comp['gap_sales_m'] = category_comp['struggling_sales_m'] - category_comp['good_sales_m']

display(spark.createDataFrame(
    category_comp[['category_name', 'avg_struggling_pct', 'avg_good_pct', 'gap_pct', 
                   'struggling_sales_m', 'good_sales_m', 'gap_sales_m']].rename(columns={
        'category_name': 'カテゴリ', 'avg_struggling_pct': '低迷店構成比(%)',
        'avg_good_pct': '好調店構成比(%)', 'gap_pct': '構成比差(%)',
        'struggling_sales_m': '低迷店平均売上(百万)', 'good_sales_m': '好調店平均売上(百万)',
        'gap_sales_m': '売上差(百万)'
    })
))

# --- 可視化: カテゴリ構成比の比較 ---
fig, axes = plt.subplots(1, 2, figsize=(15, 6))

# 左: 構成比比較
x = np.arange(len(category_comp))
w = 0.35
axes[0].bar(x - w/2, category_comp['avg_struggling_pct'], w, 
            label='低迷店舗', color='#d32f2f', alpha=0.8)
axes[0].bar(x + w/2, category_comp['avg_good_pct'], w, 
            label='好調店舗', color='#4caf50', alpha=0.8)
axes[0].set_xticks(x)
axes[0].set_xticklabels(category_comp['category_name'], rotation=45, ha='right', fontsize=9)
axes[0].set_ylabel('構成比 (%)')
axes[0].set_title('カテゴリ別売上構成比', fontweight='bold')
axes[0].legend()

# 右: ギャップ（低迷-好調）
gap_sorted = category_comp.sort_values('gap_pct')
colors_gap = ['#d32f2f' if g < 0 else '#4caf50' for g in gap_sorted['gap_pct']]
axes[1].barh(range(len(gap_sorted)), gap_sorted['gap_pct'], color=colors_gap, alpha=0.8)
axes[1].set_yticks(range(len(gap_sorted)))
axes[1].set_yticklabels(gap_sorted['category_name'], fontsize=9)
axes[1].axvline(x=0, color='gray', linestyle='-', alpha=0.5)
axes[1].set_xlabel('構成比差 (低迷店 - 好調店)')
axes[1].set_title('カテゴリ構成のギャップ', fontweight='bold')

for i, (_, row) in enumerate(gap_sorted.iterrows()):
    axes[1].annotate(f"{row['gap_pct']:+.1f}%", 
                    (row['gap_pct'], i),
                    textcoords='offset points', xytext=(5 if row['gap_pct'] >= 0 else -35, 0),
                    fontsize=8, va='center')

plt.suptitle('【Lv2-2】カテゴリ別売上構成比: 低迷 vs 好調', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.show()
print("\n🔴 赤 = 低迷店舗が弱いカテゴリ / 🟢 緑 = 低迷店舗が強いカテゴリ")

# COMMAND ----------

# DBTITLE 1,Lv2-2b: 商圏ポテンシャルとのギャップ
# ========================================
# Lv2-2b: 商圏消費支出ポテンシャル vs 実績売上のギャップ
# ========================================
# 使用データ: trade_area_expenditure, sales_by_category, categories
# 分析手法: 商圏ポテンシャル(消費支出 × 世帯数 × 係数)と実績売上の差分を算出

# ワースト3店舗のポテンシャルギャップ分析
worst3_ids = struggling.nsmallest(3, 'avg_yoy_pct')['store_id'].tolist()

potential_gap = spark.sql(f"""
    WITH expenditure AS (
        SELECT te.store_id, ta.households,
            -- 各カテゴリの商圏ポテンシャル = 消費支出(千円/年/世帯) × 世帯数
            (te.garden_plants + te.garden_supplies) * ta.households * 1000 AS potential_garden,
            te.equipment_materials * ta.households * 1000 AS potential_diy,
            te.housing_repair_total * ta.households * 1000 AS potential_repair,
            te.construction_services * ta.households * 1000 AS potential_construction,
            te.furniture_household_total * ta.households * 1000 AS potential_furniture,
            te.household_consumables * ta.households * 1000 AS potential_consumables,
            (te.pet_food + te.pet_supplies) * ta.households * 1000 AS potential_pet
        FROM komae_demo_v4.trade_area.trade_area_expenditure te
        JOIN komae_demo_v4.trade_area.trade_area ta ON te.store_id = ta.store_id
    ),
    actual_sales AS (
        SELECT sc.store_id, c.category_name, c.category_id,
               SUM(sc.sales_amount) AS actual_annual
        FROM komae_demo_v4.trade_area.sales_by_category sc
        JOIN komae_demo_v4.trade_area.categories c ON sc.category_id = c.category_id
        WHERE sc.month >= '2024-01-01' AND sc.month < '2025-01-01'
        GROUP BY sc.store_id, c.category_name, c.category_id
    )
    SELECT 
        s.store_name, e.store_id,
        -- 園芸
        ROUND(e.potential_garden / 1e6, 1) AS `園芸_ポテンシャル(百万)`,
        ROUND(a_garden.actual_annual / 1e6, 1) AS `園芸_実績(百万)`,
        ROUND((e.potential_garden - a_garden.actual_annual) / 1e6, 1) AS `園芸_GAP(百万)`,
        -- DIY工具
        ROUND(e.potential_diy / 1e6, 1) AS `DIY_ポテンシャル(百万)`,
        ROUND(a_diy.actual_annual / 1e6, 1) AS `DIY_実績(百万)`,
        ROUND((e.potential_diy - a_diy.actual_annual) / 1e6, 1) AS `DIY_GAP(百万)`,
        -- ペット
        ROUND(e.potential_pet / 1e6, 1) AS `ペット_ポテンシャル(百万)`,
        ROUND(a_pet.actual_annual / 1e6, 1) AS `ペット_実績(百万)`,
        ROUND((e.potential_pet - a_pet.actual_annual) / 1e6, 1) AS `ペット_GAP(百万)`
    FROM expenditure e
    JOIN komae_demo_v4.trade_area.stores s ON e.store_id = s.store_id
    LEFT JOIN actual_sales a_garden ON e.store_id = a_garden.store_id AND a_garden.category_id = 'CAT01'
    LEFT JOIN actual_sales a_diy ON e.store_id = a_diy.store_id AND a_diy.category_id = 'CAT02'
    LEFT JOIN actual_sales a_pet ON e.store_id = a_pet.store_id AND a_pet.category_id = 'CAT10'
    WHERE e.store_id IN ({','.join([f"'{sid}'" for sid in worst3_ids])})
    ORDER BY s.store_name
""")

print("🎯 ワースト3店舗: 商圏ポテンシャル vs 実績売上")
print("※ ポテンシャル = 商圏内消費支出(千円/年/世帯) × 世帯数")
print("※ GAP(正) = 取りこぼしが大きい = 改善余地あり")
print("=" * 60)
display(potential_gap)

# COMMAND ----------

# DBTITLE 1,Lv2-3: 店舗クラスタリング
# ========================================
# Lv2-3: 商圏特性による店舗クラスタリング
# ========================================
# 使用データ: trade_profile (Lv1-2で作成済み)
# 分析手法: K-Meansクラスタリング（商圏人口、年収、高齢化率、戸建て比率）
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# クラスタリング用の特徴量
cluster_features = ['population_5km', 'avg_income', 'elderly_rate_pct', 'detached_house_pct']
X = trade_profile[cluster_features].values

# 標準化
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# K-Means (クラスタ数=4)
kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
trade_profile['cluster'] = kmeans.fit_predict(X_scaled)

# クラスタ別の特徴を表示
cluster_names = {}
print("🎯 クラスタ別の特徴")
print("=" * 80)
for c in sorted(trade_profile['cluster'].unique()):
    grp = trade_profile[trade_profile['cluster'] == c]
    n_struggling = grp[grp['store_id'].isin(struggling_ids)].shape[0]
    n_total = len(grp)
    
    # クラスタ名を特徴から命名
    pop = grp['population_5km'].mean()
    inc = grp['avg_income'].mean()
    eld = grp['elderly_rate_pct'].mean()
    det = grp['detached_house_pct'].mean()
    
    if pop > 150000 and inc > 500:
        name = '都市富裕層'
    elif pop > 100000:
        name = '都市中間層'
    elif det > 55:
        name = '郊外戸建て層'
    else:
        name = '地方コンパクト'
    cluster_names[c] = name
    
    struggling_pct = n_struggling / n_total * 100
    print(f"\n■ クラスタ{c} [{name}] ({n_total}店舗, 低迷: {n_struggling}店 = {struggling_pct:.0f}%)")
    print(f"  商圏人口: {pop:,.0f} / 年収: {inc/10000:.0f}万円 / 高齢化: {eld:.1f}% / 戸建て: {det:.1f}%")
    print(f"  前年比平均: {grp['avg_yoy_pct'].mean():.1f}% / 年売上平均: {grp['annual_sales_oku'].mean():.2f}億円")

trade_profile['cluster_name'] = trade_profile['cluster'].map(cluster_names)

# --- 可視化: クラスタの散布図 ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

cluster_colors = {0: '#1f77b4', 1: '#ff7f0e', 2: '#2ca02c', 3: '#d62728'}

for c in sorted(trade_profile['cluster'].unique()):
    grp = trade_profile[trade_profile['cluster'] == c]
    grp_ok = grp[~grp['store_id'].isin(struggling_ids)]
    grp_bad = grp[grp['store_id'].isin(struggling_ids)]
    
    # 左: 人口 vs 年収
    axes[0].scatter(grp_ok['population_5km'], grp_ok['avg_income']/10000, 
                   c=cluster_colors[c], s=80, alpha=0.6, label=f'{cluster_names[c]}', marker='o')
    axes[0].scatter(grp_bad['population_5km'], grp_bad['avg_income']/10000,
                   c=cluster_colors[c], s=120, alpha=0.9, marker='X', edgecolors='red', linewidths=2)
    
    # 右: 高齢化率 vs 前年比
    axes[1].scatter(grp_ok['elderly_rate_pct'], grp_ok['avg_yoy_pct'], 
                   c=cluster_colors[c], s=80, alpha=0.6, label=f'{cluster_names[c]}', marker='o')
    axes[1].scatter(grp_bad['elderly_rate_pct'], grp_bad['avg_yoy_pct'],
                   c=cluster_colors[c], s=120, alpha=0.9, marker='X', edgecolors='red', linewidths=2)

axes[0].set_xlabel('商圏人口')
axes[0].set_ylabel('平均年収 (万円)')
axes[0].set_title('商圏人口 × 年収によるクラスタ', fontweight='bold')
axes[0].legend(fontsize=8, loc='upper right')

axes[1].set_xlabel('高齢化率 (%)')
axes[1].set_ylabel('前年比 (%)')
axes[1].set_title('高齢化率 × 前年比によるクラスタ', fontweight='bold')
axes[1].axhline(y=-10, color='red', linestyle='--', alpha=0.3)
axes[1].legend(fontsize=8, loc='lower right')

plt.suptitle('【Lv2-3】店舗クラスタリング（×印 = 低迷店舗）', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.show()
print("○ = 通常店舗 / ×(赤枠) = 低迷店舗")

# COMMAND ----------

# DBTITLE 1,Lv2 まとめ
# MAGIC %md
# MAGIC ### 📝 Lv2 深掘分析結果まとめ
# MAGIC
# MAGIC **① 相関分析の結果:**
# MAGIC - 戸建て比率（r=-0.27）と商圏人口（r=-0.21）が売上低迷と弱い負の相関
# MAGIC - ただし統計的に有意な変数はなし → **単一の商圏特性だけでは低迷は説明できない**
# MAGIC - 競合店数はわずかに正の相関（r=+0.12）→ 競合が多い = 市場が大きいという側面も
# MAGIC
# MAGIC **② カテゴリギャップ分析:**
# MAGIC - 構成比の差は小さい（最大0.6%ポイント）→ 「特定カテゴリが極端に弱い」というより「**全カテゴリで一様に売上が低い**」
# MAGIC - 商圏ポテンシャルとのGAPは巨大: 松本市は園芸だけで**7.7億円**、DIYで**6.8億円**の取りこぼし
# MAGIC
# MAGIC **③ クラスタリング:**
# MAGIC - 「都市富裕層」クラスタの低迷率が**45%**と最高 → 人口・年収が高いのに売上が低い
# MAGIC - 「地方コンパクト」クラスタは低迷率**17%**で最も安定
# MAGIC
# MAGIC **⚡ Lv2の結論:**
# MAGIC > 「低迷の主因は商圏ポテンシャルの『巨大な取りこぼし』。特に都市型の高ポテンシャル店舗で、商圏ニーズに合った品揃え・マーケティングができていない」
# MAGIC
# MAGIC → Lv3で类似成功店舗をベンチマークとし、具体的な施策を提案します。

# COMMAND ----------

# DBTITLE 1,Lv3 施策立案 ヘッダー
# MAGIC %md
# MAGIC ---
# MAGIC # 🚀 Lv3: 施策立案 — アクション導出
# MAGIC
# MAGIC Lv2で「商圏ポテンシャルの取りこぼし」が低迷の主因とわかりました。ここでは「どうすれば改善できるか」を具体化します。
# MAGIC
# MAGIC **分析ステップ:**
# MAGIC 1. 類似成功店舗をベンチマークとして発見
# MAGIC 2. 過去施策の効果を検証し、具体的な改善施策を提案
# MAGIC 3. 総合レポートで全体像をまとめる

# COMMAND ----------

# DBTITLE 1,Lv3-1: ベンチマーク店舗の発見
# ========================================
# Lv3-1: 類似成功店舗をベンチマークとして発見
# ========================================
# 使用データ: similar_stores, sales_monthly, trade_area, store_measures
# 分析手法: ワースト3店舗それぞれの類似店舗から、売上好調な店舗をベンチマークとして抽出

benchmark = spark.sql(f"""
    WITH worst3_similar AS (
        SELECT ss.store_id AS target_id, 
               ss.similar_store_id AS benchmark_id,
               ss.similarity_rank, ss.similarity_score,
               t_store.store_name AS target_name,
               b_store.store_name AS benchmark_name,
               b_store.prefecture AS benchmark_pref,
               b_store.city AS benchmark_city,
               b_store.store_type AS benchmark_type
        FROM komae_demo_v4.trade_area.similar_stores ss
        JOIN komae_demo_v4.trade_area.stores t_store ON ss.store_id = t_store.store_id
        JOIN komae_demo_v4.trade_area.stores b_store ON ss.similar_store_id = b_store.store_id
        WHERE ss.store_id IN ({','.join([f"'{sid}'" for sid in worst3_ids])})
    ),
    benchmark_perf AS (
        SELECT store_id, 
               ROUND(AVG(yoy_change) * 100, 1) AS avg_yoy_pct,
               ROUND(SUM(sales_amount) / 1e8, 2) AS annual_sales_oku
        FROM komae_demo_v4.trade_area.sales_monthly
        WHERE month >= '2024-01-01' AND month < '2025-01-01'
        GROUP BY store_id
    )
    SELECT 
        w.target_name AS `低迷店舗`,
        w.target_id,
        w.benchmark_name AS `ベンチマーク店舗`,
        w.benchmark_id,
        w.benchmark_pref || ' ' || w.benchmark_city AS `所在地`,
        w.benchmark_type AS `タイプ`,
        w.similarity_rank AS `類似ランク`,
        w.similarity_score AS `類似度`,
        bp_target.avg_yoy_pct AS `低迷店前年比`,
        bp_bench.avg_yoy_pct AS `BM店前年比`,
        bp_bench.avg_yoy_pct - bp_target.avg_yoy_pct AS `前年比差`,
        bp_target.annual_sales_oku AS `低迷店売上(億)`,
        bp_bench.annual_sales_oku AS `BM店売上(億)`
    FROM worst3_similar w
    JOIN benchmark_perf bp_target ON w.target_id = bp_target.store_id
    JOIN benchmark_perf bp_bench ON w.benchmark_id = bp_bench.store_id
    WHERE bp_bench.avg_yoy_pct > -5  -- 好調な店舗のみ
    ORDER BY w.target_id, w.similarity_rank
""")

print("🎯 ワースト3店舗のベンチマーク（類似商圏で好調な店舗）")
print("※ 類似度が高く、かつ前年比-5%以上の店舗を抽出")
print("=" * 60)
display(benchmark)

benchmark_pd = benchmark.toPandas()

# --- ベンチマークとのカテゴリ比較 ---
print("\n📊 ベンチマーク店舗とのカテゴリ別売上比較")
print("=" * 60)

# 最も類似度が高いBM店舗を各低迷店から取得
best_benchmarks = benchmark_pd.sort_values('\u985e\u4f3c\u30e9\u30f3\u30af').groupby('target_id').first().reset_index()

for _, bm_row in best_benchmarks.iterrows():
    target_id = bm_row['target_id']
    bench_id = bm_row['benchmark_id']
    
    cat_compare = spark.sql(f"""
        SELECT c.category_name,
               ROUND(SUM(CASE WHEN sc.store_id = '{target_id}' THEN sc.sales_amount END) / 1e6, 1) AS `低迷店売上(百万)`,
               ROUND(SUM(CASE WHEN sc.store_id = '{bench_id}' THEN sc.sales_amount END) / 1e6, 1) AS `BM店売上(百万)`,
               ROUND((SUM(CASE WHEN sc.store_id = '{bench_id}' THEN sc.sales_amount END) - 
                      SUM(CASE WHEN sc.store_id = '{target_id}' THEN sc.sales_amount END)) / 1e6, 1) AS `差分(百万)`
        FROM komae_demo_v4.trade_area.sales_by_category sc
        JOIN komae_demo_v4.trade_area.categories c ON sc.category_id = c.category_id
        WHERE sc.month >= '2024-01-01' AND sc.month < '2025-01-01'
          AND sc.store_id IN ('{target_id}', '{bench_id}')
        GROUP BY c.category_name
        ORDER BY `差分(百万)` DESC
    """)
    print(f"\n■ {bm_row['低迷店舗']}({target_id}) vs {bm_row['ベンチマーク店舗']}({bench_id}) 類似度:{bm_row['類似度']}")
    display(cat_compare)

# COMMAND ----------

# DBTITLE 1,Lv3-2: 過去施策の効果検証と改善提案
# ========================================
# Lv3-2: 過去施策の効果検証 & 改善施策の提案
# ========================================
# 使用データ: store_measures, nearby_facilities
# 分析手法: 過去施策の効果を検証し、新たな施策を提案

# --- 過去施策の効果一覧 ---
measures = spark.sql("""
    SELECT 
        s.store_name AS `店舗名`,
        sm.measure_type AS `施策タイプ`,
        sm.description AS `内容`,
        sm.status AS `ステータス`,
        ROUND(sm.expected_effect * 100, 1) AS `期待効果(%)`,
        ROUND(sm.actual_effect * 100, 1) AS `実績効果(%)`,
        sm.related_categories AS `対象カテゴリ`,
        ROUND(sm.investment_amount / 1e4, 0) AS `投資額(万円)`
    FROM komae_demo_v4.trade_area.store_measures sm
    JOIN komae_demo_v4.trade_area.stores s ON sm.store_id = s.store_id
    WHERE sm.store_id IN ({worst3_sql})
    ORDER BY s.store_id, sm.start_date
""".format(worst3_sql=','.join([f"'{sid}'" for sid in worst3_ids])))

print("📋 ワースト3店舗の過去施策と効果")
print("=" * 60)
display(measures)

# --- 近隣施設の活用可能性 ---
facilities = spark.sql("""
    SELECT 
        s.store_name AS `店舗名`,
        nf.facility_type AS `施設タイプ`,
        nf.facility_name AS `施設名`,
        nf.distance_km AS `距離(km)`,
        nf.traffic_impact AS `集客影響度`,
        nf.opening_hours AS `営業時間`
    FROM komae_demo_v4.trade_area.nearby_facilities nf
    JOIN komae_demo_v4.trade_area.stores s ON nf.store_id = s.store_id
    WHERE nf.store_id IN ({worst3_sql})
      AND nf.traffic_impact >= 0.2
    ORDER BY nf.traffic_impact DESC
""".format(worst3_sql=','.join([f"'{sid}'" for sid in worst3_ids])))

print("\n🏢 近隣の集客施設（影響度0.2以上）")
print("=" * 60)
display(facilities)

# --- 施策効果の可視化 ---
measures_pd = measures.toPandas()
completed = measures_pd[measures_pd['実績効果(%)'].notna()].copy()

if len(completed) > 0:
    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(completed))
    w = 0.35
    ax.bar(x - w/2, completed['期待効果(%)'], w, label='期待効果', color='#1976d2', alpha=0.7)
    ax.bar(x + w/2, completed['実績効果(%)'], w, label='実績効果', color='#4caf50', alpha=0.8)
    labels = [f"{row['店舗名'].replace('店舗_', '')}\n{row['施策タイプ']}" for _, row in completed.iterrows()]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel('効果 (%)')
    ax.set_title('【Lv3-2】過去施策の期待効果 vs 実績効果', fontweight='bold')
    ax.legend()
    plt.tight_layout()
    plt.show()
    print("\n✅ 実績が期待を上回った施策 = 他店への横展開候補")

# COMMAND ----------

# DBTITLE 1,Lv3-3: 総合レポート
# ========================================
# Lv3-3: 全店舗の施策効果ランキング
# ========================================

all_effective_measures = spark.sql("""
    SELECT sm.measure_type AS `施策タイプ`, 
           COUNT(*) AS `実施数`,
           ROUND(AVG(sm.actual_effect) * 100, 1) AS `平均実績効果(%)`,
           ROUND(AVG(sm.expected_effect) * 100, 1) AS `平均期待効果(%)`,
           ROUND(AVG(sm.investment_amount) / 1e4, 0) AS `平均投資額(万円)`
    FROM komae_demo_v4.trade_area.store_measures sm
    WHERE sm.status IN ('完了', '効果測定中')
      AND sm.actual_effect IS NOT NULL
    GROUP BY sm.measure_type
    ORDER BY `平均実績効果(%)` DESC
""")
print("📊 全店舗の施策効果ランキング（完了・測定中のみ）")
display(all_effective_measures)

# COMMAND ----------

# DBTITLE 1,総合レポート
# MAGIC %md
# MAGIC ---
# MAGIC # 📊 商圏分析 総合レポート
# MAGIC
# MAGIC ## 1. 現状サマリー
# MAGIC
# MAGIC | 指標 | 値 |
# MAGIC | --- | --- |
# MAGIC | 分析対象店舗数 | **50店舗** |
# MAGIC | 低迷店舗数（前年比-10%以下） | **15店舗（30%）** |
# MAGIC | 分析期間 | 2024年1月～12月 |
# MAGIC
# MAGIC **ワースト3店舗:**
# MAGIC
# MAGIC | 順位 | 店舗名 | 前年比 | 年間売上 | 特徴 |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1 | 店舗\_岐阜市（S031） | **-20.7%** | 5.04億円 | 高齢化率36.4%、商圏人口4.2万人 |
# MAGIC | 2 | 店舗\_松本市（S025） | **-20.6%** | 5.02億円 | 年収577万円、商圏人口20.8万人 |
# MAGIC | 3 | 店舗\_鹿児島市（S048） | **-18.7%** | 2.98億円 | 戸建て比率68.2%、商圏人口11.8万人 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. 商圏特性の比較（低迷 vs 好調）
# MAGIC
# MAGIC | 指標 | 低迷店舗（15店） | 好調店舗（35店） | 示唆 |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 商圏人口 | **13.9万人** | 10.0万人 | 人が多いのに売れない = 取りこぼし |
# MAGIC | 平均年収 | 491万円 | 496万円 | ほぼ同等 |
# MAGIC | 高齢化率 | 25.0% | 25.6% | ほぼ同等 |
# MAGIC | 戸建て比率 | **51.8%** | 44.0% | DIY・園芸のポテンシャルがあるはず |
# MAGIC | 競合店数 | 3.3店 | 3.7店 | 競合の多少は主因ではない |
# MAGIC
# MAGIC > **ポイント:** 低迷店舗は商圏人口が「大きい」のに売上が低い。これは「商圏ポテンシャルを活かしきれていない」ことを意味します。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. 競合環境の影響
# MAGIC
# MAGIC | 競合状況 | 店舗数 | 平均前年比 |
# MAGIC | --- | --- | --- |
# MAGIC | 新規競合あり | 8店舗 | **-2.8%** |
# MAGIC | 新規競合なし | 42店舗 | -5.3% |
# MAGIC
# MAGIC > **示唆:** 新規競合の影響は限定的。「競合が少ないから安心」ではなく、「競合が少ない = 市場自体が小さい」可能性があります。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. 低迷要因の仮説検証
# MAGIC
# MAGIC | 仮説 | 結果 | 詳細 |
# MAGIC | --- | --- | --- |
# MAGIC | 商圏人口が少ないから | ❌ **否定** | 低迷店の方が人口が多い |
# MAGIC | 新規競合に奢われた | ❌ **否定** | 新規競合の影響は統計的に限定的 |
# MAGIC | 特定カテゴリが極端に弱い | ❌ **否定** | 構成比差は最大0.6%。全カテゴリ一様に低い |
# MAGIC | 商圏ポテンシャルの取りこぼし | ✅ **確認** | 松本市: 園芸7.7億、DIY6.8億のGAP |
# MAGIC | 都市型高ポテンシャル店が低迷 | ✅ **確認** | 「都市富裕層」クラスタの低迷率45% |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. ベンチマーク分析の結果
# MAGIC
# MAGIC 各低迷店舗に対し、商圏特性が似ているのに売上が好調な「お手本店舗」を特定しました。
# MAGIC
# MAGIC | 低迷店舗 | ベンチマーク | 類似度 | BM店前年比 | 最大カテゴリGAP |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 松本市（-20.6%） | 札幌市（S001） | 87.3 | **+5.8%** | 園芸 +3,060万円 |
# MAGIC | 岐阜市（-20.7%） | 福山市（S043） | 88.6 | -0.5% | ※岐阜市が上回るカテゴリもあり |
# MAGIC | 鹿児島市（-18.7%） | 函館市（S003） | **96.1** | +1.7% | 園芸 +4,000万円 |
# MAGIC
# MAGIC > **示唆:** 鹿児島市と函館市は類似度96.1と非常に高いのに前年比に20%近い差があります。函館市の成功要因を横展開できれば大きな改善が見込めます。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. 過去施策の効果検証
# MAGIC
# MAGIC 全店舗の実績データから、**最も効果が高かった施策**をランキングしました。
# MAGIC
# MAGIC | 順位 | 施策タイプ | 平均実績効果 | 平均投資額 | コストパフォーマンス |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1 | 🏆 リニューアル | **+14.2%** | 3,472万円 | ◎ 最も効果が高い |
# MAGIC | 2 | 品揃え強化 | +9.9% | 2,443万円 | ◎ 投資対効果が良い |
# MAGIC | 3 | 棚割り最適化 | +9.4% | 2,729万円 | ○ |
# MAGIC | 4 | 駐車場拡張 | +7.3% | 2,889万円 | ○ |
# MAGIC | 5 | 販促強化 | +5.7% | 3,229万円 | △ 投資対効果が低い |
# MAGIC | 6 | 価格施策 | +5.1% | 2,646万円 | △ |
# MAGIC | 7 | 接客改善 | +3.7% | 2,156万円 | △ |
# MAGIC | 8 | 営業時間延長 | +2.7% | 3,614万円 | ✖ 投資対効果が悪い |
# MAGIC
# MAGIC 松本市での実績でも、リニューアルは期待+15%に対し**実績+18.7%**、棚割り最適化は期待+8%に対し**実績+11.4%**と期待を上回っています。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7. 改善施策の提案
# MAGIC
# MAGIC ### 施策① リニューアル + 棚割り最適化 〔優先度: ⭐⭐⭐〕
# MAGIC
# MAGIC | 項目 | 内容 |
# MAGIC | --- | --- |
# MAGIC | **根拠** | 松本市での実績：リニューアル+18.7%、棚割り+11.4%（いずれも期待以上） |
# MAGIC | **対象** | 全低迷店舗（特に岐阜市・鹿児島市） |
# MAGIC | **内容** | 園芸・ペット売場の拡充、DIY初心者向けコーナー設置 |
# MAGIC | **期待効果** | 前年比 **+10～15%** 改善 |
# MAGIC | **投資規模** | 約4,000～5,000万円/店 |
# MAGIC
# MAGIC ### 施策② 地域連携・集客強化 〔優先度: ⭐⭐〕
# MAGIC
# MAGIC | 項目 | 内容 |
# MAGIC | --- | --- |
# MAGIC | **根拠** | 近隣にイオンモール（集客影響度0.61）や駅があるが連携不足 |
# MAGIC | **内容** | イオンモール客へのクロスクーポン、地域イベント協賛 |
# MAGIC | **期待効果** | 前年比 **+3～5%** 改善 |
# MAGIC | **投資規模** | 約500～1,000万円/店 |
# MAGIC
# MAGIC ### 施策③ 商圏ニーズ適応型品揃え強化 〔優先度: ⭐⭐⭐〕
# MAGIC
# MAGIC | 項目 | 内容 |
# MAGIC | --- | --- |
# MAGIC | **根拠** | カテゴリギャップ分析で「全カテゴリ一様に弱い」ことが判明 |
# MAGIC | **岐阜市** | 高齢化率36.4% → バリアフリー商品・園芸用品を強化 |
# MAGIC | **松本市** | 年収577万円・戸建て比率高 → DIY・リフォーム商品拡充 |
# MAGIC | **鹿児島市** | 戸建て比率68.2% → 園芸・エクステリア商品強化 |
# MAGIC | **期待効果** | 前年比 **+5～8%** 改善 |
# MAGIC | **投資規模** | 約2,000～3,000万円/店 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 8. 実行ロードマップ
# MAGIC
# MAGIC | 時期 | アクション | 担当 |
# MAGIC | --- | --- | --- |
# MAGIC | **Q1**（4-6月） | 商圏ニーズ分析 → 品揃え見直し計画策定 | 商品部・店舗開発部 |
# MAGIC | **Q2**（7-9月） | 岐阜市・鹿児島市のリニューアル実施 | 店舗開発部 |
# MAGIC | **Q3**（10-12月） | 効果測定 → 他店舗への横展開判断 | 経営企画部 |
# MAGIC | **Q4**（1-3月） | 次年度計画への反映 | 全部署 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **総括:** 低迷の主因は「商圏ポテンシャルの取りこぼし」であり、特に都市型の高ポテンシャル店舗で顕著です。リニューアル（実績+14.2%）と品揃え強化（+9.9%）を組み合わせることで、**前年比+10～15%の改善**が見込めます。まずは類似成功店舗（函館市・札幌市）の成功要因を詳細にヒアリングし、Q2でのリニューアル実施に向けた計画策定を推奨します。

# COMMAND ----------

# DBTITLE 1,優先店舗深掘レポート ヘッダー
# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 優先店舗 個別深掘レポート
# MAGIC
# MAGIC Lv1～Lv3の総合分析の結果、「商圏ポテンシャルの取りこぼし」が低迷の主因であることがわかりました。
# MAGIC
# MAGIC ここからは、ワースト3店舗それぞれについて **「誰が、どんな暮らしをしていて、なぜこの店に来ないのか」** を深く考察し、具体的な打ち手とその検証方法まで提示します。
# MAGIC
# MAGIC **分析対象:**
# MAGIC
# MAGIC | 優先度 | 店舗 | 前年比 | 改善ポテンシャル |
# MAGIC | --- | --- | --- | --- |
# MAGIC | ★★★ | 鹿児島市（S048） | -18.7% | ベンチマーク類似度96.1、改善余地最大 |
# MAGIC | ★★★ | 松本市（S025） | -20.6% | 20万人商圏の巨大ポテンシャル |
# MAGIC | ★★ | 岐阜市（S031） | -20.7% | 高齢化率最高、世帯単価の改善が鍵 |

# COMMAND ----------

# DBTITLE 1,優先店舗: データ収集と可視化
import datetime

# ========================================
# 優先店舗 深掘分析用データ収集 & 可視化
# ========================================
# 使用データ: sales_monthly, sales_by_category, trade_area_expenditure,
#             trade_area, stores, competitors, nearby_facilities, store_measures

# --- データ収集 ---
worst3_ids = ['S031', 'S025', 'S048']
bench_ids = ['S038', 'S001', 'S003']  # 各店のベストベンチマーク
pairs = [('S048', 'S003', '鹿児島市', '函館市'),
         ('S025', 'S001', '松本市', '札幌市'),
         ('S031', 'S038', '岐阜市', '姫路市')]

all_ids = worst3_ids + bench_ids
all_ids_sql = ','.join([f"'{s}'" for s in all_ids])

monthly = spark.sql(f"""
    SELECT s.store_name, sm.store_id, sm.month, 
           sm.sales_amount, sm.customer_count, sm.avg_basket,
           ROUND(sm.yoy_change * 100, 1) AS yoy_pct
    FROM komae_demo_v4.trade_area.sales_monthly sm
    JOIN komae_demo_v4.trade_area.stores s ON sm.store_id = s.store_id
    WHERE sm.store_id IN ({all_ids_sql}) AND sm.month >= '2023-01-01'
    ORDER BY sm.store_id, sm.month
""").toPandas()

cat_sales = spark.sql(f"""
    SELECT sc.store_id, c.category_name,
           SUM(sc.sales_amount) AS annual_sales
    FROM komae_demo_v4.trade_area.sales_by_category sc
    JOIN komae_demo_v4.trade_area.categories c ON sc.category_id = c.category_id
    WHERE sc.store_id IN ({all_ids_sql})
      AND sc.month >= '2024-01-01' AND sc.month < '2025-01-01'
    GROUP BY sc.store_id, c.category_name
    ORDER BY sc.store_id, c.category_name
""").toPandas()

# --- ① 月次売上推移: 低迷店 vs ベンチマーク ---
fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True)

for idx, (w_id, b_id, w_name, b_name) in enumerate(pairs):
    ax = axes[idx]
    w_data = monthly[monthly['store_id'] == w_id].copy()
    b_data = monthly[monthly['store_id'] == b_id].copy()
    
    ax.plot(w_data['month'], w_data['sales_amount']/1e6, 
            'o-', color='#d32f2f', linewidth=2, markersize=4, label=f'{w_name}(低迷)', alpha=0.9)
    ax.plot(b_data['month'], b_data['sales_amount']/1e6, 
            's--', color='#4caf50', linewidth=2, markersize=4, label=f'{b_name}(BM)', alpha=0.9)
    ax.fill_between(w_data['month'], w_data['sales_amount']/1e6, 
                    b_data['sales_amount'].values[:len(w_data)]/1e6, alpha=0.1, color='red')
    ax.set_ylabel('売上 (百万円)')
    ax.set_title(f'{w_name} vs {b_name}（ベンチマーク）', fontweight='bold')
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)
    # 2024年開始ライン
    d2024 = datetime.date(2024, 1, 1)
    ax.axvline(x=d2024, color='gray', linestyle=':', alpha=0.5)
    ax.annotate('2024年', (d2024, ax.get_ylim()[1]*0.95), fontsize=8, color='gray')

plt.suptitle('【深掘】低迷店舗 vs ベンチマーク店舗の売上推移', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()
print('🔴赤線=低迷店舗 / 🟢緑線=ベンチマーク / 赤网掛け=売上差分')

# --- ② カテゴリ別売上比較: 各ペア ---
fig, axes = plt.subplots(1, 3, figsize=(18, 6))

for idx, (w_id, b_id, w_name, b_name) in enumerate(pairs):
    ax = axes[idx]
    w_cat = cat_sales[cat_sales['store_id'] == w_id].set_index('category_name')['annual_sales']
    b_cat = cat_sales[cat_sales['store_id'] == b_id].set_index('category_name')['annual_sales']
    cats = w_cat.index.tolist()
    
    x = np.arange(len(cats))
    w = 0.35
    ax.barh(x - w/2, w_cat.values/1e6, w, label=f'{w_name}', color='#d32f2f', alpha=0.8)
    ax.barh(x + w/2, b_cat.reindex(cats).values/1e6, w, label=f'{b_name}(BM)', color='#4caf50', alpha=0.8)
    ax.set_yticks(x)
    ax.set_yticklabels([c.replace('用品', '') for c in cats], fontsize=8)
    ax.set_xlabel('売上 (百万円)')
    ax.set_title(f'{w_name} vs {b_name}', fontweight='bold', fontsize=11)
    ax.legend(fontsize=8)
    ax.grid(axis='x', alpha=0.3)

plt.suptitle('【深掘】カテゴリ別売上: 低迷 vs ベンチマーク', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()
