---
name: trade-area-analysis
description: |
  小売店舗の商圏分析を行うスキルセット。
  売上低迷店舗の要因特定、競合影響分析、改善施策の提案まで一貫して実行できる。
  DCMホールディングス向けPoC用に設計。
---

# 商圏分析スキル（Trade Area Analysis）

このスキルは、小売店舗の商圏分析を体系的に行うためのフレームワークを提供します。

## 分析フレームワーク

商圏分析は3つのレベルで構成されます：

| レベル | 目的 | 含まれるスキル |
|--------|------|---------------|
| **Lv1: 基礎分析** | 現状把握 | store_performance, trade_area_profile, competitor_analysis |
| **Lv2: 深掘分析** | 要因特定 | huff_model, gap_analysis, store_clustering |
| **Lv3: 施策立案** | アクション導出 | benchmark, prioritize, action_plan |

## 使い方

### 基本的な分析フロー

1. **低迷店舗の特定**: `analyze_store_performance()` で売上低迷店舗を抽出
2. **商圏特性の把握**: `get_trade_area_profile(store_id)` で商圏情報を取得
3. **競合状況の確認**: `analyze_competitors(store_id)` で競合店舗を分析
4. **要因の深掘り**: `calculate_huff_model()` や `analyze_sales_gap()` で原因特定
5. **改善策の提案**: `find_benchmark_stores()` と `generate_action_plan()` で施策立案

### 例：売上低迷店舗の分析

```python
# Step 1: 低迷店舗を特定
underperforming = analyze_store_performance(threshold=-0.10)
print(f"売上前年比-10%以上の店舗: {len(underperforming)}店")

# Step 2: 最も深刻な店舗の商圏を分析
worst_store = underperforming.iloc[0]['store_id']
profile = get_trade_area_profile(worst_store)
competitors = analyze_competitors(worst_store)

# Step 3: 要因を深掘り
gap = analyze_sales_gap(worst_store)
huff = calculate_huff_model(worst_store)

# Step 4: 類似成功店舗と比較
benchmarks = find_benchmark_stores(worst_store, top_n=3)

# Step 5: 改善策を提案
actions = generate_action_plan(worst_store, benchmarks)
```

## 利用可能なスキル一覧

### Lv1: 基礎分析

#### `analyze_store_performance(threshold=-0.10)`
売上パフォーマンスを分析し、低迷店舗を特定する。

**パラメータ:**
- `threshold`: 前年比の閾値（デフォルト: -0.10 = -10%）

**戻り値:** 店舗別のKPIサマリー（売上、前年比、坪効率など）

---

#### `get_trade_area_profile(store_id)`
指定店舗の商圏特性を取得する。

**パラメータ:**
- `store_id`: 店舗ID

**戻り値:** 商圏プロファイル（人口、世帯数、平均年収、年齢分布、住居形態など）

---

#### `analyze_competitors(store_id, radius_km=5)`
指定店舗周辺の競合店舗を分析する。

**パラメータ:**
- `store_id`: 店舗ID
- `radius_km`: 分析半径（デフォルト: 5km）

**戻り値:** 競合店舗リスト（距離、業態、売場面積、出店時期など）

---

### Lv2: 深掘分析

#### `calculate_huff_model(store_id)`
ハフモデルを使用して競合を考慮した商圏シェアを推定する。

**パラメータ:**
- `store_id`: 店舗ID

**戻り値:** シェア推定値、競合影響度、感度分析結果

---

#### `analyze_sales_gap(store_id)`
商圏ポテンシャルと実績売上のギャップを分析する。

**パラメータ:**
- `store_id`: 店舗ID

**戻り値:** カテゴリ別のギャップ（ポテンシャル、実績、乖離率）

---

#### `cluster_stores(n_clusters=5)`
商圏特性に基づいて店舗をクラスタリングする。

**パラメータ:**
- `n_clusters`: クラスタ数（デフォルト: 5）

**戻り値:** 店舗クラスタ分類、各クラスタの特徴

---

### Lv3: 施策立案

#### `find_benchmark_stores(store_id, top_n=3)`
商圏が類似していて売上好調な店舗を発見する。

**パラメータ:**
- `store_id`: 対象店舗ID
- `top_n`: 取得する類似店舗数

**戻り値:** 類似成功店舗リスト、差分分析結果

---

#### `prioritize_actions(store_id, actions)`
改善施策の優先順位を付ける（インパクト × 実現容易性）。

**パラメータ:**
- `store_id`: 店舗ID
- `actions`: 施策リスト

**戻り値:** 優先度マトリクス（2x2）

---

#### `generate_action_plan(store_id, benchmark_stores)`
具体的な改善アクションプランを生成する。

**パラメータ:**
- `store_id`: 対象店舗ID
- `benchmark_stores`: ベンチマーク店舗リスト

**戻り値:** 施策一覧（内容、期待効果、優先度）

---

## データテーブル

このスキルは以下のテーブルを使用します：

| テーブル名 | 説明 |
|-----------|------|
| `stores` | 店舗マスタ（ID、名前、所在地、緯度経度、面積など） |
| `sales_monthly` | 月別売上（店舗ID、月、売上、客数、客単価など） |
| `trade_area` | 商圏情報（店舗ID、人口、世帯数、年収、年齢分布など） |
| `competitors` | 競合店舗（店舗ID、競合名、距離、業態、面積など） |
| `categories` | カテゴリマスタ（カテゴリID、名前、ポテンシャル係数など） |

## 注意事項

- すべてのスキルは `spark` セッションが利用可能であることを前提としています
- 可視化には `matplotlib` と `plotly` を使用します
- 大規模データの場合は処理時間がかかる場合があります
