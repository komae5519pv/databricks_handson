# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Spaceを作る（商圏分析）

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Genie Spaceを作る
# MAGIC
# MAGIC カタログエクスプローラーからテーブルを選択し「Genieスペースを作成」をクリック
# MAGIC
# MAGIC ![Genieスペース作成](./_image/1_create_genie.png)
# MAGIC
# MAGIC Genieに登録したいテーブルを全て選ぶ
# MAGIC
# MAGIC ![テーブル選択](./_image/2_select_tables.png)
# MAGIC
# MAGIC ### 登録するテーブル
# MAGIC
# MAGIC | テーブル名 | 説明 |
# MAGIC |-----------|------|
# MAGIC | `komae_demo_v4.trade_area.stores` | 店舗マスタ |
# MAGIC | `komae_demo_v4.trade_area.sales_monthly` | 月別売上 |
# MAGIC | `komae_demo_v4.trade_area.sales_by_category` | カテゴリ別売上 |
# MAGIC | `komae_demo_v4.trade_area.trade_area` | 商圏情報（人口統計） |
# MAGIC | `komae_demo_v4.trade_area.trade_area_expenditure` | 商圏消費支出（ゼンリン形式） |
# MAGIC | `komae_demo_v4.trade_area.competitors` | 競合店舗 |
# MAGIC | `komae_demo_v4.trade_area.categories` | カテゴリマスタ |
# MAGIC
# MAGIC ### 設定内容
# MAGIC
# MAGIC - **Title**: `商圏分析アシスタント`
# MAGIC - **Description**: `ホームセンター店舗の商圏分析を支援。売上低迷店舗の要因特定、競合影響分析、改善施策の提案まで一貫してサポートします。`
# MAGIC - **Default Warehouse**: <任意のSQL Warehouse>
# MAGIC
# MAGIC ### サンプル質問
# MAGIC
# MAGIC ```
# MAGIC 売上が前年比10%以上落ちている店舗を特定してください
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 最も低迷している店舗の商圏特性を全店平均と比較してください
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 低迷店舗のカテゴリ別売上を分析し、弱いカテゴリを特定してください
# MAGIC ```
# MAGIC
# MAGIC ### 一般的な指示（General Instructions）
# MAGIC
# MAGIC ```
# MAGIC あなたはホームセンターチェーンの商圏分析アシスタントです。
# MAGIC
# MAGIC ## 役割
# MAGIC - 店舗の売上低迷要因を特定し、改善施策を提案する
# MAGIC - 商圏特性（人口、世帯年収、消費支出ポテンシャル）と売上の関係を分析する
# MAGIC - 競合店舗の影響を定量的に評価する
# MAGIC
# MAGIC ## 必ず守るルール
# MAGIC
# MAGIC 1. **日本語で回答してください**
# MAGIC
# MAGIC 2. **分析プロセスを説明してください**
# MAGIC    - 使用したテーブルとカラム
# MAGIC    - 分析手法（集計方法、比較対象など）
# MAGIC    - 分析期間やフィルタ条件
# MAGIC
# MAGIC 3. **全データソースを活用してください**
# MAGIC    - 売上実績: sales_monthly, sales_by_category
# MAGIC    - 商圏ポテンシャル: trade_area_expenditure（消費支出データ）
# MAGIC    - 人口統計: trade_area
# MAGIC    - 競合環境: competitors
# MAGIC
# MAGIC 4. **ギャップ分析を実施してください**
# MAGIC    - 商圏消費支出ポテンシャル（trade_area_expenditure）と実績売上（sales_by_category）を比較
# MAGIC    - ポテンシャルが高いのに売上が低いカテゴリを特定
# MAGIC
# MAGIC 5. **全店平均との比較を含めてください**
# MAGIC    - 対象店舗の数値だけでなく、全店平均も併記
# MAGIC    - 差分を明確に示す
# MAGIC
# MAGIC 6. **回答の最後に次のステップを提案してください**
# MAGIC    - 「より詳細に分析するには〇〇を確認することをお勧めします」
# MAGIC    - 深掘りの質問案を2-3個提示
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Genie Spaceに権限を付与する
# MAGIC
# MAGIC デモ用に `all workspace users` に権限を付与
# MAGIC
# MAGIC 1. Genieスペースの右上「共有」をクリック
# MAGIC 2. `all workspace users` を追加
# MAGIC 3. 権限レベル: `Can query`（または `Can edit`）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. デモの流れ
# MAGIC
# MAGIC ### Lv1 基礎分析（現状把握）
# MAGIC
# MAGIC | 質問 | 期待される分析 |
# MAGIC |------|---------------|
# MAGIC | 売上が前年比10%以上落ちている店舗を教えて | 低迷店舗リストの抽出 |
# MAGIC | 最も低迷している店舗の商圏特性を全店平均と比較して | 商圏プロファイル比較 |
# MAGIC | 低迷店舗周辺の競合状況を教えて。特に2024年以降の新規出店をハイライトして | 競合分析 |
# MAGIC
# MAGIC ### Lv2 深掘分析（要因特定）
# MAGIC
# MAGIC | 質問 | 期待される分析 |
# MAGIC |------|---------------|
# MAGIC | 低迷店舗のカテゴリ別売上を全店平均と比較して、弱いカテゴリを特定して | カテゴリ構成比分析 |
# MAGIC | 商圏の消費支出ポテンシャルと実績売上を比較して、取りこぼしているカテゴリを見つけて | ギャップ分析 |
# MAGIC | 新規競合がある店舗とない店舗で、売上前年比を比較して | 競合影響の定量化 |
# MAGIC
# MAGIC ### Lv3 施策立案（アクション導出）
# MAGIC
# MAGIC | 質問 | 期待される分析 |
# MAGIC |------|---------------|
# MAGIC | 低迷店舗と商圏特性が似ていて売上好調な店舗を見つけて、違いを分析して | ベンチマーク分析 |
# MAGIC | 分析結果に基づいて、具体的な改善施策を3つ提案して | アクションプラン生成 |
# MAGIC | 経営層向けに、低迷店舗の現状と改善提案をまとめたレポートを作成して | 総合レポート |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### エージェントモードでの質問例
# MAGIC
# MAGIC ```
# MAGIC 売上低迷店舗の要因を特定し、類似成功店舗との比較から改善策を提案してください。分析は基礎分析→深掘分析→施策立案の順で進めてください。
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 商圏の消費支出ポテンシャルを活用して、各店舗の「取りこぼし」カテゴリを特定し、売上改善余地が大きい店舗TOP5とその改善施策を提案してください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 補足：テーブル間の関係
# MAGIC
# MAGIC ```
# MAGIC stores (PK: store_id)
# MAGIC   │
# MAGIC   ├── trade_area (FK: store_id) ─────────── 人口・世帯・年収など
# MAGIC   ├── trade_area_expenditure (FK: store_id) ── 消費支出ポテンシャル
# MAGIC   ├── competitors (FK: store_id) ─────────── 競合店舗情報
# MAGIC   ├── sales_monthly (FK: store_id) ────────── 月別売上実績
# MAGIC   └── sales_by_category (FK: store_id, category_id) ─ カテゴリ別売上
# MAGIC                              │
# MAGIC categories (PK: category_id) ─┘
# MAGIC ```
# MAGIC
# MAGIC ### 重要なカラム
# MAGIC
# MAGIC | テーブル | 重要カラム | 用途 |
# MAGIC |---------|-----------|------|
# MAGIC | sales_monthly | yoy_change | 前年比で低迷店舗を特定 |
# MAGIC | trade_area | population_5km, avg_income, elderly_rate | 商圏特性の把握 |
# MAGIC | trade_area_expenditure | garden_supplies, pet_food, housing_repair_total | カテゴリ別ポテンシャル |
# MAGIC | competitors | is_new_entry, distance_km | 新規競合の影響分析 |
# MAGIC | categories | expenditure_mapping | 消費支出との対応関係 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 参考資料
# MAGIC
# MAGIC - [商圏分析とは？](https://www.zenrin-ms.co.jp/blog/2024/013/) - ゼンリンマーケティングソリューションズ
# MAGIC - [ゼンリン消費支出データ項目一覧](https://www.giken.co.jp/static/2025/02/expenditure2023.pdf)
# MAGIC - [Databricks AI/BI Genie ドキュメント](https://docs.databricks.com/ja/genie/index.html)
