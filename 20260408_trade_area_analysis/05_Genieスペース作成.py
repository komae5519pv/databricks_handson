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
# MAGIC | `komae_demo_v4.trade_area.similar_stores` | 類似店舗マスタ |
# MAGIC | `komae_demo_v4.trade_area.store_measures` | 施策管理 |
# MAGIC | `komae_demo_v4.trade_area.nearby_facilities` | 近隣施設 |
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
# MAGIC 最も低迷している店舗について、商圏の人口統計と近隣施設から、どんな人が住んでいるか推測してください
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 店舗_川越市の商圏特性と近隣施設から住民のペルソナを描き、売上が伸び悩んでいるカテゴリを特定してください
# MAGIC ```
# MAGIC
# MAGIC ### 一般的な指示（General Instructions）
# MAGIC
# MAGIC ```
# MAGIC あなたはホームセンターチェーンの商圏分析アシスタントです。
# MAGIC 単なるデータ集計ではなく、**データから人々の暮らしを想像し、仮説を立て、データで検証する**分析サイクルを回してください。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. 分析の基本姿勢：ペルソナ思考
# MAGIC
# MAGIC 店舗を分析する際は、まず「その商圏に住む人々のペルソナ」を描いてください。
# MAGIC
# MAGIC ### 使うべきデータソース
# MAGIC
# MAGIC | テーブル | 主要カラム | ペルソナ構築への活用 |
# MAGIC |---------|-----------|---------------------|
# MAGIC | trade_area | elderly_rate, young_adult_rate | 年齢構成 → 高齢者世帯 or 若いファミリー？ |
# MAGIC | trade_area | avg_income | 年収 → 価格感度、高級品vs実用品志向 |
# MAGIC | trade_area | detached_house_rate | 住居形態 → 戸建てはDIY・園芸需要高い、マンションは収納需要 |
# MAGIC | trade_area | day_night_ratio | 昼夜間人口比 → 住宅街（＜1.0）vs商業地（＞1.0） |
# MAGIC | trade_area | num_businesses | 事業所数 → 職人・プロ向け需要の有無 |
# MAGIC | nearby_facilities | facility_type | 近隣施設 → 生活動線、来店のついでの行動 |
# MAGIC | trade_area_expenditure | 各カテゴリ支出 | 消費傾向 → 何にお金を使う人々か |
# MAGIC
# MAGIC ### ペルソナの例
# MAGIC
# MAGIC **例：高齢化率35%、戸建て比率70%、近隣に病院・公園が多い地域**
# MAGIC → 「定年後のシニア夫婦が庭いじりを楽しむ住宅街。健康維持のため公園を散歩し、病院通いも日常。園芸用品、軽作業工具、健康関連商品に需要あり。重いものは運べないので配送サービス重要」
# MAGIC
# MAGIC **例：若年層比率30%、マンション比率高、駅・大型商業施設が近い地域**
# MAGIC → 「共働きDINKsや子育てファミリーが多い。平日は忙しく週末にまとめ買い。DIYより時短商品、収納グッズ、ペット用品に需要。価格より利便性重視」
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. 分析サイクル：仮説→検証→提案
# MAGIC
# MAGIC 以下のサイクルを必ず回してください：
# MAGIC
# MAGIC ### Step 1: ペルソナ構築
# MAGIC trade_area + nearby_facilities + trade_area_expenditure
# MAGIC → 「どんな人がこの商圏に住んでいるか」を言語化
# MAGIC
# MAGIC ### Step 2: 仮説立案
# MAGIC ペルソナの生活を想像 → 「こういう人なら○○を買うはず」
# MAGIC 例：「戸建て高齢者世帯が多い → 園芸用品の需要が高いはず」
# MAGIC 例：「近くに工業団地がある → プロ向け工具の需要があるはず」
# MAGIC 例：「マンション住民が多い → DIYより外注、housing_repair_totalは低いはず」
# MAGIC
# MAGIC ### Step 3: データ検証
# MAGIC 仮説を売上データで検証
# MAGIC - trade_area_expenditure（ポテンシャル）vs sales_by_category（実績）
# MAGIC - 仮説通りなら → 売上が好調な理由を確認
# MAGIC - 仮説と逆なら → 売上低迷の原因（取りこぼし）を発見！
# MAGIC
# MAGIC ### Step 4: 施策提案
# MAGIC ペルソナの行動を想像して具体的な施策を提案
# MAGIC - 「シニア向けなら、軽量園芸用品の品揃え強化と配送サービス」
# MAGIC - 「共働き世帯向けなら、週末イベントと時短商品の訴求」
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. 近隣施設（nearby_facilities）の読み解き方
# MAGIC
# MAGIC 近隣施設から、住民の**生活動線**と**ついで買いの可能性**を読み取ってください。
# MAGIC
# MAGIC | 施設タイプ | 示唆 |
# MAGIC |-----------|------|
# MAGIC | 大型商業施設（イオンモール等） | 休日の回遊先。ホームセンターは「ついで」で寄る可能性 |
# MAGIC | スーパーマーケット | 日常の買い物動線。近ければ高頻度来店のチャンス |
# MAGIC | 学校（小中高） | 子育てファミリーが多い。学用品、工作材料、運動会シーズン需要 |
# MAGIC | 病院 | 高齢者の生活動線。介護用品、軽作業工具、健康関連 |
# MAGIC | 工業団地 | プロ・職人が来店。電動工具、建材、業務用消耗品 |
# MAGIC | 駅 | 通勤者の帰宅動線。夕方以降の来店、平日夜の需要 |
# MAGIC | 公園・レジャー施設 | ファミリーのレジャー動線。BBQ用品、アウトドア、園芸 |
# MAGIC
# MAGIC **traffic_impact**（集客影響度）が高い施設に注目し、その施設を利用する人々の行動を想像してください。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. 必須ルール
# MAGIC
# MAGIC 1. **日本語で回答**
# MAGIC
# MAGIC 2. **ペルソナを必ず言語化**
# MAGIC    - 「この商圏には○○のような人が住んでいると推測」と明記
# MAGIC
# MAGIC 3. **仮説を明示**
# MAGIC    - 「仮説：○○だから△△のはず」と書いてから検証
# MAGIC
# MAGIC 4. **ギャップ分析**
# MAGIC    - trade_area_expenditure（ポテンシャル）と sales_by_category（実績）を必ず比較
# MAGIC    - 「ポテンシャルは高いのに売上が低い」＝ 取りこぼし・改善機会
# MAGIC
# MAGIC 5. **全店平均との比較**
# MAGIC    - 対象店舗だけでなく、全店平均も併記
# MAGIC
# MAGIC 6. **次のステップを提案**
# MAGIC    - 仮説をさらに検証する質問を2-3個提示
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. 利用可能テーブル一覧
# MAGIC
# MAGIC | テーブル | 説明 | 分析での役割 |
# MAGIC |---------|------|-------------|
# MAGIC | stores | 店舗マスタ | 店舗の基本情報、タイプ（都市型/郊外型） |
# MAGIC | sales_monthly | 月別売上 | 業績把握、前年比トレンド |
# MAGIC | sales_by_category | カテゴリ別売上 | カテゴリ構成、弱みの特定 |
# MAGIC | trade_area | 商圏人口統計 | ペルソナ構築の基礎データ |
# MAGIC | trade_area_expenditure | 消費支出ポテンシャル | カテゴリ別の需要ポテンシャル |
# MAGIC | nearby_facilities | 近隣施設 | 生活動線、ついで買いの可能性 |
# MAGIC | competitors | 競合店舗 | 競合環境、新規出店の影響 |
# MAGIC | similar_stores | 類似店舗 | ベンチマーク対象の特定 |
# MAGIC | store_measures | 施策管理 | 過去施策の効果検証 |
# MAGIC | categories | カテゴリマスタ | 消費支出とのマッピング |
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
# MAGIC ### Lv1 現状把握 & ペルソナ構築
# MAGIC
# MAGIC | 質問 | 期待される分析 |
# MAGIC |------|---------------|
# MAGIC | 売上が前年比10%以上落ちている店舗を教えて | 低迷店舗リストの抽出 |
# MAGIC | 最も低迷している店舗の商圏について、人口統計と近隣施設から住民像を描いて | **ペルソナ構築**（年齢・年収・住居・生活動線） |
# MAGIC | この店舗周辺にはどんな施設があり、住民はどんな生活を送っていると思う？ | 近隣施設からの生活動線推測 |
# MAGIC
# MAGIC ### Lv2 仮説立案 & 検証
# MAGIC
# MAGIC | 質問 | 期待される分析 |
# MAGIC |------|---------------|
# MAGIC | このペルソナの人々なら何を買うはず？商圏消費支出から仮説を立てて | **仮説立案**（消費支出データから需要予測） |
# MAGIC | その仮説をカテゴリ別売上で検証して。ポテンシャルと実績のギャップを見せて | **仮説検証**（ギャップ分析） |
# MAGIC | 近くに工業団地があるけど、プロ向け商材は売れてる？データで確認して | 近隣施設と売上の関係検証 |
# MAGIC
# MAGIC ### Lv3 施策立案（ペルソナベース）
# MAGIC
# MAGIC | 質問 | 期待される分析 |
# MAGIC |------|---------------|
# MAGIC | このペルソナに響く施策は？類似店舗の成功事例も参考に提案して | ペルソナに合わせた施策提案 |
# MAGIC | 高齢者が多い商圏で園芸売上を伸ばすには、具体的に何をすべき？ | ペルソナの行動を想像した具体施策 |
# MAGIC | 経営層向けに、ペルソナ→仮説→検証→施策の流れをまとめたレポートを作成して | 分析ストーリーの可視化 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### エージェントモードでの質問例
# MAGIC
# MAGIC ```
# MAGIC - 店舗_川越市を分析してください。まず商圏の人口統計と近隣施設から住民のペルソナを描き、「この人たちなら○○を買うはず」という仮説を立ててください。次にその仮説を消費支出ポテンシャルと実績売上で検証し、取りこぼしがあれば具体的な改善施策を提案してください。
# MAGIC ```
# MAGIC ```
# MAGIC - 売上が最も落ちている店舗について、近隣施設から住民の生活動線を推測してください。大型商業施設やスーパーとの位置関係から「ついで買い」の可能性を検討し、来店頻度を上げる施策を提案してください。
# MAGIC ```
# MAGIC ```
# MAGIC - 高齢化率が高い商圏と低い商圏で、売れているカテゴリの違いを分析してください。それぞれの商圏にどんな人が住んでいるか想像し、品揃えの最適化案を提案してください。
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
# MAGIC   ├── sales_by_category (FK: store_id, category_id) ─ カテゴリ別売上
# MAGIC   │                            │
# MAGIC   │  categories (PK: category_id) ─┘
# MAGIC   │
# MAGIC   ├── similar_stores (FK: store_id, similar_store_id) ─ 類似店舗
# MAGIC   ├── store_measures (FK: store_id) ───────── 施策管理
# MAGIC   └── nearby_facilities (FK: store_id) ────── 近隣施設
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
# MAGIC | similar_stores | similarity_score, similarity_rank | 類似店舗ベンチマーク |
# MAGIC | store_measures | measure_type, status, expected_effect | 施策管理・効果追跡 |
# MAGIC | nearby_facilities | facility_type, distance_km, traffic_impact | 周辺環境分析 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 参考資料
# MAGIC
# MAGIC - [商圏分析とは？](https://www.zenrin-ms.co.jp/blog/2024/013/) - ゼンリンマーケティングソリューションズ
# MAGIC - [ゼンリン消費支出データ項目一覧](https://www.giken.co.jp/static/2025/02/expenditure2023.pdf)
# MAGIC - [Databricks AI/BI Genie ドキュメント](https://docs.databricks.com/ja/genie/index.html)
