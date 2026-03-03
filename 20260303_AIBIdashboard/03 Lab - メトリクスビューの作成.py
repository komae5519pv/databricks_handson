# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # ラボ - AI/BIダッシュボード用のメトリックビュー作成
# MAGIC ## 概要
# MAGIC
# MAGIC このハンズオンラボでは、Databricks Unity Catalogを使用してAI/BIダッシュボード向けの包括的な[メトリックビュー](https://docs.databricks.com/aws/en/metric-views/)を作成する方法を案内します。カナダの[プーティン](https://ja.wikipedia.org/wiki/%E3%83%97%E3%83%BC%E3%83%86%E3%82%A3%E3%83%B3)販売データセットを用いて、ビジネスメトリックを標準化し、組織全体で一貫した分析を可能にするセマンティックレイヤーを構築します。
# MAGIC
# MAGIC このラボでは、[スター・スキーマ](https://www.databricks.com/blog/five-simple-steps-for-implementing-a-star-schema-in-databricks-with-delta-lake)を形成する3つの相互接続されたテーブルを探索し、[ファクトテーブルとディメンションテーブル](https://www.databricks.com/blog/implementing-dimensional-data-warehouse-databricks-sql-part-1)間の結合を作成し、リッチなセマンティックメタデータを持つビジネスメトリックを定義します。作成したメトリックビューはAI/BIダッシュボードの基盤となり、ビジネスロジックを集中管理し、すべての下流利用者が信頼できる一貫した定義で作業できることを示します。
# MAGIC
# MAGIC このラボはUnity Catalogのメトリックビュー機能の実践的な活用に重点を置いており、セマンティックレイヤーがアナリスト、ビジネスユーザー、AIツールによるデータ利用を簡素化しつつ、データガバナンスと一貫性を維持する方法を紹介します。
# MAGIC ## 学習目標
# MAGIC
# MAGIC このノートブックを終えると、以下ができるようになります：
# MAGIC
# MAGIC - ファクトテーブルとディメンションテーブル間のスター・スキーマ関係を調べ、分析用データモデリングを理解する
# MAGIC
# MAGIC - YAML構成を用いてビジネスメトリック、結合、集計ロジックを集中管理するセマンティックレイヤーを構築する
# MAGIC
# MAGIC - 表示名、フォーマット、同義語、コメントなどのメトリックビューのセマンティックメタデータを定義し、使いやすさを向上させる
# MAGIC
# MAGIC - メトリックビューをAI/BIダッシュボード構築の基盤として活用する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 必須 - コンピュート環境の選択
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">SQLウェアハウスを選択してください</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC このノートブックを開始する前に、必要なコンピュート環境を選択してください。
# MAGIC - **SQLウェアハウス**（X-Smallで十分です）
# MAGIC
# MAGIC SQLウェアハウスの選択方法:
# MAGIC
# MAGIC 1. ノートブック右上のコンピュートタイルを選択します。
# MAGIC 2. SQLウェアハウスが表示されている場合は、それを選択します。
# MAGIC 3. 表示されていない場合は、**More** > **SQL Warehouse** を選択し、リストからSQLウェアハウスを選択してください。
# MAGIC
# MAGIC **注意:** このノートブックは **SQLウェアハウスで開発・テストされています**。他のコンピュートオプションでも動作する場合がありますが、同じ動作やすべての機能を保証するものではありません。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. 必須 - 教室セットアップ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### 必須 - 環境セットアップ
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     0 - 必須セットアップノートブック
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC   このノートブックを実行する前に、前のノートブック <strong>0 - 必須セットアップ</strong> を実行し、カタログ、スキーマ、ファイルの準備が完了していることを確認してください。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 次に、下のセルを実行して、デフォルトのカタログとスキーマを設定し、**labuser.poutine_aibi** スキーマ内に必要なテーブルを作成します。

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC 2. ノートブックのサイドバーで **Catalog** アイコンを選択し、**labuser_yourusername** カタログと **poutine_aibi** スキーマに移動します（必要に応じてカタログリストを更新）。次の内容を確認してください：
# MAGIC
# MAGIC    - **labuser_yourusername** という名前のカタログが存在する  
# MAGIC       - Vocareumでは、**labuser12345...** のように表示される場合があります
# MAGIC
# MAGIC    - カタログ内に **poutine_aibi** という名前のスキーマが存在する
# MAGIC
# MAGIC    - **poutine_aibi** スキーマに以下のテーブルが含まれていること：
# MAGIC       - **line_items_poutine_sales**
# MAGIC       - **products_poutine**
# MAGIC       - **stores_canada**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. テーブルの探索
# MAGIC
# MAGIC 始める前に、ワークショップで使用するテーブルを確認しましょう。データの構造と関係性を理解することで、例題やメトリック作成がより分かりやすくなります。
# MAGIC
# MAGIC 下図は、ファクトテーブルが2つのディメンションテーブルとどのように接続されているか、そしてワークショップ全体で使用される結合関係を示しています。この論理モデルは、後ほどメトリックビュー作成時にも利用されます。
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC #### 論理データモデル（スター・スキーマ）
# MAGIC
# MAGIC
# MAGIC <div class="mermaid">
# MAGIC erDiagram
# MAGIC     line_items_poutine_sales {
# MAGIC         string transaction_id
# MAGIC         int line_id
# MAGIC         int product_id
# MAGIC         int store_id
# MAGIC         timestamp transaction_ts
# MAGIC     }
# MAGIC     products_poutine {
# MAGIC         int product_id
# MAGIC         string product_name
# MAGIC         string category
# MAGIC         string subcategory
# MAGIC         decimal unit_price
# MAGIC         decimal unit_cost
# MAGIC     }
# MAGIC     stores_canada {
# MAGIC         int store_id
# MAGIC         string location
# MAGIC         float latitude
# MAGIC         float longitude
# MAGIC     }
# MAGIC     line_items_poutine_sales }o--|| products_poutine : "line_items_poutine_sales.product_id = products_poutine.product_id"
# MAGIC     line_items_poutine_sales }o--|| stores_canada : "line_items_poutine_sales.store_id = stores_canada.store_id"
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default" });
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. 売上テーブルの探索 - `line_items_poutine_sales`（ファクトテーブル）
# MAGIC
# MAGIC 1. 下のセルを実行してデータを確認し、次の点に注目してください：
# MAGIC
# MAGIC     - このテーブルには、カナダ各地の店舗での**プーティン販売トランザクションの明細行**が含まれています。
# MAGIC     - 各行は1つのトランザクション内で販売された1つの商品を表します。各**transaction_id**は複数回登場し、販売された商品ごとに1行となります。
# MAGIC     - **product_id**と**store_id**は、後述する商品テーブルと店舗テーブルへの結合に使用されます。

# COMMAND ----------

spark.sql(f"""
SELECT *
FROM line_items_poutine_sales
ORDER BY transaction_id
LIMIT 15
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. 商品テーブルの探索 - `products_poutine`（ディメンションテーブル）
# MAGIC
# MAGIC 1. 下のセルを実行してデータを確認し、次の点に注目してください：
# MAGIC
# MAGIC     - 各 **product_id** はメニューアイテムを一意に識別します
# MAGIC     - 商品は **category**（カテゴリ）と **subcategory**（サブカテゴリ）でグループ化されています
# MAGIC     - このテーブルは **line_items_poutine_sales** と **product_id** で結合されます

# COMMAND ----------

spark.sql(f"""
SELECT *
FROM products_poutine
LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. 店舗ロケーションテーブルの探索 - `stores_canada`（ディメンションテーブル）
# MAGIC
# MAGIC 1. 下のセルを実行してデータを確認し、次の点に注目してください：
# MAGIC
# MAGIC     - このテーブルは、カナダ全域のプーティン販売店舗とロケーション情報を提供します。
# MAGIC     - 各 **store_id** は1つの実店舗を表します
# MAGIC     - ロケーションフィールドは地理的・地域分析に利用できます
# MAGIC     - このテーブルは **line_items_poutine_sales** と **store_id** で結合されます

# COMMAND ----------

spark.sql(f"""
SELECT *
FROM stores_canada
LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### B4. データの結合
# MAGIC 1. この探索では、売上、商品、店舗テーブルを `LEFT JOIN` して、結合された出力を表示します。
# MAGIC
# MAGIC     結果に注目してください
# MAGIC     - **1,621 行**が含まれており、売上明細の数と一致し、すべての関連する売上、商品、ロケーション情報が1つのデータセットに含まれています。
# MAGIC     - この結合ビューは、後ほどメトリックビューで論理モデルとして再利用されます。

# COMMAND ----------

spark.sql(f"""
SELECT
  s.transaction_id,
  s.line_id,
  s.transaction_ts,
  p.unit_price,
  p.unit_cost,
  p.product_id,
  p.category,
  p.subcategory,
  p.product_name,
  st.store_id,
  st.location,
  st.latitude,
  st.longitude
FROM line_items_poutine_sales s
LEFT JOIN products_poutine p
  ON s.product_id = p.product_id
LEFT JOIN stores_canada st
  ON s.store_id = st.store_id
ORDER BY transaction_id
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. メトリックビューの定義
# MAGIC
# MAGIC このステップでは、プーティン売上データモデル（以下のテーブルで構成）上に**メトリックビュー**を作成します：
# MAGIC   - **line_items_poutine_sales**
# MAGIC   - **products_poutine**
# MAGIC   - **stores_canada**
# MAGIC
# MAGIC メトリックビューは、**信頼できるセマンティックレイヤー**を提供します。主な特徴は以下の通りです：
# MAGIC - ビジネスメトリック、結合、集計ロジックを集中管理
# MAGIC - ダッシュボード、ノートブック、**AI/BI**体験など、すべての下流ユーザーで一貫した計算と定義を保証
# MAGIC - アナリストやBIツールが、基礎となるテーブルの関係を理解せずにメトリックを簡単にクエリできるようにする
# MAGIC
# MAGIC **注意:** メトリックビューの詳細な解説は本ワークショップの範囲外です。演習の一部として作成方法を案内します。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="mermaid">
# MAGIC ---
# MAGIC title: "図: メトリックビューのアーキテクチャと利用モデル"
# MAGIC ---
# MAGIC flowchart LR
# MAGIC   subgraph SourceTables["ソーステーブル"]
# MAGIC     direction TB
# MAGIC     t1_label["line_items_poutine_sales（売上明細）"]
# MAGIC     t1[("<img src='./Includes/images/icons/table.png'/>")]
# MAGIC     t2_label["products_poutine（商品）"]
# MAGIC     t2[("<img src='./Includes/images/icons/table.png'/>")]
# MAGIC     t3_label["stores_canada（店舗）"]
# MAGIC     t3[("<img src='./Includes/images/icons/table.png'/>")]
# MAGIC   end
# MAGIC   mv("メトリックビュー <br/>セマンティックレイヤー <img src='./Includes/images/icons/metric_view.png'/>")
# MAGIC   consumers_note["ユーザーは<br/>標準化されたセマンティックレイヤーをクエリし<br/>一貫したメトリックと定義を取得"]
# MAGIC   subgraph Consumers["利用者"]
# MAGIC     direction LR
# MAGIC     c1["<img src='./Includes/images/icons/people.png'/>"]
# MAGIC     c2["<img src='./Includes/images/icons/people.png'/>"]
# MAGIC     c3["<img src='./Includes/images/icons/people.png'/>"]
# MAGIC     c4["<img src='./Includes/images/icons/people.png'/>"]
# MAGIC   end
# MAGIC   t1_label --> t1
# MAGIC   t2_label --> t2
# MAGIC   t3_label --> t3
# MAGIC   t1 --> mv
# MAGIC   t2 --> mv
# MAGIC   t3 --> mv
# MAGIC   mv --> consumers_note
# MAGIC   consumers_note --> c1
# MAGIC   consumers_note --> c2
# MAGIC   consumers_note --> c3
# MAGIC   consumers_note --> c4
# MAGIC   style SourceTables fill:#e3f2fd,stroke:#1B5162,stroke-width:2px
# MAGIC   style Consumers fill:#EEF2F7,stroke:#64748B,stroke-width:1.25px
# MAGIC   style mv fill:#FFFFFF,stroke:#FF6F3D,stroke-width:2px
# MAGIC   style consumers_note fill:#FFFFFF,stroke:#FF6F3D,stroke-width:1px
# MAGIC   classDef consumerNode fill:#FFFFFF,stroke:#FF6F3D,stroke-width:1.25px,rx:6,ry:6;
# MAGIC   class c1,c2,c3,c4 consumerNode;
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default" });
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     情報
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC メトリックビューは、ダッシュボードの基盤として推奨されるアプローチであり、一貫したビジネスロジックの促進に役立ちます。進化を続けており、多くの一般的な分析パターンをサポートしていますが、複雑または高度にカスタマイズされた計算が必要な場合はSQLが必要になることもあります。その場合、SQLをメトリックビューと併用してギャップを埋めることができます。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. メトリックビューの作成
# MAGIC
# MAGIC このステップでは、[メトリックビュー](https://docs.databricks.com/aws/en/metric-views/create/) **poutine_sales_metrics** を作成します。
# MAGIC
# MAGIC 1. ノートブックのサイドバーで、**labuser** カタログを探し、右クリックして **Catalog Explorer で開く** を選択します。
# MAGIC
# MAGIC 2. **YOUR_LABUSER_CATALOG > poutine_aibi > line_items_poutine_sales** テーブルを見つけて選択します。
# MAGIC
# MAGIC 3. 右上で **Create** -> **Metric View** を選択します。
# MAGIC
# MAGIC 4. プロンプトが表示されたら以下を入力します:
# MAGIC    - **Name:** `poutine_sales_metrics`
# MAGIC    - **Catalog:** **YOUR_LABUSER_CATALOG**
# MAGIC    - **Schema:** **poutine_aibi**
# MAGIC    - **Create** を選択
# MAGIC
# MAGIC 5. YAML構成を次のように確認・更新します:
# MAGIC    - **version** が `1.1` になっていることを確認し、必要なら変更します。
# MAGIC    - **dimensions** と **measures** の既存エントリがあればすべて削除し、空の状態にします。
# MAGIC    - YAMLは次のようになります:
# MAGIC
# MAGIC       yaml
# MAGIC       version: 1.1
# MAGIC
# MAGIC       source: YOUR_LABUSER_CATALOG.poutine_aibi.line_items_poutine_sales
# MAGIC       
# MAGIC
# MAGIC 6. 下の **Metric View YAML** セルを展開し、提供されたメトリックビュー構文を `source` マッピングの直下に貼り付けてください。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### メトリックビュー YAML 解答例
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC
# MAGIC ##### メトリックビュー YAML
# MAGIC <details>
# MAGIC   <summary>YAMLを展開</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">クリップボードにコピー</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC joins:
# MAGIC   - name: stores_canada
# MAGIC     source: <カタログ名>.aibi_workshop.stores_canada
# MAGIC     "on": source.store_id = stores_canada.store_id
# MAGIC   - name: products_poutine
# MAGIC     source: <カタログ名>.aibi_workshop.products_poutine
# MAGIC     "on": source.product_id = products_poutine.product_id
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: transaction_date
# MAGIC     expr: date(source.transaction_ts)
# MAGIC     comment: "取引が発生した日付（日単位に切り捨て）"
# MAGIC     display_name: 取引日
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC       leading_zeros: false
# MAGIC     synonyms:
# MAGIC       - 取引の日付
# MAGIC       - 取引日
# MAGIC       - 売上日
# MAGIC   - name: store_location
# MAGIC     expr: stores_canada.location
# MAGIC     comment: 売上が発生した店舗の地理的ロケーション
# MAGIC     display_name: 店舗ロケーション
# MAGIC   - name: product_subcategory
# MAGIC     expr: products_poutine.subcategory
# MAGIC     comment: 販売されたプーティン商品のサブカテゴリ
# MAGIC     display_name: 商品サブカテゴリ
# MAGIC   - name: product_category
# MAGIC     expr: products_poutine.category
# MAGIC     comment: 販売されたプーティン商品のカテゴリ
# MAGIC     display_name: 商品カテゴリ
# MAGIC   - name: transaction_hour
# MAGIC     expr: extract(hour from source.transaction_ts)
# MAGIC     comment: 取引が発生した時間（0-23時）
# MAGIC   - name: store_latitude
# MAGIC     expr: stores_canada.latitude
# MAGIC     comment: 店舗ロケーションの緯度座標
# MAGIC     display_name: 店舗緯度
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 6
# MAGIC       abbreviation: none
# MAGIC     synonyms:
# MAGIC       - 緯度
# MAGIC       - 店舗緯度
# MAGIC       - 店舗lat
# MAGIC   - name: store_longitude
# MAGIC     expr: stores_canada.longitude
# MAGIC     comment: 店舗ロケーションの経度座標
# MAGIC     display_name: 店舗経度
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 6
# MAGIC       abbreviation: none
# MAGIC     synonyms:
# MAGIC       - 経度
# MAGIC       - 店舗経度
# MAGIC       - 店舗long
# MAGIC   - name: product_name
# MAGIC     expr: products_poutine.product_name
# MAGIC     comment: 販売されたプーティン商品の名称
# MAGIC     display_name: 商品名
# MAGIC     synonyms:
# MAGIC       - プーティン名
# MAGIC       - 商品
# MAGIC       - アイテム名
# MAGIC   - name: transaction_id
# MAGIC     expr: source.transaction_id
# MAGIC     display_name: 取引ID
# MAGIC
# MAGIC measures:
# MAGIC   - name: transaction_count
# MAGIC     expr: count(distinct source.transaction_id)
# MAGIC     comment: 一意な取引件数
# MAGIC     display_name: 取引件数
# MAGIC     synonyms:
# MAGIC       - 取引数
# MAGIC       - 売上件数
# MAGIC   - name: units_sold
# MAGIC     expr: count(line_id)
# MAGIC     comment: 販売されたユニット数（明細行数）
# MAGIC     display_name: 販売数
# MAGIC     synonyms:
# MAGIC       - 販売数量
# MAGIC       - 売上個数
# MAGIC   - name: total_sales
# MAGIC     expr: sum(products_poutine.unit_price)
# MAGIC     comment: 売上合計（単価の合計）
# MAGIC     display_name: 売上合計
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: CAD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC       abbreviation: compact
# MAGIC     synonyms:
# MAGIC       - 売上
# MAGIC       - 収益
# MAGIC   - name: cost_of_goods_sold
# MAGIC     expr: sum(products_poutine.unit_cost)
# MAGIC     comment: 売上原価（単価原価の合計）
# MAGIC     display_name: 売上原価
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: CAD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC       abbreviation: compact
# MAGIC     synonyms:
# MAGIC       - COGS
# MAGIC       - 原価
# MAGIC   - name: gross_profit
# MAGIC     expr: MEASURE(total_sales) - MEASURE(cost_of_goods_sold)
# MAGIC     comment: 粗利益（売上合計から売上原価を差し引いた値）
# MAGIC     display_name: 粗利益
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: CAD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC       abbreviation: compact
# MAGIC     synonyms:
# MAGIC       - 利益
# MAGIC       - 粗利率
# MAGIC   - name: t7_transaction_count
# MAGIC     expr: MEASURE(transaction_count)
# MAGIC     window:
# MAGIC       - order: transaction_date
# MAGIC         semiadditive: last
# MAGIC         range: trailing 7 day
# MAGIC     comment: 直近7日間の一意な取引件数
# MAGIC     display_name: 直近7日取引件数
# MAGIC   - name: t7_units_sold
# MAGIC     expr: MEASURE(units_sold)
# MAGIC     window:
# MAGIC       - order: transaction_date
# MAGIC         semiadditive: last
# MAGIC         range: trailing 7 day
# MAGIC     comment: 直近7日間の販売数
# MAGIC     display_name: 直近7日販売数
# MAGIC   - name: t7_total_sales
# MAGIC     expr: MEASURE(total_sales)
# MAGIC     window:
# MAGIC       - order: transaction_date
# MAGIC         semiadditive: last
# MAGIC         range: trailing 7 day
# MAGIC     comment: 直近7日間の売上合計
# MAGIC     display_name: 直近7日売上合計
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: CAD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC       abbreviation: compact
# MAGIC   - name: t7_cost_of_goods_sold
# MAGIC     expr: MEASURE(cost_of_goods_sold)
# MAGIC     window:
# MAGIC       - order: transaction_date
# MAGIC         semiadditive: last
# MAGIC         range: trailing 7 day
# MAGIC     comment: 直近7日間の売上原価
# MAGIC     display_name: 直近7日売上原価
# MAGIC   - name: t7_gross_profit
# MAGIC     expr: MEASURE(gross_profit)
# MAGIC     window:
# MAGIC       - order: transaction_date
# MAGIC         semiadditive: last
# MAGIC         range: trailing 7 day
# MAGIC     comment: 直近7日間の粗利益
# MAGIC     display_name: 直近7日粗利益
# MAGIC   - name: average_basket_size
# MAGIC     expr: "MEASURE(units_sold) * 1.0 / NULLIF(MEASURE(transaction_count), 0)"
# MAGIC     comment: 1取引あたりの平均アイテム数
# MAGIC     display_name: 平均バスケットサイズ
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC     synonyms:
# MAGIC       - 平均バスケットサイズ
# MAGIC       - 取引あたり平均アイテム数
# MAGIC   - name: average_unit_price
# MAGIC     expr: AVG(products_poutine.unit_price)
# MAGIC     comment: 全明細行の平均単価
# MAGIC     display_name: 平均単価
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: CAD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC     synonyms:
# MAGIC       - 平均単価
# MAGIC       - 単価平均
# MAGIC   - name: average_transaction_amount
# MAGIC     expr: "MEASURE(total_sales) * 1.0 / NULLIF(MEASURE(transaction_count), 0)"
# MAGIC     comment: 1取引あたりの平均売上合計
# MAGIC     display_name: 平均取引金額
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: CAD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC     synonyms:
# MAGIC       - 平均取引金額
# MAGIC       - 取引平均値
# MAGIC   - name: average_unit_cost
# MAGIC     expr: AVG(products_poutine.unit_cost)
# MAGIC     comment: 全明細行の平均原価
# MAGIC     display_name: 平均原価
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: CAD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC     synonyms:
# MAGIC       - 平均原価
# MAGIC       - 原価平均
# MAGIC   - name: gross_margin
# MAGIC     expr: (MEASURE(gross_profit)) / MEASURE(total_sales)
# MAGIC     comment: 売上合計に対する粗利益率（%）
# MAGIC     display_name: 粗利益率
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC     synonyms:
# MAGIC       - 粗利益率
# MAGIC       - 粗利率
# MAGIC   - name: basket_items
# MAGIC     expr: "COUNT(DISTINCT source.product_id) * 1.0 / NULLIF(COUNT(DISTINCT source.transaction_id),\
# MAGIC       \ 0)"
# MAGIC     comment: 1取引あたりの平均ユニーク商品数
# MAGIC     display_name: バスケット商品数
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC     synonyms:
# MAGIC       - 平均バスケット商品数
# MAGIC       - 取引あたり平均ユニーク商品数
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("クリップボードにコピーしました"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("クリップボードにコピーしました");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("コピーできませんでした。手動でコピーしてください。");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     メトリックビュー YAML のカタログ名を更新
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC 7. メトリックビュー YAML 構文の先頭までスクロールし、`joins` マッピング内のカタログプレースホルダー <strong>YOUR_LABUSER_CATALOG</strong> を両方ともご自身のカタログ名に置き換えてください。
# MAGIC   
# MAGIC - これは `source` のすぐ下、YAMLの上部にあります。
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC 8. **Save** を選択し、メトリックビューが正常に保存されたことを確認してください。
# MAGIC
# MAGIC 9. エディタ上部で、メトリックビューが **YAML** モードになっていることを確認してください。  
# MAGIC    - **UI Preview** に切り替えてメトリックビューのUIを表示することもできますが、この入門トレーニングの範囲外です。
# MAGIC
# MAGIC 10. **Close** を選択してください。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     メトリックビューのトラブルシューティング
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC **Save** を選択した際、次のエラーが表示された場合：
# MAGIC
# MAGIC `[TABLE_OR_VIEW_NOT_FOUND] The table or view YOUR_LABUSER_CATALOG.poutine_aibi.stores_canada cannot be found. Verify the spelling and correctness of the schema and catalog.`
# MAGIC
# MAGIC これは、`join` マッピング内のカタログ名が更新されていないことを示しています。結合定義を確認し、すべてのテーブル参照が正しいカタログ名になっているか確認してください。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. カタログエクスプローラーでメトリックビューを確認（チェックポイント - 部分イメージ）
# MAGIC
# MAGIC 1. **Catalog Explorer** でメトリックビュー **poutine_sales_metrics** を表示し、詳細を確認してください。
# MAGIC
# MAGIC     設定例は下記のようになります。
# MAGIC
# MAGIC <br></br>
# MAGIC ![Metric View UI Checkpoint](./Includes/images/metric_view_checkpoint.png)
# MAGIC
# MAGIC <br></br>
# MAGIC 2. **Catalog Explorer** で次の点に注目してください：
# MAGIC - `source` と `joins` のマッピングが右側に表示されます
# MAGIC - `measures`（メジャー）と `dimensions`（ディメンション）がUI上で分かれて表示されます
# MAGIC - `measures` と `dimensions` の**表示名**と元の**カラム名**が「Name」列に表示されます
# MAGIC - メトリックビューで定義した `comments`（コメント）がカラムレベルで確認できます
# MAGIC
# MAGIC <br></br>
# MAGIC 3. **Gross Profit**（粗利益）メジャーを探し、右端の情報アイコンを選択してください。次の点に注目：
# MAGIC - メジャーに定義されたセマンティックメタデータ
# MAGIC - 設定された `format`（フォーマット）、`synonyms`（同義語）、`comment`（コメント）
# MAGIC - これらのマッピングは全ユーザーで標準化され、可視化やAI/BI、LLMツールで再利用されます
# MAGIC
# MAGIC <img
# MAGIC   src="./Includes/images/gross_profit_info.png"
# MAGIC   alt="Gross Profit"
# MAGIC   width=250
# MAGIC />

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. メトリックビューのクエリ
# MAGIC
# MAGIC メトリックビューが定義・標準化されたことで、**ユーザー**は直接クエリできるようになりました。
# MAGIC
# MAGIC メトリックビューをクエリすることで、組織内のユーザーは**一貫性のある信頼できるメトリック**を、基礎となるテーブル構造や結合ロジックを理解せずに利用できます。

# COMMAND ----------

# MAGIC %md
# MAGIC 1. ノートブックのサイドバーで **Catalog** アイコンを選択し、メトリックビュー **YOUR_LABUSER_CATALOG > poutine_aibi > poutine_sales_metrics** を展開します（必要に応じてリフレッシュしてください）。
# MAGIC
# MAGIC 2. `measures`（メジャー）と `dimensions`（ディメンション）が明確にグループ化されており、利用可能なメトリックや属性を簡単に発見・理解できます。
# MAGIC
# MAGIC <img
# MAGIC   src="./Includes/images/poutine_sales_metrics.png"
# MAGIC   alt="metrics sidebar"
# MAGIC   width=250
# MAGIC />

# COMMAND ----------

# MAGIC %md
# MAGIC 3. メトリックビューを使うと、テーブルを手動で結合したり集計ロジックを書き直すことなく、信頼できるビジネスメトリックをクエリできます。
# MAGIC
# MAGIC     下記のクエリで注目すべき点：
# MAGIC     - `transaction_date`ディメンションと2つのメジャーを[MEASURE](https://docs.databricks.com/aws/en/sql/language-manual/functions/measure)関数で選択しています。
# MAGIC     - `MEASURE(transaction_count)`は日ごとの一意な取引件数を計算します。
# MAGIC     - `MEASURE(t7_transaction_count)`はメトリックビューのウィンドウ定義に基づく直近7日間の取引件数を返します。
# MAGIC     - `GROUP BY transaction_date`で、メジャーを日単位（ディメンション）で集計しています。
# MAGIC
# MAGIC     この仕組みが有用な理由：
# MAGIC     - メトリックビューは`source`、`joins`、メジャー定義、セマンティックメタデータを一元管理します。
# MAGIC     - ユーザーは基礎となるテーブルや結合キー、ウィンドウロジックを理解せずに一貫した結果を取得できます。
# MAGIC     - チームが複数の一時ビューや重複計算を作成し、定義が時間とともにズレるのを防げます。

# COMMAND ----------

spark.sql(f"""
SELECT 
  transaction_date, 
  MEASURE(transaction_count) AS daily_transaction_count,
  MEASURE(t7_transaction_count) AS trailing_7_day_transaction_count
FROM poutine_sales_metrics
GROUP BY transaction_date
ORDER BY transaction_date
""").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     注記
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC これにより、すべての利用者がSQL、ダッシュボード、自然言語ツールを通じて同じ信頼できるメトリックを使い、オンデマンドでビジネス上の質問に答えることができます。データ利用者が自分でロジックを書く必要がなくなります。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. メトリックビューをデータソースとしてAI/BIダッシュボードを作成する
# MAGIC
# MAGIC メトリックビューが作成されたので、これを使ってAI/BIダッシュボードを構築します。
# MAGIC
# MAGIC - メトリックビューはAI/BIのセマンティック基盤として機能し、信頼できるメトリック、標準化された定義、豊富なセマンティックメタデータを提供します。
# MAGIC - ダッシュボードをメトリックビューの上に構築することで、可視化やAIによるインサイトが組織全体で一貫性・ガバナンス・再利用性を確保できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. AI/BIダッシュボードを作成する
# MAGIC
# MAGIC 1. メインナビゲーションバーで **Dashboards** を右クリックし、**新しいタブでリンクを開く** を選択します。
# MAGIC
# MAGIC 2. 上部で **Create dashboard**（ダッシュボード作成）を選択します。
# MAGIC
# MAGIC 3. ダッシュボード名を **firstname_lastinitials - Canadian Poutine Sales** に変更します。
# MAGIC
# MAGIC 4. タブ **Untitled page**（無題ページ）を選択し、**Sales Overview**（売上概要）に名前を変更します。
# MAGIC
# MAGIC 5. **+** アイコンを選択して新しいページを作成し、**Detailed Breakdown**（詳細分析）と名付けます。
# MAGIC
# MAGIC 6. ダッシュボードウィンドウは開いたままにしておきます。
# MAGIC
# MAGIC ![Checkpoint Dashboard Setup](./Includes/images/checkpoint_dashboard_setup.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. ダッシュボードにメトリックビューを追加する
# MAGIC
# MAGIC 1. ダッシュボードのデータソースを追加するために **Data** タブを選択します。
# MAGIC
# MAGIC 2. 上記で作成した**メトリックビュー**を追加します。以下を完了してください：
# MAGIC     - **Data** > **Add data source** を選択
# MAGIC     - **poutine_sales_metrics** メトリックビューを選択
# MAGIC     - **Confirm** を選択
# MAGIC
# MAGIC 3. UIでデータを表示します。次の点に注目してください：
# MAGIC     - カラム名とコメントが表示されます。
# MAGIC     - 表示名はここでは表示されません（2025年1月23日時点）、ただしダッシュボードでメジャーやディメンションを使用する際に表示されます。
# MAGIC
# MAGIC ![Checkpoint Dashboard Setup](./Includes/images/checkpoint_metric_view_data.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. カスタム計算を追加する
# MAGIC
# MAGIC メトリックビューに**カナダの州情報が含まれていない**場合、かつメトリックビューを編集する権限がない場合でも、ダッシュボードでこの値が必要なことがあります。
# MAGIC
# MAGIC ダッシュボード上で**カスタム計算列**を作成できます。
# MAGIC
# MAGIC 1. **+ カスタム計算を追加** を選択します。
# MAGIC
# MAGIC 2. 計算を設定します：
# MAGIC    - **名前**: `store_provinces`
# MAGIC    - **コメント**: `カナダ州略称`
# MAGIC    - **式**: `trim(split_part(store_location, ',', 2))`
# MAGIC
# MAGIC 3. 下部の **作成** ボタンをクリックします。
# MAGIC
# MAGIC 4. テーブルの `Fields` セクションの一番下に **store_provinces** カスタム計算が作成されたことを確認します。
# MAGIC
# MAGIC **注記:**
# MAGIC - [カスタム計算とは？](https://docs.databricks.com/aws/en/dashboards/custom-calculations)
# MAGIC - [カスタム計算関数リファレンス](https://docs.databricks.com/aws/en/dashboards/custom-calculations/function-reference)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. 次のノートブックへ進む
# MAGIC
# MAGIC メトリックビューを作成し、AI/BIダッシュボードのデータソースとしてメトリックビューを追加したら、次のノートブック **2 Lab - Sales Overview Page** に進み、ダッシュボードのページ1を作成してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. まとめと重要なポイント
# MAGIC
# MAGIC このノートブックでは、AI/BIダッシュボードの基本的な構成要素について学びました。
# MAGIC
# MAGIC 売上データが商品や店舗とどのように関連しているかを理解するために、シンプルなスター・スキーマを探索し、メトリックビューを軽量なセマンティックレイヤーとして利用して、一貫したビジネスメトリックを公開しました。目的はメトリックビュー設計を深く掘り下げることではなく、分析を簡素化しダッシュボードを強化する方法を示すことでした。
# MAGIC
# MAGIC このノートブックの終わりまでに、あなたは以下を行いました：
# MAGIC - 分析に使用する主要テーブルを確認
# MAGIC - ビジネスメトリックを集約するメトリックビューを作成
# MAGIC - AI/BIダッシュボードのデータソースとしてメトリックビューを利用
# MAGIC
# MAGIC 重要なポイントは、メトリックビューがダッシュボードやAI/BI体験のためのクリーンで再利用可能な基盤を提供し、チームが結合や計算ではなく分析や可視化に集中できるようになることです。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
