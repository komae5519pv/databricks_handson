# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # ラボ - 売上概要ページ
# MAGIC
# MAGIC ## 概要
# MAGIC
# MAGIC このハンズオンラボでは、Databricksでカナダ全域のプーティン売上データを可視化する包括的なAI/BIダッシュボードを構築する方法を学びます。メトリックビューを使用して、インタラクティブな売上概要ページを作成します。ページにはカウンターウィジェット、コンボチャート、エリアチャート、ピボットテーブル、マップなど複数の可視化タイプが含まれます。
# MAGIC
# MAGIC メトリックビューのセマンティックメタデータを活用し、通貨値の自動フォーマット、表示名の適用、最小限の設定でプロフェッショナルなダッシュボードを作成する方法を紹介します。
# MAGIC
# MAGIC この演習を通じて、売上パフォーマンス、商品カテゴリ、地理的分布、主要業績指標に関する実用的な洞察を提供する、実運用向けのビジネスインテリジェンスダッシュボードの構築方法を学びます。ダッシュボードは意思決定の基盤となり、追加ページや可視化の拡張も容易です。
# MAGIC
# MAGIC ## 学習目標
# MAGIC
# MAGIC このノートブックの終了時には、以下ができるようになります：
# MAGIC
# MAGIC - 年間比較付きの主要業績指標を表示するインタラクティブなカウンターウィジェットの作成
# MAGIC - 複数の指標（バーとライン）を共有軸で効果的に組み合わせるコンボチャートの構築
# MAGIC - カテゴリ別のローリングメトリックの時系列分析のためのエリアチャートの実装
# MAGIC - 階層データ探索のための条件付き書式とカラースケール付きピボットテーブルの設計
# MAGIC - 場所ごとの洞察のためにサイズと色エンコーディングを用いたポイントマップによる地理的可視化の構成
# MAGIC
# MAGIC #### 最終成果物 - ページ1 売上概要
# MAGIC プーティン売上メトリックを表示する8種類以上のインタラクティブな可視化を備えた、ステークホルダー向けの意思決定に活用できる完全な売上概要ダッシュボードページ。
# MAGIC <br></br>
# MAGIC <img src="./Includes/images/dashboard_01_sales_overview.png" width="800">

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
# MAGIC SQLウェアハウスを選択する方法：
# MAGIC
# MAGIC 1. ノートブック右上のコンピュートタイルを選択します。
# MAGIC 2. 既にSQLウェアハウスが表示されている場合は、それを選択します。
# MAGIC 3. 表示されていない場合は、**More** > **SQL Warehouse** を選択し、リストからSQLウェアハウスを選択してください。
# MAGIC
# MAGIC **注意:** このノートブックは **SQLウェアハウスで開発・テストされています**。他のコンピュートオプションでも動作する場合がありますが、同じ動作やすべての機能が保証されるわけではありません。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. 必須 - クラスルームセットアップ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### 前提条件 - 環境セットアップ
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     1 ラボ - AI-BIダッシュボード用メトリックビューの作成
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC   このノートブックを実行する前に、前のノートブック <strong>1 ラボ - AI-BIダッシュボード用メトリックビューの作成</strong> を完了していることを確認してください。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. ページ1の作成 - 売上概要

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. テキストボックスページ説明の追加
# MAGIC
# MAGIC 1. **売上概要**タブを選択します。
# MAGIC
# MAGIC 2. **テキストボックスの追加**を選択し、キャンバスの上部にドラッグします。
# MAGIC
# MAGIC 3. テキストボックスの高さを2ブロックに調整します。
# MAGIC
# MAGIC 4. テキストボックスに以下のMarkdownを入力します：
# MAGIC
# MAGIC text
# MAGIC # Poutine Sales Overview
# MAGIC このページでは、カナダ全域のプーティン売上パフォーマンスの概要を表示します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_checkpoint_sales_overview_title.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. 主要4指標カウンターウィジェットの追加
# MAGIC
# MAGIC このデータセットは架空のもので新しい日次データが追加されないため、このセクションの[**カウンター**ウィジェット](https://docs.databricks.com/aws/en/dashboards/visualizations/types#counter-visualization)は、**今年の総売上と昨年の総売上**を比較します。
# MAGIC
# MAGIC **注意:** 実際の運用環境で継続的にデータが取り込まれる場合、通常は**今年の年初から現在までの売上と、昨年同期間の売上**を比較するカウンターウィジェットを作成します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.1. 年間総売上 vs 前年カウンターウィジェット
# MAGIC
# MAGIC 1. **年間総売上 vs 前年**カウンターウィジェットを追加します。
# MAGIC
# MAGIC    a. ツールバーから**ビジュアライゼーション追加**アイコン <img src="./Includes/images/icons/add_visualization.png" width="20"> を選択します。  
# MAGIC    b. ウィジェットをテキストボックスのすぐ下、最左端にドラッグします。  
# MAGIC    c. ウィジェットのサイズを**2W x 3H**ブロックに調整します。
# MAGIC
# MAGIC 2. 右側の**ウィジェット設定パネル**で以下のオプションを設定します：
# MAGIC
# MAGIC | 項目 | 値 |
# MAGIC |------|------|
# MAGIC | **ウィジェット** | **タイトル**を選択 |
# MAGIC | **データセット** | **poutine_sales_metrics** |
# MAGIC | **ビジュアライゼーション** | カウンター |
# MAGIC | **日付** | **YEARLY(transaction_date)**。デフォルトは`MONTHLY(transaction_date)`なので、値を選択して`YEARLY`に変更します。 |
# MAGIC | **値** | **MEASURE(total_sales)** |
# MAGIC | **比較** | **MEASURE(total_sales)** |
# MAGIC | **前年オフセット** | **1**に設定し、**変化**は**%**を選択 |
# MAGIC
# MAGIC 3. **ウィジェットタイトル**をダブルクリックし、`年間総売上 vs 前年`に変更します。
# MAGIC
# MAGIC 4. 値が自動的にフォーマット・省略されて表示されることに注目してください。これは、メトリックビューで**total_sales**メジャーのセマンティックメタデータが定義されているためです。
# MAGIC
# MAGIC ##### メトリックビュー内の**total_sales** `measure`:
# MAGIC <br>
# MAGIC
# MAGIC yaml
# MAGIC   - name: total_sales
# MAGIC     expr: sum(source.unit_price)
# MAGIC     comment: 総売上額（単価の合計）
# MAGIC     display_name: 総売上
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
# MAGIC
# MAGIC
# MAGIC **注意:** このデータセットは架空です。最新の売上日は**2026/01/05**です。このカウンターは**今年の総売上と前年の総売上**を比較しており、昨年同日までの累計売上ではありません。

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_checkpoint_01_counter1value.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.2. 州別の追加カウンターウィジェット3つの追加
# MAGIC
# MAGIC 下記の手順に従い、例のように2 x 2グリッドで3つの追加カウンターウィジェットを配置してください。
# MAGIC
# MAGIC **チェックポイント**
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_checkpoint-02counters.png" width="900">

# COMMAND ----------

# MAGIC %md
# MAGIC ##### B2.2A. オリジナルカウンターウィジェットの複製
# MAGIC
# MAGIC 1. 最初の**カウンター**ウィジェットを複製し、オリジナルウィジェットの右側に配置します：**その他のオプション**アイコン > **複製**。

# COMMAND ----------

# MAGIC %md
# MAGIC ##### B2.2B. 粗利益カウンターウィジェットの設定
# MAGIC
# MAGIC 1. 最初のカウンターの右側に複製したウィジェットを配置し、以下のオプションを設定します：
# MAGIC
# MAGIC | 項目 | 値 |
# MAGIC |------|------|
# MAGIC | **ウィジェット** | **タイトル**が選択されていることを確認 |
# MAGIC | **データセット** | **poutine_sales_metrics** |
# MAGIC | **ビジュアライゼーション** | カウンター |
# MAGIC | **日付** | **YEARLY(transaction_date)** |
# MAGIC | **値** | **MEASURE(gross_profit)** |
# MAGIC | **比較** | **MEASURE(gross_profit)** |
# MAGIC | **前年オフセット** | **1**に設定し、**変化**は**%**を選択 |
# MAGIC
# MAGIC 2. ウィジェットタイトルを `今年の粗利益 vs 前年` に設定します。

# COMMAND ----------

# MAGIC %md
# MAGIC ##### B2.2C. 販売数量カウンターウィジェットの設定
# MAGIC
# MAGIC 1. 前のウィジェットを複製し、**年間総売上 vs 前年**カウンターの下に配置します。以下のオプションを更新してください：
# MAGIC
# MAGIC  項目 | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトル**が選択されていることを確認 |
# MAGIC  **データセット** | **poutine_sales_metrics** |
# MAGIC  **ビジュアライゼーション** | カウンター |
# MAGIC  **日付** | **YEARLY(transaction_date)** |
# MAGIC  **値** | **MEASURE(units_sold)** |
# MAGIC  **比較** | **MEASURE(units_sold)** |
# MAGIC  **前年オフセット** | **1**に設定し、**変化**は**%**を選択 |
# MAGIC
# MAGIC 2. ウィジェットタイトルを `今年の販売数量 vs 前年` に設定します。

# COMMAND ----------

# MAGIC %md
# MAGIC ##### B2.2D. 売上原価カウンターウィジェットの設定
# MAGIC
# MAGIC 1. 前のウィジェットを複製し、**今年の粗利益 vs 前年**カウンターの下に配置します。以下のオプションを更新してください：
# MAGIC
# MAGIC  項目 | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトル**が選択されていることを確認 |
# MAGIC  **データセット** | **poutine_sales_metrics** |
# MAGIC  **ビジュアライゼーション** | カウンター |
# MAGIC  **日付** | **YEARLY(transaction_date)** |
# MAGIC  **値** | **MEASURE(cost_of_goods_sold)** |
# MAGIC  **比較** | **MEASURE(cost_of_goods_sold)** |
# MAGIC  **前年オフセット** | **1**に設定し、**変化**は**%**を選択 |
# MAGIC
# MAGIC 2. ウィジェットタイトルを `今年の売上原価 vs 前年` に設定します。

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. 年間総売上と粗利益のコンボ（バー・ライン）チャートの追加
# MAGIC
# MAGIC このセクションでは、**年間ごとの総売上と粗利益**を**共通Y軸**で表示する[**コンボ**ウィジェット](https://docs.databricks.com/aws/en/dashboards/visualizations/types#combo-chart)を作成します。
# MAGIC
# MAGIC 1. ツールバーから**ビジュアライゼーション追加**アイコンを選択します。
# MAGIC
# MAGIC 2. 新しいウィジェットをダッシュボードにドラッグします。
# MAGIC
# MAGIC 3. ウィジェットの高さをカウンターウィジェットと同じにし、右側の残りの幅に合わせてサイズを調整します。
# MAGIC
# MAGIC 4. ウィジェットを**2 x 2カウンターウィジェット**の右側に配置します。
# MAGIC
# MAGIC 5. **ウィジェット設定パネル**で以下のオプションを設定します：
# MAGIC
# MAGIC  項目 | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトル**が選択されていることを確認 |
# MAGIC  **データセット** | **poutine_sales_metrics** |
# MAGIC  **ビジュアライゼーション** | コンボ |
# MAGIC  **X軸** | **YEARLY(transaction_date)**（デフォルトは**MONTHLY(transaction_date)**なので変更） |
# MAGIC  **Y軸** | - バー **MEASURE(total_sales)** <br> - ライン **MEASURE(gross_profit)** |
# MAGIC  **Y軸オプション** | - Y軸の**その他のオプション**を選択 <br> - `軸タイトルを表示`と`グリッドラインを表示`のチェックを外す |
# MAGIC
# MAGIC 6. ウィジェットタイトルを `売上と粗利益の比較（$）` に設定します。
# MAGIC
# MAGIC 7. このビジュアライゼーションでは、メトリックビューから以下が自動的に適用されることに注目してください：
# MAGIC    - **Transaction Date**の`display_name`
# MAGIC    - Y軸の両メジャーに通貨フォーマットとコンパクト表記

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_checkpoint-combo_chart.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ### B4. エリアウィジェットの追加
# MAGIC
# MAGIC このセクションでは、アシスタントを使って[**エリア**ウィジェット](https://docs.databricks.com/aws/en/dashboards/visualizations/types#area-visualization)を作成し、**カテゴリごとの7日間ローリング売上**を表示します。
# MAGIC
# MAGIC 1. ツールバーから**ビジュアライゼーション**アイコンを選択します。
# MAGIC
# MAGIC 2. 新しいウィジェットをダッシュボードにドラッグします。
# MAGIC
# MAGIC 3. ウィジェットのサイズを**6H**ブロック、キャンバス全体の幅に調整します。
# MAGIC
# MAGIC 4. ウィジェットをカウンターとコンボチャートの下に配置します。
# MAGIC
# MAGIC 5. アシスタントに次のビジュアライゼーションを作成するよう依頼します：`各商品カテゴリごとの7日間ローリング売上エリアチャート`
# MAGIC
# MAGIC 6. チェックポイント例と同様のビジュアライゼーションになっているか確認し、必要に応じてウィジェット設定を手動で調整してください。
# MAGIC 注意点：
# MAGIC    - `display_names`はメトリックビューで定義された名前が使用されます
# MAGIC    - **Trailing 7 Day Total Sales**と**Transaction Date**は、メトリックビューで定義された`format`と`display_name`で自動的にフォーマットされます

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_checkpoint_area_ai.png" alt='チェックポイント' width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ### B5. 商品カテゴリ・サブカテゴリ別売上ピボットテーブルの追加
# MAGIC
# MAGIC このセクションでは、[**ピボット**ウィジェット](https://docs.databricks.com/aws/en/dashboards/visualizations/types#pivot-visualization)を作成し、カテゴリ・サブカテゴリ別の売上を表示します。
# MAGIC
# MAGIC 1. ツールバーから**ビジュアライゼーション**アイコンを選択します。
# MAGIC
# MAGIC 2. 新しいウィジェットをダッシュボードにドラッグします。
# MAGIC
# MAGIC 3. ウィジェットのサイズを**7H**ブロック、キャンバス幅の半分に調整します。
# MAGIC
# MAGIC 4. ウィジェットを**エリア**ウィジェットの下、左端に配置します。
# MAGIC
# MAGIC 5. **ウィジェット設定パネル**で以下のオプションを設定します：
# MAGIC
# MAGIC  項目 | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトル**が選択されていることを確認 |
# MAGIC  **データセット** | **poutine_sales_metrics** |
# MAGIC  **ビジュアライゼーション** | ピボット |
# MAGIC  **行** | - **product_category** <br> - **product_subcategory** <br> - **product_name** |
# MAGIC  **値** | - **MEASURE(total_sales)** |
# MAGIC  **カラースケール** | **MEASURE(total_sales)** > **スタイル** > **カラースケール**を選択し、値にカラースケールを適用します。任意のカラースケールを選択できます。 |
# MAGIC
# MAGIC 6. ウィジェットタイトルを `商品カテゴリ・サブカテゴリ・商品名別売上` に設定します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_checkpoint-pivot.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ### B6. 商品サブカテゴリ別粗利益の横棒グラフの追加
# MAGIC
# MAGIC このセクションでは、アシスタントを使って[**バー**ウィジェット](https://docs.databricks.com/aws/en/dashboards/visualizations/types#bar-chart)を作成し、商品サブカテゴリごとの粗利益を表示します。
# MAGIC
# MAGIC 1. ツールバーから**ビジュアライゼーション**アイコンを選択します。
# MAGIC
# MAGIC 2. ウィジェットのサイズをピボットチャートと同じに調整し、右側に配置します。
# MAGIC
# MAGIC 3. アシスタントに次のビジュアライゼーションを作成するよう依頼します：`商品カテゴリごとにグループ化したサブカテゴリ別粗利益`
# MAGIC
# MAGIC 4. チェックポイント例と同様のビジュアライゼーションになっているか確認し、必要に応じてウィジェット設定を手動で調整してください。  
# MAGIC    - バーチャートは最初は縦型で表示されます。次のステップで横棒グラフに変換します。
# MAGIC    - 必要に応じてウィジェット設定を調整してください。
# MAGIC
# MAGIC 5. ビジュアライゼーション作成後、**X軸**と**Y軸**の間の矢印を選択して、チャートを横棒グラフに変換します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_checkpoint-category_horizontal.png" alt='Checkpoint' width="1000">  

# COMMAND ----------

# MAGIC %md
# MAGIC ### B7. 店舗位置のポイントマップ（粗利益でサイズ付け）
# MAGIC
# MAGIC このセクションでは、店舗の位置を粗利益でサイズ付けして表示する[**ポイントマップ**ウィジェット](https://docs.databricks.com/aws/en/dashboards/visualizations/types#point-map)を作成します。
# MAGIC
# MAGIC 1. ツールバーから**ビジュアライゼーション**アイコンを選択します。
# MAGIC
# MAGIC 2. 新しいウィジェットをダッシュボードにドラッグします。
# MAGIC
# MAGIC 3. ウィジェットのサイズを**8H**ブロック、キャンバスの半分に調整します。
# MAGIC
# MAGIC 4. ウィジェットを**ピボット**ウィジェットの下に配置します。
# MAGIC
# MAGIC 5. **ウィジェット設定パネル**で以下のオプションを設定します：
# MAGIC
# MAGIC  項目 | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトル**が選択されていることを確認 |
# MAGIC  **データセット** | **poutine_sales_metrics** |
# MAGIC  **ビジュアライゼーション** | ポイントマップ |
# MAGIC  **座標** | 経度: **store_longitude** <br> 緯度: **store_latitude** |
# MAGIC  **色** | **store_location** |
# MAGIC  **サイズ** | **MEASURE(gross_profit)** |
# MAGIC  **ツールチップ** | - **MEASURE(gross_profit)** <br> - **MEASURE(total_sales)** |
# MAGIC
# MAGIC 6. ウィジェットタイトルを `店舗位置（粗利益でサイズ付け）` に設定します。
# MAGIC
# MAGIC 7. マップ上の店舗にカーソルを合わせると、ツールチップで追加情報が表示されることを確認してください。
# MAGIC
# MAGIC    **注意:** メトリックビューで定義された`display_name`は現在ツールチップに表示されません。

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_checkpoint_point_map.png" alt='チェックポイント' width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ### B8. 州・カテゴリ・サブカテゴリ別総売上のサンキー図追加
# MAGIC
# MAGIC このセクションでは、**州から商品カテゴリ・サブカテゴリへの総売上の流れ**を可視化する[**サンキー**ウィジェット](https://docs.databricks.com/aws/en/dashboards/visualizations/types#sankey-diagram)を作成します。
# MAGIC
# MAGIC 1. ツールバーから**ビジュアライゼーション**アイコンを選択します。
# MAGIC
# MAGIC 2. 新しいウィジェットをダッシュボードの**ポイントマップ**の右側にドラッグします。
# MAGIC
# MAGIC 3. ウィジェットを**ポイントマップ**の右側に移動します。
# MAGIC
# MAGIC 4. **ウィジェット設定パネル**で以下のオプションを設定します：
# MAGIC
# MAGIC  項目 | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトル**が選択されていることを確認 |
# MAGIC  **データセット** | **poutine_sales_metrics** |
# MAGIC  **ビジュアライゼーション** | サンキー |
# MAGIC  **ステージ** | **store_provinces** <br> **product_category** <br> **product_subcategory** |
# MAGIC  **値** | **MEASURE(total_sales)** |
# MAGIC
# MAGIC 5. ウィジェットタイトルを `総売上の流れ：州 > カテゴリ > サブカテゴリ` に設定します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_checkpoint_sankey.png" alt='チェックポイント' width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. ダッシュボードテーマの変更
# MAGIC
# MAGIC **Sales Overview**ページが完成したので、[AI/BIダッシュボードのテーマ](https://docs.databricks.com/aws/en/dashboards/settings#theme-settings)を更新します。
# MAGIC
# MAGIC ダッシュボードは組織のブランドや好みに合わせて幅広くカスタマイズできますが、このワークショップでは一貫性のため特定のテーマを使用します。
# MAGIC
# MAGIC 1. コンピュートセレクター付近の**その他のオプション**アイコン（三点リーダー）を選択します。
# MAGIC
# MAGIC 2. **設定**を選択します。
# MAGIC
# MAGIC 3. **設定**パネルの**テーマ**タブで、組織の基準に合わせて色やテーマなど様々なオプションをカスタマイズできることを確認します。
# MAGIC
# MAGIC 4. このワークショップでは、**テーマ**を`Cascade`に設定してください。
# MAGIC
# MAGIC 5. ダッシュボード全体をスクロールして、更新されたテーマを確認します。
# MAGIC
# MAGIC 6. 必要に応じて、ダッシュボードテーマの他の設定もカスタマイズしてみてください。
# MAGIC
# MAGIC 7. **設定**内で**一般**タブを選択します。
# MAGIC
# MAGIC 8. **一般**タブでは以下が可能です：
# MAGIC     - **タグ**の追加
# MAGIC     - **ロケール**の指定
# MAGIC     - **フィルター適用方法**の変更
# MAGIC     - **Genieの有効化**（デフォルトで**Genieスペースの自動生成**が作成されます）

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC <br/>
# MAGIC <img src="./Includes/images/dashboard_01_sales_overview.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. 次のノートブックへ進む
# MAGIC
# MAGIC Page 1が完了したら、次のノートブック **3 Lab - Dashboard Management and Automation** に進みましょう。ダッシュボードの公開方法や、自動更新のスケジュール設定による下流ユーザー向けの最新化、AI/BI Genieの活用方法を学びます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. まとめと主要なポイント
# MAGIC
# MAGIC #### まとめ:
# MAGIC - 8種類以上のインタラクティブなビジュアライゼーションを備えた包括的なSales Overviewダッシュボードを構築しました
# MAGIC - メトリックビューのセマンティックメタデータを活用し、自動フォーマットと表示名を適用しました
# MAGIC - カウンター、コンボチャート、マップ、ピボットテーブルなど多様なビジュアライゼーションを作成しました
# MAGIC - プロフェッショナルなテーマとカスタマイズオプションを適用し、実運用向けダッシュボードを実現しました
# MAGIC
# MAGIC #### 主要なポイント:
# MAGIC - カウンターウィジェットは最小限の設定で効果的な前年比KPI比較を提供します
# MAGIC - メトリックビューのセマンティックメタデータにより手動フォーマットが不要となり、一貫性が保たれます
# MAGIC - AIアシスタントはカスタマイズ性を維持しつつ、ビジュアライゼーション作成を加速します
# MAGIC - サイズや色による地理的ビジュアライゼーションは、店舗ごとのパフォーマンスを可視化します
# MAGIC - サンキー図は複数次元にわたるデータフロー関係を効果的に表現します
# MAGIC - ダッシュボードのテーマや設定により、組織のブランドやユーザー体験を最適化できます

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
