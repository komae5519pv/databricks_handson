# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # ラボ - 詳細分析ページ
# MAGIC
# MAGIC ## 概要
# MAGIC
# MAGIC このノートブックでは、Databricks AI-BIダッシュボードを強化し、包括的な詳細分析ページを構築します。この実践的な演習では、グローバルおよびページレベルのフィルター、カウンターウィジェット、パレートチャートなどの高度なダッシュボード機能の作成に焦点を当てます。インタラクティブなフィルタリング機構を実装し、ユーザーが異なるダッシュボードページでデータビューを動的に制御できるようにします。
# MAGIC
# MAGIC このノートブックは、ビジネスインテリジェンスアプリケーションで一般的に使用される実用的なダッシュボード設計パターンを強調しています。パフォーマンス指標の表示や分析的な可視化（主要なビジネスドライバーの特定に役立つパレート分析の構築など）を含みます。パレート分析を構築することで、戦略的意思決定プロセスを支援するデータ駆動型の洞察を作成する経験を得ることができます。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このラボの終了時には、以下ができるようになります:
# MAGIC - インタラクティブなダッシュボード体験を作成するためにグローバルおよびページレベルのフィルターを構成する
# MAGIC - 主要なパフォーマンス指標を表示するカウンターウィジェットを構築・カスタマイズする
# MAGIC - AIアシスタントを使用して折れ線グラフやグループ化棒グラフなどの高度な可視化を作成する
# MAGIC - パレート分析を実装し、影響の大きいビジネスドライバーを特定する
# MAGIC
# MAGIC #### 最終成果物
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_02_detailed_breakdown_page.png" width="1000">

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
# MAGIC このノートブックを開始する前に、下記の必要なコンピュート環境を選択してください。
# MAGIC
# MAGIC - **SQLウェアハウス**（X-Smallで十分です）
# MAGIC
# MAGIC **注意:** このノートブックは**SQLウェアハウスで開発・テストされています**。他のコンピュートオプションでも動作する場合がありますが、すべての機能が同じように動作する保証はありません。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. 必須 - クラスルームセットアップ

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
# MAGIC     すべての前提ノートブック（0 - 必須セットアップ、1 ラボ、2 ラボ、3 ラボ）を完了してください
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC このノートブックを実行する前に、すべての前提ノートブックを完了していることを確認してください。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. ダッシュボードにグローバルフィルターを追加
# MAGIC
# MAGIC グローバルインタラクティブフィルターは、ダッシュボード内のすべてのページに適用され、1つ以上のデータセットを共有する可視化に対して機能します。閲覧者はこれらの値を動的に調整でき、変更はダッシュボード全体に影響します。
# MAGIC
# MAGIC 詳細は [フィルターのインタラクティビティとスコープ](https://docs.databricks.com/aws/en/dashboards/filters#filter-interactivity-and-scope) を参照してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. グローバルフィルターを2つ追加
# MAGIC
# MAGIC 以下の手順でダッシュボードにグローバルフィルターを追加します。
# MAGIC
# MAGIC 1. **Edit Draft** を選択します。
# MAGIC
# MAGIC 2. **Filter** ![Filter Icon](./Includes/images/filter_icon.png) タブを **Data** タブの近くで選択します。
# MAGIC
# MAGIC 3. **Add a global filter +** を選択します。
# MAGIC
# MAGIC 4. 次の2つのグローバルフィルターを作成します。
# MAGIC
# MAGIC ##### フィルター1 - 店舗都道府県
# MAGIC
# MAGIC  フィールド | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトルを選択** |
# MAGIC  **フィルター** | **単一値** |
# MAGIC  **フィールド** | **store_provinces** |
# MAGIC  **設定** | **すべて許可** |
# MAGIC
# MAGIC - タイトルを `Store Provinces Filter` に設定します
# MAGIC
# MAGIC
# MAGIC ##### フィルター2 - 日付範囲
# MAGIC
# MAGIC  フィールド | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトルを選択** |
# MAGIC  **フィルター** | **日付範囲ピッカー** |
# MAGIC  **フィールド** | **transaction_date** |
# MAGIC
# MAGIC - タイトルを `Date Range Filter` に設定します

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_checkpoint_global_filters.png" alt='チェックポイント' width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. グローバルフィルターのテスト
# MAGIC
# MAGIC 1. **Sales Overview** ページを選択し、フィルターを確認します。確認が終わったら、すべてのフィルター値をクリアし、デフォルト設定に戻してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. 詳細分析ページの説明テキストボックスを追加
# MAGIC
# MAGIC 1. **Detailed Breakdown** タブを選択します。
# MAGIC
# MAGIC 2. **テキストボックスを追加** を選択し、キャンバスの上部にドラッグします。
# MAGIC
# MAGIC 3. テキストボックスの高さを **2H** ブロック、幅をキャンバス全体に調整します。
# MAGIC
# MAGIC 4. テキストボックスに以下のMarkdownを入力します:
# MAGIC
# MAGIC text
# MAGIC # 売上詳細分析
# MAGIC このページでは、カナダ全域のプーティン売上パフォーマンスの詳細ビューを提供します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_checkpoint_pg2_text.png" alt='チェックポイント' width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. 「詳細分析」ページにページフィルターを追加
# MAGIC
# MAGIC ページフィルターは、同じページ上で1つ以上のデータセットを共有するすべての可視化に適用されます。閲覧者はこれらの値を調整することで、そのページのみのビューを変更できます。
# MAGIC
# MAGIC 詳細は [フィルターのインタラクティビティとスコープ](https://docs.databricks.com/aws/en/dashboards/filters#filter-interactivity-and-scope) を参照してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. ページフィルターを追加
# MAGIC
# MAGIC 以下の手順で **詳細分析** ページに4つのページフィルターを追加します。
# MAGIC
# MAGIC 1. **詳細分析** ページを選択します。
# MAGIC
# MAGIC 2. 下部ツールバーで **フィルター** アイコンを選択します。
# MAGIC
# MAGIC 3. テキストボックスの下、左端に **1H x 4W** のフィルターブロックを追加します。
# MAGIC
# MAGIC 4. ウィジェット設定パネルで以下のオプションを設定します。
# MAGIC
# MAGIC  フィールド | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトルを選択** |
# MAGIC  **フィルター** | **複数値** |
# MAGIC  **フィールド** | **store_location** |
# MAGIC  **デフォルト値** | **すべて** |
# MAGIC
# MAGIC 5. ウィジェットタイトルを `Stores` に設定します。

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. ページフィルターをさらに3つ追加
# MAGIC
# MAGIC **Stores** フィルターの下に、以下の3つのページフィルターを追加します。
# MAGIC
# MAGIC 1. **product_category**
# MAGIC    - **複数値**でフィルター
# MAGIC    - ウィジェットタイトルを `Category` に設定
# MAGIC
# MAGIC 2. **product_subcategory**
# MAGIC    - **複数値**でフィルター
# MAGIC    - ウィジェットタイトルを `Subcategory` に設定
# MAGIC
# MAGIC 3. **product_name**
# MAGIC    - **複数値**でフィルター
# MAGIC    - ウィジェットタイトルを `Product Name` に設定

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_checkpoint_pg2_filters.png" alt='チェックポイント' width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. 4つのカウンターウィジェットを追加
# MAGIC
# MAGIC これまで学んだ内容を活用し、ページ右側のヘッダー下に**2H x 2W**のカウンターウィジェットを4つ追加してください。
# MAGIC
# MAGIC 1. 以下の各指標ごとに1つずつカウンターウィジェットを作成し、指定されたタイトルを使用します。
# MAGIC
# MAGIC - **total_sales**：タイトルは `Total Sales`
# MAGIC - **gross_profit**：タイトルは `Gross Profit`
# MAGIC - **gross_margin**：タイトルは `Gross Margin`
# MAGIC - **units_sold**：タイトルは `Units Sold`

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_checkpoint_pg2_counter_widgets.png" alt='チェックポイント' width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. 直近7日間の売上推移を追加
# MAGIC
# MAGIC このセクションでは、アシスタントを使って[**エリア**ウィジェット](https://docs.databricks.com/aws/en/dashboards/visualizations/types#line-visualization)を作成し、**週ごとの直近7日間の売上合計**を表示します。
# MAGIC
# MAGIC 1. ツールバーで**ビジュアライゼーション**アイコンを選択します。
# MAGIC
# MAGIC 2. 新しいウィジェットをダッシュボードにドラッグします。
# MAGIC
# MAGIC 3. ウィジェットのサイズを**9H**ブロック、フィルターの右側でカウンターの下の残り幅に調整します。
# MAGIC
# MAGIC 4. ウィジェットをカウンターウィジェットの下のスペースに移動します。
# MAGIC
# MAGIC 5. アシスタントに次のビジュアライゼーションを作成するよう依頼します: `週ごとの直近7日間売上合計の折れ線グラフ`
# MAGIC
# MAGIC 6. ビジュアライゼーションが下記のチェックポイント例と似ているか確認します。必要に応じてウィジェット設定を手動で調整してください。
# MAGIC    - `display_names`はメトリックビューで定義された名前を使用します
# MAGIC    - **直近7日間売上合計**と**取引日**は、メトリックビューで定義された`format`と`display_name`で自動的にフォーマットされます
# MAGIC
# MAGIC 7. ウィジェットオプションで**ラベル**を追加します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_checkpoint_pg2_line.png" alt='チェックポイント' width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. 店舗別の売上合計と粗利益の棒グラフを追加
# MAGIC
# MAGIC このセクションでは、アシスタントを使って[**棒グラフ**ウィジェット](https://docs.databricks.com/aws/en/dashboards/visualizations/types#bar-chart)を作成し、**店舗別の売上合計と粗利益**を表示します。
# MAGIC
# MAGIC 1. ツールバーで**ビジュアライゼーション**アイコンを選択します。
# MAGIC
# MAGIC 2. 新しいウィジェットをダッシュボードにドラッグします。
# MAGIC
# MAGIC 3. ウィジェットを折れ線グラフの左側のスペースにリサイズします。
# MAGIC
# MAGIC 5. アシスタントに次のビジュアライゼーションを作成するよう依頼します: `店舗別の売上合計と粗利益の棒グラフ`
# MAGIC
# MAGIC 6. アシスタントは通常、**積み上げ縦棒グラフ**を作成します。下記のチェックポイント例と似ているか確認し、必要に応じてウィジェット設定を手動で調整してください。
# MAGIC
# MAGIC 7. ウィジェットオプションで以下を設定します:
# MAGIC    - 棒グラフが横向きになるように、**X軸**と**Y軸**の間の矢印を使って切り替えます。
# MAGIC    - **X軸**オプションボタン（三点）を選択し、次を完了します:
# MAGIC      - 軸タイトルを `Total ($)` に更新
# MAGIC      - **レイアウト**セクションで**グループ**を選択

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_checkpoint_group_bar_vertical.png" alt='チェックポイント' width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. 簡易パレートチャート
# MAGIC
# MAGIC ### ビジネス上の質問:
# MAGIC - どの**プーティン**商品が総売上の大部分を占めているか？
# MAGIC - 売上貢献度の低い商品は、プロモーション変更や販売終了の候補となるか？
# MAGIC
# MAGIC パレートチャートは、**どの商品が全体の結果に最も大きな影響を与えているか**を理解するのに役立ちます。これは、**少数の商品が総売上の大部分を生み出す**という考え方に基づいています。
# MAGIC
# MAGIC **注意:** このデータセットはトレーニング目的の合成データです。結果はパレートの概念を示すものであり、実際の売上パターンを表すものではありません。

# COMMAND ----------

# MAGIC %md
# MAGIC ### H1. パレート準備クエリの実行
# MAGIC
# MAGIC 1. 下記のクエリを実行して、商品ごとの売上合計と各商品の全体売上への累積貢献度を計算します。
# MAGIC
# MAGIC     このクエリは以下を行います:
# MAGIC     - `poutine`商品の**product_name**ごとに売上合計を集計
# MAGIC     - 商品を**売上合計が高い順**に並べ替え
# MAGIC     - **売上合計の累積割合**を計算
# MAGIC     - 各商品を**上位80%（パレート閾値）内外**で分類
# MAGIC
# MAGIC     クエリ実行後、以下を確認してください:
# MAGIC     - 商品が**売上合計の降順**で並んでいる
# MAGIC     - **cumulative_pct**列が0から1に向かって増加している
# MAGIC     - 一部の商品が**Inside Pareto (Top 80%)**（最も売れている商品）に分類されている
# MAGIC     - 売上が低い商品は**Outside Pareto (Remaining 20%)**（売上が低い商品）に分類されている
# MAGIC
# MAGIC     この結果はダッシュボードのパレート可視化作成に使用します。

# COMMAND ----------

spark.sql(f"""
WITH agg AS (
  SELECT
    product_name,
    MEASURE(total_sales) AS total_sales
  FROM poutine_sales_metrics
  WHERE product_category = 'Poutine'
  GROUP BY product_name
),
ranked AS (
  SELECT
    product_name,
    total_sales,
    SUM(total_sales) OVER () AS grand_total,
    SUM(total_sales) OVER (
      ORDER BY total_sales DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
  FROM agg
)
SELECT
  product_name,
  ROUND(total_sales, 2) AS total_sales,
  ROUND(running_total / grand_total, 3) AS cumulative_pct,
  CASE
    WHEN running_total / grand_total <= 0.8 THEN 'Inside Pareto (Top 80%)'
    ELSE 'Outside Pareto (Remaining 20%)'
  END AS pareto_bucket
FROM ranked
ORDER BY total_sales DESC
""").display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### H2. パレート可視化の作成

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
# MAGIC     コンボプロットの制限事項
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - 現在の**コンボプロット**では、売上合計（バー）と累積割合（ライン）を組み合わせ、売上降順で並べる典型的なパレートチャートは作成できません。
# MAGIC
# MAGIC - この制限への対応として、作成するチャートは**累積売上割合をバーで昇順表示（売上降順と同等）**し、**80%の基準線で主要な売上貢献ポイントを強調**します。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 以下の手順でパレートクエリをダッシュボードに追加します:
# MAGIC
# MAGIC    a. ダッシュボードで**データ**タブを選択します。
# MAGIC
# MAGIC    b. **SQLから作成**を選択します。
# MAGIC
# MAGIC    c. **デフォルトのカタログとスキーマ**を設定します。
# MAGIC       - カタログ: **labuser_xxx**
# MAGIC       - スキーマ: **poutine_aibi**
# MAGIC
# MAGIC    d. 前のセクションのパレートクエリをエディタに貼り付けます。
# MAGIC
# MAGIC    e. **実行**を選択してクエリを作成します。
# MAGIC
# MAGIC    f. クエリが正常に実行されることを確認します。
# MAGIC
# MAGIC    g. クエリ名を `poutine_sales_performance_analysis` に設定します。

# COMMAND ----------

# MAGIC %md
# MAGIC 2. 以下の手順で簡易パレート可視化を作成します:
# MAGIC
# MAGIC    a. **詳細分析**ページを選択します。
# MAGIC
# MAGIC    b. ツールバーで**ビジュアライゼーション**アイコンを選択します。
# MAGIC
# MAGIC    c. 新しいウィジェットをダッシュボードにドラッグします。
# MAGIC
# MAGIC    d. ウィジェットのサイズを**9H**ブロック、キャンバスの幅に調整します。
# MAGIC
# MAGIC    e. ウィジェットを横棒グラフと折れ線グラフの下に配置します。
# MAGIC
# MAGIC 3. **ウィジェット**設定パネルで以下のオプションを設定します。
# MAGIC
# MAGIC  フィールド | 値 |
# MAGIC ------|------|
# MAGIC  **ウィジェット** | **タイトル**と**説明**が有効になっていることを確認 |
# MAGIC  **データセット** | **poutine_sales_performance_analysis** |
# MAGIC  **ビジュアライゼーション** | **バー** |
# MAGIC  **X軸** | **product_name** |
# MAGIC  **Y軸** | **SUM(cumulative_pct)** |
# MAGIC  **Y軸オプション** | **その他のオプション**を選択 <br> - 表示名を `Cumulative Sales Pct` に設定 <br> - **フォーマット > カスタム > %** を選択 |
# MAGIC  **X軸オプション** | **その他のオプション**を選択 <br> - **昇順**で並べ替え <br> - 表示名を `Product Name` に設定 |
# MAGIC  **色** | **pareto_bucket** <br> **Outside Pareto (Remaining 20%)** を16進カラー `#FCA5A5` に設定 |
# MAGIC  **色の詳細オプション** | <br> - 表示名を `Sales Contribution Group` に設定 <br> - **凡例位置**を `Top` に設定 |
# MAGIC  **ツールチップ** | **SUM(total_sales)** |
# MAGIC  **ラベル** | - **オン**にする <br> - **フィールド** > **total_sales**（フォーマットは `$`） <br> |
# MAGIC  **アノテーション** | **水平**アノテーションを**0.80**に追加 <br> ラベルを `80% of Total Sales` に設定 |
# MAGIC
# MAGIC 4. ウィジェットタイトルを: `どのプーティン商品が売上の大部分を占めているか` に設定します。
# MAGIC
# MAGIC 5. ウィジェット説明を: `このチャートは、商品ごとの売上貢献度順に並べた累積売上割合を示しています。チャートを右に進むにつれ売上が100%に近づき、80%の基準線が主要な売上貢献ポイントを強調します。` に設定します。

# COMMAND ----------

# MAGIC %md
# MAGIC #### チェックポイント
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_checkpoint_pg2_pareto_red.png" alt='チェックポイント' width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### ビジュアライゼーションの読み方
# MAGIC <img src="./Includes/images/pareto_explained_viz.png" alt='チェックポイント' width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. まとめと主要ポイント
# MAGIC
# MAGIC - インタラクティブダッシュボード用のグローバルおよびページレベルのフィルターを設定  
# MAGIC - 主要なビジュアル、カウンター、7日間売上、比較チャートを作成  
# MAGIC - 80/20ルールを用いたパレートチャートで主要商品を強調  
# MAGIC - より高度な分析のためにカスタムSQLを作成  
# MAGIC - クリーンで読みやすいダッシュボードを設計し、AIの助けで洗練

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
