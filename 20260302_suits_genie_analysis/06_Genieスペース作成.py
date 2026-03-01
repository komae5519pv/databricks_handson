# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space 設定ガイド
# MAGIC
# MAGIC このノートブックは実行するものではなく、Genie Space を手動で作成する際のガイドです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Genie Space を作る
# MAGIC ![Genieスペース作成.gif](./_image/Genieスペース作成.gif "Genieスペース作成.gif")
# MAGIC
# MAGIC ### 基本設定
# MAGIC - **Title**: `コールセンター × 売上データ分析`
# MAGIC - **Description**: `通話ログと売上データを掛け合わせた横断分析アシスタント。媒体別の効果分析、返品原因の深掘り、顧客ジャーニーの可視化が可能です。`
# MAGIC - **Default Warehouse**: 任意のSQL Warehouse
# MAGIC
# MAGIC ### テーブル
# MAGIC 以下の3テーブルを追加してください：
# MAGIC - `gd_customer_journey`
# MAGIC - `gd_channel_performance`
# MAGIC - `sv_call_analysis`
# MAGIC
# MAGIC ### サンプル質問
# MAGIC ```
# MAGIC 媒体別のコンバージョン率と返品率を比較して
# MAGIC ```
# MAGIC ```
# MAGIC 返品理由の内訳を媒体別に見せて。特に「割引条件違い」の割合に注目して
# MAGIC ```
# MAGIC ```
# MAGIC 通話ログで割引に関する問い合わせが多い媒体はどこ？
# MAGIC ```
# MAGIC ```
# MAGIC 割引条件のトラブルがある顧客とない顧客の返品率を比較して
# MAGIC ```
# MAGIC ```
# MAGIC 問い合わせが解決した顧客と未解決の顧客で、リピート購入率に差はある？
# MAGIC ```
# MAGIC ```
# MAGIC 緊急度が高い（面接・結婚式等）顧客の平均購買単価は、全体平均と比べてどう？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SQL式（ディメンション）
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 本日
# MAGIC - **タイプ**: ディメンション
# MAGIC - **名前**: `本日`
# MAGIC - **コード**: `from_utc_timestamp(CURRENT_TIMESTAMP(), 'Asia/Tokyo')::DATE`
# MAGIC - **同義語**: `今日, 本日`
# MAGIC - **指示**: `現在を起点とする場合、必ず「SELECT from_utc_timestamp(CURRENT_TIMESTAMP(), 'Asia/Tokyo')::DATE;」で日時特定してください。`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 年齢層
# MAGIC - **タイプ**: ディメンション
# MAGIC - **テーブル**: `gd_customer_journey`
# MAGIC - **名前**: `年齢層`
# MAGIC - **コード**:
# MAGIC ```sql
# MAGIC CASE
# MAGIC   WHEN gd_customer_journey.age < 25 THEN '20代前半'
# MAGIC   WHEN gd_customer_journey.age < 30 THEN '20代後半'
# MAGIC   WHEN gd_customer_journey.age < 35 THEN '30代前半'
# MAGIC   WHEN gd_customer_journey.age < 40 THEN '30代後半'
# MAGIC   WHEN gd_customer_journey.age < 45 THEN '40代前半'
# MAGIC   WHEN gd_customer_journey.age < 50 THEN '40代後半'
# MAGIC   WHEN gd_customer_journey.age < 55 THEN '50代前半'
# MAGIC   WHEN gd_customer_journey.age < 60 THEN '50代後半'
# MAGIC   ELSE '60代以上'
# MAGIC END
# MAGIC ```
# MAGIC - **同義語**: `年代, 年齢グループ, age group`
# MAGIC - **指示**: `年齢を5歳刻みのグループに分類したディメンションです。年齢層ごとの購買傾向や返品率の分析に使用します。`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 返品リスクカテゴリ
# MAGIC - **タイプ**: ディメンション
# MAGIC - **テーブル**: `gd_customer_journey`
# MAGIC - **名前**: `返品リスクカテゴリ`
# MAGIC - **コード**:
# MAGIC ```sql
# MAGIC CASE
# MAGIC   WHEN gd_customer_journey.return_rate >= 0.3 THEN '高リスク'
# MAGIC   WHEN gd_customer_journey.return_rate >= 0.15 THEN '中リスク'
# MAGIC   WHEN gd_customer_journey.return_rate > 0 THEN '低リスク'
# MAGIC   ELSE '返品なし'
# MAGIC END
# MAGIC ```
# MAGIC - **同義語**: `返品リスク, return risk`
# MAGIC - **指示**: `顧客の返品率に基づいたリスク分類です。高リスク顧客への対策検討に使用します。`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 顧客ランク（購買金額）
# MAGIC - **タイプ**: ディメンション
# MAGIC - **テーブル**: `gd_customer_journey`
# MAGIC - **名前**: `顧客ランク`
# MAGIC - **コード**:
# MAGIC ```sql
# MAGIC CASE
# MAGIC   WHEN gd_customer_journey.total_spend >= 100000 THEN 'プレミアム'
# MAGIC   WHEN gd_customer_journey.total_spend >= 50000 THEN 'スタンダード'
# MAGIC   WHEN gd_customer_journey.total_spend > 0 THEN 'ライト'
# MAGIC   ELSE '未購入'
# MAGIC END
# MAGIC ```
# MAGIC - **同義語**: `購買ランク, 購入ランク, spend rank`
# MAGIC - **指示**: `合計購入金額に基づく顧客ランクです。セグメント別の施策検討に使用します。`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 解決ステータスグループ
# MAGIC - **タイプ**: ディメンション
# MAGIC - **テーブル**: `gd_customer_journey`
# MAGIC - **名前**: `解決ステータスグループ`
# MAGIC - **コード**:
# MAGIC ```sql
# MAGIC CASE
# MAGIC   WHEN gd_customer_journey.resolution_rate >= 0.8 THEN '解決率80%以上'
# MAGIC   WHEN gd_customer_journey.resolution_rate >= 0.5 THEN '解決率50-79%'
# MAGIC   WHEN gd_customer_journey.resolution_rate > 0    THEN '解決率50%未満'
# MAGIC   ELSE '通話なし'
# MAGIC END
# MAGIC ```
# MAGIC - **同義語**: `解決グループ, 解決率カテゴリ, resolution group`
# MAGIC - **指示**: `問い合わせ解決率に基づくグループ分類です。解決率が高い顧客ほどリピート購入率が高い傾向を確認するために使用します。`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SQL式（メジャー）
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 平均返品率
# MAGIC - **タイプ**: メジャー
# MAGIC - **テーブル**: `gd_customer_journey`
# MAGIC - **名前**: `平均返品率`
# MAGIC - **コード**: `AVG(gd_customer_journey.return_rate)`
# MAGIC - **同義語**: `返品率, return rate`
# MAGIC - **指示**: `顧客ごとの返品率の平均値です。0〜1の値で、1に近いほど返品が多いことを意味します。`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 平均購買金額
# MAGIC - **タイプ**: メジャー
# MAGIC - **テーブル**: `gd_customer_journey`
# MAGIC - **名前**: `平均購買金額`
# MAGIC - **コード**: `AVG(gd_customer_journey.total_spend)`
# MAGIC - **同義語**: `顧客単価, LTV, 平均購入額`
# MAGIC - **指示**: `顧客あたりの平均合計購買金額です。`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 割引トラブル率
# MAGIC - **タイプ**: メジャー
# MAGIC - **テーブル**: `gd_channel_performance`
# MAGIC - **名前**: `割引トラブル率`
# MAGIC - **コード**: `SUM(gd_channel_performance.discount_trouble_returns) * 1.0 / NULLIF(SUM(gd_channel_performance.return_count), 0)`
# MAGIC - **同義語**: `割引トラブル率, discount trouble rate`
# MAGIC - **指示**: `返品全体に対する「割引条件違い」による返品の割合です。この値が高い媒体は、割引条件の表記改善が必要です。`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 平均解決率
# MAGIC - **タイプ**: メジャー
# MAGIC - **テーブル**: `gd_customer_journey`
# MAGIC - **名前**: `平均解決率`
# MAGIC - **コード**: `AVG(gd_customer_journey.resolution_rate)`
# MAGIC - **同義語**: `解決率, resolution rate`
# MAGIC - **指示**: `問い合わせが「解決」で完了した割合の平均値です。0〜1の値で、1に近いほど問い合わせが解決されていることを意味します。`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 平均注文件数
# MAGIC - **タイプ**: メジャー
# MAGIC - **テーブル**: `gd_customer_journey`
# MAGIC - **名前**: `平均注文件数`
# MAGIC - **コード**: `AVG(gd_customer_journey.order_count)`
# MAGIC - **同義語**: `リピート率, 購入頻度, average orders`
# MAGIC - **指示**: `顧客あたりの平均注文件数です。リピート購入の指標として使用します。`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 指示（General Instructions）
# MAGIC
# MAGIC Genie Spaceの「General Instructions」に以下を設定してください：
# MAGIC
# MAGIC ```
# MAGIC あなたはコールセンターと売上データの分析アシスタントです。
# MAGIC
# MAGIC このデータセットには、スーツ販売を主軸とする小売企業の以下のデータが含まれています：
# MAGIC - 顧客30,000人の購買・キャンペーン接触・通話ログの横断データ（gd_customer_journey）
# MAGIC - 媒体（Web広告/LINE/DM/店舗チラシ/アプリ）× 商品カテゴリ別のパフォーマンスデータ（gd_channel_performance）
# MAGIC - 通話ログのAI構造化抽出結果（sv_call_analysis）
# MAGIC
# MAGIC 分析のポイント：
# MAGIC 1. 媒体間で割引条件の表記が統一されておらず、特にLINE経由の顧客で「割引条件違い」による返品が多い傾向があります
# MAGIC 2. 通話ログには10種類の問い合わせシナリオがあります：割引クレーム、返品交換、配送納期、注文変更、在庫確認、商品不良、他社比較、お直し（裾上げ）、請求支払い、お手入れ相談
# MAGIC 3. 「結果データ」（売上・返品）と「理由データ」（通話ログ）を組み合わせた分析を心がけてください
# MAGIC 4. 問い合わせが「解決」した顧客はリピート購入率が高く、コールセンターの対応品質が売上に直結する傾向があります
# MAGIC 5. 緊急度が高い顧客（面接・結婚式等）は購買単価が高い傾向があります
# MAGIC
# MAGIC 回答は日本語で、ビジネスパーソンにわかりやすいトーンでお願いします。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 権限設定
# MAGIC
# MAGIC デモ用途の場合：
# MAGIC - `All workspace users` に閲覧権限を付与
# MAGIC
# MAGIC 本番用途の場合：
# MAGIC - 必要なユーザー/グループのみに権限を付与
