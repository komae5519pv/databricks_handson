# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # ラボ - ダッシュボード管理と自動化
# MAGIC
# MAGIC ## 概要
# MAGIC
# MAGIC このノートブックでは、DatabricksでAI/BIダッシュボードを公開、認証、管理する方法を学びます。ビジネスユーザー向けの公開から自動更新スケジュールの設定まで、ダッシュボード管理のライフサイクル全体を探ります。ダッシュボード認証、Databricks Oneとの統合、Genieによる自然言語分析など、実践的な運用手法をカバーします。
# MAGIC
# MAGIC プーティン売上ダッシュボードを使い、ステークホルダーへの公開、企業イメージなどの視覚要素の追加、組み込みスケジューリングやLakeflow Jobsによる自動データ更新ワークフローの実装方法を学びます。このラボは、データの変化に合わせてダッシュボードを最新状態に保ち、技術的な知識がないビジネスユーザーにもアクセス可能にする現実的なシナリオを重視しています。
# MAGIC
# MAGIC ## 学習目標
# MAGIC
# MAGIC - ビジネスユーザー向けに適切なデータ権限設定でAI/BIダッシュボードを公開する
# MAGIC - ダッシュボードに認証ステータスを適用し、データ品質やガバナンス基準を示す
# MAGIC - ビジネスユーザー用ダッシュボードアクセスのためにDatabricks Oneインターフェースを操作・活用する
# MAGIC - ダッシュボードの自動更新スケジュールや購読通知を設定する
# MAGIC - テーブル更新トリガー付きLakeflow Jobsを用いた高度なオーケストレーションワークフローを実装する

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
# MAGIC     すべての前のノートブック（0 - 必須セットアップ、1 ラボ、2 ラボ）を完了してください
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC このノートブックを実行する前に、すべての前のノートブックを完了していることを確認してください。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 下のセルを実行して、デフォルトのカタログとスキーマを設定してください。

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. AI/BIダッシュボードの公開と認証
# MAGIC ダッシュボードが完成したら、ビジネスユーザー向けに公開し、認証（認定）を行いましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. AI/BIダッシュボードの公開
# MAGIC
# MAGIC このセクションでは、AI/BIダッシュボードを下流の利用者向けに公開します。[ダッシュボードの公開](https://docs.databricks.com/aws/en/dashboards/share)時、閲覧者のデータアクセス方法を選択できます。
# MAGIC
# MAGIC このワークショップでは、以下の手順を完了してください:
# MAGIC
# MAGIC 1. ダッシュボード右上の **Publish** を選択します。
# MAGIC
# MAGIC    **共有データ権限（デフォルト）:** 閲覧者は公開者のデータ権限でクエリを実行します。閲覧者が基盤データへの直接アクセス権を持っていなくてもダッシュボードを閲覧できます。
# MAGIC
# MAGIC    ✅ **チームが通常この方法を使う理由**
# MAGIC    - 共有ダッシュボードキャッシュによる高速な閲覧体験
# MAGIC    - コンピュートコストの削減（閲覧者ごとの繰り返しクエリが減少）
# MAGIC    - シンプルな共有体験 — 閲覧者は基盤データへの直接アクセス権が不要
# MAGIC
# MAGIC    **個別データ権限:** 閲覧者は自身の認証情報でクエリを実行します。閲覧者のデータ権限によって表示される結果が決まり、基盤データへのアクセス権が必要です。
# MAGIC
# MAGIC    💡 **この方法を使う場面**
# MAGIC    - 閲覧者ごとに権限を厳密に管理したい場合（例：行・列レベルのアクセス制御）
# MAGIC    - 同じダッシュボードでも閲覧者ごとに異なる結果を表示したい場合
# MAGIC
# MAGIC **注意:** コンピュートアクセスは公開者の認証情報で付与されます。キャッシュはユーザーごとに管理され、共有されません。
# MAGIC
# MAGIC 2. **Share data permission (default)** を選択します。
# MAGIC
# MAGIC 3. **Publish** を選択します。
# MAGIC
# MAGIC 4. 公開後、ダッシュボード上部の **View Published** を選択し、公開されたダッシュボードをスクロールして確認します。  
# MAGIC    - **注意:** 編集モードに戻るには、ダッシュボード上部の **Edit Draft** を選択してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. ダッシュボードに認証ステータス（Certified）を追加する
# MAGIC
# MAGIC **認証ステータス（certified status）** システムタグは、カタログ、スキーマ、ダッシュボード、テーブルなどのオブジェクトにデータ品質やライフサイクル状態を示すラベルを付与できます。このタグはシステム管理されており、値は **certified**（認証済み）と **deprecated**（非推奨）の2種類です。
# MAGIC
# MAGIC ダッシュボードを認証する手順は以下の通りです：
# MAGIC
# MAGIC 1. ダッシュボードを **編集ドラフト（Edit Draft）** モードに戻します。
# MAGIC
# MAGIC 2. ダッシュボード上部の **その他オプション** アイコン ![More Options](./Includes/images/more_options.png) を選択します。
# MAGIC
# MAGIC 3. **認証の割り当て（Assign certification）** > **認証済み（Certified）** を選択します。
# MAGIC
# MAGIC 4. **保存（Save）** を選択します。
# MAGIC
# MAGIC 5. ダッシュボード名の近くに **認証済み（Certified）** アイコンが表示されます。
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_certified.png" width="500">
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC **参考:** [データを認証済みまたは非推奨としてフラグ付けする](https://docs.databricks.com/aws/en/data-governance/unity-catalog/certify-deprecate-data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Databricks OneでAI/BIダッシュボードを表示する
# MAGIC
# MAGIC Databricks Oneはビジネスユーザー向けに設計されたユーザーインターフェースです。クラスタやクエリ、モデル、ノートブックなどの技術的な概念を意識せず、Databricks上のデータやAIに直感的にアクセスできる単一の入口を提供します。
# MAGIC
# MAGIC **Databricks One**でAI/BIダッシュボードを表示する手順は以下の通りです：
# MAGIC
# MAGIC 1. ワークスペース右上の**App** <img src="./Includes/images/workspace_apps.png" width="25"> アイコンを選択します。
# MAGIC
# MAGIC 2. **Databricks One**を右クリックし、**新しいタブでリンクを開く**を選択します。
# MAGIC
# MAGIC 3. **Databricks One**内でダッシュボードを探し、タイルを選択して開きます。ビジネスユーザーはこのインターフェースから利用可能なリソースを素早く発見・アクセスできます。
# MAGIC
# MAGIC 4. **Databricks One**のタブを閉じてダッシュボードに戻ります。
# MAGIC
# MAGIC **注意:**  
# MAGIC - [Databricks Oneとは？](https://docs.databricks.com/aws/en/workspace/databricks-one)

# COMMAND ----------

# MAGIC %md
# MAGIC ### B4. AI/BIダッシュボードの更新公開（画像追加）
# MAGIC
# MAGIC ダッシュボード作成後、新しいビジュアライゼーションやテキスト、ページ、画像などを追加して更新する場合があります。
# MAGIC
# MAGIC この例では、Databricksボリューム **labuser.poutine_aibi.corporate_images** に保存された企業プーティン画像を **Sales Overview** ページに追加します。
# MAGIC
# MAGIC 以下の手順を完了してください:
# MAGIC
# MAGIC 1. AI/BIダッシュボードで **Edit Draft** を選択します。  
# MAGIC    - すでにドラフトモードの場合、ダッシュボード上部に **View Published** が表示されます。
# MAGIC
# MAGIC 2. **Poutine Sales Overview** のヘッダーテキストウィジェットを左に1ブロック移動します。
# MAGIC
# MAGIC 3. 空いたスペースに **1L x 1H** サイズの **テキストボックス** を追加します。
# MAGIC
# MAGIC 4. ダッシュボードページから、`.com` まで含めたワークスペースURLをコピーし、下記の `workspace_URL` SQL変数に貼り付けます。  
# MAGIC    - **例:** `https://databricks-instance.cloud.databricks.com`
# MAGIC
# MAGIC 5. **poutine_corporate_icon.png** 画像のパスをワークスペースサイドバーから取得します:
# MAGIC    - **Catalog** アイコンを選択
# MAGIC    - **labuser** カタログを展開
# MAGIC    - **poutine_aibi** スキーマを展開
# MAGIC    - **Volumes** を展開
# MAGIC    - **corporate_images** を展開
# MAGIC    - **poutine_corporate_icon.png** を右クリックし **Copy volume file path** を選択
# MAGIC    - パスを `img_vol_path` SQL変数に貼り付け
# MAGIC
# MAGIC 6. 下のセルを実行し、出力をコピーします。  
# MAGIC    - **例:** https://myworkspace.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/labuser_peter_styliadis/poutine_aibi/corporate_images/poutine_corporate_icon.png
# MAGIC
# MAGIC 7. ダッシュボードで画像を新しいテキストボックスに追加します:
# MAGIC    - 新しい空白テキストウィジェットを選択
# MAGIC    - ツールバーで **画像** アイコンを選択
# MAGIC    - コピーした画像URLをURL欄に貼り付け
# MAGIC    - **保存** を選択
# MAGIC
# MAGIC <img src="./Includes/images/add_image_to_dashboard.png" width="600">
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC **注意:**  
# MAGIC 詳細は [画像パスとURLのドキュメント](https://docs.databricks.com/aws/en/dashboards/#image-paths-and-urls) を参照してください。

# COMMAND ----------

spark.sql(f"""
DECLARE OR REPLACE VARIABLE workspace_URL STRING;
DECLARE OR REPLACE VARIABLE img_vol_path STRING
""")

spark.sql(f"""
SET VAR workspace_URL = 'https://dbc-9a0a0e37-05e1.cloud.databricks.com';
SET VAR img_vol_path = '/Volumes/labuser14024729_1772529614/poutine_aibi/corporate_images/poutine_corporate_icon.png'
""")

spark.sql(f"""SELECT CONCAT(workspace_URL, '/ajax-api/2.0/fs/files/', img_vol_path) AS `Copy Image Path`""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_checkpoint_poutine_image.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ### B5. 公開ダッシュボードの確認
# MAGIC
# MAGIC ダッシュボードを更新した後、変更が公開されているか確認しましょう。
# MAGIC
# MAGIC 1. AI/BIダッシュボード上部で **View Published（公開ビュー）** を選択します。
# MAGIC
# MAGIC 2. 公開モードでは、**最近の変更がまだダッシュボードに反映されていない**ことに注意してください。
# MAGIC
# MAGIC 3. **Edit Draft（編集ドラフト）** を選択して編集モードに戻ります。
# MAGIC
# MAGIC 4. **Publish（公開）** > **Share data permission（データ権限の共有）** > **Publish（公開）** を選択します。
# MAGIC
# MAGIC 5. 再度 **Published（公開）** ダッシュボードを表示し、変更が反映されていることを確認します。

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
# MAGIC     注意
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC ダッシュボードを公開した後に変更を加えた場合、その変更を反映させるにはダッシュボードを再度公開する必要があります。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Enable Genie and Use Genie

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. AI/BIダッシュボードのGenieスペースを表示する
# MAGIC
# MAGIC 1. AI/BIダッシュボードを**公開モード**で表示します。
# MAGIC
# MAGIC 2. ダッシュボード下部にある**Genieに質問**ボタンを探して選択します。
# MAGIC
# MAGIC 3. ここから、ユーザーは自然言語でデータに関する質問ができます。興味があれば質問してみましょう。サンプル例：
# MAGIC    - _どの州がより高い売上を持ち、どの製品カテゴリでですか？_
# MAGIC    - _どの州が最も低い総売上フローですか？_
# MAGIC
# MAGIC **注意:** ダッシュボードを作成すると、Databricksは自動的に**Genieスペース**を作成します。これにより、ビジネスユーザーは自然言語でセルフサービス分析が可能になります。
# MAGIC
# MAGIC    - [ダッシュボードからGenieスペースを有効化する](https://docs.databricks.com/aws/en/dashboards/#enable-a-genie-space-from-your-dashboard)
# MAGIC    - [AI/BI Genieスペースとは](https://docs.databricks.com/aws/en/genie/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. ダッシュボード更新のスケジュール
# MAGIC
# MAGIC AI/BIダッシュボードが完成したら、データの更新に合わせてダッシュボードを自動でリフレッシュするようスケジュール設定が必要です。手動でリフレッシュすることもできますが、自動更新のスケジュール設定を推奨します。

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. 新しい売上レコードの挿入

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 下のセルを実行して、**line_items_poutine_sales** テーブルに新しい売上レコードを **25件** 挿入します。
# MAGIC
# MAGIC    これにより、**2026-01-06 から 2026-01-10** までの新しい売上データが追加されます。

# COMMAND ----------

spark.sql(f"""
INSERT INTO line_items_poutine_sales (
  transaction_id,
  line_id,
  store_id,
  product_id,
  transaction_ts
)
VALUES
('TX-001622', 1622, 1, 201, '2026-01-06 09:05:00'),
('TX-001623', 1623, 2, 204, '2026-01-06 10:18:00'),
('TX-001624', 1624, 3, 211, '2026-01-06 11:42:00'),
('TX-001625', 1625, 4, 214, '2026-01-06 13:10:00'),
('TX-001626', 1626, 5, 304, '2026-01-06 16:35:00'),
""")

spark.sql(f"""
('TX-001627', 1627, 6, 205, '2026-01-07 09:20:00'),
('TX-001628', 1628, 7, 202, '2026-01-07 10:50:00'),
('TX-001629', 1629, 8, 212, '2026-01-07 12:15:00'),
('TX-001630', 1630, 9, 403, '2026-01-07 14:40:00'),
('TX-001631', 1631, 10, 210, '2026-01-07 18:05:00'),
""")

spark.sql(f"""
('TX-001632', 1632, 1, 206, '2026-01-08 09:10:00'),
('TX-001633', 1633, 2, 203, '2026-01-08 10:35:00'),
('TX-001634', 1634, 3, 208, '2026-01-08 12:05:00'),
('TX-001635', 1635, 4, 502, '2026-01-08 14:25:00'),
('TX-001636', 1636, 5, 213, '2026-01-08 17:55:00'),
""")

spark.sql(f"""
('TX-001637', 1637, 6, 209, '2026-01-09 09:35:00'),
('TX-001638', 1638, 7, 207, '2026-01-09 11:00:00'),
('TX-001639', 1639, 8, 301, '2026-01-09 12:30:00'),
('TX-001640', 1640, 9, 214, '2026-01-09 15:10:00'),
('TX-001641', 1641, 10, 211, '2026-01-09 18:20:00'),
""")

spark.sql(f"""
('TX-001642', 1642, 1, 602, '2026-01-10 09:15:00'),
('TX-001643', 1643, 2, 404, '2026-01-10 10:55:00'),
('TX-001644', 1644, 3, 205, '2026-01-10 12:40:00'),
('TX-001645', 1645, 4, 212, '2026-01-10 15:05:00'),
('TX-001646', 1646, 5, 204, '2026-01-10 18:30:00')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. ダッシュボードの手動リフレッシュ

# COMMAND ----------

# MAGIC %md
# MAGIC 1. このセルを実行する前に、データ更新後にAI/BIダッシュボードが自動でリフレッシュされるかどうかを考えてみましょう。
# MAGIC
# MAGIC    **View Published（公開ビュー）**でダッシュボードに戻り、**Area**プロットを確認してください。最新の売上が**2026-1-5**のままになっていることに注目してください。

# COMMAND ----------

# MAGIC %md
# MAGIC 2. 新しいレコードはダッシュボードに自動で反映されないことに注意してください。手動でダッシュボードをリフレッシュする方法もあります。
# MAGIC
# MAGIC    - ダッシュボード上部の**リフレッシュ**アイコンを選択してください。
# MAGIC
# MAGIC    - ダッシュボードが更新され、新しい売上レコードが表示されることを確認できます。
# MAGIC
# MAGIC <img src="./Includes/images/dashboard_checkpoint_refresh_01_manual.png" width="1100">

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. AI/BIダッシュボードのスケジュールと購読
# MAGIC
# MAGIC スケジュールを設定すると、ダッシュボードのキャッシュが自動でリフレッシュされ、ダッシュボードのスナップショット（PDF）がメールや通知先（Slack、Microsoft Teams）に送信されます。
# MAGIC
# MAGIC ##### 権限:
# MAGIC - スケジュールを作成するには、ユーザーに**編集可能（CAN EDIT）**権限が必要です
# MAGIC - ダッシュボードにアクセスできる人は、既存のスケジュールに購読できます
# MAGIC
# MAGIC ##### スケジュールを使う理由
# MAGIC - ダッシュボードのリフレッシュをスケジューリングすることで、チームが必要な時にキャッシュ済みの最新データをすぐに利用できます。
# MAGIC
# MAGIC - **例:** チームが毎朝8:00にダッシュボードを見る場合、7:50にリフレッシュをスケジュールしておくと、共有データ権限が有効な状態で、全員が最新データのダッシュボードを即座に閲覧できます。
# MAGIC
# MAGIC [ダッシュボードのスケジュール更新と購読の管理](https://docs.databricks.com/aws/en/dashboards/schedule-subscribe)

# COMMAND ----------

# MAGIC %md
# MAGIC #### D3.1. ダッシュボードのリフレッシュと購読者のスケジュール設定
# MAGIC
# MAGIC 1. **View Published（公開ビュー）**でダッシュボード上部の**スケジュール**を選択します。
# MAGIC
# MAGIC 2. **設定**タブで、ダッシュボードの自動リフレッシュスケジュールを設定できます。  
# MAGIC    - 頻度、時刻、タイムゾーンを指定できます。
# MAGIC    
# MAGIC    - **注意:** 自分のワークスペースでダッシュボードのスケジュールを設定した場合、ラボ終了後にスケジュールを停止または削除してください。これはトレーニング用ダッシュボードなので、継続的なリフレッシュは不要です。
# MAGIC
# MAGIC 3. **購読者**タブを選択し、ユーザーや通知先を追加できます。

# COMMAND ----------

# MAGIC %md
# MAGIC #### D3.2.（ボーナス・任意）Lakeflow Jobsでダッシュボードのリフレッシュをスケジュールする
# MAGIC
# MAGIC **Lakeflow Jobs** を使ってダッシュボードのリフレッシュをスケジュールすることもできます。Lakeflow Jobsでは、次のようなエンドツーエンドのワークフローをオーケストレーションできます：
# MAGIC
# MAGIC - データの取り込み
# MAGIC - ETL処理
# MAGIC - ダッシュボードのリフレッシュ（単一パイプラインの一部として）
# MAGIC - 基本的なダッシュボードスケジューリングを超えた追加のオーケストレーションオプション
# MAGIC
# MAGIC Lakeflow Jobsは柔軟なトリガータイプも提供します：
# MAGIC
# MAGIC - 時間ベースのスケジュール（**Scheduled** または **Continuous**）
# MAGIC - イベントベースのトリガー（**ファイル到着** や **テーブル更新**）
# MAGIC
# MAGIC このセクションでは、Lakeflow Jobsを使ってダッシュボード更新をより広範なワークフローの一部として調整する方法を簡単に紹介します。
# MAGIC
# MAGIC 1. メインナビゲーションバーで **Jobs & Pipelines** を右クリックし、**新しいタブでリンクを開く** を選択します。
# MAGIC
# MAGIC 2. **Create** > **Job** を選択します。
# MAGIC
# MAGIC 3. ジョブ名を `firstname_lastinitials_poutine_dashboard` に変更します。
# MAGIC
# MAGIC 4. このトレーニングではシンプルに、ダッシュボードをリフレッシュする単一タスクを作成します。  
# MAGIC    - **注意:** 本番環境では、通常は上流タスク（取り込みやETL）を追加して、フルワークフローをオーケストレーションします。
# MAGIC
# MAGIC 5. **+ Add another task type** を選択します。
# MAGIC
# MAGIC 6. **Dashboard** を選択します。
# MAGIC
# MAGIC 7. 以下の例を参考にタスク設定を行います：
# MAGIC
# MAGIC  フィールド | 値 |
# MAGIC ------|------|
# MAGIC  タスク名 | `poutine_refresh` |
# MAGIC  タイプ | **Dashboard** |
# MAGIC  ダッシュボード | `Your Dashboard Name` |
# MAGIC  SQLウェアハウス | あなたの **Serverless SQL Warehouse** |
# MAGIC
# MAGIC 8. **Save task** を選択します。
# MAGIC
# MAGIC 9. ジョブ詳細ペイン（右側）で：
# MAGIC
# MAGIC    a. **Add trigger** を選択します。  
# MAGIC       - ここで複数のオプションが表示されます：[スケジュールとトリガーによるジョブの自動化](https://docs.databricks.com/aws/en/jobs/triggers)
# MAGIC
# MAGIC    b. **Table updates** を選択します。  
# MAGIC       - **注意:** このトリガーは、ソーステーブルが更新されるたびにダッシュボードをリフレッシュします。
# MAGIC
# MAGIC    c. **Tables** でソーステーブル（メトリックビューではなく、売上の元テーブル）を選択します：`YOUR_LABUSER_CATALOG.poutine_aibi.line_items_poutine_sales`
# MAGIC
# MAGIC    d. **Save** をクリックします。
# MAGIC
# MAGIC    e. **Lakeflow Jobs** ページを開いたままにします。
# MAGIC
# MAGIC <img src="./Includes/images/lakeflow_jobs_setup_checkpoint.png" width="1100">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC 10. Lakeflow Jobs で:
# MAGIC   - **Runs** タブを選択します
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">お待ちください - 以下を必ず読んでください！</strong>
# MAGIC   <div style="color:#333;">
# MAGIC テーブルの横に次のアイコンが表示されるまで待ちます（必要に応じてブラウザタブをリフレッシュしてください。約1～2分かかります）
# MAGIC <br></br>
# MAGIC <img src="./Includes/images/lakeflow_jobs_update_icon.png" width="500">
# MAGIC <br></br>
# MAGIC その後、下のコードを実行して <strong>2026-01-11 から 2026-01-15</strong> のレコードをソーステーブル <strong>line_items_poutine_sales</strong> に挿入してください
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

spark.sql(f"""
INSERT INTO line_items_poutine_sales (
  transaction_id,
  line_id,
  store_id,
  product_id,
  transaction_ts
)
VALUES
('TX-001648', 1622, 1, 201, '2026-01-11 09:05:00'),
('TX-001649', 1623, 2, 204, '2026-01-12 10:18:00'),
('TX-001650', 1624, 3, 211, '2026-01-13 11:42:00'),
('TX-001651', 1625, 4, 214, '2026-01-14 13:10:00'),
('TX-001652', 1626, 5, 304, '2026-01-15 16:35:00')

""")

# COMMAND ----------

# MAGIC %md
# MAGIC 11. **Lakeflow Jobs** ページの **Runs** タブを開きます。
# MAGIC
# MAGIC     テーブルが更新された後、ジョブが自動的にトリガーされたことを確認できます。
# MAGIC     - **注意:** 
# MAGIC         - 自動実行までに **約1～2分** かかる場合があります（必要に応じてブラウザをリフレッシュしてください）。
# MAGIC         - 待機中に、追加情報として [ソーステーブル更新時のジョブトリガー](https://docs.databricks.com/aws/en/jobs/trigger-table-update) ドキュメントを参照してください。
# MAGIC
# MAGIC ##### ジョブ実行チェックポイント
# MAGIC
# MAGIC <img src="./Includes/images/lakeflow_jobs_table_update.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC 12. ジョブが完了したら、**公開ビュー**のダッシュボードに戻ります。ダッシュボードがすでに開いている場合は、ブラウザタブをリフレッシュする必要があるかもしれません。
# MAGIC
# MAGIC     ダッシュボードが**Lakeflow Job**によってリフレッシュされたことを確認してください。
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC ##### AI/BIダッシュボード更新チェックポイント
# MAGIC
# MAGIC <img src="./Includes/images/lakeflow_jobs_dashboard_update.png" width="1100">

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Lakeflowジョブの削除
# MAGIC
# MAGIC この環境はトレーニング用なので、ジョブを残したままにしないようにしましょう。
# MAGIC
# MAGIC Lakeflowジョブのページに移動し、このラボで作成したリソースをクリーンアップするためにジョブを削除してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. 次のボーナスノートブックへ進みましょう
# MAGIC
# MAGIC 時間に余裕がある場合や、AI/BIダッシュボードをさらに探求したい場合は、次のボーナスノートブック **4 BONUS Lab** に進み、Page 2 の **詳細内訳** を作成してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. まとめと重要ポイント
# MAGIC - **ダッシュボード公開**: 「データ権限の共有」を使うと、閲覧者は直接データ権限がなくてもアクセス可能。「個別データ権限」はロールベースのアクセス制御に利用
# MAGIC - **認証管理**: データ品質やライフサイクル状態を示す認証ステータスタグを付与し、ガバナンスと信頼性を担保
# MAGIC - **ビジネスユーザーアクセス**: Databricks Oneは非技術者向けにダッシュボードの発見・操作を簡易化
# MAGIC - **コンテンツ更新**: ダッシュボードの変更は再公開しないと公開ビューに反映されない
# MAGIC - **自動リフレッシュ**: 内蔵スケジューリングやLakeflow Jobs連携で定期的な更新を自動化
# MAGIC - **イベント駆動更新**: Lakeflow Jobsのテーブル更新トリガーで、ソースデータ変更時にリアルタイムでダッシュボードをリフレッシュ
# MAGIC - **Genie統合**: 自然言語クエリ機能が自動で有効化され、ビジネスユーザーが会話形式でデータ探索可能

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
