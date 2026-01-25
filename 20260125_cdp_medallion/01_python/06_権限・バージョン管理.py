# Databricks notebook source
# MAGIC %md
# MAGIC # 権限・バージョン管理
# MAGIC Unity Catalogの権限管理とDelta LakeのTime Travel機能を体験

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 権限付与・剥奪

# COMMAND ----------

# DBTITLE 1,現在の権限を確認
# カタログレベルの権限を確認
print(f"カタログ '{MY_CATALOG}' の権限一覧:")
display(spark.sql(f"SHOW GRANTS ON CATALOG {MY_CATALOG}"))

# COMMAND ----------

# DBTITLE 1,スキーマレベルの権限を確認
# スキーマレベルの権限を確認
print(f"スキーマ '{MY_CATALOG}.{MY_SCHEMA}' の権限一覧:")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {MY_CATALOG}.{MY_SCHEMA}"))

# COMMAND ----------

# DBTITLE 1,テーブルレベルの権限を確認
# Goldテーブルの権限を確認
print(f"テーブル 'gd_orders' の権限一覧:")
display(spark.sql(f"SHOW GRANTS ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 権限付与の例（参考）
# MAGIC
# MAGIC 以下は権限付与のサンプルコードです。実際に実行する場合は、コメントを外してください。
# MAGIC
# MAGIC **ユーザーへの権限付与:**
# MAGIC ```sql
# MAGIC -- カタログレベルの権限付与
# MAGIC GRANT USE CATALOG ON CATALOG <catalog_name> TO `user@example.com`;
# MAGIC
# MAGIC -- スキーマレベルの権限付与
# MAGIC GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `user@example.com`;
# MAGIC GRANT CREATE TABLE ON SCHEMA <catalog>.<schema> TO `user@example.com`;
# MAGIC
# MAGIC -- テーブルレベルの権限付与
# MAGIC GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `user@example.com`;
# MAGIC GRANT MODIFY ON TABLE <catalog>.<schema>.<table> TO `user@example.com`;
# MAGIC ```
# MAGIC
# MAGIC **グループへの権限付与:**
# MAGIC ```sql
# MAGIC -- カタログレベルの権限付与（グループ）
# MAGIC GRANT USE CATALOG ON CATALOG <catalog_name> TO `data_analysts`;
# MAGIC
# MAGIC -- スキーマレベルの権限付与（グループ）
# MAGIC GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `data_analysts`;
# MAGIC GRANT CREATE TABLE ON SCHEMA <catalog>.<schema> TO `data_engineers`;
# MAGIC
# MAGIC -- テーブルレベルの権限付与（グループ）
# MAGIC GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `data_analysts`;
# MAGIC GRANT MODIFY ON TABLE <catalog>.<schema>.<table> TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC **権限の剥奪:**
# MAGIC ```sql
# MAGIC REVOKE SELECT ON TABLE <catalog>.<schema>.<table> FROM `user@example.com`;
# MAGIC REVOKE SELECT ON TABLE <catalog>.<schema>.<table> FROM `data_analysts`;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,権限付与のサンプル - ユーザー（コメントアウト）
# ============================================================
# 【実行例】特定ユーザーにGoldテーブルの読み取り権限を付与
# ============================================================
# 以下のコードを実行する場合は、コメントを外してユーザー名を変更してください

# --- カタログの使用権限を付与 ---
# spark.sql(f"GRANT USE CATALOG ON CATALOG {MY_CATALOG} TO `analyst@example.com`")

# --- スキーマの使用権限を付与 ---
# spark.sql(f"GRANT USE SCHEMA ON SCHEMA {MY_CATALOG}.{MY_SCHEMA} TO `analyst@example.com`")

# --- Goldテーブルの読み取り権限を付与 ---
# spark.sql(f"GRANT SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_users TO `analyst@example.com`")
# spark.sql(f"GRANT SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_orders TO `analyst@example.com`")
# spark.sql(f"GRANT SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_order_items TO `analyst@example.com`")

# print("権限を付与しました")

# COMMAND ----------

# DBTITLE 1,グループへの権限付与のサンプル（コメントアウト）
# ============================================================
# 【実行例】グループにGoldテーブルの読み取り権限を付与
# ============================================================
# 以下のコードを実行する場合は、コメントを外してグループ名を変更してください
# グループはDatabricks Account Consoleまたはワークスペース設定で作成できます

# --- カタログの使用権限を付与（グループ） ---
# spark.sql(f"GRANT USE CATALOG ON CATALOG {MY_CATALOG} TO `data_analysts`")

# --- スキーマの使用権限を付与（グループ） ---
# spark.sql(f"GRANT USE SCHEMA ON SCHEMA {MY_CATALOG}.{MY_SCHEMA} TO `data_analysts`")

# --- Goldテーブルの読み取り権限を付与（グループ） ---
# spark.sql(f"GRANT SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_users TO `data_analysts`")
# spark.sql(f"GRANT SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_orders TO `data_analysts`")
# spark.sql(f"GRANT SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_order_items TO `data_analysts`")

# print("グループ 'data_analysts' に権限を付与しました")

# COMMAND ----------

# DBTITLE 1,権限剥奪のサンプル - ユーザー（コメントアウト）
# ============================================================
# 【実行例】特定ユーザーから権限を剥奪
# ============================================================
# 以下のコードを実行する場合は、コメントを外してユーザー名を変更してください

# --- Goldテーブルの読み取り権限を剥奪 ---
# spark.sql(f"REVOKE SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_users FROM `analyst@example.com`")
# spark.sql(f"REVOKE SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_orders FROM `analyst@example.com`")
# spark.sql(f"REVOKE SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_order_items FROM `analyst@example.com`")

# print("ユーザーから権限を剥奪しました")

# COMMAND ----------

# DBTITLE 1,権限剥奪のサンプル - グループ（コメントアウト）
# ============================================================
# 【実行例】グループから権限を剥奪
# ============================================================
# 以下のコードを実行する場合は、コメントを外してグループ名を変更してください

# --- Goldテーブルの読み取り権限を剥奪（グループ） ---
# spark.sql(f"REVOKE SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_users FROM `data_analysts`")
# spark.sql(f"REVOKE SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_orders FROM `data_analysts`")
# spark.sql(f"REVOKE SELECT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_order_items FROM `data_analysts`")

# print("グループ 'data_analysts' から権限を剥奪しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. データ更新とバージョン管理

# COMMAND ----------

# DBTITLE 1,更新対象テーブルの設定
# 更新対象テーブル（Silver層の注文テーブルを使用）
target_table = f"{MY_CATALOG}.{MY_SCHEMA}.sl_orders"
print(f"更新対象テーブル: {target_table}")

# 更新対象のorder_idを取得（テーブル内の最小order_id）
target_order_id = spark.sql(f"SELECT MIN(order_id) AS min_id FROM {target_table}").collect()[0]["min_id"]
print(f"更新対象のorder_id: {target_order_id}")

# COMMAND ----------

# DBTITLE 1,更新前のデータを確認
# 更新対象のレコードを確認
print(f"更新前のデータ（order_id = {target_order_id}）:")
display(spark.sql(f"""
    SELECT order_id, user_id, store_name, total_amount, order_status, order_datetime
    FROM {target_table}
    WHERE order_id = {target_order_id}
"""))

# COMMAND ----------

# DBTITLE 1,データ更新の実行
# 更新前のバージョンを記録
current_version = spark.sql(f"""
    SELECT MAX(version) AS ver FROM (DESCRIBE HISTORY {target_table})
""").collect()[0]["ver"]
print(f"更新前のバージョン: {current_version}")

# 特定の注文のステータスを更新
print("データを更新中...")
spark.sql(f"""
    UPDATE {target_table}
    SET order_status = '！テスト更新！'
    WHERE order_id = {target_order_id}
""")

print(f"order_id={target_order_id} のorder_statusを 'テスト更新！' に更新しました")

# 更新後のバージョンを確認
new_version = spark.sql(f"""
    SELECT MAX(version) AS ver FROM (DESCRIBE HISTORY {target_table})
""").collect()[0]["ver"]
print(f"バージョンが {current_version} → {new_version} に更新されました")

# COMMAND ----------

# DBTITLE 1,更新後のデータを確認
# 更新後のレコードを確認
print(f"更新後のデータ（order_id = {target_order_id}）:")
display(spark.sql(f"""
    SELECT order_id, user_id, store_name, total_amount, order_status, order_datetime
    FROM {target_table}
    WHERE order_id = {target_order_id}
"""))

# COMMAND ----------

# DBTITLE 1,テーブルの変更履歴を確認
# Delta Lakeの変更履歴（DESCRIBE HISTORY）を確認
print(f"テーブル '{target_table}' の変更履歴:")
display(spark.sql(f"DESCRIBE HISTORY {target_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time Travel体験

# COMMAND ----------

# DBTITLE 1,Time Travelでのデータ確認
print(f"Time Travel: バージョン{current_version}（更新前）のデータを確認")
display(spark.sql(f"""
    SELECT order_id, user_id, store_name, total_amount, order_status, order_datetime
    FROM {target_table} VERSION AS OF {current_version}
    WHERE order_id = {target_order_id}
"""))

print(f"現在のバージョン{new_version}（更新後）のデータを確認")
display(spark.sql(f"""
    SELECT order_id, user_id, store_name, total_amount, order_status, order_datetime
    FROM {target_table}
    WHERE order_id = {target_order_id}
"""))

print("Time Travelにより、過去のバージョンのデータを参照できました！")

# COMMAND ----------

# DBTITLE 1,タイムスタンプでのTime Travel
# タイムスタンプを使ったTime Travelも可能
print("タイムスタンプでのTime Travel例:")
print("""
-- 特定の日時時点のデータを取得
SELECT * FROM <table_name> TIMESTAMP AS OF '2024-01-01 00:00:00'

-- 相対的な時間指定も可能
SELECT * FROM <table_name> TIMESTAMP AS OF current_timestamp() - INTERVAL 1 HOUR
""")

# COMMAND ----------

# DBTITLE 1,データを元に戻す（RESTORE）
# 更新前のバージョンに戻す
print(f"バージョン{current_version}（更新前）にデータを復元中...")
spark.sql(f"RESTORE {target_table} TO VERSION AS OF {current_version}")
print(f"RESTORE {target_table} TO VERSION AS OF {current_version}")

# 復元後のデータを確認
print(f"復元後のデータ（order_id = {target_order_id}）:")
display(spark.sql(f"""
    SELECT order_id, user_id, store_name, total_amount, order_status, order_datetime
    FROM {target_table}
    WHERE order_id = {target_order_id}
"""))

# 復元後のバージョンを確認
restored_version = spark.sql(f"""
    SELECT MAX(version) AS ver FROM (DESCRIBE HISTORY {target_table})
""").collect()[0]["ver"]
print(f"データを復元しました（現在のバージョン: {restored_version}）")
print("RESTOREにより、過去のバージョンにデータを戻すことができました！")
