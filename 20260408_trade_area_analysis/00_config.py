# Databricks notebook source
# MAGIC %md
# MAGIC # 設定ノートブック
# MAGIC
# MAGIC サーバレスコンピュートまたはSQLウェアハウスで実行してください

# COMMAND ----------

# DBTITLE 1,変数設定
catalog_name = "komae_demo_v4"       # 任意のカタログ名に変更してください
schema_name = "trade_area"           # 商圏分析用スキーマ

# COMMAND ----------

# DBTITLE 1,リセット用（必要な場合のみコメント解除）
# spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマ作成
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};")

# 使うカタログ、スキーマを指定
spark.sql(f"USE CATALOG {catalog_name};")
spark.sql(f"USE SCHEMA {schema_name};")

# COMMAND ----------

# DBTITLE 1,設定内容の表示
print(f"catalog: {catalog_name}")
print(f"schema: {schema_name}")
print(f"実行済み: USE CATALOG {catalog_name}")
print(f"実行済み: USE SCHEMA {schema_name}")
