# Databricks notebook source
# カタログ
catalog = "komae_demo_v4"      # ご自分のカタログ名に変更してください
schema = "call_center"
volume = "raw"

# COMMAND ----------

# DBTITLE 1,リセット用（必要な場合のみコメント解除）
# spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマ・ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# 使うカタログ、スキーマを指定
spark.sql(f"USE CATALOG {catalog};")
spark.sql(f"USE SCHEMA {schema};")

# COMMAND ----------

# DBTITLE 1,Volume内のサブディレクトリ作成
# Volumeを作成
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/call_logs")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/structured_data")

# COMMAND ----------

# DBTITLE 1,設定内容の表示
print("=== Config ===")
print(f"catalog    : {catalog}")
print(f"schema     : {schema}")
print(f"volume     : {volume}")
print(f"構造データパス : /Volumes/{catalog}/{schema}/{volume}/structured_data")
print(f"通話ログパス  : /Volumes/{catalog}/{schema}/{volume}/call_logs")
