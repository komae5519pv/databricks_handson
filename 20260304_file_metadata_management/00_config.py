# Databricks notebook source
# DBTITLE 1,共通変数
# カタログ
catalog = "komae_demo_v4"      # ご自分のカタログ名に変更してください
schema = "survey_analysis"
volume = "raw_data"

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

# DBTITLE 1,設定内容の表示
print("=== Config ===")
print(f"catalog : {catalog}")
print(f"schema  : {schema}")
print(f"volume  : {volume}")
print(f"Volume  : /Volumes/{catalog}/{schema}/{volume}/")
