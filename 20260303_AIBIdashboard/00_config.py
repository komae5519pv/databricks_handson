# Databricks notebook source
# MAGIC %md
# MAGIC サーバレスコンピュートで実行してください

# COMMAND ----------

# DBTITLE 1,変数設定
catalog = "<カタログ名>"      # 任意のカタログ名に変更してください
schema = "aibi_workshop"    # 任意のスキーマ名に変更してください
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
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/full_data")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/images")

# COMMAND ----------

# DBTITLE 1,設定内容の表示
print("=== Config ===")
print(f"catalog  : {catalog}")
print(f"schema   : {schema}")
print(f"volume   : {volume}")
print(f"CSVパス   : /Volumes/{catalog}/{schema}/{volume}/full_data")
print(f"画像パス  : /Volumes/{catalog}/{schema}/{volume}/images")
