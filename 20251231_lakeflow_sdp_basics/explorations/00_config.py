# Databricks notebook source
# カタログ、スキーマ、ボリューム
MY_CATALOG = "lakeflow_sdp_workshop"          # 使用したいカタログ名に変更してください
MY_SCHEMA = "basic_<your_name>"               # 使用したいスキーマ名に変更してください
MY_VOLUME = "raw"

# 商品マスターのテーブル名 (上流データソース)
products_source_table = f"{MY_CATALOG}.{MY_SCHEMA}.source_products"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")

spark.sql(f"USE CATALOG {MY_CATALOG}")
spark.sql(f"USE SCHEMA {MY_SCHEMA}")

# 出力ディレクトリの作成

# Volumeのルートディレクトリ
volume_dir = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/"

# 各種データを格納するディレクトリ
users_dir = f"{volume_dir}users/"                              # 顧客マスタ
transactions_east_dir = f"{volume_dir}/transactions_east/"    # 購買履歴（East）
transactions_west_dir = f"{volume_dir}/transactions_west/"    # 購買履歴（West）

# 上記で指定したディレクトリの作成
dbutils.fs.mkdirs(users_dir)
dbutils.fs.mkdirs(transactions_east_dir)
dbutils.fs.mkdirs(transactions_west_dir)

print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_SCHEMA}")
print(f"MY_VOLUME: {MY_VOLUME}")
print(f"users_dir: {users_dir}")
print(f"transactions_east_dir: {transactions_east_dir}")
print(f"transactions_west_dir: {transactions_west_dir}")
print(f"products_source_table: {products_source_table}")
