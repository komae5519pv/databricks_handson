# Databricks notebook source
# カタログ情報
catalog = "komae_demo_v4"              # ご自分のカタログ名に変更してください
schema = "demo_multi_agent"
volume = "raw"

# # Embedding Model Endpoint
# EMBEDDING_MODEL_ENDPOINT_NAME = "komae-text-embedding-3-small"

# # ベクターサーチエンドポイント
# MY_VECTOR_SEARCH_ENDPOINT = "one-env-shared-endpoint-2"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# # ディレクトリ作成（Structured: Genie用）
# dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/structured/inventory")
# dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/structured/product")
# dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/structured/reservation")

# # ディレクトリ作成（Unstructured: RAG用）
# dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/unstructured/operations")
# dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/unstructured/training")
# dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/unstructured/faq")
# dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/unstructured/brand")

# 使うカタログ、スキーマを指定
spark.sql(f"USE CATALOG {catalog};")
spark.sql(f"USE SCHEMA {schema};")

# 設定内容を表示
print("=== Catalog / Schema / Volume ===")
print(f"catalog: {catalog}")
print(f"schema: {schema}")
print(f"volume: {volume}")

print("\n=== Genie用構造化データの保存先 ===")
print(f"/Volumes/{catalog}/{schema}/{volume}/structured/inventory")
print(f"/Volumes/{catalog}/{schema}/{volume}/structured/product")
print(f"/Volumes/{catalog}/{schema}/{volume}/structured/reservation")

print("\n=== RAG用非構造化データの保存先 ===")
print(f"/Volumes/{catalog}/{schema}/{volume}/unstructured/operations")
print(f"/Volumes/{catalog}/{schema}/{volume}/unstructured/training")
print(f"/Volumes/{catalog}/{schema}/{volume}/unstructured/faq")
print(f"/Volumes/{catalog}/{schema}/{volume}/unstructured/brand")

# print("\n=== Endpoints ===")
# print(f"EMBEDDING_MODEL_ENDPOINT_NAME: {EMBEDDING_MODEL_ENDPOINT_NAME}")
# print(f"MY_VECTOR_SEARCH_ENDPOINT: {MY_VECTOR_SEARCH_ENDPOINT}")
