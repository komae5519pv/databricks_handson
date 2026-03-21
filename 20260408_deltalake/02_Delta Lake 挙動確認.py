# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake の挙動を確認する
# MAGIC
# MAGIC このノートブックでは、Delta Lake の基本的な仕組みを理解するために、実際のストレージ構造を確認します。
# MAGIC
# MAGIC ## 学習目標
# MAGIC - Delta Table の物理的な構造（Parquet ファイル + `_delta_log`）を理解する
# MAGIC - トランザクションログの役割を確認する
# MAGIC - データ更新時のファイル変化を観察する

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. 環境セットアップ

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# 現在のカタログ・スキーマを確認
spark.sql('SELECT current_catalog(), current_schema()').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. サンプルデータの生成
# MAGIC
# MAGIC 小売店の店舗売上データを模したサンプルデータを生成します。
# MAGIC **複数のParquetファイル**が生成されるよう、十分なデータ量を作成します。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 TPC-DS風の店舗売上データを生成
# MAGIC
# MAGIC - 約100万行のデータを生成
# MAGIC - 店舗数: 50店舗
# MAGIC - 商品数: 10,000 SKU
# MAGIC - 期間: 3年分（日別）

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, rand, floor, date_add, to_date,
    concat, lpad, expr, when, round as spark_round
)
from pyspark.sql.types import *
import random

# データ生成パラメータ
num_stores = 50           # 店舗数
num_products = 10000      # 商品数
num_days = 365 * 3        # 3年分
records_per_day = 1000    # 1日あたりのレコード数（各店舗から約20件）

total_records = num_days * records_per_day  # 約109.5万件

print(f"生成予定レコード数: {total_records:,} 件")
print(f"期待されるParquetファイル数: 約 {total_records // 100000 + 1} ファイル")

# COMMAND ----------

# 基準日
base_date = "2023-01-01"

# 売上データの生成
df_sales = (
    spark.range(0, total_records)
    .withColumn("store_id", (floor(rand() * num_stores) + 1).cast("int"))
    .withColumn("product_id", (floor(rand() * num_products) + 1).cast("int"))
    .withColumn("day_offset", (floor(rand() * num_days)).cast("int"))
    .withColumn("sale_date", date_add(to_date(lit(base_date)), col("day_offset")))
    .withColumn("quantity", (floor(rand() * 10) + 1).cast("int"))
    .withColumn("unit_price", spark_round(rand() * 5000 + 100, 0).cast("int"))  # 100円〜5100円
    .withColumn("sales_amount", col("quantity") * col("unit_price"))
    .withColumn("store_name", concat(lit("店舗_"), lpad(col("store_id").cast("string"), 3, "0")))
    .withColumn("product_category",
        when(col("product_id") % 10 == 0, "園芸用品")
        .when(col("product_id") % 10 == 1, "工具")
        .when(col("product_id") % 10 == 2, "塗料")
        .when(col("product_id") % 10 == 3, "木材")
        .when(col("product_id") % 10 == 4, "金物")
        .when(col("product_id") % 10 == 5, "電材")
        .when(col("product_id") % 10 == 6, "水道用品")
        .when(col("product_id") % 10 == 7, "収納用品")
        .when(col("product_id") % 10 == 8, "インテリア")
        .otherwise("日用品")
    )
    .withColumn("prefecture",
        when(col("store_id") <= 10, "北海道")
        .when(col("store_id") <= 20, "東北")
        .when(col("store_id") <= 30, "関東")
        .when(col("store_id") <= 40, "中部")
        .otherwise("関西")
    )
    .select(
        "store_id", "store_name", "prefecture",
        "product_id", "product_category",
        "sale_date", "quantity", "unit_price", "sales_amount"
    )
)

print("サンプルデータのスキーマ:")
df_sales.printSchema()

# COMMAND ----------

# データのプレビュー
display(df_sales.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta Table として保存
# MAGIC
# MAGIC データを Delta Table として保存します。
# MAGIC `repartition` で複数のParquetファイルに分割します。

# COMMAND ----------

# 約10個のParquetファイルに分割して保存
num_partitions = 10

(
    df_sales
    .repartition(num_partitions)
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"store_sales")
    # .saveAsTable(f"{catalog_name}.{schema_name}.store_sales")
)

print(f"テーブル {catalog_name}.{schema_name}.store_sales を作成しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. テーブル詳細を確認する

# COMMAND ----------

display(spark.sql(f'DESCRIBE EXTENDED store_sales'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ストレージの中身を確認する（重要！）
# MAGIC
# MAGIC Delta Lake は以下の構造でデータを保存します：
# MAGIC
# MAGIC ```
# MAGIC テーブルの場所/
# MAGIC ├── _delta_log/           # トランザクションログ（JSON形式）
# MAGIC │   ├── 00000000000000000000.json
# MAGIC │   ├── 00000000000000000001.json
# MAGIC │   └── ...
# MAGIC ├── part-00000-xxx.parquet  # 実データ（Parquet形式）
# MAGIC ├── part-00001-xxx.parquet
# MAGIC └── ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 テーブルの保存場所を取得

# COMMAND ----------

# テーブルの詳細情報を取得
table_detail = spark.sql(f"DESCRIBE DETAIL store_sales")
display(table_detail)

# COMMAND ----------

# Location（保存場所）を取得
location = table_detail.select("location").collect()[0][0]
print(f"テーブルの保存場所: {location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Parquet ファイルの確認

# COMMAND ----------

# 直下のファイル一覧を表示
files = dbutils.fs.ls(location)
display(files)

# COMMAND ----------

# Parquetファイルの数をカウント
parquet_files = [f for f in files if f.name.endswith('.parquet')]
print(f"Parquet ファイル数: {len(parquet_files)} 個")
print(f"合計サイズ: {sum(f.size for f in parquet_files) / (1024*1024):.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 _delta_log（トランザクションログ）の確認
# MAGIC
# MAGIC `_delta_log` には、テーブルに対する全ての操作履歴が JSON 形式で記録されています。

# COMMAND ----------

delta_log_path = f"{location}/_delta_log"
print(f"Delta Log パス: {delta_log_path}\n")

delta_log_files = dbutils.fs.ls(delta_log_path)
display(delta_log_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 トランザクションログの中身を見る
# MAGIC
# MAGIC `DESCRIBE HISTORY` コマンドで、トランザクションログに記録された操作履歴を確認できます。
# MAGIC
# MAGIC 参考: [Databricks ドキュメント - DESCRIBE HISTORY](https://docs.databricks.com/ja/sql/language-manual/delta-describe-history.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- トランザクションログの履歴を表示
# MAGIC DESCRIBE HISTORY store_sales

# COMMAND ----------

# MAGIC %md
# MAGIC **履歴の見方：**
# MAGIC - `version`: トランザクションの連番（0から開始）
# MAGIC - `timestamp`: 操作が実行された日時
# MAGIC - `operation`: 操作の種類（CREATE TABLE, WRITE, MERGE など）
# MAGIC - `operationParameters`: 操作に使用されたパラメータ
# MAGIC - `operationMetrics`: 追加/削除されたファイル数、行数など

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. データを更新してファイルの変化を観察する

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 新しいデータを追加

# COMMAND ----------

# テストデータを追加
spark.sql(f"""
    INSERT INTO store_sales
    VALUES (
        99,
        '店舗_テスト',
        'テスト地域',
        99999,
        '特別商品',
        current_date(),
        100,
        9999,
        999900
    )
""")

print("テストデータを1件追加しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 ファイルの変化を確認

# COMMAND ----------

# Parquet ファイルを再確認
files_after = dbutils.fs.ls(location)
parquet_files_after = [f for f in files_after if f.name.endswith('.parquet')]

print(f"更新後の Parquet ファイル数: {len(parquet_files_after)} 個")
print(f"（更新前: {len(parquet_files)} 個）")
print(f"\n→ INSERT により新しい Parquet ファイルが1つ追加されました")

# COMMAND ----------

# delta_log を再確認
delta_log_files_after = dbutils.fs.ls(delta_log_path)
display(delta_log_files_after)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 新しいトランザクションログの内容を確認
# MAGIC
# MAGIC INSERT 後、履歴に新しいバージョンが追加されていることを確認します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 履歴を再確認（バージョン1が追加されている）
# MAGIC DESCRIBE HISTORY store_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta Lake の履歴を確認

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY store_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. クリーンアップ（オプション）
# MAGIC
# MAGIC デモ終了後、テーブルを削除する場合は以下を実行してください。

# COMMAND ----------

# テーブルを削除（コメントアウト解除で実行）
# spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.store_sales")
# print(f"テーブル store_sales を削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC ### Delta Lake の構造
# MAGIC
# MAGIC | 要素 | 説明 |
# MAGIC |------|------|
# MAGIC | **Parquet ファイル** | 実際のデータを列指向形式で保存。高い圧縮率と高速なクエリ性能を実現 |
# MAGIC | **_delta_log/** | トランザクションログ。全ての変更履歴を JSON で記録 |
# MAGIC | **チェックポイント** | 一定間隔で作成される状態のスナップショット（.parquet形式） |
# MAGIC
# MAGIC ### Delta Lake のメリット
# MAGIC
# MAGIC 1. **ACID トランザクション**: データの整合性を保証
# MAGIC 2. **タイムトラベル**: 過去の任意の時点のデータにアクセス可能
# MAGIC 3. **スキーマ管理**: スキーマの進化とバリデーション
# MAGIC 4. **ファイル管理**: 自動的な小ファイル問題の解決（OPTIMIZE）
