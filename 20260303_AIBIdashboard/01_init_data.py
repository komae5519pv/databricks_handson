# Databricks notebook source
# DBTITLE 1,設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,データファイルをボリュームにコピー
import os, shutil

# コピー元：ワークスペース上の Includes/data/
data_folder = os.path.join(os.getcwd(), "Includes", "data")

# CSV → full_data サブディレクトリ
csv_files = [
    "line_items_poutine_sales_expanded_multi_basket.csv",
    "products_poutine_expanded.csv",
    "stores_canada_with_latlong.csv",
]

for file_name in csv_files:
    src = os.path.join(data_folder, file_name)
    dst = f"/Volumes/{catalog}/{schema}/{volume}/full_data/{file_name}"

    if not os.path.exists(src):
        print(f"[SKIP] ソースが見つかりません: {src}")
        continue
    if os.path.exists(dst):
        print(f"[SKIP] 既に存在します: {dst}")
        continue

    shutil.copy(src, dst)
    print(f"[COPY] {file_name} → {dst}")

# PNG → images サブディレクトリ
image_files = [
    "poutine_corporate_icon.png",
]

for file_name in image_files:
    src = os.path.join(data_folder, file_name)
    dst = f"/Volumes/{catalog}/{schema}/{volume}/images/{file_name}"

    if not os.path.exists(src):
        print(f"[SKIP] ソースが見つかりません: {src}")
        continue
    if os.path.exists(dst):
        print(f"[SKIP] 既に存在します: {dst}")
        continue

    shutil.copy(src, dst)
    print(f"[COPY] {file_name} → {dst}")

print("\nデータコピー完了")
