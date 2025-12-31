from pyspark import pipelines as dp

# パラメータとして設定したカタログ名、スキーマ名、ボリューム名を取得
catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")
volume_name = spark.conf.get("volume_name")

# ストリーミングテーブルの作成
# 上級編では discount カラムが動的に追加されるため、スキーマ固定はせず Auto Loader に任せます
dp.create_streaming_table("bz_transactions")

# 東エリアの販売履歴を取り込むAppendフロー
@dp.append_flow(name="transactions_east", target="bz_transactions")
def transactions_east():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      # --- スキーマ進化 (Evolution) 設定 ---
      # 新カラム検出時はストリームが一旦停止し、新規カラムを追加いsて再起動して自動取り込み（Jobsの自動再起動を有効化しておく）
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      # 救済カラム設定/解析不可の壊れたデータや型不一致はこのカラムにJSONとして退避（列追加とは両立）
      .option("cloudFiles.rescuedDataColumn", "_rescued_data_transactions")
      # （推奨）スキーマ保存パス：ソース/フローごとに分けると衝突回避やデバッグが容易
      .option("cloudFiles.schemaLocation",
        f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/_schemas/transactions_east")
      .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/transactions_east/")
  )

# 西エリアの販売履歴を取り込むAppendフロー
@dp.append_flow(name="transactions_west", target="bz_transactions")
def transactions_west():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      # --- スキーマ進化 (Evolution) 設定 ---
      # 新カラム検出時はストリームが一旦停止し、新規カラムを追加して再起動して自動取り込み（Jobsの自動再起動を有効化しておく）
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      # 救済カラム設定/解析不可の壊れたデータや型不一致はこのカラムにJSONとして退避（列追加とは両立）
      .option("cloudFiles.rescuedDataColumn", "_rescued_data_transactions")
      # （推奨）スキーマ保存パス：ソース/フローごとに分けると衝突回避やデバッグが容易
      .option("cloudFiles.schemaLocation",
        f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/_schemas/transactions_west")
      .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/transactions_west/")
  )