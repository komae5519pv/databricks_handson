# from pyspark import pipelines as dp

# # パラメータとして設定したカタログ名とスキーマ名を取得
# catalog_name = spark.conf.get("catalog_name")
# schema_name = spark.conf.get("schema_name")
# volume_name = spark.conf.get("volume_name")

# # 追加ポイント：振り分け前のログを格納するストリーミングテーブルの作成
# dp.create_streaming_table(name="bz_transactions_all")

# # 東エリアの販売履歴を取り込むAppendフロー
# @dp.append_flow(name="transactions_east", target="bz_transactions_all")
# def transactions_east():
#   return (
#     spark.readStream.format("cloudFiles")
#      .option("cloudFiles.format", "csv")
#      .option("cloudFiles.inferColumnTypes", "true")
#      .option("cloudFiles.rescuedDataColumn", "_rescued_data_transactions")
#      .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/transactions_east/")
#   )

# # 西エリアの販売履歴を取り込むAppendフロー
# @dp.append_flow(name="transactions_west", target="bz_transactions_all")
# def transactions_west():
#   return (
#     spark.readStream.format("cloudFiles")
#      .option("cloudFiles.format", "csv")
#      .option("cloudFiles.inferColumnTypes", "true")
#      .option("cloudFiles.rescuedDataColumn", "_rescued_data_transactions")
#      .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/transactions_west/")
#   )