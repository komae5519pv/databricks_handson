from pyspark import pipelines as dp

# パラメータとして設定したカタログ名とスキーマ名を取得
catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")
volume_name = spark.conf.get("volume_name")

# 顧客マスタのChange Data Feed (CDF)を読み込んで一時的なビューにする
@dp.view(name="cdf_users")
def cdf_users():
    return (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.rescuedDataColumn", "_rescued_data_users")
      .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/users/")
    )

# 顧客マスタを格納する空のストリーミングテーブルを作成
dp.create_streaming_table(name="bz_users")

# AutoCDCを使って、顧客のCDFに基づいて顧客マスターを差分更新する
dp.create_auto_cdc_flow(
  name="users_auto_cdc",
  target="bz_users",
  source="cdf_users",
  keys=["user_id"],
  sequence_by="last_updated",
  # apply_as_deletes="operation='DELETE'",
  except_column_list = ["operation"],
  stored_as_scd_type=1
)
