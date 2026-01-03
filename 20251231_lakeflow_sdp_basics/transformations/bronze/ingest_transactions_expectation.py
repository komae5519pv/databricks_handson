from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities import expectation_rules

# パラメータとして設定したカタログ名とスキーマ名を取得
catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")
volume_name = spark.conf.get("volume_name")

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities import expectation_rules

# データ品質を担保するためのExpectationを定義
quantity_price_rules = expectation_rules.get_quantity_price_rules() # 追加ポイント
validation_filter = " AND ".join(quantity_price_rules.values())     # 追加ポイント

# 追加ポイント：振り分け前のログを格納するストリーミングテーブルの作成
dp.create_streaming_table(
  name="bz_transactions_all",
  expect_all=quantity_price_rules                                   # WARN (警告)
)

# 東エリアの販売履歴を取り込むAppendフロー
@dp.append_flow(name="transactions_east", target="bz_transactions_all")
def transactions_east():
  return (
    spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "csv")
     .option("cloudFiles.inferColumnTypes", "true")
     .option("cloudFiles.rescuedDataColumn", "_rescued_data_transactions")
     .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/transactions_east/")
     .withColumn("valid", F.expr(validation_filter)) # 追加ポイント
  )

# 西エリアの販売履歴を取り込むAppendフロー
@dp.append_flow(name="transactions_west", target="bz_transactions_all")
def transactions_west():
  return (
    spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "csv")
     .option("cloudFiles.inferColumnTypes", "true")
     .option("cloudFiles.rescuedDataColumn", "_rescued_data_transactions")
     .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/transactions_west/")
     .withColumn("valid", F.expr(validation_filter)) # 追加ポイント
  )