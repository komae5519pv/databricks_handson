from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = spark.conf.get("catalog_name")
schema_name  = spark.conf.get("schema_name")
volume_name  = spark.conf.get("volume_name")

backfill_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/users_backfill/"

# 遅れて届いた顧客マスタのバックフィルデータをbz_usersに反映させる
def setup_users_backfill_flow(backfill_dir):
    @dp.view(name="users_backfill_source")
    # スキーマ定義
    def users_backfill_source():
        raw_schema = StructType([
            StructField("user_id", StringType()),
            StructField("name", StringType()),
            StructField("age", IntegerType()),
            StructField("gender", StringType()),
            StructField("email", StringType()),
            StructField("region", StringType()),
            StructField("registration_date", StringType()),  # 文字列で受ける
            StructField("email_opt_in", StringType()),       # 文字列で受ける
            StructField("last_updated", TimestampType()),
            StructField("operation", StringType())
        ])

        df = (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .schema(raw_schema)
                .option("cloudFiles.rescuedDataColumn", "_rescued_data_users")
                .load(backfill_dir))

        # bz_users の型に合わせる（DATE/BOOLEAN）
        df = (df
              .withColumn("registration_date", F.to_date("registration_date"))  # String -> Date
              .withColumn(
                  "email_opt_in",
                  F.when(F.lower(F.col("email_opt_in")) == "true",  F.lit(True))
                   .when(F.lower(F.col("email_opt_in")) == "false", F.lit(False))
                   .otherwise(F.col("email_opt_in").cast("boolean"))
              )
        )
        return df

    # AutoCDC で upsert（SCD1）
    dp.create_auto_cdc_flow(
        name="users_auto_cdc_backfill",
        target="bz_users",
        source="users_backfill_source",
        keys=["user_id"],
        sequence_by="last_updated",
        apply_as_deletes="operation='DELETE'",
        except_column_list=["operation", "_rescued_data_users"],
        stored_as_scd_type=1,
        once=True  # CSV 置いた“後”にパイプライン起動（または再起動）してください
    )

setup_users_backfill_flow(backfill_dir)