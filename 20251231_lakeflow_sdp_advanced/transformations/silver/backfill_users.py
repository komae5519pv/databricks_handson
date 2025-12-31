from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog_name = spark.conf.get("catalog_name")
schema_name  = spark.conf.get("schema_name")
volume_name  = spark.conf.get("volume_name")

backfill_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/users_backfill/"

def setup_users_backfill_flow(backfill_dir):
    # 1) 入力はまず“文字列中心”で受ける（CSV由来の揺らぎ吸収）
    @dp.view(name="users_backfill_source")
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

        # 2) bz_users の型に“ここで”合わせる（DATE/BOOLEAN）
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

    # 3) AutoCDC で upsert（SCD1）
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


# from pyspark import pipelines as dp
# from pyspark.sql import functions as F

# # 実行環境からパス情報を取得
# catalog_name = spark.conf.get("catalog_name")
# schema_name = spark.conf.get("schema_name")
# volume_name = spark.conf.get("volume_name")

# # バックフィル用データのディレクトリパス
# backfill_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/users_backfill/"

# def setup_users_backfill_flow(backfill_dir):
#     """
#     マスタ不足によるデータ欠損を解消するためのバックフィル用フロー。
#     """

#     # 1. バックフィル用の差分CSVを読み込むビュー
#     @dp.view(name="users_backfill_source")
#     def users_backfill_source():
#         # スキーマを明示的に定義。
#         # registration_date を STRING にすることで、既存テーブルとの型衝突(DateType vs StringType)を防ぎます。
#         user_schema = """
#             user_id STRING, name STRING, age INT, gender STRING, email STRING, 
#             region STRING, registration_date STRING, email_opt_in STRING, 
#             last_updated TIMESTAMP, operation STRING
#         """
        
#         return (
#           spark.readStream.format("cloudFiles")
#           .option("cloudFiles.format", "csv")
#           .schema(user_schema) 
#           .option("cloudFiles.rescuedDataColumn", "_rescued_data_users")
#           .load(backfill_dir)
#         )

#     # 2. AutoCDC を使い、既存の顧客マスタ(bz_users)を差分更新（Upsert）
#     dp.create_auto_cdc_flow(
#       name="users_auto_cdc_backfill",
#       target="bz_users",
#       source="users_backfill_source",
#       keys=["user_id"],
#       sequence_by="last_updated",
#       # operation列が 'DELETE' の場合は削除として扱う
#       apply_as_deletes="operation='DELETE'",
#       # operation列自体はマスタのデータ列ではないため除外
#       except_column_list = ["operation"],
#       stored_as_scd_type=1,
#       # 起動時に一度だけ実行するリカバリモード
#       once=True 
#     )

# # バックフィルフローのセットアップ実行
# setup_users_backfill_flow(backfill_dir)