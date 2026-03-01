# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze レイヤー: 通話ログの取り込み
# MAGIC サーバレスコンピュートで実行してください。
# MAGIC
# MAGIC VolumeのJSONLファイルを読み込み、Bronzeテーブルとして保存します。
# MAGIC 生データの構造をそのまま忠実に取り込む「RAW取り込み」レイヤーです。
# MAGIC
# MAGIC **ポイント**: transcriptフィールドは非構造化テキスト（通話記録そのまま）。
# MAGIC 次のSilverレイヤーで`ai_extract`を使って構造化抽出を行います。

# COMMAND ----------

# DBTITLE 1,変数設定
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,VolumeからJSONLを読み込み
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

call_logs_path = f"/Volumes/{catalog}/{schema}/{volume}/call_logs/"

# スキーマを明示的に定義（型安全な取り込み）
call_log_schema = StructType([
    StructField("call_id", StringType(), False),
    StructField("call_datetime", StringType(), True),
    StructField("call_duration_sec", IntegerType(), True),
    StructField("caller_number", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("customer_gender", StringType(), True),
    StructField("customer_prefecture", StringType(), True),
    StructField("past_inquiry_count", IntegerType(), True),
    StructField("inquiry_type", StringType(), True),
    StructField("inquiry_detail", StringType(), True),
    StructField("transcript", StringType(), True),
    StructField("operator_action", StringType(), True),
    StructField("proposed_solution", StringType(), True),
    StructField("info_provided", StringType(), True),
    StructField("resolved", BooleanType(), True),
    StructField("customer_satisfaction", IntegerType(), True),
    StructField("follow_up_required", BooleanType(), True),
])

df_raw = (
    spark.read
    .schema(call_log_schema)
    .option("multiLine", "false")
    .json(call_logs_path)
)

print(f"読み込み件数: {df_raw.count()}")
df_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,型変換・カラム整理
df_bronze = (
    df_raw
    .withColumn("call_date", F.to_date(F.col("call_datetime")))
    .withColumn("call_time", F.date_format(F.to_timestamp("call_datetime"), "HH:mm:ss"))
    .select(
        "call_id",
        "call_date",
        "call_time",
        "call_duration_sec",
        "caller_number",
        "customer_id",
        "customer_name",
        F.col("customer_age").alias("age"),
        F.col("customer_gender").alias("gender"),
        F.col("customer_prefecture").alias("prefecture"),
        "past_inquiry_count",
        "inquiry_type",
        "inquiry_detail",
        "transcript",
        "operator_action",
        "proposed_solution",
        "info_provided",
        "resolved",
        "customer_satisfaction",
        "follow_up_required",
    )
    .orderBy("call_id")
)

print(f"Bronze テーブル行数: {df_bronze.count()}")
df_bronze.display()

# COMMAND ----------

# DBTITLE 1,Bronzeテーブルとして保存
df_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.bz_call_logs"
)

# COMMAND ----------

# DBTITLE 1,主キー・コメント設定
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_call_logs ALTER COLUMN call_id SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_call_logs DROP CONSTRAINT IF EXISTS pk_bz_call_logs")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_call_logs ADD CONSTRAINT pk_bz_call_logs PRIMARY KEY (call_id)")

spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.bz_call_logs IS
'通話ログBronzeテーブル。VolumeのJSONLファイルから取り込んだ生データ。1行1コール、全5,000件。transcriptフィールドは非構造化テキスト（オペレーターと顧客の会話記録）。Silverレイヤーでai_extractによる構造化抽出を行う。'
""")

column_comments = {
    "call_id": "通話ID（CALL-00001形式）",
    "call_date": "通話日",
    "call_time": "通話開始時刻",
    "call_duration_sec": "通話時間（秒）",
    "caller_number": "発信者電話番号（マスク済み）",
    "customer_id": "顧客ID（FK → customers）",
    "customer_name": "顧客氏名",
    "age": "顧客年齢",
    "gender": "顧客性別",
    "prefecture": "顧客都道府県",
    "past_inquiry_count": "過去の問い合わせ回数",
    "inquiry_type": "問い合わせ種別（質問/苦情/要望/相談/確認）",
    "inquiry_detail": "問い合わせ内容の概要",
    "transcript": "通話記録テキスト（非構造化データ・会話のやり取り全文）",
    "operator_action": "オペレーター対応内容",
    "proposed_solution": "提案された解決策",
    "info_provided": "提供された情報",
    "resolved": "解決済みフラグ",
    "customer_satisfaction": "顧客満足度（1-5、未取得の場合NULL）",
    "follow_up_required": "フォローアップ要否",
}
for col, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_call_logs ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# DBTITLE 1,確認: 基本統計
spark.sql(f"""
SELECT
    COUNT(*) AS total_calls,
    MIN(call_date) AS earliest_call,
    MAX(call_date) AS latest_call,
    ROUND(AVG(call_duration_sec), 0) AS avg_duration_sec,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(AVG(LENGTH(transcript)), 0) AS avg_transcript_length
FROM {catalog}.{schema}.bz_call_logs
""").display()

# COMMAND ----------

# DBTITLE 1,確認: 問い合わせ種別の分布
spark.sql(f"""
SELECT
    inquiry_type,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM {catalog}.{schema}.bz_call_logs
GROUP BY inquiry_type
ORDER BY cnt DESC
""").display()

# COMMAND ----------

# DBTITLE 1,確認: transcriptサンプル（非構造化データの中身を確認）
spark.sql(f"""
SELECT call_id, inquiry_type, LEFT(transcript, 300) AS transcript_preview
FROM {catalog}.{schema}.bz_call_logs
LIMIT 3
""").display()
