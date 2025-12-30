from pyspark import pipelines as dp
from pyspark.sql.functions import col, date_format
from utilities import expectation_rules

master_check = expectation_rules.get_master_check_rules()

@dp.table(name="sl_transactions")

# --- データ品質管理 (Expectations) ---
@dp.expect("amount_positive", "amount > 0")                     # WARN (警告)
@dp.expect("valid_email_format", "email LIKE '%@%'" )           # WARN (警告)
@dp.expect("valid_date", "transaction_date >= '2020-01-01'")    # WARN (警告)
@dp.expect_all_or_drop(master_check)                            # DROP (除外)
# @dp.expect_or_drop("valid_date", "transaction_date >= '2020-01-01'")  # DROP (除外)
# @dp.expect_all_or_fail(master_check)                                  # FAIL (停止)

def transactions_enriched():
    return (
        dp.read_stream("bz_transactions")
        .join(dp.read("bz_users"), "user_id", "left")
        .join(dp.read("bz_products"), "product_id", "left")

        # リネーム
        .withColumnRenamed("name", "user_name")
        .withColumnRenamed("registration_date", "user_registration_date")
        .withColumnRenamed("last_updated", "user_last_updated")

        # カラム拡張
        .withColumn("transaction_yyyymmdd", date_format(col("transaction_date"), "yyyy-MM-dd"))
        .withColumn("transaction_yyyymm", date_format(col("transaction_date"), "yyyy-MM"))
        .withColumn("transaction_yyyy", date_format(col("transaction_date"), "yyyy"))

        # データ型変更
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("price", col("price").cast("double"))

        # 不要カラム削除
        .drop('_rescued_data_users', '_rescued_data_transactions')
    )