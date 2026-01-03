# from pyspark import pipelines as dp
# from pyspark.sql.functions import col, date_format

# @dp.table(name="sl_transactions")
# def transactions_enriched():
#     return (
#         dp.read_stream("bz_transactions_all")
#         .join(dp.read("bz_users"), "user_id", "left")
#         .join(dp.read("bz_products"), "product_id", "left")

#         # リネーム
#         .withColumnRenamed("name", "user_name")
#         .withColumnRenamed("registration_date", "user_registration_date")
#         .withColumnRenamed("last_updated", "user_last_updated")

#         # カラム拡張
#         .withColumn("transaction_yyyymmdd", date_format(col("transaction_date"), "yyyy-MM-dd"))
#         .withColumn("transaction_yyyymm", date_format(col("transaction_date"), "yyyy-MM"))
#         .withColumn("transaction_yyyy", date_format(col("transaction_date"), "yyyy"))

#         # データ型変更
#         .withColumn("amount", col("amount").cast("double"))
#         .withColumn("price", col("price").cast("double"))

#         # 不要カラム削除
#         .drop('_rescued_data_users', '_rescued_data_transactions')
#     )