from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="gd_revenue_by_subcategory",
    # 更新時の計算コストを抑えるための優先順位指定
    # - SPUJReplaceWhere: 差分のみ入れ替え
    # - PartitionOverwrite: 影響のあったパーティションごと入れ替え
    # - CompleteRecompute: フルリフレッシュ
    spark_conf={
        "pipelines.enzyme.physicalFlowPriority": "SPUJReplaceWhere > PartitionOverwrite > CompleteRecompute"
    }
)
def revenue_by_subcategory():
    return (
        dp.read("sl_transactions")
        .groupBy("category", "subcategory")
        .agg(
            F.sum("amount").alias("sum_revenue"),
            F.sum("quantity").alias("sum_quantity"),
            F.count("order_id").alias("order_count")
        )
    )