from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="gd_revenue_by_user_segment",
    # 更新時の計算コストを抑えるための優先順位指定
    # - SPUJReplaceWhere: 差分のみ入れ替え
    # - PartitionOverwrite: 影響のあったパーティションごと入れ替え
    # - CompleteRecompute: フルリフレッシュ
    spark_conf={
        "pipelines.enzyme.physicalFlowPriority": "SPUJReplaceWhere > PartitionOverwrite > CompleteRecompute"
    }
)
def revenue_by_user_segment():
    return (
        dp.read("sl_transactions")
        .withColumn("age_group", F.expr("""
            CASE 
                WHEN age < 35 THEN '若年層' 
                WHEN age < 55 THEN '中年層' 
                ELSE 'シニア層' 
            END
        """))
        # 性別、地域、年代別に集計
        .groupBy("age_group", "gender", "region")
        .agg(
            F.sum("amount").alias("sum_revenue"),
            F.count("order_id").alias("order_count")
        )
    )