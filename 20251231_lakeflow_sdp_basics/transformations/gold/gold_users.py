from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="gd_user",
    # 更新時の計算コストを抑えるための優先順位指定
    # - SPUJReplaceWhere: 差分のみ入れ替え
    # - PartitionOverwrite: 影響のあったパーティションごと入れ替え
    # - CompleteRecompute: フルリフレッシュ
    spark_conf={
        "pipelines.enzyme.physicalFlowPriority": "SPUJReplaceWhere > PartitionOverwrite > CompleteRecompute"
    }
)
def user_metrics():
    return (
        spark.read.table("sl_transactions")
        .groupBy("user_id", "age")
        .agg(
            # 年齢層セグメントの作成
            F.expr("""
                CASE
                    WHEN age < 35 THEN '若年層'
                    WHEN age < 55 THEN '中年層'
                    ELSE 'シニア層'
                END
            """).alias("age_group"),

            # カテゴリ別集計：PC本体 (ノートパソコン等)
            F.sum(F.expr("CASE WHEN subcategory = 'PC本体' THEN quantity ELSE 0 END")).alias("pc_quantity"),

            # カテゴリ別集計：周辺機器 (マウス、モニター等)
            F.sum(F.expr("CASE WHEN subcategory = '周辺機器' THEN quantity ELSE 0 END")).alias("peripheral_quantity"),

            # カテゴリ別集計：その他 (もし今後カテゴリが増えた場合)
            F.sum(F.expr("CASE WHEN subcategory NOT IN ('PC本体', '周辺機器') THEN quantity ELSE 0 END")).alias("other_quantity")
        )
    )