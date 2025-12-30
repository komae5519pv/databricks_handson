from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="gd_user")
def user_metrics():
    return (
        spark.read.table("bz_users").alias("u")
        .join(
            spark.read.table("sl_transactions").alias("t"),
            "user_id",
            "left"
        )
        .join(
            spark.read.table("bz_products").alias("p"),
            "product_id",
            "left"
        )
        .groupBy("u.user_id", "u.age")
        .agg(
            # 年齢層セグメントの作成
            F.expr("""
                CASE
                    WHEN u.age < 35 THEN '若年層'
                    WHEN u.age < 55 THEN '中年層'
                    ELSE 'シニア層'
                END
            """).alias("age_group"),

            # カテゴリ別集計：PC本体 (ノートパソコン等)
            F.sum(F.expr("CASE WHEN p.subcategory = 'PC本体' THEN t.quantity ELSE 0 END")).alias("pc_quantity"),

            # カテゴリ別集計：周辺機器 (マウス、モニター等)
            F.sum(F.expr("CASE WHEN p.subcategory = '周辺機器' THEN t.quantity ELSE 0 END")).alias("peripheral_quantity"),

            # カテゴリ別集計：その他 (もし今後カテゴリが増えた場合)
            F.sum(F.expr("CASE WHEN p.subcategory NOT IN ('PC本体', '周辺機器') THEN t.quantity ELSE 0 END")).alias("other_quantity")
        )
    )