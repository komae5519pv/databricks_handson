from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="gd_revenue_report",
    comment="[Materialized View] 多角的な分析用ディメンションを付与した売上レポート。",
    # 更新時の計算コストを抑えるための優先順位指定
    # - SPUJReplaceWhere: 差分のみ入れ替え
    # - PartitionOverwrite: 影響のあったパーティションごと入れ替え
    # - CompleteRecompute: フルリフレッシュ
    spark_conf={
        "pipelines.enzyme.physicalFlowPriority": "SPUJReplaceWhere > PartitionOverwrite > CompleteRecompute"
    }
)
def gd_revenue_report():
    """
    概要: ユーザー属性、時間軸、カテゴリ内訳を統合した分析用マテリアライズド・ビュー。
    
    追加ディメンション:
    - 年齢層 (age_group): 若年・中年・シニアのセグメント
    - 時間軸 (YYYY-MM-DD, YYYY-MM, YYYY): 時系列分析用
    - 商品カテゴリ内訳 (PC本体, 周辺機器, その他): 横持ちでの数量集計
    """
    
    return (
        dp.read("sl_transactions_enriched")
        .groupBy("user_id", "user_name", "gender", "age")
        .agg(
            # 1. 年齢層セグメントの作成
            F.expr("""
                CASE
                    WHEN age < 35 THEN '若年層'
                    WHEN age < 55 THEN '中年層'
                    ELSE 'シニア層'
                END
            """).alias("age_group"),

            # 2. 売上・注文数の基本集計
            F.sum("amount").alias("total_sales"),
            F.count("order_id").alias("total_orders"),

            # 3. 最終購入日の日付加工 (yyyy-mm-dd, yyyy-mm, yyyy)
            F.max("transaction_date").alias("last_purchase_timestamp"),
            F.date_format(F.max("transaction_date"), "yyyy-MM-dd").alias("last_purchase_date"),
            F.date_format(F.max("transaction_date"), "yyyy-MM").alias("last_purchase_month"),
            F.date_format(F.max("transaction_date"), "yyyy").alias("last_purchase_year"),

            # 4. カテゴリ別・数量内訳 (横持ち集計)
            F.sum(F.expr("CASE WHEN subcategory = 'PC本体' THEN quantity ELSE 0 END")).alias("pc_quantity"),
            F.sum(F.expr("CASE WHEN subcategory = '周辺機器' THEN quantity ELSE 0 END")).alias("peripheral_quantity"),
            F.sum(F.expr("CASE WHEN subcategory NOT IN ('PC本体', '周辺機器') THEN quantity ELSE 0 END")).alias("other_quantity")
        )
        # 最後に売上順にソート
        .orderBy(F.desc("total_sales"))
    )