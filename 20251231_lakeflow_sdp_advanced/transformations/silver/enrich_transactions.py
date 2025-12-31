from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Factデータの確定 (Streaming + Watermark) ---
# このテーブルは「新着データの制御」に専念します。
@dp.table(
    name="sl_transactions_watermarked",
    comment="販売履歴のストリーミング確定テーブル。Watermarkによる遅延制御と重複排除を実施。"
)
@dp.expect_or_drop("product_id_is_not_null", "product_id IS NOT NULL")
@dp.expect_or_drop("user_id_is_not_null", "user_id IS NOT NULL")
@dp.expect("amount_positive", "amount > 0")
def sl_transactions_watermarked():
    '''
    1) 型保証 → 2) Watermark → 3) 遅延ドロップ(状態算子)
       ポイント:
       - Watermark は「単体では行を捨てない」。状態を持つ演算（dropDuplicates等）と
         組み合わせたときにのみ“水位より古いイベント”を安全に破棄できる。
       - 型は withWatermark 直前に timestamp/double へ明示 cast し、推論ズレを排除。
    '''
    return (
        dp.read_stream("bz_transactions")
            # [型保証] withWatermark 対象カラムは timestamp 型が必須。
            .withColumn("transaction_date", F.col("transaction_date").cast("timestamp"))
            .withColumn("amount", F.col("amount").cast("double"))

            # [Watermark] イベント時刻を基準に遅延許容幅を宣言（1日）。
            #             実運用では到着遅延の実績に合わせて調整。
            .withWatermark("transaction_date", "1 day")

            # [状態算子で遅延ドロップを発動]
            #  - Watermarkと組み合わせることで、水位より古いイベントを状態外として破棄。
            #  - 状態サイズは「Watermark窓幅 × ユニークID数」に依存するため、短めに設定するのが高効率。
            .dropDuplicates(["order_id"])
    )


# --- ビジネス情報の付加  ---
# このテーブルが、マスタ補充（Backfill）時の「自動補完」を担います。
@dp.materialized_view(
    name="sl_transactions_enriched",
    comment="マスタ情報を結合した分析用Silverテーブル。マスタ更新時に過去のNULLを自動補完する。",
    # 更新時の計算コストを抑えるための優先順位指定
    # - SPUJReplaceWhere: 差分のみ入れ替え
    # - PartitionOverwrite: 影響のあったパーティションごと入れ替え
    # - CompleteRecompute: フルリフレッシュ
    spark_conf={
        "pipelines.enzyme.physicalFlowPriority": "SPUJReplaceWhere > PartitionOverwrite > CompleteRecompute"
    }
)
# マスタ不在（user_nameがNULL）を検知。
# マテリアライズド・ビューなので、バックフィル後にここが「パス（警告なし）」に変わる様子が見せられる。
@dp.expect("user_name_is_not_null", "user_name IS NOT NULL") 
def sl_transactions_enriched():
    '''
    [マスタ結合と整合性確保の自動化]
    - ストリーム(read_stream)ではなく通常読み(read)を行うことで、マテリアライズド・ビューとして定義。
    - これにより、結合先のマスタ(bz_users)がバックフィル等で更新された際、
      DLTがこのテーブルを自動的に再計算の対象に含め、過去のNULLを最新の名称で上書き（補完）する。
    - 遅延ドロップ等の重い処理は上流の sl_transactions で完結させておくのが安定運用の定石。
    '''
    return (
        dp.read("sl_transactions_watermarked")
            .join(dp.read("bz_users"), "user_id", "left")
            .join(dp.read("bz_products"), "product_id", "left")
            .withColumnRenamed("name", "user_name")
            .drop("_rescued_data_users", "_rescued_data_transactions")
    )