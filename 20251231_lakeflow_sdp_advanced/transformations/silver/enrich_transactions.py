from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Step 1: Factデータの確定 (Streaming + Watermark) ---
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


# --- ビジネス用情報の付加  ---
# このテーブルが、マスタ補充（Backfill）時の「自動補完」を担います。
@dp.table(
    name="sl_transactions_enriched",
    comment="マスタ情報を結合した分析用Silverテーブル。マスタ更新時に過去のNULLを自動補完する。"
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


# from pyspark import pipelines as dp
# from pyspark.sql import functions as F
# from utilities import expectation_rules

# # マスタ存在チェックルールをロード
# master_check = expectation_rules.get_master_check_rules()

# @dp.table(name="sl_transactions")

# # --- データ品質管理 (Expectations) ---
# # マスタに存在しない（U04など）レコードを一時的に除外(DROP)し、後のBackfillで救済する流れを作ります
# # @dp.expect_all_or_drop(master_check)                                    # DROP (除外)
# @dp.expect_or_drop("product_id_is_not_null", "product_id IS NOT NULL")  # DROP (除外)
# @dp.expect_or_drop("user_id_is_not_null", "user_id IS NOT NULL")        # DROP (除外)
# @dp.expect("user_name_is_not_null", "user_name IS NOT NULL")            # WARN (警告)
# @dp.expect("amount_positive", "amount > 0")                             # WARN (警告)

# def enrich_transactions():
#     '''
#     1) 型保証 → 2) Watermark → 3) 遅延ドロップ(状態算子)
#        ポイント:
#        - Watermark は「単体では行を捨てない」。状態を持つ演算（例: dropDuplicates, ウィンドウ集約, stream-stream join）と
#          組み合わせたときにのみ“水位より古いイベント”を安全に破棄できる。
#        - ここでは最小コストの状態算子として dropDuplicates を入れて、遅延ドロップを発動させる。
#        - 型は withWatermark 直前に timestamp/double へ明示 cast し、推論ズレを排除する（cast は早い段で行うほど安全）。
#     '''
#     fact = (
#         dp.read_stream("bz_transactions")
#             # [型保証] 推論やスキーマ進化の揺らぎで string になる事故を予防。
#             #          withWatermark 対象カラムは timestamp 型必須。
#             .withColumn("transaction_date", F.col("transaction_date").cast("timestamp"))
#             .withColumn("amount", F.col("amount").cast("double"))

#             # [Watermark] イベント時刻(transaction_date)を基準に“遅延許容幅”を宣言。
#             #             ここでは 1日。水位より古いイベントは「状態算子の観点で安全に無視」できる。
#             #             実運用では到着遅延の実態に合わせて最小限に調整（短いほど状態が小さく高効率）。
#             .withWatermark("transaction_date", "1 day")

#             # [状態算子で遅延ドロップを発動] order_id で一意性を担保。
#             #  - dropDuplicates は Watermark と組み合わせると、水位より古いイベントを状態外として破棄。
#             #  - 状態サイズ ≒ 「Watermark 窓幅 × その間のユニーク order_id 数」。1日分が保持上限の目安。
#             #  - 同一 order_id の再送が日を跨いで来るなら ["order_id", "transaction_date"] などに広げる。
#             .dropDuplicates(["order_id"])
#     )

#     # [マスタ結合と整形]
#     #  - ここは static join 相当（dp.read はスナップショット読み）で状態を増やさない。
#     #  - 遅延ドロップは上流の fact で完結させ、結合側には Watermark を持ち込まないのが安定運用の定石。
#     return (
#         fact
#         .join(dp.read("bz_users"), "user_id", "left")
#         .join(dp.read("bz_products"), "product_id", "left")
#         .withColumnRenamed("name", "user_name")
#         .drop("_rescued_data_users", "_rescued_data_transactions")
#     )
