# Databricks notebook source
# MAGIC %md
# MAGIC # Silver レイヤー: 通話ログの構造化抽出
# MAGIC サーバレスコンピュートで実行してください。
# MAGIC
# MAGIC Bronzeテーブルの **transcript（非構造化テキスト）** から
# MAGIC `ai_extract` を使って以下の構造化情報を抽出します：
# MAGIC
# MAGIC | フィールド | 説明 |
# MAGIC |-----------|------|
# MAGIC | purpose | 問い合わせの主な目的 |
# MAGIC | urgency_level | 緊急度（高/中/低） |
# MAGIC | trouble_type | トラブル要因 |
# MAGIC | competitor_mention | 他社への言及有無 |
# MAGIC | operator_action | オペレーター対応の要約 |
# MAGIC | next_intent | 顧客の次のアクション意向 |
# MAGIC | resolution_status | 解決状況 |
# MAGIC | transcript_summary | 通話内容の要約（ai_query生成） |
# MAGIC
# MAGIC **これがDatabricksの非構造化データ処理の真価です。**
# MAGIC 通話記録のテキスト（自然言語）から、AIが自動で構造化データを生成します。

# COMMAND ----------

# DBTITLE 1,変数設定
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronzeテーブルからデータ読み込み

# COMMAND ----------

# DBTITLE 1,Bronzeテーブルの読み込み
from pyspark.sql import functions as F

df_bronze = spark.table(f"{catalog}.{schema}.bz_call_logs")

print(f"Bronze レコード数: {df_bronze.count()}")
print(f"平均 transcript 長: {df_bronze.select(F.avg(F.length('transcript'))).first()[0]:.0f} 文字")

# transcriptのサンプルを表示
df_bronze.select("call_id", "inquiry_type", F.substring("transcript", 1, 200).alias("transcript_preview")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ai_extract で構造化抽出 → Silverテーブル保存
# MAGIC
# MAGIC `ai_extract` は Databricks の組み込みAI関数です。
# MAGIC SQLから直接LLMを呼び出し、テキストから構造化データを抽出します。
# MAGIC 抽出結果はそのまま `sv_call_analysis` テーブルとして保存します。

# COMMAND ----------

# DBTITLE 1,ai_extract による構造化抽出 → Silverテーブル保存
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.sv_call_analysis AS
SELECT
    call_id,
    customer_id,
    call_date,
    call_duration_sec,
    extracted.purpose,
    extracted.urgency_level,
    extracted.trouble_type,
    TRY_CAST(extracted.competitor_mention AS BOOLEAN) AS competitor_mention,
    extracted.operator_action,
    extracted.next_intent,
    extracted.resolution_status,
    transcript,
    ai_query(
        'databricks-claude-opus-4-5',
        CONCAT('以下のコールセンター通話記録を1文（50文字以内）で要約してください。要約のみ出力してください。\n\n', transcript)
    ) AS transcript_summary
FROM (
    SELECT
        call_id,
        customer_id,
        call_date,
        call_duration_sec,
        transcript,
        ai_extract(
            -- 指示（プロンプト）をここにまとめる
            CONCAT(
                '以下の通話記録から、指定された項目を抽出してください。',
                '\n【抽出ルール】',
                '\n- purpose: 割引クレーム, 返品交換, 配送納期, 注文変更, 在庫確認, 商品不良, 他社比較, お直し, 請求支払い, お手入れ, その他から選択',
                '\n- urgency_level: 高, 中, 低（期限付きなら高、急ぎなら中）',
                '\n- trouble_type: 割引条件, サイズ, 納期, 品質, 請求, なしから選択',
                '\n- competitor_mention: 他社への言及があれば true、なければ false',
                '\n- operator_action: オペレーターの対応要約（20文字以内）',
                '\n- next_intent: 購入予定, 検討中, キャンセル, 来店予定, 交換依頼, 返品依頼から選択',
                '\n- resolution_status: 解決, 未解決, エスカレーションから選択',
                '\n\n通話記録:\n',
                transcript
            ),
            -- 第2引数は「短いフィールド名」だけにする
            ARRAY(
                'purpose', 
                'urgency_level', 
                'trouble_type', 
                'competitor_mention', 
                'operator_action', 
                'next_intent', 
                'resolution_status'
            )
        ) AS extracted
    FROM {catalog}.{schema}.bz_call_logs
)
""")

print(f"sv_call_analysis: {spark.table(f'{catalog}.{schema}.sv_call_analysis').count()} 件")
spark.table(f"{catalog}.{schema}.sv_call_analysis").display()

# COMMAND ----------

# DBTITLE 1,主キー・コメント設定
spark.sql(f"ALTER TABLE {catalog}.{schema}.sv_call_analysis ALTER COLUMN call_id SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{schema}.sv_call_analysis DROP CONSTRAINT IF EXISTS pk_call_analysis")
spark.sql(f"ALTER TABLE {catalog}.{schema}.sv_call_analysis ADD CONSTRAINT pk_call_analysis PRIMARY KEY (call_id)")

spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.sv_call_analysis IS
'通話ログSilverテーブル。Bronzeテーブルのtranscript（非構造化テキスト）からai_extractで構造化情報を抽出し、ai_queryで通話要約を生成した結果。問い合わせ目的、緊急度、トラブル要因、他社言及、オペレーター対応、次の意向、解決状況、通話要約を含む。'
""")

silver_comments = {
    "call_id": "通話ID（主キー）",
    "customer_id": "顧客ID（FK → customers）",
    "call_date": "通話日",
    "call_duration_sec": "通話時間（秒）",
    "purpose": "問い合わせ目的（割引クレーム/返品交換/配送納期/注文変更/在庫確認/商品不良/他社比較/お直し/請求支払い/お手入れ/その他）",
    "urgency_level": "緊急度（高/中/低）",
    "trouble_type": "トラブル要因（割引条件/サイズ/納期/品質/請求/なし）",
    "competitor_mention": "他社言及の有無",
    "operator_action": "オペレーターの主な対応内容（AI抽出）",
    "next_intent": "顧客の次のアクション意向（購入予定/検討中/キャンセル/来店予定/交換依頼/返品依頼）",
    "resolution_status": "解決状況（解決/未解決/エスカレーション）",
    "transcript": "通話ログ全文（非構造化テキスト）",
    "transcript_summary": "通話内容の要約（ai_queryで生成、50文字以内）",
}
for col, comment in silver_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.sv_call_analysis ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 抽出結果の確認

# COMMAND ----------

# DBTITLE 1,目的別の件数分布
spark.sql(f"""
SELECT purpose, COUNT(*) AS cnt,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM {catalog}.{schema}.sv_call_analysis
GROUP BY purpose
ORDER BY cnt DESC
""").display()

# COMMAND ----------

# DBTITLE 1,トラブル要因別の件数分布
spark.sql(f"""
SELECT trouble_type, COUNT(*) AS cnt,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM {catalog}.{schema}.sv_call_analysis
GROUP BY trouble_type
ORDER BY cnt DESC
""").display()

# COMMAND ----------

# DBTITLE 1,解決状況の分布
spark.sql(f"""
SELECT resolution_status, COUNT(*) AS cnt,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM {catalog}.{schema}.sv_call_analysis
GROUP BY resolution_status
ORDER BY cnt DESC
""").display()

# COMMAND ----------

# DBTITLE 1,Bronze vs Silver 比較（ai_extractの威力を確認）
spark.sql(f"""
SELECT
    b.call_id,
    b.inquiry_type AS bronze_inquiry_type,
    s.purpose AS silver_purpose,
    s.trouble_type AS silver_trouble_type,
    s.urgency_level AS silver_urgency_level,
    LEFT(b.transcript, 150) AS transcript_preview
FROM {catalog}.{schema}.bz_call_logs b
JOIN {catalog}.{schema}.sv_call_analysis s
    ON b.call_id = s.call_id
LIMIT 10
""").display()
