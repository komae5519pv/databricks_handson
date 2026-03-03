# Databricks notebook source
# MAGIC %md
# MAGIC # AI抽出 & メタデータテーブル作成
# MAGIC サーバレスコンピュートで実行してください。
# MAGIC
# MAGIC VolumeのJSONファイルを読み込み **Bronzeテーブル** に格納した後、
# MAGIC `ai_extract` を使って **free_comment（非構造化テキスト）** から以下の構造化情報を抽出します：
# MAGIC
# MAGIC | フィールド | 説明 | 活用例 |
# MAGIC |-----------|------|--------|
# MAGIC | tone | 感情トーン | positive/negative/mixed/neutral で傾向把握 |
# MAGIC | churn_risk | 離反リスク | high の顧客を早期フォロー |
# MAGIC | person_names | 登場人名 | 担当者の表彰・指導に活用 |
# MAGIC | request | 顧客の要望 | 改善アクションの優先度付け |
# MAGIC | praise_point | 良かった点 | 強みの可視化・成功事例の横展開 |
# MAGIC | summary | 要約（ai_query） | 一覧での素早い内容把握 |

# COMMAND ----------

# DBTITLE 1,変数設定
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronzeテーブル作成（JSONファイルの取り込み）
# MAGIC
# MAGIC `read_files` でVolume上のJSONファイルを一括読み込みし、
# MAGIC Bronzeテーブル `bz_survey_raw` に格納します。
# MAGIC この段階ではLLMは使いません。

# COMMAND ----------

# DBTITLE 1,VolumeのJSONファイルを読み込み → bz_survey_raw
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.bz_survey_raw AS
SELECT
    -- 連番IDを付与
    ROW_NUMBER() OVER (ORDER BY element_at(split(_metadata.file_path, '/'), -1)) AS survey_id,
    -- _metadata.file_path からディレクトリパスとファイル名を取得
    regexp_replace(_metadata.file_path, '/[^/]+$', '') AS file_dir,
    element_at(split(_metadata.file_path, '/'), -1) AS file_name,
    -- JSONファイルの構造化フィールド
    survey_type,
    survey_date,
    satisfaction_score,
    free_comment,
    -- ネストされた respondent オブジェクトをフラットに展開（氏名はPII保護のためハッシュ化）
    sha2(respondent.name, 256) AS respondent_name_hash,
    respondent.age AS respondent_age,
    respondent.gender AS respondent_gender,
    respondent.prefecture AS respondent_prefecture
FROM read_files(
    '/Volumes/{catalog}/{schema}/{volume}/*.json',
    format => 'json',
    multiLine => 'true'
)
""")

print(f"bz_survey_raw: {spark.table(f'{catalog}.{schema}.bz_survey_raw').count()} 件")
spark.table(f"{catalog}.{schema}.bz_survey_raw").display()

# COMMAND ----------

# DBTITLE 1,bz_survey_raw 主キー・コメント設定
# 主キー制約
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_survey_raw ALTER COLUMN survey_id SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_survey_raw DROP CONSTRAINT IF EXISTS pk_bz_survey_raw")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_survey_raw ADD CONSTRAINT pk_bz_survey_raw PRIMARY KEY (survey_id)")

# テーブルコメント
spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.bz_survey_raw IS
'アンケートBronzeテーブル。VolumeのJSONファイルから共通フィールドを取り込んだ生データ。free_commentは非構造化テキスト。'
""")

# カラムコメント
bronze_comments = {
    "survey_id": "アンケートID（主キー）",
    "file_dir": "Volume上のディレクトリパス",
    "file_name": "ファイル名",
    "survey_type": "アンケート種別（宿泊/ツアー/店舗/オンライン/法人）",
    "survey_date": "回答日",
    "satisfaction_score": "満足度スコア（1-5）",
    "free_comment": "自由記述コメント（非構造化テキスト）",
    "respondent_name_hash": "回答者氏名（SHA-256ハッシュ、PII保護）",
    "respondent_age": "回答者年代",
    "respondent_gender": "回答者性別",
    "respondent_prefecture": "回答者都道府県",
}
for col, comment in bronze_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.bz_survey_raw ALTER COLUMN {col} COMMENT '{comment}'")

print("✅ bz_survey_raw 主キー・コメント設定完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silverテーブル作成（ai_extract でラベリング）
# MAGIC
# MAGIC `ai_extract` は Databricks の組み込みAI関数です。
# MAGIC SQLから直接LLMを呼び出し、テキストから構造化データを抽出します。
# MAGIC
# MAGIC Bronzeテーブルの **free_comment（非構造化テキスト）** に対して
# MAGIC `ai_extract` でラベリングし、`ai_query` で要約を生成して
# MAGIC Silverテーブル `sv_file_metadata` として保存します。

# COMMAND ----------

# DBTITLE 1,ai_extract による構造化抽出 → sv_file_metadata
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.sv_file_metadata AS
SELECT
    -- Bronzeから引き継ぐID・ファイル情報
    survey_id,
    file_dir,
    file_name,
    survey_type,
    survey_date,
    satisfaction_score,
    -- ai_extract で抽出した構造化フィールド
    extracted.tone,
    extracted.churn_risk,
    extracted.person_names,
    extracted.request,
    extracted.praise_point,
    -- 原文（トレーサビリティのため保持）
    free_comment,
    -- ai_query で要約を生成
    ai_query(
        'databricks-claude-opus-4-5',
        CONCAT('以下のアンケートを15文字以内で要約してください。要約のみ出力。\\n\\n', free_comment)
    ) AS summary,
    respondent_name_hash,
    respondent_prefecture
FROM (
    SELECT
        *,
        ai_extract(
            -- 第1引数: 抽出指示（プロンプト）+ 対象テキスト
            CONCAT(
                '以下のアンケート自由記述を分析して、指定された項目を抽出してください。',
                '\\n【抽出ルール】',
                '\\n- tone: positive, negative, mixed, neutral から選択',
                '\\n- churn_risk: high, medium, low から選択（他社言及・強い不満・解約示唆があれば high）',
                '\\n- person_names: テキスト中に登場する担当者・スタッフ名（敬称なし、カンマ区切り、なければ空文字）',
                '\\n- request: 何を・どう改善してほしいか具体的に（50文字以内、なければ空文字）',
                '\\n- praise_point: 何が・どう良かったか具体的に（50文字以内、なければ空文字）',
                '\\n\\nアンケート記述:\\n',
                free_comment
            ),
            -- 第2引数: 抽出するフィールド名の配列
            ARRAY('tone', 'churn_risk', 'person_names', 'request', 'praise_point')
        ) AS extracted
    FROM {catalog}.{schema}.bz_survey_raw
)
""")

print(f"sv_file_metadata: {spark.table(f'{catalog}.{schema}.sv_file_metadata').count()} 件")
spark.table(f"{catalog}.{schema}.sv_file_metadata").display()

# COMMAND ----------

# DBTITLE 1,sv_file_metadata 主キー・コメント設定
# 主キー制約
spark.sql(f"ALTER TABLE {catalog}.{schema}.sv_file_metadata ALTER COLUMN survey_id SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{schema}.sv_file_metadata DROP CONSTRAINT IF EXISTS pk_sv_file_metadata")
spark.sql(f"ALTER TABLE {catalog}.{schema}.sv_file_metadata ADD CONSTRAINT pk_sv_file_metadata PRIMARY KEY (survey_id)")

# テーブルコメント
spark.sql(f"""COMMENT ON TABLE {catalog}.{schema}.sv_file_metadata IS
'アンケートSilverテーブル。Bronzeテーブルのfree_comment（非構造化テキスト）からai_extractで構造化情報を抽出し、ai_queryで要約を生成。離反リスク判定・担当者名抽出・要望/良い点の自動分類を含む。'
""")

# カラムコメント
silver_comments = {
    "survey_id": "アンケートID（主キー、Bronze連携）",
    "file_dir": "Volume上のディレクトリパス",
    "file_name": "ファイル名",
    "survey_type": "アンケート種別",
    "survey_date": "回答日",
    "satisfaction_score": "満足度スコア（1-5）",
    "tone": "感情トーン（positive/negative/mixed/neutral）",
    "churn_risk": "離反リスク（high/medium/low）",
    "person_names": "登場する担当者・スタッフ名（敬称なし、カンマ区切り）",
    "request": "顧客の要望・改善点（AI抽出、50文字以内）",
    "praise_point": "顧客が評価している良かった点（AI抽出、50文字以内）",
    "free_comment": "自由記述コメント原文",
    "summary": "自由記述の要約（ai_queryで生成、15文字以内）",
    "respondent_name_hash": "回答者氏名（SHA-256ハッシュ、PII保護）",
    "respondent_prefecture": "回答者都道府県",
}
for col, comment in silver_comments.items():
    spark.sql(f"ALTER TABLE {catalog}.{schema}.sv_file_metadata ALTER COLUMN {col} COMMENT '{comment}'")

print("✅ sv_file_metadata 主キー・コメント設定完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 抽出結果の確認

# COMMAND ----------

# DBTITLE 1,離反リスク別の件数分布
spark.sql(f"""
SELECT churn_risk, COUNT(*) AS cnt,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM {catalog}.{schema}.sv_file_metadata
GROUP BY churn_risk
ORDER BY cnt DESC
""").display()

# COMMAND ----------

# DBTITLE 1,トーン × 離反リスク クロス集計
spark.sql(f"""
SELECT tone, churn_risk, COUNT(*) AS cnt
FROM {catalog}.{schema}.sv_file_metadata
GROUP BY tone, churn_risk
ORDER BY tone, churn_risk
""").display()

# COMMAND ----------

# DBTITLE 1,Bronze vs Silver 比較（ai_extractの効果を確認）
spark.sql(f"""
SELECT
    b.survey_id,
    b.file_name,
    b.survey_type,
    s.tone,
    s.churn_risk,
    s.praise_point,
    s.request,
    s.summary
FROM {catalog}.{schema}.bz_survey_raw b
JOIN {catalog}.{schema}.sv_file_metadata s
    ON b.survey_id = s.survey_id
LIMIT 10
""").display()
