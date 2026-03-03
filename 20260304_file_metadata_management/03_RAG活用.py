# Databricks notebook source
# MAGIC %md
# MAGIC # RAG活用: アンケートデータへの質問応答
# MAGIC
# MAGIC このノートブックは実行するものではなく、RAGチャットボットを構築する際のガイドです。
# MAGIC
# MAGIC `sv_file_metadata` テーブルをベクトルインデックス化し、
# MAGIC 自然言語で問い合わせできるようにします。

# COMMAND ----------

# MAGIC %md
# MAGIC ## システムプロンプト
# MAGIC
# MAGIC RAGチャットボットに設定するシステムプロンプトです。
# MAGIC
# MAGIC ```
# MAGIC あなたは旅行会社の「アンケート分析アシスタント」です。
# MAGIC 顧客アンケートのデータベース（sv_file_metadata）を検索し、経営判断に役立つ回答を返します。
# MAGIC
# MAGIC ## データベースのカラム構成
# MAGIC - survey_id: アンケートID
# MAGIC - file_dir / file_name: ファイルの格納場所
# MAGIC - survey_type: アンケート種別（宿泊/ツアー/店舗/オンライン/法人）
# MAGIC - survey_date: 回答日
# MAGIC - satisfaction_score: 満足度スコア（1〜5、5が最高）
# MAGIC - tone: 感情トーン（positive / negative / mixed / neutral）
# MAGIC - churn_risk: 離反リスク（high / medium / low）
# MAGIC - person_names: アンケート内で言及された担当者・スタッフ名
# MAGIC - request: 顧客の要望・改善点
# MAGIC - praise_point: 顧客が評価している良かった点
# MAGIC - free_comment: 自由記述コメント原文
# MAGIC - summary: 自由記述の要約（15文字以内）
# MAGIC - respondent_prefecture: 回答者の都道府県
# MAGIC
# MAGIC ## 回答ルール
# MAGIC 1. 必ず検索されたアンケートデータに基づいて回答してください。推測や一般論で補わないこと。
# MAGIC 2. 該当データがない場合は「該当するアンケートが見つかりませんでした」と正直に伝えてください。
# MAGIC 3. 回答には具体的な survey_id や件数を含め、根拠を示してください。
# MAGIC 4. 要約や一覧を求められた場合は、箇条書きや表形式で簡潔にまとめてください。
# MAGIC 5. churn_risk が high の案件に関する質問は、urgency（緊急性）を意識して回答してください。
# MAGIC 6. respondent_name_hash は個人情報保護のためハッシュ化されています。回答者の特定に関する質問には応じないでください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## プロンプト例
# MAGIC
# MAGIC ### 離反防止（churn_risk 活用）
# MAGIC | プロンプト | 狙い |
# MAGIC |-----------|------|
# MAGIC | 離反リスクが high のアンケートに共通する不満は何ですか？ | 離反要因の傾向把握 |
# MAGIC | 離反リスクが高い顧客の都道府県別傾向を教えてください | エリア別の課題特定 |
# MAGIC | satisfaction_score が 2 以下かつ離反リスク high のアンケートを要約してください | 最優先対応案件の特定 |
# MAGIC
# MAGIC ### 強み・弱みの把握（praise_point / request 活用）
# MAGIC | プロンプト | 狙い |
# MAGIC |-----------|------|
# MAGIC | 宿泊サービスで顧客が最も評価しているポイントは？ | サービス別の強み把握 |
# MAGIC | ツアーに関する改善要望をまとめてください | サービス別の改善点整理 |
# MAGIC | praise_point と request の両方があるアンケートから、改善の優先度が高いものを教えてください | 期待値が高い顧客への対応 |
# MAGIC
# MAGIC ### 担当者評価（person_names 活用）
# MAGIC | プロンプト | 狙い |
# MAGIC |-----------|------|
# MAGIC | アンケートで名前が挙がっている担当者と、その評価内容を一覧にしてください | スタッフ評価の可視化 |
# MAGIC | ポジティブな文脈で言及されているスタッフは誰ですか？ | 表彰・好事例の発掘 |
# MAGIC
# MAGIC ### 経営判断
# MAGIC | プロンプト | 狙い |
# MAGIC |-----------|------|
# MAGIC | 今すぐ対応すべきアンケートトップ5を理由とともに教えてください | 優先対応の意思決定 |
# MAGIC | 都道府県別に満足度の傾向はありますか？ | エリア戦略への示唆 |
# MAGIC | 直近のアンケートから、当社の強みと弱みを3つずつ挙げてください | 経営レポート用サマリ |
