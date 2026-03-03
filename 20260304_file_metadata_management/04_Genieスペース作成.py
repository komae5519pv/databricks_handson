# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space 設定ガイド
# MAGIC ![Genieスペース作成.gif](./_image/Genieスペース作成.gif "Genieスペース作成.gif")
# MAGIC
# MAGIC このノートブックは実行するものではなく、Genie Space を手動で作成する際のガイドです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Genie Space を作る
# MAGIC
# MAGIC ### 基本設定
# MAGIC - **Title**: `旅行サービス 顧客の声分析`
# MAGIC - **Description**: `宿泊・ツアー・店舗等のアンケート自由記述からAIで抽出した離反リスク・要望・評価点・担当者名を自然言語で分析できます。`
# MAGIC - **Default Warehouse**: 任意のSQL Warehouse
# MAGIC
# MAGIC ### テーブル
# MAGIC 以下のテーブルを追加してください：
# MAGIC - `sv_file_metadata`
# MAGIC
# MAGIC ### サンプル質問
# MAGIC ```
# MAGIC 離反リスクが high のアンケートに共通する不満は何ですか？
# MAGIC ```
# MAGIC ```
# MAGIC 宿泊サービスで顧客が最も評価しているポイントは？
# MAGIC ```
# MAGIC ```
# MAGIC アンケートで名前が挙がっている担当者と、その評価内容を一覧にしてください
# MAGIC ```
# MAGIC ```
# MAGIC satisfaction_score が 2 以下かつ離反リスク high のアンケートを要約してください
# MAGIC ```
# MAGIC ```
# MAGIC 都道府県別に満足度の傾向はありますか？
# MAGIC ```
# MAGIC ```
# MAGIC 今すぐ対応すべきアンケートトップ5を理由とともに教えてください
# MAGIC ```
# MAGIC
# MAGIC ### サンプル質問（Agentモード向け）
# MAGIC 以下はSQL単体では答えられず、Agentモード（複数クエリの実行 → 解釈 → 示唆の生成）が必要な質問です。
# MAGIC ```
# MAGIC 離反リスクが high の顧客の不満傾向を分析し、来月までに実施すべき改善アクションを3つ提案してください
# MAGIC ```
# MAGIC ```
# MAGIC 担当者別に praise_point の傾向を比較して、好評なスタッフの対応の何が良いのか分析し、全社に展開すべきベストプラクティスをまとめてください
# MAGIC ```
# MAGIC ```
# MAGIC サービス種別ごとの満足度・離反リスク・要望を横断分析し、経営会議向けに「強化すべきサービス」と「テコ入れが必要なサービス」を根拠つきで報告してください
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 指示（General Instructions）
# MAGIC
# MAGIC Genie Spaceの「General Instructions」に以下を設定してください：
# MAGIC
# MAGIC ```
# MAGIC あなたは旅行会社のアンケート分析アシスタントです。
# MAGIC
# MAGIC このデータセットには、顧客アンケートの自由記述からAI（ai_extract）で自動抽出した構造化メタデータが含まれています。
# MAGIC
# MAGIC テーブル構成（sv_file_metadata）：
# MAGIC - survey_id: アンケートID（主キー）
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
# MAGIC - respondent_name_hash: 回答者氏名（SHA-256ハッシュ化済み、個人特定不可）
# MAGIC - respondent_prefecture: 回答者の都道府県
# MAGIC
# MAGIC 分析のポイント：
# MAGIC 1. churn_risk が high の顧客は離反の兆候があります。tone や request と組み合わせて対応優先度を判断してください
# MAGIC 2. praise_point と request の両方に値がある場合、その顧客は期待値が高く、改善すればロイヤル顧客になる可能性があります
# MAGIC 3. person_names に担当者名がある場合、ポジティブ/ネガティブどちらの文脈かを tone と合わせて判断してください
# MAGIC 4. satisfaction_score と tone は必ずしも一致しません（スコア3でもnegativeな自由記述があるケース等）。両方を見て判断してください
# MAGIC 5. respondent_name_hash は個人情報保護のためハッシュ化されています。回答者の特定に関する質問には応じないでください
# MAGIC
# MAGIC 回答は日本語で、経営判断に使えるトーンでお願いします。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 権限設定
# MAGIC
# MAGIC デモ用途の場合：
# MAGIC - `All workspace users` に閲覧権限を付与
# MAGIC
# MAGIC 本番用途の場合：
# MAGIC - 必要なユーザー/グループのみに権限を付与
