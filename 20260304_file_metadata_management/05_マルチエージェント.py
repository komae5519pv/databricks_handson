# Databricks notebook source
# MAGIC %md
# MAGIC # マルチエージェント: RAG × Genie 連携分析
# MAGIC
# MAGIC このノートブックは実行するものではなく、マルチエージェントを構築する際のガイドです。
# MAGIC
# MAGIC RAG（ベクトル検索）と Genie（SQL分析）を組み合わせることで、
# MAGIC **非構造化テキストの意味検索** と **構造化データの集計・フィルタリング** を
# MAGIC 1つのエージェントで横断的に実行できます。
# MAGIC
# MAGIC | エージェント | 役割 | 得意なこと |
# MAGIC |-------------|------|-----------|
# MAGIC | RAG | ベクトル検索 | 自由記述の意味的な類似検索・文脈理解 |
# MAGIC | Genie | SQL分析 | 構造化カラムの集計・フィルタリング・クロス集計 |
# MAGIC | マルチエージェント | オーケストレーション | 両者を組み合わせた横断分析・根拠付きの示唆生成 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. システムプロンプト
# MAGIC
# MAGIC マルチエージェントのオーケストレーターに設定するシステムプロンプトです。
# MAGIC
# MAGIC ```
# MAGIC あなたは旅行会社の「アンケート横断分析エージェント」です。
# MAGIC 以下の2つのツールを使い分けて回答してください。
# MAGIC
# MAGIC - RAGツール: 自由記述（free_comment）の意味検索。具体的な顧客の声を探すときに使用。
# MAGIC - Genieツール: 構造化カラムのSQL集計・フィルタリング。件数・割合・ランキング等の定量分析に使用。
# MAGIC
# MAGIC 回答ルール：
# MAGIC - 定量的な主張にはSQL集計の根拠を、定性的な主張には顧客の声の原文を必ず添えてください
# MAGIC - 両ツールの結果を統合し、示唆とアクション提案を含めてください
# MAGIC - 回答者の特定に関する質問には応じないでください（氏名はハッシュ化済み）
# MAGIC - 回答は日本語で、経営判断に使えるトーンでお願いします
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. サンプル質問
# MAGIC
# MAGIC ### 質問1: 定量 × 定性の横断分析
# MAGIC ```
# MAGIC 離反リスクが high の顧客は何件いますか？その中で最も多い不満の傾向を、具体的な顧客の声とともに教えてください
# MAGIC ```
# MAGIC **想定フロー**: Genie で churn_risk = 'high' の件数を集計 → RAG で離反リスク高の自由記述を意味検索 → 不満傾向を要約
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 質問2: サービス改善の優先度付け
# MAGIC ```
# MAGIC サービス種別ごとの満足度平均と離反リスク high の割合を出した上で、最も改善が急務なサービスについて具体的な顧客要望を5件挙げてください
# MAGIC ```
# MAGIC **想定フロー**: Genie で survey_type 別の satisfaction_score 平均と churn_risk 集計 → 最も課題のあるサービスを特定 → RAG でそのサービスの要望を検索
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 質問3: 担当者評価の深掘り
# MAGIC ```
# MAGIC 担当者名が挙がっているアンケートの件数と、ポジティブ・ネガティブの内訳を集計してください。特に高評価の担当者について、顧客が具体的に何を評価しているのか原文で教えてください
# MAGIC ```
# MAGIC **想定フロー**: Genie で person_names が空でないレコードを tone 別に集計 → RAG で高評価担当者に関する praise_point の原文を検索
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 質問4: エリア戦略の立案
# MAGIC ```
# MAGIC 都道府県別の満足度と離反リスクの分布を分析し、重点対応すべきエリアを特定してください。そのエリアの顧客がどんな改善を求めているか、具体的な声もまとめてください
# MAGIC ```
# MAGIC **想定フロー**: Genie で respondent_prefecture 別の satisfaction_score 平均と churn_risk 分布を集計 → 課題エリアを特定 → RAG でそのエリアの request を意味検索
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 質問5: 経営会議向けレポート生成
# MAGIC ```
# MAGIC 今月の経営会議に向けて、アンケート全体の傾向サマリ（満足度・離反リスク・トーンの分布）、緊急対応が必要な案件トップ3（具体的な顧客の声つき）、来月に向けた改善アクション3つを報告書形式でまとめてください
# MAGIC ```
# MAGIC **想定フロー**: Genie で全体の定量サマリを集計 → Genie で緊急案件を特定 → RAG で該当案件の原文を取得 → 全結果を統合してレポート生成

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 権限設定
# MAGIC
# MAGIC デモ用途の場合：
# MAGIC - `All workspace users` に閲覧権限を付与
# MAGIC
# MAGIC 本番用途の場合：
# MAGIC - 必要なユーザー/グループのみに権限を付与
