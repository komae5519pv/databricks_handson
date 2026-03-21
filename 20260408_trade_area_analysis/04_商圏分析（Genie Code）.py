# Databricks notebook source
# MAGIC %md
# MAGIC # 商圏分析（Genie Code）
# MAGIC
# MAGIC このノートブックでは、Genie Code の Agent mode と Skills を使って商圏分析を行います。
# MAGIC
# MAGIC ## 前提条件
# MAGIC 1. `01_サンプルデータ作成` を実行済み
# MAGIC 2. Genie Code Agent mode を有効化
# MAGIC 3. `trade-area-analysis` Skill が設定済み（`/.assistant/skills/trade-area-analysis/SKILL.md`）
# MAGIC
# MAGIC ## 使い方
# MAGIC 1. 右側の Genie Code パネルを開く
# MAGIC 2. Agent mode をオンにする
# MAGIC 3. 下記のサンプルプロンプトをコピーして実行

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## データ確認

# COMMAND ----------

# 利用可能なテーブル一覧
spark.sql("SHOW TABLES").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # サンプルプロンプト集
# MAGIC
# MAGIC 以下のプロンプトを Genie Code にコピー＆ペーストして試してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lv1: 基礎分析
# MAGIC
# MAGIC ### プロンプト 1-1: 低迷店舗の特定
# MAGIC ```
# MAGIC 売上が前年比で10%以上落ちている店舗を特定して、
# MAGIC 店舗名、所在地、前年比をテーブルで表示してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 1-2: 商圏特性の確認
# MAGIC ```
# MAGIC 最も売上が低迷している店舗の商圏特性を教えてください。
# MAGIC 人口、世帯年収、高齢化率、競合店舗数を含めて分析してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 1-3: 競合状況の可視化
# MAGIC ```
# MAGIC 低迷店舗の周辺にある競合店舗を一覧表示し、
# MAGIC 距離と売場面積でソートしてください。
# MAGIC 特に2024年以降に出店した新規競合をハイライトしてください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lv2: 深掘分析
# MAGIC
# MAGIC ### プロンプト 2-1: 売上低迷要因の分析
# MAGIC ```
# MAGIC 低迷店舗と好調店舗を比較して、
# MAGIC 商圏特性（人口、年収、高齢化率）と売上の相関を分析してください。
# MAGIC 散布図で可視化してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 2-2: カテゴリ別ギャップ分析
# MAGIC ```
# MAGIC 低迷店舗のカテゴリ別売上構成を、
# MAGIC 全店平均と比較してください。
# MAGIC どのカテゴリで差が大きいか特定してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 2-3: 店舗クラスタリング
# MAGIC ```
# MAGIC 商圏特性（人口、年収、高齢化率、戸建て比率）を使って
# MAGIC 店舗をクラスタリングしてください。
# MAGIC 各クラスタの特徴と、低迷店舗がどのクラスタに属するか教えてください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lv3: 施策立案
# MAGIC
# MAGIC ### プロンプト 3-1: ベンチマーク店舗の発見
# MAGIC ```
# MAGIC 最も低迷している店舗と商圏特性が似ていて、
# MAGIC かつ売上が好調な店舗を見つけてください。
# MAGIC その店舗との違い（カテゴリ構成など）を分析してください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 3-2: 改善施策の提案
# MAGIC ```
# MAGIC 低迷店舗の分析結果に基づいて、
# MAGIC 具体的な改善施策を3つ提案してください。
# MAGIC 各施策の期待効果と優先度も教えてください。
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 3-3: 総合レポート作成
# MAGIC ```
# MAGIC 低迷店舗について、以下の内容を含む分析レポートを作成してください：
# MAGIC 1. 現状サマリー（売上推移、前年比）
# MAGIC 2. 商圏特性の特徴
# MAGIC 3. 競合環境
# MAGIC 4. 低迷要因の仮説
# MAGIC 5. 改善施策の提案
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 応用プロンプト
# MAGIC
# MAGIC ### 複合的な分析
# MAGIC ```
# MAGIC 商圏分析スキルを使って、
# MAGIC 売上低迷店舗の要因を特定し、
# MAGIC 類似成功店舗との比較から改善策を提案してください。
# MAGIC 分析は基礎分析→深掘分析→施策立案の順で進めてください。
# MAGIC ```
# MAGIC
# MAGIC ### 可視化重視
# MAGIC ```
# MAGIC 全店舗の売上と商圏人口の関係を散布図で可視化し、
# MAGIC 低迷店舗を赤色でハイライトしてください。
# MAGIC 回帰直線も追加してください。
# MAGIC ```
# MAGIC
# MAGIC ### 競合影響の定量化
# MAGIC ```
# MAGIC 新規競合（2024年以降出店）がある店舗とない店舗で、
# MAGIC 売上前年比の平均を比較してください。
# MAGIC 競合の出店が売上に与える影響を定量化してください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## メモ欄
# MAGIC
# MAGIC Genie Code の回答や発見をここにメモしてください。

# COMMAND ----------

# 分析メモ用セル


