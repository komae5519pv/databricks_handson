# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Spaceを活用したホームセンター在庫分析エージェント

# COMMAND ----------

# %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Genie Spaceを作る
# MAGIC ![Genieスペース作成.gif](./_image/Genieスペース作成.gif "Genieスペース作成.gif")
# MAGIC
# MAGIC #### データ
# MAGIC - `product`
# MAGIC - `inventory`
# MAGIC - `reservation`
# MAGIC - `sales`
# MAGIC
# MAGIC #### 設定
# MAGIC - Title: `ホームセンター在庫分析`
# MAGIC - Description: `店舗横断の在庫・商品マスタ・取り置き状況を自然言語で検索・分析できるエージェントです。ユーザーの質問に対応し、在庫・入荷・取り寄せを横断的に分析して最適な回答を提示します。`
# MAGIC - Default Warehouse: <任意のSQL Warehouse>
# MAGIC
# MAGIC #### 指示
# MAGIC <テキスト>
# MAGIC 一般的な指示:
# MAGIC ```
# MAGIC * 必ず日本語で回答してください
# MAGIC * 地域に関して、都内といわれたら東京のことです
# MAGIC * 在庫を聞かれた場合、店舗名・在庫数・予約数・販売可能数・次回入荷予定を一緒に返してください
# MAGIC * 商品について聞かれた場合、詳細を返してください。詳細は次のとおり
# MAGIC
# MAGIC 詳細:
# MAGIC - 商品名・ブランド・カテゴリ
# MAGIC - スペック要約
# MAGIC - 対象レベル（プロ向け / DIY初心者向け / 兼用）
# MAGIC - スタッフコメント
# MAGIC - 平均レビュー点数
# MAGIC - 税込価格
# MAGIC - 返品可否
# MAGIC - 安全区分
# MAGIC ```
# MAGIC
# MAGIC #### サンプル質問
# MAGIC **チャットモード用**
# MAGIC 1. `レビュー評価が4.0以上の商品の売上合計と利益合計を教えて`（sales × product）
# MAGIC 2. `現在取り置き希望が入っている商品の店舗別在庫数と販売可能数を教えて`（inventory × reservation）
# MAGIC 3. `直近1年で売上数量が多い商品TOP5について、各店舗の現在庫数を教えて`（sales × inventory）
# MAGIC
# MAGIC **リサーチエージェント用**
# MAGIC 1. `カテゴリ別に売上推移と現在の在庫充足率を分析して、仕入れ優先度が高い商品を特定して`（sales × inventory × product）
# MAGIC 2. `都内店舗で、売上が伸びているのに在庫が少ない商品はある？予約状況も含めて教えて`（sales × inventory × reservation）
# MAGIC 3. `顧客区分ごとの購買傾向を分析して。プロ向け商品と初心者向け商品で売上・利益率に差はある？割引率との相関も見て`（sales × product）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Genie Spaceに権限を付与する
# MAGIC
# MAGIC ここでは、`all workspace user`に付与する（デモなので）
# MAGIC
