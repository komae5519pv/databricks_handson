# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space を作成します

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Genie Spaceを作る
# MAGIC ![Genieスペース作成.gif](../_img/Genieスペース作成.gif "Genieスペース作成.gif")
# MAGIC
# MAGIC #### データ
# MAGIC - `sl_users`
# MAGIC - `sl_items`
# MAGIC - `sl_stores`
# MAGIC - `sl_orders`
# MAGIC - `sl_order_items`
# MAGIC
# MAGIC OR
# MAGIC
# MAGIC - `gd_users`
# MAGIC - `gd_orders`
# MAGIC - `gd_order_items`
# MAGIC
# MAGIC
# MAGIC #### 設定
# MAGIC - Title: `メダリオンハンズオン｜小売スーパー・CDP購買分析`
# MAGIC - Description: `ブリックスマートの販売データを用いたCDP顧客分析アシスタント`
# MAGIC - Default Warehouse: <任意のSQL Warehouse>
# MAGIC - サンプル質問:
# MAGIC   - `来月の売上を上げるために、どの顧客層にどんなアプローチをすべき？データに基づいて具体的な施策を3つ提案して。`
# MAGIC   - `惣菜を買う人は他に何を一緒に買ってることが多い？おすすめの併売商品を提案して。`
# MAGIC   - `時間帯別の売上を見せて。夜（19時以降）に買い物する人はどんな商品を買ってる？`
# MAGIC   - `購入金額トップ10%のお客さんの特徴を教えて。どうすればこういうお客さんを増やせる？`
# MAGIC   - `この5年間の売上推移を年別に教えて。成長してる？どれくらいのペースで伸びてる？`
# MAGIC   - `お客さんを購買行動でグループ分けして、それぞれの特徴を教えて。どのグループが一番売上に貢献してる？`
# MAGIC
# MAGIC #### 指示
# MAGIC <テキスト>
# MAGIC 一般的な指示:
# MAGIC ```
# MAGIC * 必ず日本語で回答してください
# MAGIC * あなたはブリックスマーのマーケティングアナリストです。CDPマーケティング施策策定に向けた顧客理解を目的に購買データを用いた分析を行います。分析設計〜施策検討に向け、分析および洞察を導き出す補助をしてください。
# MAGIC * SQLのFROM句でテーブル指定する際、``で括らないでください
# MAGIC * 現在を起点とする場合、必ず「SELECT from_utc_timestamp(CURRENT_TIMESTAMP(), 'Asia/Tokyo')::DATE;」で日時特定してください
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Genie Spaceに権限を付与する
# MAGIC
# MAGIC ここでは、`all workspace user`に付与する（デモなので）
# MAGIC
