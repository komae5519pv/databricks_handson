# Databricks notebook source
# DBTITLE 1,基本設定（ここを変更してください）
# ===========================================
# 基本設定 - 必要に応じて変更してください
# ===========================================

MY_CATALOG = '<カタログ名>'       # 使用するカタログ名に変えてください
MY_SCHEMA = 'medallion_handson'    # 使用するスキーマ名に変えてください
MY_VOLUME = 'raw'                  # CSVファイルを格納するVolume名

# ===========================================
# データ件数設定 - 生成するデータ量を調整できます
# ===========================================
# 以下の4つを変更すると、生成されるサンプルデータの件数が変わります
#
# ■ NUM_USERSについて（動的顧客成長モデル）
#   - NUM_USERS は「データ開始時点の既存顧客数」を表します
#   - 新規顧客は毎日動的に生成され、顧客数は自然に成長します
#   - 最終的な総顧客数 ≒ NUM_USERS + (NUM_ORDERS × 平均新規割合)
#     例: 100人 + (100,000 × 20%) = 約20,100人
#
# ■ 設定のコツ
#   - NUM_PRODUCTS は 50以上にしてください（商品カテゴリが12種類あるため）
#   - NUM_USERS は初期の既存顧客数（少なめでOK）
#
# ■ order_items（注文明細）について
#   - order_items の件数は指定不要です（自動で生成されます）
#   - 1注文あたりの商品点数はクラスターごとに異なります
#     （週末まとめ買い族: 15-30点、時短族: 3-6点 など）
#   - 全体の目安: NUM_ORDERS × 約7件
#     例: 10000注文 → 約70,000件の注文明細
#
# ■ 5年分のデータについて
#   - 日次推移を分析するには、5年間（約1825日）に渡る十分なデータ量が必要
#   - NUM_ORDERS = 100000 で、1日あたり約55件の注文が生成されます

NUM_USERS = 100        # 初期会員数（データ開始時点の既存顧客）
NUM_PRODUCTS = 50      # 商品マスタの件数
NUM_STORES = 10        # 店舗マスタの件数
NUM_ORDERS = 100000    # 注文（会計単位）の件数 ※5年分のデータを生成

# ===========================================
# 売上成長・顧客セグメント設定（参考情報）
# ===========================================
# データ生成では以下のトレンドが自動適用されます：
#
# ■ 売上成長トレンド
#   - 5年間で約2倍に成長（年率約15%成長）
#   - 季節変動: 12月+30-50%, GW/お盆+15-20%
#
# ■ 新規顧客割合の変動
#   - 初年度: 約30% → 最終年度: 約10%（事業成熟に伴い減少）
#   - 季節変動: 4月（新生活）+8%, 12月（年末商戦）+5%
#   - 日次でランダムノイズ±3%

# COMMAND ----------

# DBTITLE 1,変数確認
print(f"変数設定完了！")
print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_SCHEMA}")
print(f"MY_VOLUME: {MY_VOLUME}")

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマ・Volume作成
# スキーマ作成（カタログは既存のものを使用）
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}")

# Volume作成（CSVファイル格納用）
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")

# 使用するカタログ・スキーマを指定
spark.sql(f"USE CATALOG {MY_CATALOG}")
spark.sql(f"USE SCHEMA {MY_SCHEMA}")

print("初期値設定完了!")
print(f"  - USE CATALOG: {MY_CATALOG}")
print(f"  - USE SCHEMA: {MY_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Volumeサブディレクトリ作成
# CSVファイル格納用のディレクトリを作成
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/users")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/items")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/stores")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/orders")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/order_items")

print("Volumeディレクトリ作成完了!")
print(f"  - /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/users")
print(f"  - /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/items")
print(f"  - /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/stores")
print(f"  - /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/orders")
print(f"  - /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/order_items")
