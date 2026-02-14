# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# # スキーマ全削除
# spark.sql(f'drop schema {catalog}.{schema} cascade')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. テーブル作成

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. inventory / 在庫

# COMMAND ----------

# DBTITLE 1,テーブル作成
# ===== 共通インポート =====
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import expr, col
from datetime import datetime, timedelta

# 今日の日付と一週間後の日付（ダミー用）
today = datetime.today()
one_week_later = today + timedelta(days=7)

# ===== 1) inventory（在庫情報） =====
inventory_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("sku_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("on_hand", IntegerType(), True),
    StructField("reserved", IntegerType(), True),
    StructField("safety_stock", IntegerType(), True),
    StructField("next_eta_date", DateType(), True),
    StructField("next_eta_qty", IntegerType(), True),
])

# 店舗：東京2拠点＋横浜/さいたま/千葉/名古屋/大阪
inventory_data = [
    # 1001 インパクトドライバー 10.8V（CRAFT BASE / 電動工具）★主役商品
    ('T01','多摩店','東京',   11001,1001,'インパクトドライバー 10.8V','CRAFT BASE','電動工具','台', 2,1,1, one_week_later.date(), 2),
    ('T02','八王子店','東京', 11001,1001,'インパクトドライバー 10.8V','CRAFT BASE','電動工具','台', 3,0,1, None, None),
    ('K01','横浜店','神奈川', 11001,1001,'インパクトドライバー 10.8V','CRAFT BASE','電動工具','台', 1,0,1, one_week_later.date(), 1),
    ('S01','さいたま店','埼玉',11001,1001,'インパクトドライバー 10.8V','CRAFT BASE','電動工具','台', 2,0,1, None, None),

    # 1002 インパクトドライバー 18V（マキタ / 電動工具）
    ('T01','多摩店','東京',   11002,1002,'インパクトドライバー 18V','マキタ','電動工具','台', 3,0,1, None, None),
    ('T02','八王子店','東京', 11002,1002,'インパクトドライバー 18V','マキタ','電動工具','台', 1,0,1, None, None),
    ('O01','大阪店','大阪',   11002,1002,'インパクトドライバー 18V','マキタ','電動工具','台', 2,0,1, one_week_later.date(), 2),

    # 1003 インパクトドライバー 18V（HiKOKI / 電動工具）
    ('T01','多摩店','東京',   11003,1003,'インパクトドライバー 18V','HiKOKI','電動工具','台', 2,0,1, None, None),
    ('S01','さいたま店','埼玉',11003,1003,'インパクトドライバー 18V','HiKOKI','電動工具','台', 1,0,1, one_week_later.date(), 1),

    # 1004 丸のこ 165mm（CRAFT BASE / 電動工具）
    ('T01','多摩店','東京',   11004,1004,'丸のこ 165mm','CRAFT BASE','電動工具','台', 2,0,1, None, None),
    ('T02','八王子店','東京', 11004,1004,'丸のこ 165mm','CRAFT BASE','電動工具','台', 3,0,1, None, None),
    ('K01','横浜店','神奈川', 11004,1004,'丸のこ 165mm','CRAFT BASE','電動工具','台', 0,0,0, one_week_later.date(), 2),

    # 1005 水性木部保護塗料 1.6L（カンペハピオ / 塗料）★ウッドデッキ用
    ('T01','多摩店','東京',   11005,1005,'水性木部保護塗料 1.6L','カンペハピオ','塗料','缶', 3,1,2, one_week_later.date(), 3),
    ('T02','八王子店','東京', 11005,1005,'水性木部保護塗料 1.6L','カンペハピオ','塗料','缶', 0,0,0, None, None),
    ('S01','さいたま店','埼玉',11005,1005,'水性木部保護塗料 1.6L','カンペハピオ','塗料','缶', 5,0,2, None, None),
    ('O01','大阪店','大阪',   11005,1005,'水性木部保護塗料 1.6L','カンペハピオ','塗料','缶', 2,0,2, None, None),

    # 1006 防水塗料 0.7L（アサヒペン / 塗料）
    ('T01','多摩店','東京',   11006,1006,'防水塗料 0.7L','アサヒペン','塗料','缶', 4,0,2, None, None),
    ('K01','横浜店','神奈川', 11006,1006,'防水塗料 0.7L','アサヒペン','塗料','缶', 3,1,2, None, None),
    ('N01','名古屋店','愛知', 11006,1006,'防水塗料 0.7L','アサヒペン','塗料','缶', 2,0,2, one_week_later.date(), 3),

    # 1007 防カビ塗料 0.7L（アサヒペン / 塗料）
    ('T01','多摩店','東京',   11007,1007,'防カビ塗料 0.7L','アサヒペン','塗料','缶', 3,0,2, None, None),
    ('S01','さいたま店','埼玉',11007,1007,'防カビ塗料 0.7L','アサヒペン','塗料','缶', 2,0,2, None, None),
    ('C01','千葉店','千葉',   11007,1007,'防カビ塗料 0.7L','アサヒペン','塗料','缶', 4,0,2, None, None),

    # 1008 SPF 2×4材 6フィート（木材・建材）★ウッドデッキ用
    ('T01','多摩店','東京',   11008,1008,'SPF 2×4材 6フィート','ノーブランド','木材・建材','本', 15,0,3, one_week_later.date(), 10),
    ('T02','八王子店','東京', 11008,1008,'SPF 2×4材 6フィート','ノーブランド','木材・建材','本', 20,0,3, None, None),
    ('S01','さいたま店','埼玉',11008,1008,'SPF 2×4材 6フィート','ノーブランド','木材・建材','本', 10,0,3, None, None),
    ('K01','横浜店','神奈川', 11008,1008,'SPF 2×4材 6フィート','ノーブランド','木材・建材','本', 8,0,3, one_week_later.date(), 10),

    # 1009 ステンレスコーススレッド 箱（CRAFT BASE / 金物）
    ('T01','多摩店','東京',   11009,1009,'ステンレスコーススレッド 75mm 100本入','CRAFT BASE','金物','箱', 10,0,1, None, None),
    ('T02','八王子店','東京', 11009,1009,'ステンレスコーススレッド 75mm 100本入','CRAFT BASE','金物','箱', 8,0,1, None, None),
    ('K01','横浜店','神奈川', 11009,1009,'ステンレスコーススレッド 75mm 100本入','CRAFT BASE','金物','箱', 5,0,1, None, None),

    # 1010 サンドペーパー 5枚セット（CRAFT BASE / 消耗品）
    ('T01','多摩店','東京',   11010,1010,'サンドペーパー 5枚セット','CRAFT BASE','消耗品','セット', 8,0,1, None, None),
    ('T02','八王子店','東京', 11010,1010,'サンドペーパー 5枚セット','CRAFT BASE','消耗品','セット', 6,0,1, None, None),

    # 1011 木工用ボンド 500g（コニシ / 消耗品）
    ('T01','多摩店','東京',   11011,1011,'木工用ボンド 500g','コニシ','消耗品','個', 5,0,1, None, None),
    ('K01','横浜店','神奈川', 11011,1011,'木工用ボンド 500g','コニシ','消耗品','個', 3,0,1, None, None),

    # 1012 培養土 25L（アイリスオーヤマ / 園芸）
    ('T01','多摩店','東京',   11012,1012,'培養土 25L','アイリスオーヤマ','園芸','袋', 20,0,3, None, None),
    ('C01','千葉店','千葉',   11012,1012,'培養土 25L','アイリスオーヤマ','園芸','袋', 30,0,3, None, None),
    ('S01','さいたま店','埼玉',11012,1012,'培養土 25L','アイリスオーヤマ','園芸','袋', 15,10,3, one_week_later.date(), 20),

    # 1013 伸縮脚立 3段（CRAFT BASE / 作業用品）
    ('T01','多摩店','東京',   11013,1013,'伸縮脚立 3段','CRAFT BASE','作業用品','台', 3,0,1, None, None),
    ('T02','八王子店','東京', 11013,1013,'伸縮脚立 3段','CRAFT BASE','作業用品','台', 2,0,1, None, None),

    # 1014 充電式ブロワー 18V（マキタ / 電動工具）
    ('T01','多摩店','東京',   11014,1014,'充電式ブロワー 18V','マキタ','電動工具','台', 2,0,1, None, None),
    ('T02','八王子店','東京', 11014,1014,'充電式ブロワー 18V','マキタ','電動工具','台', 1,0,1, one_week_later.date(), 2),
    ('O01','大阪店','大阪',   11014,1014,'充電式ブロワー 18V','マキタ','電動工具','台', 3,0,1, None, None),
]

df_inventory = spark.createDataFrame(inventory_data, schema=inventory_schema)

# UUID列 & 実効在庫列
df_inventory = (
    df_inventory
    .withColumn("id", expr("uuid()"))
    .withColumn("effective_available", col("on_hand") - col("reserved") - col("safety_stock"))
)

# 列順整形
df_inventory = df_inventory.select(
    "id", "store_id", "store_name", "region",
    "sku_id", "product_id",
    "product_name", "brand", "category", "unit",
    "on_hand", "reserved", "safety_stock", "effective_available",
    "next_eta_date", "next_eta_qty"
)

# テーブル保存
df_inventory.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.inventory")


print(df_inventory.columns)
print(df_inventory.count())
display(df_inventory)

# COMMAND ----------

# DBTITLE 1,CSV出力
# # pandasに変換してCSV文字列を作る
# csv_data = df_inventory.toPandas().to_csv(index=False)

# # CSV保存
# file_path = f"/Volumes/{catalog}/{schema}/{volume}/structured/inventory/inventory.csv"
# dbutils.fs.put(file_path, csv_data, True)
# print(f"CSV保存完了: {file_path}")

# COMMAND ----------

# DBTITLE 1,認定済みタグの追加
certified_tag = 'system.Certified'

try:
    spark.sql(f"ALTER TABLE inventory SET TAGS ('{certified_tag}')")
    print(f"認定済みタグ '{certified_tag}' の追加が完了しました。")

except Exception as e:
    print(f"認定済みタグ '{certified_tag}' の追加中にエラーが発生しました: {str(e)}")
    print("このエラーはタグ機能に対応していないワークスペースで実行した場合に発生する可能性があります。")

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f"{catalog}.{schema}.inventory"

# テーブルコメント
comment = """
テーブル名: `inventory` / 在庫情報テーブル
説明: 各店舗・SKU単位の在庫状況を保持するテーブル。ブランド・商品カテゴリを含む。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "id": "自動採番したユニークID（UUID）",
    "store_id": "店舗ID",
    "store_name": "店舗名",
    "region": "地域（例：東京, 大阪など）",
    "sku_id": "SKU ID（バリアントID）",
    "product_id": "商品ID（マスタ連携用／FK）",
    "product_name": "商品名",
    "brand": "ブランド名（例：CRAFT BASE、マキタ、カンペハピオ など）",
    "category": "商品カテゴリ（例：電動工具、塗料、木材・建材、園芸など）",
    "unit": "在庫管理単位（台、缶、本、箱、セット、個、袋）",
    "on_hand": "在庫数（実際に棚にある数量）",
    "reserved": "予約・引当済み数量",
    "safety_stock": "安全在庫数（最低限確保すべき在庫）",
    "effective_available": "販売可能在庫（on_hand - reserved - safety_stock）",
    "next_eta_date": "次回入荷予定日（Date型・未定の場合はnull）",
    "next_eta_qty": "次回入荷予定数（Int型・未定の場合はnull）"
}

for column, comment in column_comments.items():
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-2. product / 商品マスタ

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import expr

schema_product = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True),
    StructField("description", StringType(), True),
    StructField("spec_summary", StringType(), True),
    StructField("staff_comment", StringType(), True),
    StructField("review_score", DoubleType(), True),
    StructField("pro_or_diy", StringType(), True),
    StructField("safety_class", StringType(), True),
    StructField("price_tax_included", IntegerType(), True),
    StructField("returnable", StringType(), True),
    StructField("volume_path", StringType(), True),
])

# 共通ルート
base_path = f"/Volumes/{catalog}/{schema}/{volume}/"

data_product = [
    (1001,"インパクトドライバー 10.8V","CRAFT BASE","電動工具",
     "【カテゴリ】電動工具  【対象】DIY初心者〜中級  【用途】ネジ締め・穴あけ全般  【電源】10.8Vバッテリー式  【特長】軽量・コンパクト・取り回し◎  【セット提案】ビットセット・下穴ドリル",
     "電圧:10.8V／トルク:25N・m／重量:0.9kg／充電時間:60分／バッテリー式／LEDライト付き",
     "片手で扱えるので初心者にも安心。棚の取り付けに使用。",
     4.2,"DIY初心者向け","一般", 8980,"条件付き", f"{base_path}001.jpg"),

    (1002,"インパクトドライバー 18V","マキタ","電動工具",
     "【カテゴリ】電動工具  【対象】プロ・上級DIY  【用途】ネジ締め・穴あけ・ボルト締め  【電源】18Vバッテリー式  【特長】高トルク・長時間作業対応・防塵防滴  【セット提案】18Vバッテリー共用の他工具（ブロワー等）",
     "電圧:18V／トルク:175N・m／重量:1.5kg／充電時間:40分／バッテリー式／防塵防滴",
     "プロの現場でも定番。パワーは申し分なし。",
     4.6,"プロ向け","一般", 24800,"条件付き", f"{base_path}002.jpg"),

    (1003,"インパクトドライバー 18V","HiKOKI","電動工具",
     "【カテゴリ】電動工具  【対象】プロ・上級DIY  【用途】ネジ締め・穴あけ・ボルト締め  【電源】18Vバッテリー式（マルチボルト対応）  【特長】高トルク・コンパクトヘッド・振動軽減  【セット提案】マルチボルト対応の他工具",
     "電圧:18V(マルチボルト)／トルク:180N・m／重量:1.4kg／充電時間:35分／バッテリー式",
     "コンパクトで狭い場所の作業に向いている。",
     4.4,"プロ向け","一般", 22800,"条件付き", f"{base_path}003.jpg"),

    (1004,"丸のこ 165mm","CRAFT BASE","電動工具",
     "【カテゴリ】電動工具  【対象】DIY中級〜  【用途】木材の直線カット  【電源】コード式（AC100V）  【特長】安定した切断力・ガイド定規対応  【安全注意】キックバック注意・保護メガネ必須  【セット提案】丸のこガイド定規",
     "刃径:165mm／回転数:5500min⁻¹／最大切込:57mm／重量:3.0kg／コード式",
     "DIYでウッドデッキや棚板のカットに。初心者はガイド定規必須。",
     4.0,"兼用","刃物類", 12800,"条件付き", f"{base_path}004.jpg"),

    (1005,"水性木部保護塗料 1.6L","カンペハピオ","塗料",
     "【カテゴリ】塗料  【対象】DIY全般  【用途】ウッドデッキ・ウッドフェンス・木製家具の保護  【特長】水性で臭い少ない・防虫防腐効果・UV保護  【注意】開封後は密閉保管  【セット提案】ハケ・ローラー・マスキングテープ・養生シート",
     "容量:1.6L／塗り面積:畳4〜5枚分(2回塗り)／乾燥時間:約2時間／水性／防虫防腐",
     "ウッドデッキの塗り替えに最適。2回塗りで仕上がりがきれい。",
     4.3,"DIY初心者向け","化学製品", 3480,"不可(開封後)", f"{base_path}005.jpg"),

    (1006,"防水塗料 0.7L","アサヒペン","塗料",
     "【カテゴリ】塗料  【対象】DIY全般  【用途】ベランダ・屋上・外壁の防水処理  【特長】水の浸入を防ぐ・耐候性高い・水性  【注意】開封後は密閉保管・換気必須  【セット提案】ローラー・養生シート",
     "容量:0.7L／塗り面積:畳約2枚分(2回塗り)／乾燥時間:約3時間／水性",
     "ベランダの防水補修に。水性なので臭いが少ない。",
     3.8,"DIY初心者向け","化学製品", 2180,"不可(開封後)", f"{base_path}006.jpg"),

    (1007,"防カビ塗料 0.7L","アサヒペン","塗料",
     "【カテゴリ】塗料  【対象】DIY全般  【用途】浴室・洗面所・北側の壁のカビ対策  【特長】カビの発生を抑制・防藻効果あり・水性  【注意】開封後は密閉保管・換気必須  【セット提案】ハケ・マスキングテープ",
     "容量:0.7L／塗り面積:畳約2枚分(2回塗り)／乾燥時間:約2時間／水性／防カビ防藻",
     "浴室のカビが気になる方に。塗るだけで予防できる。",
     3.6,"DIY初心者向け","化学製品", 2380,"不可(開封後)", f"{base_path}007.jpg"),

    (1008,"SPF 2×4材 6フィート","ノーブランド","木材・建材",
     "【カテゴリ】木材・建材  【対象】DIY全般  【用途】棚・テーブル・ウッドデッキ等のDIY定番素材  【特長】加工しやすい・安価・軽量  【注意】屋外使用時は防腐塗装必要  【セット提案】サンドペーパー・木工用ボンド",
     "寸法:38×89×1820mm(6フィート)／樹種:SPF(スプルース・パイン・ファー)／未塗装",
     "DIYの定番。まっすぐな材を選ぶのがコツ。",
     4.1,"兼用","一般", 598,"可", f"{base_path}008.jpg"),

    (1009,"ステンレスコーススレッド 75mm 100本入","CRAFT BASE","金物",
     "【カテゴリ】金物  【用途】木材のネジ止め全般・ウッドデッキ・棚・フェンス  【特長】ステンレス製で錆びにくい・屋外使用対応・先割れ加工で木割れ防止",
     "長さ:75mm／材質:ステンレス／入数:100本／先割れ加工",
     "屋外DIYにはステンレスが安心。",
     4.0,"兼用","一般", 980,"可", f"{base_path}009.jpg"),

    (1010,"サンドペーパー 5枚セット","CRAFT BASE","消耗品",
     "【カテゴリ】消耗品  【用途】木材の研磨・塗装前の下地処理  【特長】#120（中目）と#240（細目）の2種セット・木工DIYの必需品",
     "番手:#120×3枚・#240×2枚／サイズ:228×280mm",
     "塗装前に必ず使う。#120で粗研磨→#240で仕上げ。",
     3.9,"兼用","一般", 398,"可", f"{base_path}010.jpg"),

    (1011,"木工用ボンド 500g","コニシ","消耗品",
     "【カテゴリ】消耗品  【用途】木材の接着全般  【特長】速乾タイプ・水性・安全  【注意】完全硬化まで24時間・クランプ推奨",
     "容量:500g／速乾タイプ／水性／硬化時間:約24時間",
     "棚やテーブルの組み立てに。ネジと併用すると強度アップ。",
     4.3,"兼用","一般", 580,"可", f"{base_path}011.jpg"),

    (1012,"培養土 25L","アイリスオーヤマ","園芸",
     "【カテゴリ】園芸  【用途】花・野菜・ハーブの植え付け全般  【特長】元肥入り・軽量タイプ・排水性良好  【シーズン】春〜秋",
     "容量:25L／元肥入り／pH調整済み／軽量タイプ",
     "プランター栽培ならこれだけでOK。",
     4.0,"DIY初心者向け","一般", 698,"可", f"{base_path}012.jpg"),

    (1013,"伸縮脚立 3段","CRAFT BASE","作業用品",
     "【カテゴリ】作業用品  【用途】高所作業・棚の取り付け・照明交換・庭木の剪定  【特長】伸縮式でコンパクト収納・滑り止めステップ・安全ロック付き  【安全注意】天板に立たない・安定した場所で使用",
     "天板高さ:79cm／耐荷重:100kg／収納時:厚さ8cm／重量:4.5kg／アルミ製",
     "家庭用ならこのサイズで十分。収納場所を取らない。",
     4.1,"兼用","一般", 6980,"可", f"{base_path}013.jpg"),

    (1014,"充電式ブロワー 18V","マキタ","電動工具",
     "【カテゴリ】電動工具  【対象】DIY〜プロ  【用途】落ち葉・おが屑・粉塵の吹き飛ばし  【電源】18Vバッテリー式（マキタ18Vバッテリー共用）  【特長】軽量・パワフル・低騒音  【セット提案】マキタ18Vバッテリー共用の他工具",
     "電圧:18V／風量:3.2m³/min／重量:1.7kg(バッテリー含む)／バッテリー式",
     "庭の落ち葉掃除からDIYの粉塵処理まで。マキタバッテリー共用が便利。",
     4.4,"兼用","一般", 15800,"条件付き", f"{base_path}014.jpg"),
]

df_product = spark.createDataFrame(data_product, schema=schema_product)
df_product = df_product.withColumn("id", expr("uuid()"))
df_product.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.product")

print(df_product.columns)
print(df_product.count())
display(df_product)

# COMMAND ----------

# DBTITLE 1,CSV出力
# # pandasに変換してCSV文字列を作る
# csv_data = df_product.toPandas().to_csv(index=False)

# # CSV保存
# file_path = f"/Volumes/{catalog}/{schema}/{volume}/structured/product/product.csv"
# dbutils.fs.put(file_path, csv_data, True)
# print(f"CSV保存完了: {file_path}")

# COMMAND ----------

# DBTITLE 1,認定済みタグの追加
certified_tag = 'system.Certified'

try:
    spark.sql(f"ALTER TABLE product SET TAGS ('{certified_tag}')")
    print(f"認定済みタグ '{certified_tag}' の追加が完了しました。")

except Exception as e:
    print(f"認定済みタグ '{certified_tag}' の追加中にエラーが発生しました: {str(e)}")
    print("このエラーはタグ機能に対応していないワークスペースで実行した場合に発生する可能性があります。")

# COMMAND ----------

# DBTITLE 1,コメント追加
# ===== product: テーブル & カラムコメント =====
table_name = f"{catalog}.{schema}.product"

comment = """
テーブル名: `product` / 商品マスタ
説明: 商品の説明・スペック・スタッフコメント・レビュー・対象レベル・安全区分・税込価格・返品可否を保持。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

column_comments = {
    "product_id": "商品ID（在庫や予約と紐づくキー）",
    "product_name": "商品名",
    "brand": "ブランド名（例：CRAFT BASE、マキタ、カンペハピオ など）",
    "category": "商品カテゴリ（例：電動工具、塗料、木材・建材、園芸 など）",
    "description": "アイテム説明（用途、特長、セット提案、安全注意など）",
    "spec_summary": "主要スペック要約（電圧、容量、寸法、重量など）",
    "staff_comment": "スタッフコメント（使用感・おすすめポイント）",
    "review_score": "平均レビュー点数（5段階）",
    "pro_or_diy": "対象レベル（プロ向け／DIY初心者向け／兼用）",
    "safety_class": "安全区分（一般／刃物類／化学製品）",
    "price_tax_included": "税込価格（円）",
    "returnable": "返品可否（可／条件付き／不可(開封後)）",
    "volume_path": "画像ファイルの格納パス（volumeパス）",
    "id": "自動採番したユニークID（UUID）"
}

for column, cmt in column_comments.items():
    escaped_cmt = cmt.replace("'", "\\'")
    sql = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_cmt}'"
    spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-3. reservation / 取り置き・予約管理

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import expr
from datetime import datetime, timedelta

# 予約（取り置き）テーブルのスキーマ
reservation_schema = StructType([
    StructField("reservation_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_type", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("limit_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("note", StringType(), True),
    StructField("staff", StringType(), True),
    StructField("created_at", TimestampType(), True),
])

today = datetime.today()
one_week_later = today + timedelta(days=7)

# サンプルデータ
reservation_data = [
    ("", "T01", "多摩店", 1001, "インパクトドライバー 10.8V", "佐藤工務店", "法人(プロ会員)", 1, one_week_later.date(), "希望中", "現場で必要。来週月曜までに確保希望", "担当A", today - timedelta(days=3)),
    ("", "T01", "多摩店", 1005, "水性木部保護塗料 1.6L", "田中太郎", "個人", 1, one_week_later.date(), "希望中", "ウッドデッキ塗り替え用", "担当B", today - timedelta(days=2)),
    ("", "K01", "横浜店", 1006, "防水塗料 0.7L", "鈴木花子", "個人", 1, one_week_later.date(), "希望中", "色味確認中。週末引取予定", "担当C", today - timedelta(days=1)),
    ("", "S01", "さいたま店", 1012, "培養土 25L", "山田園芸", "法人(プロ会員)", 10, one_week_later.date(), "希望中", "植栽工事用。50袋中10袋先行確保", "担当A", today)
]

df_reservation = spark.createDataFrame(reservation_data, schema=reservation_schema)

# UUIDを付与
df_reservation = df_reservation.withColumn("reservation_id", expr("uuid()"))

# 保存用にカラム並び替え
df_reservation = df_reservation.select(
    "reservation_id", "store_id", "store_name", "product_id", "product_name",
    "customer_name", "customer_type", "quantity", "limit_date", "status", "note", "staff", "created_at"
)

# Deltaテーブルとして保存
df_reservation.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.reservation")

# 確認出力
print(df_reservation.columns)
print(df_reservation.count())
display(df_reservation)

# COMMAND ----------

# DBTITLE 1,CSV出力
# # pandasに変換してCSV文字列を作る
# csv_data = df_reservation.toPandas().to_csv(index=False)

# # CSV保存
# file_path = f"/Volumes/{catalog}/{schema}/{volume}/structured/reservation/reservation.csv"
# dbutils.fs.put(file_path, csv_data, True)
# print(f"CSV保存完了: {file_path}")

# COMMAND ----------

# DBTITLE 1,認定済みタグの追加
certified_tag = 'system.Certified'

try:
    spark.sql(f"ALTER TABLE reservation SET TAGS ('{certified_tag}')")
    print(f"認定済みタグ '{certified_tag}' の追加が完了しました。")

except Exception as e:
    print(f"認定済みタグ '{certified_tag}' の追加中にエラーが発生しました: {str(e)}")
    print("このエラーはタグ機能に対応していないワークスペースで実行した場合に発生する可能性があります。")

# COMMAND ----------

# DBTITLE 1,コメント追加
# reservation: テーブル・カラムコメント付与
table_name = f"{catalog}.{schema}.reservation"

comment = """
テーブル名: `reservation` / 取り置き・予約管理
説明: 各店舗・各商品に対する顧客取り置き希望（予約）を記録。顧客情報、数量、期限、担当スタッフ、メモ、受付日時などを保持。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

column_comments = {
    "reservation_id": "予約・取り置きエントリのユニークID（UUID）",
    "store_id": "店舗ID（事業所識別子）",
    "store_name": "店舗名",
    "product_id": "商品ID（inventoryやproductと紐づくキー）",
    "product_name": "商品名",
    "customer_name": "取置き希望の顧客名",
    "customer_type": "顧客区分（個人／法人(プロ会員)）",
    "quantity": "取り置き数量",
    "limit_date": "取り置き期限日",
    "status": "希望中／確定／キャンセルなどの状態区分",
    "note": "備考・理由・申込状況など",
    "staff": "受付担当スタッフ名",
    "created_at": "受付日時（レコード作成タイムスタンプ）"
}

for column, cmt in column_comments.items():
    escaped_cmt = cmt.replace("'", "\\'")
    sql = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_cmt}'"
    spark.sql(sql)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-4. sales / 購買履歴
# MAGIC
# MAGIC ホームセンター全国店舗の購買トランザクションデータ（約10,000行）をPySparkで生成する。
# MAGIC ダッシュボード可視化・Genie分析向け。product テーブル（1001〜1014）とFKで紐づく。

# COMMAND ----------

# DBTITLE 1,テーブル作成
import random
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

random.seed(42)  # 再現性のためシード固定

# === productテーブルからマスタデータを取得（category, product_name, price の正は product テーブル）===
df_product_master = spark.read.table(f"{catalog}.{schema}.product")
product_master = {row.product_id: row for row in df_product_master.collect()}

# === 商品ごとの補足属性（productテーブルにない情報: サブカテゴリ、粗利率、出現ウェイト）===
PRODUCT_ATTRS = {
    1001: {"sub_category": "ドライバー",   "margin": 0.20, "weight": 8},
    1002: {"sub_category": "ドライバー",   "margin": 0.15, "weight": 4},
    1003: {"sub_category": "ドライバー",   "margin": 0.15, "weight": 3},
    1004: {"sub_category": "切断工具",     "margin": 0.18, "weight": 4},
    1005: {"sub_category": "木部塗料",     "margin": 0.30, "weight": 10},
    1006: {"sub_category": "防水塗料",     "margin": 0.30, "weight": 8},
    1007: {"sub_category": "防カビ塗料",   "margin": 0.30, "weight": 7},
    1008: {"sub_category": "木材",         "margin": 0.12, "weight": 15},
    1009: {"sub_category": "ネジ・ビス",   "margin": 0.35, "weight": 10},
    1010: {"sub_category": "研磨材",       "margin": 0.40, "weight": 8},
    1011: {"sub_category": "接着剤",       "margin": 0.38, "weight": 7},
    1012: {"sub_category": "用土",         "margin": 0.25, "weight": 8},
    1013: {"sub_category": "脚立",         "margin": 0.22, "weight": 4},
    1014: {"sub_category": "ブロワー",     "margin": 0.18, "weight": 4},
}
PRODUCT_IDS = sorted(PRODUCT_ATTRS.keys())
PRODUCT_WEIGHTS = [PRODUCT_ATTRS[pid]["weight"] for pid in PRODUCT_IDS]  # [8,4,3,4,10,8,7,15,10,8,7,8,4,4]

# === 地域データ（市区町村, 都道府県, 地域, 緯度, 経度, 地域マネージャー, 予算）===
LOCATIONS = [
    # 北海道
    ("札幌市",     "北海道",   "北海道",   43.06, 141.35, "宮前 誠",   20000000),
    ("旭川市",     "北海道",   "北海道",   43.77, 142.36, "宮前 誠",   20000000),
    ("函館市",     "北海道",   "北海道",   41.77, 140.73, "宮前 誠",   20000000),
    # 東北地方
    ("仙台市",     "宮城県",   "東北地方", 38.27, 140.87, "駒田 静香", 20000000),
    ("盛岡市",     "岩手県",   "東北地方", 39.70, 141.15, "駒田 静香", 20000000),
    ("秋田市",     "秋田県",   "東北地方", 39.72, 140.10, "駒田 静香", 20000000),
    ("山形市",     "山形県",   "東北地方", 38.24, 140.33, "駒田 静香", 20000000),
    ("福島市",     "福島県",   "東北地方", 37.75, 140.47, "駒田 静香", 20000000),
    ("青森市",     "青森県",   "東北地方", 40.82, 140.74, "駒田 静香", 20000000),
    # 関東地方
    ("横浜市",     "神奈川県", "関東地方", 35.44, 139.64, "中吉 孝",   50000000),
    ("さいたま市", "埼玉県",   "関東地方", 35.86, 139.65, "中吉 孝",   50000000),
    ("千葉市",     "千葉県",   "関東地方", 35.61, 140.12, "中吉 孝",   50000000),
    ("八王子市",   "東京都",   "関東地方", 35.66, 139.32, "中吉 孝",   50000000),
    ("川崎市",     "神奈川県", "関東地方", 35.53, 139.70, "中吉 孝",   50000000),
    ("相模原市",   "神奈川県", "関東地方", 35.57, 139.37, "中吉 孝",   50000000),
    ("宇都宮市",   "栃木県",   "関東地方", 36.57, 139.88, "中吉 孝",   50000000),
    ("前橋市",     "群馬県",   "関東地方", 36.39, 139.06, "中吉 孝",   50000000),
    ("水戸市",     "茨城県",   "関東地方", 36.34, 140.45, "中吉 孝",   50000000),
    # 中部地方
    ("名古屋市",   "愛知県",   "中部地方", 35.18, 136.91, "辻岡 美羽", 40000000),
    ("新潟市",     "新潟県",   "中部地方", 37.92, 139.04, "辻岡 美羽", 40000000),
    ("静岡市",     "静岡県",   "中部地方", 34.98, 138.38, "辻岡 美羽", 40000000),
    ("浜松市",     "静岡県",   "中部地方", 34.71, 137.73, "辻岡 美羽", 40000000),
    ("金沢市",     "石川県",   "中部地方", 36.59, 136.63, "辻岡 美羽", 40000000),
    ("長野市",     "長野県",   "中部地方", 36.65, 138.18, "辻岡 美羽", 40000000),
    ("岐阜市",     "岐阜県",   "中部地方", 35.39, 136.72, "辻岡 美羽", 40000000),
    ("富山市",     "富山県",   "中部地方", 36.70, 137.21, "辻岡 美羽", 40000000),
    ("豊田市",     "愛知県",   "中部地方", 35.08, 137.16, "辻岡 美羽", 40000000),
    # 関西地方
    ("大阪市",     "大阪府",   "関西地方", 34.69, 135.50, "金児 皐",   50000000),
    ("神戸市",     "兵庫県",   "関西地方", 34.69, 135.18, "金児 皐",   50000000),
    ("京都市",     "京都府",   "関西地方", 35.01, 135.77, "金児 皐",   50000000),
    ("堺市",       "大阪府",   "関西地方", 34.57, 135.48, "金児 皐",   50000000),
    ("奈良市",     "奈良県",   "関西地方", 34.69, 135.80, "金児 皐",   50000000),
    ("大津市",     "滋賀県",   "関西地方", 35.00, 135.87, "金児 皐",   50000000),
    ("和歌山市",   "和歌山県", "関西地方", 34.23, 135.17, "金児 皐",   50000000),
    # 中国地方
    ("広島市",     "広島県",   "中国地方", 34.40, 132.46, "雨宮 武",   30000000),
    ("岡山市",     "岡山県",   "中国地方", 34.66, 133.93, "雨宮 武",   30000000),
    ("松江市",     "島根県",   "中国地方", 35.47, 133.05, "雨宮 武",   30000000),
    ("山口市",     "山口県",   "中国地方", 34.19, 131.47, "雨宮 武",   30000000),
    # 四国
    ("高松市",     "香川県",   "四国",     34.34, 134.04, "川波 結菜", 20000000),
    ("松山市",     "愛媛県",   "四国",     33.84, 132.77, "川波 結菜", 20000000),
    ("高知市",     "高知県",   "四国",     33.56, 133.53, "川波 結菜", 20000000),
    ("徳島市",     "徳島県",   "四国",     34.07, 134.56, "川波 結菜", 20000000),
    # 九州
    ("福岡市",     "福岡県",   "九州",     33.59, 130.40, "矢幡 翔太", 35000000),
    ("北九州市",   "福岡県",   "九州",     33.88, 130.88, "矢幡 翔太", 35000000),
    ("熊本市",     "熊本県",   "九州",     32.79, 130.74, "矢幡 翔太", 35000000),
    ("鹿児島市",   "鹿児島県", "九州",     31.60, 130.56, "矢幡 翔太", 35000000),
    ("長崎市",     "長崎県",   "九州",     32.75, 129.88, "矢幡 翔太", 35000000),
    ("大分市",     "大分県",   "九州",     33.24, 131.61, "矢幡 翔太", 35000000),
    ("宮崎市",     "宮崎県",   "九州",     31.91, 131.42, "矢幡 翔太", 35000000),
]
# 都市圏ほど出現率が高い
LOCATION_WEIGHTS = [
    5,2,1,                         # 北海道
    4,2,1,1,2,1,                   # 東北
    8,6,5,4,4,3,2,2,2,             # 関東
    6,2,3,3,2,2,2,1,3,             # 中部
    7,5,4,3,2,1,1,                 # 関西
    3,2,1,1,                       # 中国
    2,2,1,1,                       # 四国
    5,2,3,2,1,1,1,                 # 九州
]

# === 顧客名プール ===
SURNAMES = [
    "佐藤","鈴木","高橋","田中","伊藤","渡辺","山本","中村","小林","加藤",
    "吉田","山田","佐々木","松本","井上","木村","林","斎藤","清水","山口",
    "池田","橋本","阿部","石川","山崎","森","中島","前田","藤田","小川",
    "岡田","後藤","長谷川","村上","近藤","石井","上田","遠藤","青木","坂本",
]
GIVEN_NAMES = [
    "太郎","花子","翔太","美月","大輔","桜","拓也","葵","健太","愛",
    "学","由美","慶子","誠","直子","茂","千代子","海斗","麗華","大地",
    "真","静","武","正","陽","翔","優","克","結菜","涼",
]

# === 顧客マスタ生成（~800名）===
N_CUSTOMERS = 800
customers = []
for i in range(N_CUSTOMERS):
    s = SURNAMES[i % len(SURNAMES)]
    g = GIVEN_NAMES[(i * 7 + i // len(SURNAMES)) % len(GIVEN_NAMES)]
    cid = f"{s[:1]}{g[:1]}-{10000 + i}"
    seg = random.choices(["消費者", "小規模事業所", "大企業"], weights=[52, 30, 18])[0]
    loc_idx = random.choices(range(len(LOCATIONS)), weights=LOCATION_WEIGHTS)[0]
    customers.append((cid, f"{s} {g}", seg, loc_idx))

# === オーダー生成 ===
TARGET_ROWS = 10000
MONTH_WEIGHTS = [6, 5, 9, 10, 11, 10, 8, 7, 8, 9, 7, 10]  # 春〜初夏が繁忙期

rows = []
row_id = 1
order_counter = 1000000

for year in [2022, 2023, 2024, 2025]:
    if row_id > TARGET_ROWS:
        break
    n_orders = random.randint(680, 720)
    for _ in range(n_orders):
        if row_id > TARGET_ROWS:
            break

        cid, cname, cseg, loc_idx = random.choice(customers)
        city, pref, region, lat, lng, manager, budget = LOCATIONS[loc_idx]

        month = random.choices(range(1, 13), weights=MONTH_WEIGHTS)[0]
        day = random.randint(1, 28)
        order_date = datetime(year, month, day)

        ship_mode = random.choices(
            ["通常配送", "セカンド クラス", "ファースト クラス", "即日配送"],
            weights=[60, 20, 15, 5]
        )[0]
        ship_days = {"通常配送": random.randint(3,7), "セカンド クラス": random.randint(2,4),
                     "ファースト クラス": random.randint(1,3), "即日配送": 0}
        ship_date = order_date + timedelta(days=ship_days[ship_mode])

        # 1オーダーあたりの明細数（1〜7明細）
        n_items = random.choices([1,2,3,4,5,6,7], weights=[40,25,15,10,5,3,2])[0]
        selected_pids = random.choices(PRODUCT_IDS, weights=PRODUCT_WEIGHTS, k=n_items)

        order_counter += 1
        oid = f"JP-{year}-{order_counter}"

        for pid in selected_pids:
            if row_id > TARGET_ROWS:
                break

            # productテーブルから取得（マスタが正）
            pm = product_master[pid]
            attrs = PRODUCT_ATTRS[pid]
            cat = pm.category
            pname = pm.product_name
            price = pm.price_tax_included
            # productテーブルにない属性はローカル定義
            subcat = attrs["sub_category"]
            margin = attrs["margin"]
            pid_str = f"{cat[:2]}-{subcat[:2]}-{pid}"

            qty = random.choices(range(1, 12), weights=[30,20,15,10,8,5,4,3,2,2,1])[0]
            discount = random.choices([0, 0.1, 0.2, 0.3, 0.4, 0.5], weights=[65, 10, 10, 7, 5, 3])[0]

            sales = round(price * qty * (1 - discount), 1)
            eff_margin = margin - (discount * 1.5 if discount > 0 else 0)
            profit = round(sales * eff_margin, 1)

            is_return = random.random() < 0.05

            rows.append((
                row_id, oid, order_date, ship_date, ship_mode,
                cid, cname, cseg, city, pref, "日本", region,
                pid_str, cat, subcat, pname,
                sales, qty, discount, profit,
                lat, lng, manager, budget, is_return, pid
            ))
            row_id += 1

# === スキーマ定義 & Deltaテーブル保存 ===
sales_schema = StructType([
    StructField("row_id", IntegerType()),              # 行 ID
    StructField("order_id", StringType()),              # オーダー ID
    StructField("order_date", TimestampType()),         # オーダー日
    StructField("ship_date", TimestampType()),          # 出荷日
    StructField("ship_mode", StringType()),             # 出荷モード
    StructField("customer_id", StringType()),           # 顧客 ID
    StructField("customer_name", StringType()),         # 顧客名
    StructField("customer_segment", StringType()),      # 顧客区分
    StructField("city", StringType()),                  # 市区町村
    StructField("prefecture", StringType()),            # 都道府県
    StructField("country_region", StringType()),        # 国/領域
    StructField("region", StringType()),                # 地域
    StructField("product_id_str", StringType()),        # 製品 ID
    StructField("category", StringType()),              # カテゴリ
    StructField("sub_category", StringType()),          # サブカテゴリ
    StructField("product_name", StringType()),          # 製品名
    StructField("sales_amount", DoubleType()),          # 売上
    StructField("quantity", IntegerType()),             # 数量
    StructField("discount_rate", DoubleType()),         # 割引率
    StructField("profit", DoubleType()),                # 利益
    StructField("latitude", DoubleType()),              # 緯度
    StructField("longitude", DoubleType()),             # 経度
    StructField("regional_manager", StringType()),      # 地域マネージャー
    StructField("budget", IntegerType()),               # 予算
    StructField("returned", BooleanType()),             # 返品
    StructField("product_id", IntegerType()),           # 商品ID（FK）
])

df_sales = spark.createDataFrame(rows, schema=sales_schema)
df_sales.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.sales")

print(f"生成行数: {df_sales.count()}")
print(df_sales.columns)
display(df_sales)

# COMMAND ----------

# DBTITLE 1,CSV出力
# csv_data = df_sales.toPandas().to_csv(index=False)
# file_path = f"/Volumes/{catalog}/{schema}/{volume}/structured/sales/sales.csv"
# dbutils.fs.put(file_path, csv_data, True)
# print(f"CSV保存完了: {file_path}")

# COMMAND ----------

# DBTITLE 1,認定済みタグの追加
certified_tag = 'system.Certified'

try:
    spark.sql(f"ALTER TABLE sales SET TAGS ('{certified_tag}')")
    print(f"認定済みタグ '{certified_tag}' の追加が完了しました。")

except Exception as e:
    print(f"認定済みタグ '{certified_tag}' の追加中にエラーが発生しました: {str(e)}")
    print("このエラーはタグ機能に対応していないワークスペースで実行した場合に発生する可能性があります。")

# COMMAND ----------

# DBTITLE 1,コメント追加
table_name = f"{catalog}.{schema}.sales"

comment = """
テーブル名: `sales` / 購買履歴
説明: 全国店舗の購買トランザクションデータ。顧客情報、商品、売上、利益、地域、出荷モード等を含む。ダッシュボード分析・売上トレンド可視化に使用。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

column_comments = {
    "row_id":           "行番号（各トランザクション明細の連番）",
    "order_id":         "注文ID（JP-年-連番 の形式）",
    "order_date":       "注文日",
    "ship_date":        "出荷日",
    "ship_mode":        "出荷方法（即日配送／ファーストクラス／セカンドクラス／通常配送）",
    "customer_id":      "顧客ID",
    "customer_name":    "顧客名",
    "customer_segment": "顧客セグメント（消費者／小規模事業所／大企業）",
    "city":             "市区町村",
    "prefecture":       "都道府県",
    "country_region":   "国",
    "region":           "地域ブロック（関東地方／関西地方 など）",
    "product_id_str":   "製品ID（カテゴリ短縮-サブカテゴリ短縮-product_id の形式）",
    "category":         "商品カテゴリ（電動工具／塗料／木材・建材／金物／消耗品／園芸／作業用品）",
    "sub_category":     "サブカテゴリ",
    "product_name":     "製品名（ブランド + 商品名 + 補足）",
    "sales_amount":     "売上金額（税込価格 × 数量 × (1 - 割引率)）",
    "quantity":         "購入数量",
    "discount_rate":    "割引率（0.0〜0.5）",
    "profit":           "利益金額",
    "latitude":         "配送先緯度",
    "longitude":        "配送先経度",
    "regional_manager": "地域マネージャー名",
    "budget":           "地域予算（円）",
    "returned":         "返品有無（true = 返品あり）",
    "product_id":       "商品ID（productテーブルのproduct_idと紐づくFK）"
}

for column, cmt in column_comments.items():
    escaped_cmt = cmt.replace("'", "\\'")
    sql = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_cmt}'"
    spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 主キー・外部キーの設定

# COMMAND ----------

# DBTITLE 1,product / 商品マスタ
# =========================
# product：product_id 列を確保 → 主キー(product_id)
# =========================
TABLE_PATH = f"{catalog}.{schema}.product"
PK_CONSTRAINT_NAME = "pk_product"

# 列が無ければ追加（存在していればエラー→無視）
try:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ADD COLUMN product_id INT")
except Exception as e:
    print(f"[info] product_id already exists or cannot add: {e}")

# NOT NULL 制約
spark.sql(f"""
ALTER TABLE {TABLE_PATH}
ALTER COLUMN product_id SET NOT NULL;
""")

# 主キーを作り直し
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (product_id);
''')

# チェック
display(spark.sql(f'''DESCRIBE EXTENDED {TABLE_PATH}'''))

# COMMAND ----------

# DBTITLE 1,inventory / 在庫
# =========================
# inventory：複合PK + FK(product_id)
# =========================
TABLE_PATH = f"{catalog}.{schema}.inventory"
PK_CONSTRAINT_NAME = "pk_inventory_store_sku"
FK_CONSTRAINT_NAME = "fk_inventory_product"
REF_TABLE_PATH = f"{catalog}.{schema}.product"

# product_id 列が無ければ追加
try:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ADD COLUMN product_id INT")
except Exception as e:
    print(f"[info] inventory.product_id already exists or cannot add: {e}")

# デモ簡略：sku_id をそのまま product_id に流用（未設定行のみ）
spark.sql(f"UPDATE {TABLE_PATH} SET product_id = sku_id WHERE product_id IS NULL")

# NOT NULL 制約
for column in ["store_id", "sku_id", "product_id"]:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー（複合）を再作成
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')
spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (store_id, sku_id);
''')

# 外部キーを再作成（inventory.product_id → product.product_id）
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_CONSTRAINT_NAME};
''')
spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {FK_CONSTRAINT_NAME}
FOREIGN KEY (product_id) REFERENCES {REF_TABLE_PATH}(product_id);
''')

# チェック
display(spark.sql(f'''DESCRIBE EXTENDED {TABLE_PATH}'''))

# COMMAND ----------

# DBTITLE 1,reservation / 取り置き・予約管理
# =========================
# reservation：PK(reservation_id) + FK(product_id)
# =========================
TABLE_PATH = f"{catalog}.{schema}.reservation"
PK_CONSTRAINT_NAME = "pk_reservation"
FK_CONSTRAINT_NAME = "fk_reservation_product"
REF_TABLE_PATH = f"{catalog}.{schema}.product"

# NOT NULL 制約
for column in ["reservation_id", "product_id"]:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キーを再作成
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')
spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (reservation_id);
''')

# 外部キーを再作成（reservation.product_id → product.product_id）
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_CONSTRAINT_NAME};
''')
spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {FK_CONSTRAINT_NAME}
FOREIGN KEY (product_id) REFERENCES {REF_TABLE_PATH}(product_id);
''')

# チェック
display(spark.sql(f'''DESCRIBE EXTENDED {TABLE_PATH}'''))

# COMMAND ----------

# DBTITLE 1,sales / 購買履歴
# =========================
# sales：PK(row_id) + FK(product_id)
# =========================
TABLE_PATH = f"{catalog}.{schema}.sales"
PK_CONSTRAINT_NAME = "pk_sales"
FK_CONSTRAINT_NAME = "fk_sales_product"
REF_TABLE_PATH = f"{catalog}.{schema}.product"

# NOT NULL 制約
for column in ["row_id", "product_id"]:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キーを再作成
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')
spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (row_id);
''')

# 外部キーを再作成（sales.product_id → product.product_id）
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_CONSTRAINT_NAME};
''')
spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {FK_CONSTRAINT_NAME}
FOREIGN KEY (product_id) REFERENCES {REF_TABLE_PATH}(product_id);
''')

# チェック
display(spark.sql(f'''DESCRIBE EXTENDED {TABLE_PATH}'''))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. RAG用非構造化データをVolumeに設置

# COMMAND ----------

# DBTITLE 1,PDFをvolumeにコピー
import os
import shutil

'''
概要：./_unstructured 直下にあるPDFをVolumeにコピーします。AgentBricksナレッジアシスタントが参照します。
'''
# コピー対象のリスト定義 (PDF名とボリューム上のサブディレクトリ)
target_files = [
    {"pdf": "CS対応FAQ（社内用）.pdf", "subdir": "faq"},
    {"pdf": "商品知識ハンドブック（スタッフ用）.pdf", "subdir": "brand"},
    {"pdf": "店舗運営マニュアル v3.2（2024年11月改訂）.pdf", "subdir": "operations"},
    {"pdf": "接客研修資料（2025 春夏版）.pdf", "subdir": "training"}
]

# 基本設定
current_dir = os.getcwd()      # カレントディレクトリ
src_base_dir = "_unstructured" # PDFコピー元のディレクトリ

# ループでPDFコピー実行
for item in target_files:
    # パスの組み立て
    src_path = os.path.join(current_dir, src_base_dir, item["pdf"])
    dest_dir = f"/Volumes/{catalog}/{schema}/{volume}/unstructured/{item['subdir']}"
    dest_path = os.path.join(dest_dir, item["pdf"])

    # ディレクトリ作成（Volume側）
    os.makedirs(dest_dir, exist_ok=True)

    # コピー実行
    if os.path.exists(src_path):
        shutil.copyfile(src_path, dest_path)
        print(f"コピー完了: {dest_path}")
    else:
        print(f"コピー元が見つかりません -> {src_path}")
