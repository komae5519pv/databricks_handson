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

# ===== 1) inventory（在庫情報：brand/category/product_id を含む） =====
inventory_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("sku_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("on_hand", IntegerType(), True),
    StructField("reserved", IntegerType(), True),
    StructField("safety_stock", IntegerType(), True),
    StructField("next_eta_date", DateType(), True),
    StructField("next_eta_qty", IntegerType(), True),
])

# 店舗：東京4拠点＋横浜/名古屋/大阪
inventory_data = [
    # 1001 リネンシャツ（NEUTRAL WORKS / トップス）
    ('T01','新宿店','東京', 11001,1001,'リネンシャツ','NEUTRAL WORKS','トップス','白','M', 6,1,1, one_week_later.date(), 2),
    ('T02','渋谷店','東京', 11001,1001,'リネンシャツ','NEUTRAL WORKS','トップス','白','M', 0,0,0, None, None),
    ('T03','池袋店','東京', 11001,1001,'リネンシャツ','NEUTRAL WORKS','トップス','白','M', 2,0,1, one_week_later.date(), 1),
    ('T04','銀座店','東京', 11001,1001,'リネンシャツ','NEUTRAL WORKS','トップス','白','M', 3,1,1, None, None),
    ('K01','横浜店','神奈川',11001,1001,'リネンシャツ','NEUTRAL WORKS','トップス','白','M', 1,0,0, None, None),

    # 1002 ベルト付きワイドパンツ（URBAN BRICKS / パンツ）
    ('T01','新宿店','東京', 11002,1002,'ベルト付きワイドパンツ','URBAN BRICKS','パンツ','黒','M', 1,0,0, one_week_later.date(), 2),
    ('T02','渋谷店','東京', 11002,1002,'ベルト付きワイドパンツ','URBAN BRICKS','パンツ','黒','M', 2,1,0, None, None),
    ('O1','大阪店','大阪',  11002,1002,'ベルト付きワイドパンツ','URBAN BRICKS','パンツ','黒','M', 4,1,1, None, None),

    # 1003 フレアスカート（LATTICE LOOP / スカート）
    ('T03','池袋店','東京', 11003,1003,'フレアスカート','LATTICE LOOP','スカート','ベージュ','M', 3,0,1, one_week_later.date(), 1),
    ('N01','名古屋店','愛知',11003,1003,'フレアスカート','LATTICE LOOP','スカート','ベージュ','M', 0,0,0, None, None),

    # 1004 ノーカラージャケット（URBAN BRICKS / ジャケット／スーツ）
    ('T02','渋谷店','東京', 11004,1004,'ノーカラージャケット','URBAN BRICKS','ジャケット／スーツ','ネイビー','M', 2,0,0, one_week_later.date(), 2),
    ('T04','銀座店','東京', 11004,1004,'ノーカラージャケット','URBAN BRICKS','ジャケット／スーツ','ネイビー','M', 0,0,0, None, None),
    ('O1','大阪店','大阪',  11004,1004,'ノーカラージャケット','URBAN BRICKS','ジャケット／スーツ','ネイビー','M', 1,0,0, None, None),

    # 1005 スニーカー（NEUTRAL WORKS / シューズ）
    ('T01','新宿店','東京', 11005,1005,'スニーカー','NEUTRAL WORKS','シューズ','白','26', 5,2,1, one_week_later.date(), 3),
    ('K01','横浜店','神奈川',11005,1005,'スニーカー','NEUTRAL WORKS','シューズ','白','26', 0,0,0, None, None),

    # 1006 カシュクールワンピース（LATTICE LOOP / ワンピース／ドレス）
    ('T04','銀座店','東京', 11006,1006,'カシュクールワンピース','LATTICE LOOP','ワンピース／ドレス','グリーン','M', 2,0,0, None, None),
    ('N01','名古屋店','愛知',11006,1006,'カシュクールワンピース','LATTICE LOOP','ワンピース／ドレス','グリーン','M', 1,0,0, one_week_later.date(), 1),

    # 1007 レザーショルダーバッグ（URBAN BRICKS / バッグ・ファッション小物）
    ('T02','渋谷店','東京', 11007,1007,'レザーショルダーバッグ','URBAN BRICKS','バッグ・ファッション小物','ブラウン','F', 3,1,0, None, None),
    ('O1','大阪店','大阪',  11007,1007,'レザーショルダーバッグ','URBAN BRICKS','バッグ・ファッション小物','ブラウン','F', 2,0,0, one_week_later.date(), 2),

    # 1008 ライトブルゾン（NEUTRAL WORKS / ブルゾン／アウター）
    ('T03','池袋店','東京', 11008,1008,'ライトブルゾン','NEUTRAL WORKS','ブルゾン／アウター','ライトグレー','M', 1,0,0, None, None),
    ('K01','横浜店','神奈川',11008,1008,'ライトブルゾン','NEUTRAL WORKS','ブルゾン／アウター','ライトグレー','M', 0,0,0, one_week_later.date(), 2),
    ('T01','新宿店','東京', 11018,1008,'ライトブルゾン','NEUTRAL WORKS','ブルゾン／アウター','ネイビー','M', 2,0,0, None, None),
    ('T02','渋谷店','東京', 11018,1008,'ライトブルゾン','NEUTRAL WORKS','ブルゾン／アウター','ネイビー','M', 0,0,0, None, None),
    ('T04','銀座店','東京', 11038,1008,'ライトブルゾン','NEUTRAL WORKS','ブルゾン／アウター','ブラック','M', 3,1,1, one_week_later.date(), 3),

    # 別ブランドの近似アイテム（URBAN BRICKS / LATTICE LOOP）
    ('T02','渋谷店','東京', 11013,1013,'ライトマウンテンパーカ','URBAN BRICKS','ブルゾン／アウター','ネイビー','M', 2,0,0, None, None),
    ('K01','横浜店','神奈川',11013,1013,'ライトマウンテンパーカ','URBAN BRICKS','ブルゾン／アウター','ネイビー','M', 1,0,0, one_week_later.date(), 1),

    ('T04','銀座店','東京', 11014,1014,'ドライタッチブルゾン','LATTICE LOOP','ブルゾン／アウター','ライトグレー','M', 2,0,0, one_week_later.date(), 2),
    ('O1','大阪店','大阪',  11014,1014,'ドライタッチブルゾン','LATTICE LOOP','ブルゾン／アウター','ライトグレー','M', 1,0,0, None, None),
]

df_inventory = spark.createDataFrame(inventory_data, schema=inventory_schema)

# UUID列 & 実効在庫列
df_inventory = (
    df_inventory
    .withColumn("id", expr("uuid()"))
    .withColumn("effective_available", col("on_hand") - col("reserved") - col("safety_stock"))
)

# 列順整形（FK/PKに必要な列を含める）
df_inventory = df_inventory.select(
    "id", "store_id", "store_name", "region",
    "sku_id", "product_id",
    "product_name", "brand", "category",
    "color", "size",
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
# pandasに変換してCSV文字列を作る
csv_data = df_inventory.toPandas().to_csv(index=False)

# CSV保存
file_path = f"/Volumes/{catalog}/{schema}/{volume}/structured/inventory/inventory.csv"
dbutils.fs.put(file_path, csv_data, True)
print(f"CSV保存完了: {file_path}")

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
説明: 各店舗・SKU単位の在庫状況を保持するテーブル。ブランド・商品カテゴリを含む。Genieデモ用データ。
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
    "brand": "ブランド名（例：NEUTRAL WORKS、URBAN BRICKS など）",
    "category": "商品カテゴリ（例：トップス、ジャケット／スーツ、スカート、シューズなど）",
    "color": "商品の色",
    "size": "商品のサイズ",
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
    StructField("description", StringType(), True),         # カテゴリ/シーン/テイストを含む説明
    StructField("material_features", StringType(), True),
    StructField("staff_comment", StringType(), True),
    StructField("review_score", DoubleType(), True),
    StructField("web_exclusive", StringType(), True),       # 限定 / 通常
    StructField("price_tax_included", IntegerType(), True), # 円
    StructField("shipping_fee", IntegerType(), True),       # 円
    StructField("returnable", StringType(), True),          # 可 / 不可
    StructField("volume_path", StringType(), True),         # 画像ファイルの格納パス（Volumes パス）
])

# 共通ルート（例：/Volumes/<catalog>/<schema>/<volume>/）
base_path = f"/Volumes/{catalog}/{schema}/{volume}/"

data_product = [
    (1001,"リネンシャツ","NEUTRAL WORKS","トップス",
     "【カテゴリ】トップス  【シーン】通勤/週末/旅行  【テイスト】カジュアル/ミニマル  【用途】春夏の羽織り  【シルエット・フィット】レギュラー  【機能】通気/軽量  【素材感】リネン/光沢控えめ/薄手  【スタイリング】Tシャツに羽織  【カラー展開】白系",
     "透け感：ややあり／裏地：なし／伸縮：なし／光沢：控えめ／厚さ：薄め",
     "羽織にも使える万能シャツ。（身長165cm）",
     4.2,"通常",  8900, 550,"可", f"{base_path}001.jpg"),

    (1002,"ベルト付きワイドパンツ","URBAN BRICKS","パンツ",
     "【カテゴリ】パンツ  【シーン】通勤/食事/週末  【テイスト】きれいめ/モダン  【用途】オン/オフ兼用  【シルエット・フィット】ワイド/ハイウエスト  【機能】少しストレッチ/シワ抑制  【素材感】中厚/マット  【スタイリング】タックイン映え  【カラー展開】黒系",
     "透け感：なし／裏地：あり／伸縮：少しあり／光沢：なし／厚さ：中厚",
     "ウエストが楽で動きやすい。（158cm）",
     3.6,"限定", 12900,   0,"可", f"{base_path}002.jpg"),

    (1003,"フレアスカート","LATTICE LOOP","スカート",
     "【カテゴリ】スカート  【シーン】通勤/おでかけ  【テイスト】フェミニン  【用途】通年着回し  【シルエット・フィット】フレア/ミディ丈  【機能】裏地付き/透けにくい  【素材感】程よいハリ/やや光沢  【スタイリング】Tシャツ/ブラウス合わせ  【カラー展開】ベージュ系",
     "透け感：なし／裏地：あり／伸縮：なし／光沢：あり／厚さ：普通",
     "ふんわり感が上品。（160cm）",
     4.5,"通常",  9900, 550,"可", f"{base_path}003.jpg"),

    (1004,"ノーカラージャケット","URBAN BRICKS","ジャケット／スーツ",
     "【カテゴリ】ジャケット／スーツ  【シーン】オフィス/セミフォーマル/会食  【テイスト】きれいめ  【用途】オンスタイルの羽織り  【シルエット・フィット】ノーカラー/レギュラー  【機能】軽量/ストレッチ少々  【素材感】薄手/わずかに光沢  【スタイリング】ワンピ/セットアップ  【カラー展開】ネイビー系",
     "透け感：なし／裏地：あり／伸縮：あり／光沢：少しあり／厚さ：薄め",
     "袖丈短めで手首がすっきり。（170cm）",
     3.2,"限定", 14900,   0,"不可", f"{base_path}004.jpg"),

    (1005,"スニーカー","NEUTRAL WORKS","シューズ",
     "【カテゴリ】シューズ  【シーン】通勤/週末/旅行  【テイスト】カジュアル  【用途】デイリー歩行  【シルエット・フィット】レギュラー  【機能】クッション/軽量/長時間快適  【素材感】マット/厚めソール  【スタイリング】ワンマイル/通勤カジュアル  【カラー展開】白系",
     "透け感：なし／裏地：あり／伸縮：あり／光沢：なし／厚さ：厚め",
     "やや大きめ。0.5cm下推奨。（24.5cm）",
     4.1,"通常",  9900,   0,"可", f"{base_path}005.jpg"),

    (1006,"カシュクールワンピース","LATTICE LOOP","ワンピース／ドレス",
     "【カテゴリ】ワンピース／ドレス  【シーン】おでかけ/会食/二次会  【テイスト】フェミニン/きれいめ  【用途】一枚主役  【シルエット・フィット】カシュクール/ウエストマーク  【機能】少しストレッチ/裏地あり  【素材感】落ち感/上品な艶  【スタイリング】ジャケット羽織り  【カラー展開】グリーン系",
     "透け感：なし／裏地：あり／伸縮：少しあり／光沢：少しあり／厚さ：普通",
     "一枚で決まる主役ワンピ。（159cm）",
     3.8,"限定", 13900, 550,"可", f"{base_path}006.jpg"),

    (1007,"レザーショルダーバッグ","URBAN BRICKS","バッグ・ファッション小物",
     "【カテゴリ】バッグ・ファッション小物  【シーン】通勤/休日/旅行  【テイスト】きれいめ/カジュアル  【用途】毎日使い  【シルエット・フィット】ショルダー  【機能】軽量/容量十分/内ポケット  【素材感】レザー調/やや光沢/中厚  【スタイリング】ON/OFF両対応  【カラー展開】ブラウン系",
     "透け感：なし／裏地：あり／伸縮：なし／光沢：ややあり／厚さ：中厚",
     "通勤にも休日にも◎。",
     3.9,"通常",  8900, 550,"可", f"{base_path}007.jpg"),

    (1008,"ライトブルゾン","NEUTRAL WORKS","ブルゾン／アウター",
     "【カテゴリ】ブルゾン／アウター  【シーン】梅雨/春秋/自転車  【テイスト】スポーティ/ミニマル  【用途】軽い羽織り  【シルエット・フィット】ショート丈/レギュラー  【機能】撥水/軽量/裏地あり  【素材感】薄手/マット  【スタイリング】Tシャツに羽織  【カラー展開】ライトグレー/ネイビー/カーキ/ブラック",
     "透け感：なし／裏地：あり／伸縮：少しあり／光沢：なし／厚さ：薄め",
     "梅雨〜春秋に活躍。",
     4.0,"通常", 11900, 550,"可", f"{base_path}008.jpg"),

    (1009,"コットンハット","NEUTRAL WORKS","帽子",
     "【カテゴリ】帽子  【シーン】日差し対策/旅行/アウトドア  【テイスト】カジュアル  【用途】UV/暑さ対策  【シルエット・フィット】つば中/レギュラー  【機能】通気性/軽量  【素材感】コットン/マット/薄手  【スタイリング】カジュアル/アウトドア  【カラー展開】ベージュ系",
     "—",
     "日差し対策に最適。",
     3.5,"通常",  3900, 550,"可", f"{base_path}009.jpg"),

    (1010,"サークルネックレス","LATTICE LOOP","アクセサリー",
     "【カテゴリ】アクセサリー  【シーン】デイリー/オフィス/会食  【テイスト】ミニマル  【用途】ワンポイント  【シルエット・フィット】短めチェーン/小ぶり  【機能】軽量/扱いやすい  【素材感】マット寄り  【スタイリング】無地トップスに映える  【カラー展開】ゴールド系",
     "—",
     "首元のワンポイントに。",
     3.7,"通常",  2900, 350,"不可", f"{base_path}010.jpg"),

    (1011,"ポインテッドパンプス","URBAN BRICKS","シューズ",
     "【カテゴリ】シューズ  【シーン】オフィス/フォーマル  【テイスト】きれいめ  【用途】オンスタイルの足元  【シルエット・フィット】ポインテッド/ミドルヒール  【機能】安定感/滑りにくい底  【素材感】中厚/やや艶  【スタイリング】スラックス/タイトスカート  【カラー展開】ブラック系",
     "透け感：なし／裏地：あり／伸縮：なし／光沢：ややあり／厚さ：普通",
     "24.0はジャスト。（足幅普通）",
     3.4,"限定", 10900,   0,"可", f"{base_path}011.jpg"),

    (1012,"サテンオールインワン","URBAN BRICKS","オールインワン",
     "【カテゴリ】オールインワン  【シーン】おでかけ/二次会  【テイスト】きれいめ  【用途】一枚できれい見え  【シルエット・フィット】ストレート/ややリラックス  【機能】少しストレッチ/軽量  【素材感】サテン/上品な艶/薄手  【スタイリング】ジャケット合わせ◎  【カラー展開】ネイビー系",
     "透け感：なし／裏地：あり／伸縮：少しあり／光沢：あり／厚さ：薄め",
     "ジャケット合わせも◎。",
     3.9,"通常", 12900, 550,"可", f"{base_path}012.jpg"),

    # 近似アイテム（ライトブルゾン比較用）
    (1013,"ライトマウンテンパーカ","URBAN BRICKS","ブルゾン／アウター",
     "【カテゴリ】ブルゾン／アウター  【シーン】梅雨/通勤/週末  【テイスト】スポーティ  【用途】天候変化に強い羽織り  【シルエット・フィット】フード/レギュラー  【機能】撥水/少しストレッチ/収納ポケット  【素材感】薄手/マット  【スタイリング】デニム/ワンピ羽織  【カラー展開】ネイビー系",
     "透け感：なし／裏地：あり／伸縮：少しあり／光沢：なし／厚さ：薄め",
     "梅雨時期の通勤に最適。",
     3.8,"通常", 12900, 550,"可", f"{base_path}013.jpg"),

    (1014,"ドライタッチブルゾン","LATTICE LOOP","ブルゾン／アウター",
     "【カテゴリ】ブルゾン／アウター  【シーン】春秋/自転車/通勤  【テイスト】ミニマル/カジュアル  【用途】軽い羽織り  【シルエット・フィット】ショート丈/レギュラー  【機能】ドライタッチ/ベタつき軽減/軽量  【素材感】薄手/マット  【スタイリング】Tシャツ/ワンピ羽織  【カラー展開】ライトグレー系",
     "透け感：なし／裏地：あり／伸縮：少しあり／光沢：なし／厚さ：薄め",
     "さらっと羽織れて着回しやすい。",
     3.7,"通常", 11900, 550,"可", f"{base_path}014.jpg"),
]

df_product = spark.createDataFrame(data_product, schema=schema_product)
df_product = df_product.withColumn("id", expr("uuid()"))
df_product.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.product")

print(df_product.columns)
print(df_product.count())
display(df_product)

# COMMAND ----------

# DBTITLE 1,CSV出力
# pandasに変換してCSV文字列を作る
csv_data = df_product.toPandas().to_csv(index=False)

# CSV保存
file_path = f"/Volumes/{catalog}/{schema}/{volume}/structured/product/product.csv"
dbutils.fs.put(file_path, csv_data, True)
print(f"CSV保存完了: {file_path}")

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
説明: 商品の説明・素材特徴・スタッフコメント・レビュー・限定区分・税込価格・送料・返品可否を保持。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

column_comments = {
    "product_id": "商品ID（在庫や入荷予定と紐づくキー）",
    "product_name": "商品名",
    "brand": "ブランド名",
    "category": "商品カテゴリ（例：トップス、ジャケット／スーツ、パンツ など）",
    "description": "アイテム説明（素材の特徴、デザイン、コーディネート提案など）",
    "material_features": "透け感・裏地・伸縮性・光沢感・生地の厚さなどの素材特徴",
    "staff_comment": "スタッフ着用コメント（身長・サイズ感・着心地など）",
    "review_score": "平均レビュー点数（5段階）",
    "web_exclusive": "WEB限定区分（限定／通常）",
    "price_tax_included": "税込価格（円）",
    "shipping_fee": "送料（円）",
    "returnable": "返品可否（可／不可）",
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
    StructField("reservation_id", StringType(), True),    # UUID
    StructField("store_id", StringType(), True),          # 店舗ID
    StructField("store_name", StringType(), True),        # 店舗名
    StructField("product_id", IntegerType(), True),       # 商品ID
    StructField("product_name", StringType(), True),      # 商品名
    StructField("customer_name", StringType(), True),     # 顧客名
    StructField("limit_date", DateType(), True),          # 取り置き期限
    StructField("status", StringType(), True),            # 希望中、確定、キャンセルなど
    StructField("note", StringType(), True),              # メモ・理由
    StructField("staff", StringType(), True),             # 担当スタッフ名
    StructField("created_at", TimestampType(), True),     # 受付日時
])

today = datetime.today()
one_week_later = today + timedelta(days=7)

# サンプルデータ：受付日時に1～3日前を指定
reservation_data = [
    ("", "T04", "銀座店", 1001, "リネンシャツ", "田中花子", one_week_later.date(), "希望中", "誕生日プレゼントで希望", "店員A", today - timedelta(days=3)),
    ("", "T04", "銀座店", 1001, "リネンシャツ", "横山潤", one_week_later.date(), "希望中", "サイズ交換希望", "店員B", today - timedelta(days=2)),
    ("", "T02", "渋谷店", 1002, "ベルト付きワイドパンツ", "鈴木彩", one_week_later.date(), "希望中", "", "店員C", today - timedelta(days=1)),
    ("", "K01", "横浜店", 1005, "スニーカー", "山本一郎", one_week_later.date(), "希望中", "週末引取希望", "店員A", today)
]

df_reservation = spark.createDataFrame(reservation_data, schema=reservation_schema)

# UUIDを付与
df_reservation = df_reservation.withColumn("reservation_id", expr("uuid()"))

# 保存用にカラム並び替え
df_reservation = df_reservation.select(
    "reservation_id", "store_id", "store_name", "product_id", "product_name",
    "customer_name", "limit_date", "status", "note", "staff", "created_at"
)

# Deltaテーブルとして保存
df_reservation.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.reservation")

# 確認出力
print(df_reservation.columns)
print(df_reservation.count())
display(df_reservation)

# COMMAND ----------

# DBTITLE 1,CSV出力
# pandasに変換してCSV文字列を作る
csv_data = df_reservation.toPandas().to_csv(index=False)

# CSV保存
file_path = f"/Volumes/{catalog}/{schema}/{volume}/structured/reservation/reservation.csv"
dbutils.fs.put(file_path, csv_data, True)
print(f"CSV保存完了: {file_path}")

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
説明: 各店舗・各商品に対する顧客取り置き希望（予約）を記録。顧客情報、期限、担当スタッフ、メモ、受付日時などを保持。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

column_comments = {
    "reservation_id": "予約・取り置きエントリのユニークID（UUID）",
    "store_id": "店舗ID（事業所識別子）",
    "store_name": "店舗名",
    "product_id": "商品ID（inventoryやproductと紐づくキー）",
    "product_name": "商品名",
    "customer_name": "取置き希望の顧客名",
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
# MAGIC ## 2. 主キー・外部キーの設定

# COMMAND ----------

# DBTITLE 1,product_master / 商品マスタ
# =========================
# product：product_id 列を確保 → 主キー(product_id)
# =========================
TABLE_PATH = f"{catalog}.{schema}.product"     # テーブルパス
PK_CONSTRAINT_NAME = "pk_product"               # 主キー名

# 列が無ければ追加（存在していればエラー→無視）
try:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ADD COLUMN product_id INT")
except Exception as e:
    print(f"[info] product_id already exists or cannot add: {e}")

# NOT NULL 制約（既に設定済みでもOK）
spark.sql(f"""
ALTER TABLE {TABLE_PATH}
ALTER COLUMN product_id SET NOT NULL;
""")

# 主キーを作り直し（存在すれば先に DROP）
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
TABLE_PATH = f"{catalog}.{schema}.inventory"          # テーブルパス
PK_CONSTRAINT_NAME = "pk_inventory_store_sku"         # 主キー（複合）
FK_CONSTRAINT_NAME = "fk_inventory_product"           # 外部キー
REF_TABLE_PATH = f"{catalog}.{schema}.product"        # 参照先（product）

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

# MAGIC %md
# MAGIC ## 3. RAG用非構造化データをVolumeに設置

# COMMAND ----------

import os
import shutil

# コピー対象のリスト定義 (PDF名とボリューム上のサブディレクトリ)
target_files = [
    {"pdf": "CS対応FAQ（社内用）.pdf", "subdir": "faq"},
    {"pdf": "ブランドブック（営業用資料抜粋）.pdf", "subdir": "brand"},
    {"pdf": "店舗運営マニュアル v3.2（2024年11月改訂）.pdf", "subdir": "operations"},
    {"pdf": "接客研修資料（2025 春夏版）.pdf", "subdir": "training"}
]

# 基本設定
current_dir = os.getcwd()      # カレントディレクトリ
src_base_dir = "_unstructured" # PDFコピー元のディレクトリ

# 3. ループでコピー実行
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
        print(f"✅ コピー完了: {dest_path}")
    else:
        print(f"❌ コピー元が見つかりません -> {src_path}")
