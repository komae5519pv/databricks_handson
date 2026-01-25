# Databricks notebook source
# MAGIC %md
# MAGIC # ガバナンス設定
# MAGIC テーブルコメント、カラムコメント、PK/FK設定を一括で行う
# MAGIC
# MAGIC **実行タイミング**: 初回セットアップ時、またはスキーマ変更時のみ（ETLジョブには含めない）

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze層 コメント設定

# COMMAND ----------

# DBTITLE 1,bz_users コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.bz_users'
table_comment = """
テーブル名：`bz_users / 会員マスタ（Bronze）`
説明：オンラインスーパー「ブリックスマート」の会員マスタを管理するテーブルです。会員ID、氏名、性別、生年月日、都道府県、電話番号、メールアドレスなどを含む。CSVから取り込んだRawデータをそのまま保持しています。会員プロファイリングや、購買行動の分析、マーケティングキャンペーンのターゲティングに役立ちます。
補足：
* behavior_cluster（行動クラスター）はGold層で購買履歴から算出されます。Bronze/Silver層には含まれません。
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "user_id": "会員ID（U00000001形式）",
    "name": "氏名",
    "gender": "性別（M=男性, F=女性）",
    "birth_date": "生年月日",
    "pref": "都道府県",
    "phone_number": "電話番号",
    "email": "メールアドレス",
    "email_permission_flg": "メール配信許諾フラグ（true=許諾, false=拒否）",
    "registration_date": "会員登録日",
    "status": "会員ステータス（active=有効, inactive=退会・停止）",
    "last_login_at": "最終ログイン日時",
    "_ingested_at": "取り込み日時",
    "_source_file": "ソースファイルパス"
}
for column, comment in column_comments.items():
    escaped_comment = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,bz_items コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.bz_items'
table_comment = """
テーブル名：`bz_items / 商品マスタ（Bronze）`
説明：オンラインスーパー「ブリックスマート」で取り扱う商品情報を管理するテーブルです。商品ID、カテゴリ情報、商品名、価格、賞味期限、入荷日、各種属性フラグ（オーガニック、値引き対象、調理不要、高単価）を含む。CSVから取り込んだRawデータをそのまま保持しています。商品分析、在庫管理、カテゴリ別売上分析に活用できます。
補足：
* is_organic: オーガニック・無添加商品フラグ
* is_discount_target: 値引き対象商品フラグ
* is_ready_to_eat: 調理不要商品フラグ（惣菜/冷凍食品/カット野菜など）
* is_premium: 高単価・こだわり商品フラグ
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "item_id": "商品ID",
    "category_id": "カテゴリID",
    "item_name": "商品名",
    "category_name": "カテゴリ名",
    "price": "定価",
    "expiration_date": "賞味期限",
    "arrival_date": "入荷日",
    "is_organic": "オーガニック・無添加フラグ（true=オーガニック/無添加商品, false=通常商品）",
    "is_discount_target": "値引き対象フラグ（true=値引き対象, false=対象外）",
    "is_ready_to_eat": "調理不要フラグ（true=惣菜/冷凍食品/カット野菜等の調理不要商品, false=要調理）",
    "is_premium": "高単価・こだわり商品フラグ（true=プレミアム商品, false=通常商品）",
    "_ingested_at": "取り込み日時",
    "_source_file": "ソースファイルパス"
}
for column, comment in column_comments.items():
    escaped_comment = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,bz_stores コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.bz_stores'
table_comment = """
テーブル名：`bz_stores / 店舗マスタ（Bronze）`
説明：オンラインスーパー「ブリックスマート」の店舗情報を管理するテーブルです。店舗ID、店舗名、エリア、住所、郵便番号、都道府県、市区町村、電話番号、メールアドレス、営業時間、定休日、店長ID、開店日、ステータスを含む。CSVから取り込んだRawデータをそのまま保持しています。店舗別売上分析、エリア別パフォーマンス比較に活用できます。
補足：
* store_area: 関東/関西/中部/九州などのエリア区分
* status: 営業中(active)/閉店(inactive)などの店舗ステータス
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "store_id": "店舗ID",
    "store_name": "店舗名",
    "store_area": "エリア",
    "address": "住所",
    "postal_code": "郵便番号",
    "prefecture": "都道府県",
    "city": "市区町村",
    "phone_number": "電話番号",
    "email": "メールアドレス",
    "business_hours": "営業時間",
    "closed_days": "定休日",
    "manager_id": "店長ID",
    "opening_date": "開店日",
    "status": "店舗ステータス（active=営業中, inactive=閉店）",
    "_ingested_at": "取り込み日時",
    "_source_file": "ソースファイルパス"
}
for column, comment in column_comments.items():
    escaped_comment = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,bz_orders コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.bz_orders'
table_comment = """
テーブル名：`bz_orders / 注文ヘッダ（Bronze）`
説明：オンラインスーパー「ブリックスマート」のPOS注文データ（会計単位）を管理するテーブルです。注文ID、会員ID、店舗ID、注文日時、合計金額、合計数量、注文ステータスを含む。CSVから取り込んだRawデータをそのまま保持しています。売上分析、顧客別購買頻度分析、時間帯別売上分析に活用できます。
補足：
* 1注文 = 1会計（レジ精算単位）
* order_status: completed（完了）/ cancelled（キャンセル）/ pending（保留）
* total_amount: 税込合計金額
* 会計年度は4月1日から翌年3月31日まで
* 四半期の定義: Q1(4-6月), Q2(7-9月), Q3(10-12月), Q4(1-3月)
* 半期の定義: 上半期(4-9月), 下半期(10-3月)
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "order_id": "注文ID",
    "user_id": "会員ID",
    "store_id": "店舗ID",
    "order_datetime": "注文日時",
    "total_amount": "合計金額（税込）",
    "total_quantity": "合計数量",
    "order_status": "注文ステータス（completed=完了, cancelled=キャンセル, pending=保留）",
    "_ingested_at": "取り込み日時",
    "_source_file": "ソースファイルパス"
}
for column, comment in column_comments.items():
    escaped_comment = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,bz_order_items コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.bz_order_items'
table_comment = """
テーブル名：`bz_order_items / 注文明細（Bronze）`
説明：オンラインスーパー「ブリックスマート」のPOS注文明細データ（アイテム単位）を管理するテーブルです。注文明細ID、会員ID、注文ID、店舗ID、商品ID、カテゴリID、数量、単価、小計、キャンセルフラグ、注文日時を含む。CSVから取り込んだRawデータをそのまま保持しています。商品別売上分析、バスケット分析、カテゴリ別売上構成比分析に活用できます。
補足：
* 1注文に対して複数の注文明細が紐づく（1:N）
* subtotal = unit_price × quantity
* cancel_flg: 明細単位のキャンセルフラグ（True=キャンセル）
* 会計年度は4月1日から翌年3月31日まで
* 四半期の定義: Q1(4-6月), Q2(7-9月), Q3(10-12月), Q4(1-3月)
* 半期の定義: 上半期(4-9月), 下半期(10-3月)
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "order_item_id": "注文明細ID",
    "user_id": "会員ID",
    "order_id": "注文ID",
    "store_id": "店舗ID",
    "item_id": "商品ID",
    "category_id": "カテゴリID",
    "quantity": "数量",
    "unit_price": "単価",
    "subtotal": "小計（unit_price × quantity）",
    "cancel_flg": "キャンセルフラグ（true=キャンセル済, false=有効）",
    "order_datetime": "注文日時",
    "_ingested_at": "取り込み日時",
    "_source_file": "ソースファイルパス"
}
for column, comment in column_comments.items():
    escaped_comment = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,列レベルマスキングの追加
# 各層の個人情報カラムに列レベルマスキングを適用
# 対象: email（メールアドレス）, phone_number（電話番号）, name（氏名）

# ============================================================
# 1. マスキング関数の定義と Bronze層 (bz_users) への適用
# ============================================================
try:
    '''メールアドレス用関数の作成と適用'''
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {MY_CATALOG}.{MY_SCHEMA}.mask_email(email STRING)
    RETURN '***@example.com'
    """)
    spark.sql(f"""
    ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_users ALTER COLUMN email SET MASK {MY_CATALOG}.{MY_SCHEMA}.mask_email
    """)

    '''電話番号用関数の作成と適用'''
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {MY_CATALOG}.{MY_SCHEMA}.mask_phone(phone STRING)
    RETURN '***-****-****'
    """)
    spark.sql(f"""
    ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_users ALTER COLUMN phone_number SET MASK {MY_CATALOG}.{MY_SCHEMA}.mask_phone
    """)

    '''氏名用関数の作成と適用'''
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {MY_CATALOG}.{MY_SCHEMA}.mask_name(name STRING)
    RETURN '****'
    """)
    spark.sql(f"""
    ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.bz_users ALTER COLUMN name SET MASK {MY_CATALOG}.{MY_SCHEMA}.mask_name
    """)

    print("✅ Bronze層の列レベルマスキング適用完了（bz_users: email, phone_number, name）")

    # ============================================================
    # 2. Silver層 (sl_users) への適用
    # ============================================================
    # すでに定義済みのマスキング関数を Silver層のテーブルにも紐付けます
    spark.sql(f"""
    ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.sl_users ALTER COLUMN email SET MASK {MY_CATALOG}.{MY_SCHEMA}.mask_email
    """)
    spark.sql(f"""
    ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.sl_users ALTER COLUMN phone_number SET MASK {MY_CATALOG}.{MY_SCHEMA}.mask_phone
    """)
    spark.sql(f"""
    ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.sl_users ALTER COLUMN name SET MASK {MY_CATALOG}.{MY_SCHEMA}.mask_name
    """)
    print("✅ Silver層の列レベルマスキング適用完了（sl_users: email, phone_number, name）")

    # ============================================================
    # 3. Gold層 (gd_users) への適用
    # ============================================================
    # すでに定義済みのマスキング関数を Gold層のテーブルにも紐付けます
    spark.sql(f"""
    ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_users ALTER COLUMN email SET MASK {MY_CATALOG}.{MY_SCHEMA}.mask_email
    """)
    spark.sql(f"""
    ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_users ALTER COLUMN phone_number SET MASK {MY_CATALOG}.{MY_SCHEMA}.mask_phone
    """)
    spark.sql(f"""
    ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_users ALTER COLUMN name SET MASK {MY_CATALOG}.{MY_SCHEMA}.mask_name
    """)
    print("✅ Gold層の列レベルマスキング適用完了（gd_users: email, phone_number, name）")

except Exception as e:
    print(f"⚠️ 列レベルマスキングの適用中にエラーが発生しました: {str(e)}")
    print("ヒント: カラム名が正しいか、またはDBRのバージョンが列レベルマスキングをサポートしているか確認してください。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver層 コメント設定・PK/FK設定

# COMMAND ----------

# DBTITLE 1,sl_users コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.sl_users'
table_comment = """
テーブル名：`sl_users / 会員マスタ（Silver）`
説明：オンラインスーパー「ブリックスマート」の会員マスタ（クレンジング済み）です。Bronze層のbz_usersに対して、型変換・NULL処理を実施しています。会員プロファイリング、購買行動分析、マーケティングキャンペーンのターゲティングに活用できます。
加工内容：
* birth_date, registration_date: DATE型に変換
* email_permission_flg: BOOLEAN型に変換
* last_login_at: TIMESTAMP型に変換
* pref: NULL/空文字は「不明」に変換
補足：
* behavior_cluster（行動クラスター）はGold層で購買履歴から算出されます。Silver層には含まれません。
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "user_id": "会員ID",
    "name": "氏名",
    "gender": "性別（M=男性, F=女性）",
    "birth_date": "生年月日",
    "pref": "都道府県（NULL/空文字は「不明」に変換）",
    "phone_number": "電話番号",
    "email": "メールアドレス",
    "email_permission_flg": "メール配信許諾フラグ（true=許諾, false=拒否）",
    "registration_date": "会員登録日",
    "status": "会員ステータス（active=有効, inactive=退会・停止）",
    "last_login_at": "最終ログイン日時"
}
for column, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,sl_users 主キー設定
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.sl_users'
PK_CONSTRAINT_NAME = 'pk_sl_users'

columns_to_set_not_null = ['user_id']
for column in columns_to_set_not_null:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} SET NOT NULL;")

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (user_id);')
print(f"✅ {TABLE_PATH} の主キー設定完了")

# COMMAND ----------

# DBTITLE 1,sl_items コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.sl_items'
table_comment = """
テーブル名：`sl_items / 商品マスタ（Silver）`
説明：オンラインスーパー「ブリックスマート」の商品マスタ（クレンジング済み）です。Bronze層のbz_itemsに対して、型変換を実施しています。商品分析、カテゴリ別売上分析、商品属性別の購買傾向分析に活用できます。
加工内容：
* item_id, category_id, price: LONG型に変換
* expiration_date, arrival_date: DATE型に変換
* is_organic, is_discount_target, is_ready_to_eat, is_premium: BOOLEAN型に変換
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "item_id": "商品ID",
    "category_id": "カテゴリID",
    "item_name": "商品名",
    "category_name": "カテゴリ名",
    "price": "定価",
    "expiration_date": "賞味期限",
    "arrival_date": "入荷日",
    "is_organic": "オーガニック・無添加フラグ（true=オーガニック/無添加商品, false=通常商品）",
    "is_discount_target": "値引き対象フラグ（true=値引き対象, false=対象外）",
    "is_ready_to_eat": "調理不要フラグ（true=惣菜/冷凍食品/カット野菜等, false=要調理）",
    "is_premium": "高単価・こだわり商品フラグ（true=プレミアム商品, false=通常商品）"
}
for column, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,sl_items 主キー設定
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.sl_items'
PK_CONSTRAINT_NAME = 'pk_sl_items'

columns_to_set_not_null = ['item_id', 'category_id']
for column in columns_to_set_not_null:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} SET NOT NULL;")

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (item_id);')
print(f"✅ {TABLE_PATH} の主キー設定完了")

# COMMAND ----------

# DBTITLE 1,sl_stores コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.sl_stores'
table_comment = """
テーブル名：`sl_stores / 店舗マスタ（Silver）`
説明：オンラインスーパー「ブリックスマート」の店舗マスタ（クレンジング済み）です。Bronze層のbz_storesに対して、型変換を実施しています。店舗別売上分析、エリア別パフォーマンス比較に活用できます。
加工内容：
* store_id, manager_id: LONG型に変換
* opening_date: DATE型に変換
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "store_id": "店舗ID",
    "store_name": "店舗名",
    "store_area": "エリア（関東/関西/中部/九州等）",
    "address": "住所",
    "postal_code": "郵便番号",
    "prefecture": "都道府県",
    "city": "市区町村",
    "phone_number": "電話番号",
    "email": "メールアドレス",
    "business_hours": "営業時間",
    "closed_days": "定休日",
    "manager_id": "店長ID",
    "opening_date": "開店日",
    "status": "店舗ステータス（active=営業中, inactive=閉店）"
}
for column, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,sl_stores 主キー設定
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.sl_stores'
PK_CONSTRAINT_NAME = 'pk_sl_stores'

columns_to_set_not_null = ['store_id']
for column in columns_to_set_not_null:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} SET NOT NULL;")

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (store_id);')
print(f"✅ {TABLE_PATH} の主キー設定完了")

# COMMAND ----------

# DBTITLE 1,sl_orders コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.sl_orders'
table_comment = """
テーブル名：`sl_orders / 注文ヘッダ（Silver）`
説明：オンラインスーパー「ブリックスマート」の注文ヘッダ（クレンジング済み）です。Bronze層のbz_ordersに対して、型変換と店舗情報の非正規化を実施しています。売上分析、顧客別購買頻度分析、店舗別・エリア別売上分析に活用できます。
加工内容：
* order_id, store_id, total_amount: LONG型に変換
* total_quantity: INT型に変換
* order_datetime: TIMESTAMP型に変換
* store_name, store_area: sl_storesから非正規化して結合
補足：
* 会計年度は4月1日から翌年3月31日まで
* 四半期の定義: Q1(4-6月), Q2(7-9月), Q3(10-12月), Q4(1-3月)
* 半期の定義: 上半期(4-9月), 下半期(10-3月)
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "order_id": "注文ID",
    "user_id": "会員ID",
    "store_id": "店舗ID",
    "store_name": "店舗名（非正規化）",
    "store_area": "エリア（非正規化、関東/関西/中部/九州等）",
    "order_datetime": "注文日時",
    "total_amount": "合計金額（税込）",
    "total_quantity": "合計数量",
    "order_status": "注文ステータス（completed=完了, cancelled=キャンセル, pending=保留）"
}
for column, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,sl_orders 主キー・外部キー設定
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.sl_orders'
PK_CONSTRAINT_NAME = 'pk_sl_orders'
FK_STORE_CONSTRAINT_NAME = 'fk_sl_orders_store'

columns_to_set_not_null = ['order_id', 'user_id', 'store_id']
for column in columns_to_set_not_null:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} SET NOT NULL;")

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (order_id);')

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_STORE_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {FK_STORE_CONSTRAINT_NAME} FOREIGN KEY (store_id) REFERENCES {MY_CATALOG}.{MY_SCHEMA}.sl_stores(store_id);')
print(f"✅ {TABLE_PATH} の主キー・外部キー設定完了")

# COMMAND ----------

# DBTITLE 1,sl_order_items コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.sl_order_items'
table_comment = """
テーブル名：`sl_order_items / 注文明細（Silver）`
説明：オンラインスーパー「ブリックスマート」の注文明細（クレンジング・重複排除済み）です。Bronze層のbz_order_itemsに対して、型変換、重複排除、商品・店舗情報の非正規化を実施しています。商品別売上分析、バスケット分析、カテゴリ別売上構成比分析に活用できます。
加工内容：
* order_item_id, order_id, store_id, item_id, category_id, unit_price, subtotal: LONG型に変換
* quantity: INT型に変換
* cancel_flg: BOOLEAN型に変換
* order_datetime: TIMESTAMP型に変換
* 重複排除: order_item_idでパーティション、_ingested_at降順で最新のみ保持
* item_name, category_name, is_organic等: sl_itemsから非正規化
* store_name, store_area: sl_storesから非正規化
補足：
* 会計年度は4月1日から翌年3月31日まで
* 四半期の定義: Q1(4-6月), Q2(7-9月), Q3(10-12月), Q4(1-3月)
* 半期の定義: 上半期(4-9月), 下半期(10-3月)
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "order_item_id": "注文明細ID",
    "user_id": "会員ID",
    "order_id": "注文ID",
    "store_id": "店舗ID",
    "store_name": "店舗名（非正規化）",
    "store_area": "エリア（非正規化、関東/関西/中部/九州等）",
    "item_id": "商品ID",
    "category_id": "カテゴリID",
    "item_name": "商品名（非正規化）",
    "category_name": "カテゴリ名（非正規化）",
    "is_organic": "オーガニック・無添加フラグ（非正規化、true=オーガニック/無添加, false=通常）",
    "is_discount_target": "値引き対象フラグ（非正規化、true=値引き対象, false=対象外）",
    "is_ready_to_eat": "調理不要フラグ（非正規化、true=惣菜/冷凍食品等, false=要調理）",
    "is_premium": "高単価・こだわり商品フラグ（非正規化、true=プレミアム, false=通常）",
    "quantity": "数量",
    "unit_price": "単価",
    "subtotal": "小計（unit_price × quantity）",
    "cancel_flg": "キャンセルフラグ（true=キャンセル済, false=有効）",
    "order_datetime": "注文日時"
}
for column, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,sl_order_items 主キー・外部キー設定
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.sl_order_items'
PK_CONSTRAINT_NAME = 'pk_sl_order_items'
FK_ORDER_CONSTRAINT_NAME = 'fk_sl_order_items_order'
FK_ITEM_CONSTRAINT_NAME = 'fk_sl_order_items_item'
FK_STORE_CONSTRAINT_NAME = 'fk_sl_order_items_store'

columns_to_set_not_null = ['order_item_id', 'order_id', 'item_id', 'store_id']
for column in columns_to_set_not_null:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} SET NOT NULL;")

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (order_item_id);')

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_ORDER_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {FK_ORDER_CONSTRAINT_NAME} FOREIGN KEY (order_id) REFERENCES {MY_CATALOG}.{MY_SCHEMA}.sl_orders(order_id);')

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_ITEM_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {FK_ITEM_CONSTRAINT_NAME} FOREIGN KEY (item_id) REFERENCES {MY_CATALOG}.{MY_SCHEMA}.sl_items(item_id);')

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_STORE_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {FK_STORE_CONSTRAINT_NAME} FOREIGN KEY (store_id) REFERENCES {MY_CATALOG}.{MY_SCHEMA}.sl_stores(store_id);')
print(f"✅ {TABLE_PATH} の主キー・外部キー設定完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold層 コメント設定・PK/FK設定

# COMMAND ----------

# DBTITLE 1,gd_users コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.gd_users'
table_comment = """
テーブル名：`gd_users / 会員マスタ（Gold）`
説明：オンラインスーパー「ブリックスマート」の会員マスタ（分析用）です。Silver層のsl_usersにRFM分析指標とセグメント情報を付与しています。顧客セグメント分析、ロイヤルティ分析、マーケティング施策のターゲティングに活用できます。
RFMスコア閾値：
* r_score（Recency）: 7日以内=5, 14日以内=4, 30日以内=3, 60日以内=2, それ以外=1
* f_score（Frequency）: 10回以上=5, 5回以上=4, 3回以上=3, 2回以上=2, それ以外=1
* m_score（Monetary）: 5万円以上=5, 2万円以上=4, 1万円以上=3, 5千円以上=2, それ以外=1
セグメント分類ロジック：
* ロイヤル顧客: R>=4 かつ F>=4 かつ M>=4
* 常連顧客: R>=4 かつ F>=3
* 新規顧客: R>=4 かつ F<=2
* 離脱リスク: R<=2 かつ F>=3
* 休眠顧客: R<=2 かつ F<=2
* 一般顧客: 上記以外
行動クラスター算出ロジック（購買履歴から算出）：
* 1（週末まとめ買い族）: 土日購入率>=80% かつ 平均バスケットサイズ>=10点
* 2（夜型値引きハンター）: 19時以降購入率>=70% かつ 値引き商品購入率>=50%
* 3（健康・こだわり層）: オーガニック/プレミアム商品購入率>=60%
* 4（カテゴリジャンキー）: 特定カテゴリ（酒/菓子/スイーツ）購入率>=70%
* 5（平日夕方の時短族）: 平日16-18時購入率>=70% かつ 即食系購入率>=60%
* null（ランダム）: 上記いずれにも該当しない
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "user_id": "会員ID",
    "name": "氏名",
    "gender": "性別（M=男性, F=女性）",
    "birth_date": "生年月日",
    "pref": "都道府県",
    "phone_number": "電話番号",
    "email": "メールアドレス",
    "email_permission_flg": "メール配信許諾フラグ（true=許諾, false=拒否）",
    "registration_date": "会員登録日",
    "status": "会員ステータス（active=有効, inactive=退会・停止）",
    "last_login_at": "最終ログイン日時",
    "behavior_cluster": "行動クラスター（1=週末まとめ買い族, 2=夜型値引きハンター, 3=健康・こだわり層, 4=カテゴリジャンキー, 5=平日夕方の時短族, null=ランダム）",
    "behavior_cluster_name": "行動クラスター名（週末まとめ買い族/夜型値引きハンター/健康・こだわり層/カテゴリジャンキー/平日夕方の時短族/ランダム）",
    "last_purchase_date": "最終購買日",
    "recency": "最終購買からの経過日数（小さいほど最近購入）",
    "frequency": "購買回数（累計注文回数）",
    "monetary": "累計購買金額（顧客単価ではなく累計金額）",
    "r_score": "Recencyスコア（5=7日以内, 4=14日以内, 3=30日以内, 2=60日以内, 1=60日超）",
    "f_score": "Frequencyスコア（5=10回以上, 4=5-9回, 3=3-4回, 2=2回, 1=1回）",
    "m_score": "Monetaryスコア（5=5万円以上, 4=2-5万円, 3=1-2万円, 2=5千-1万円, 1=5千円未満）",
    "rfm_segment": "RFMセグメント（ロイヤル顧客/常連顧客/新規顧客/離脱リスク/休眠顧客/一般顧客）",
    "customer_type": "顧客区分（新規=直近1ヶ月内に初回購入, 既存=それ以外）"
}
for column, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,gd_users 主キー・外部キー設定
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.gd_users'
PK_CONSTRAINT_NAME = 'pk_gd_users'
FK_USER_CONSTRAINT_NAME = 'fk_gd_users_sl_user'

columns_to_set_not_null = ['user_id']
for column in columns_to_set_not_null:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} SET NOT NULL;")

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (user_id);')

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_USER_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {FK_USER_CONSTRAINT_NAME} FOREIGN KEY (user_id) REFERENCES {MY_CATALOG}.{MY_SCHEMA}.sl_users(user_id);')
print(f"✅ {TABLE_PATH} の主キー・外部キー設定完了")

# COMMAND ----------

# DBTITLE 1,gd_orders コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.gd_orders'
table_comment = """
テーブル名：`gd_orders / 注文ヘッダ（Gold）`
説明：オンラインスーパー「ブリックスマート」の注文ヘッダ（分析用）です。Silver層のsl_ordersに日時分析カラムと顧客セグメント情報を付与しています。時間帯別売上分析、曜日別売上分析、セグメント別購買分析に活用できます。
付与情報：
* 日時分析: order_date（注文日）, order_hour（注文時間0-23）, order_dayofweek（曜日1=日〜7=土）, is_weekend（週末フラグ）
* RFMスコア: r_score, f_score, m_score, rfm_segment
* behavior_cluster, behavior_cluster_name: 行動クラスター情報（購買履歴から算出）
* is_first_order: 初回注文フラグ（顧客の最初の注文かどうか）
* customer_type: 新規/既存（注文単位、初回注文=新規）
補足：
* 会計年度は4月1日から翌年3月31日まで
* 四半期の定義: Q1(4-6月), Q2(7-9月), Q3(10-12月), Q4(1-3月)
* 半期の定義: 上半期(4-9月), 下半期(10-3月)
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "order_id": "注文ID",
    "user_id": "会員ID",
    "store_id": "店舗ID",
    "store_name": "店舗名",
    "store_area": "エリア（関東/関西/中部/九州等）",
    "order_datetime": "注文日時",
    "order_date": "注文日（DATE型）",
    "order_hour": "注文時間（0-23の整数、0=0時台, 23=23時台）",
    "order_dayofweek": "曜日（1=日曜, 2=月曜, 3=火曜, 4=水曜, 5=木曜, 6=金曜, 7=土曜）",
    "is_weekend": "週末フラグ（true=土日, false=平日）",
    "total_amount": "合計金額（税込）",
    "total_quantity": "合計数量",
    "order_status": "注文ステータス（completed=完了, cancelled=キャンセル, pending=保留）",
    "behavior_cluster": "行動クラスター（1=週末まとめ買い族, 2=夜型値引きハンター, 3=健康・こだわり層, 4=カテゴリジャンキー, 5=平日夕方の時短族, null=ランダム）",
    "behavior_cluster_name": "行動クラスター名（週末まとめ買い族/夜型値引きハンター/健康・こだわり層/カテゴリジャンキー/平日夕方の時短族/ランダム）",
    "r_score": "Recencyスコア（5=7日以内, 4=14日以内, 3=30日以内, 2=60日以内, 1=60日超）",
    "f_score": "Frequencyスコア（5=10回以上, 4=5-9回, 3=3-4回, 2=2回, 1=1回）",
    "m_score": "Monetaryスコア（5=5万円以上, 4=2-5万円, 3=1-2万円, 2=5千-1万円, 1=5千円未満）",
    "rfm_segment": "RFMセグメント（ロイヤル顧客/常連顧客/新規顧客/離脱リスク/休眠顧客/一般顧客）",
    "is_first_order": "初回注文フラグ（true=その顧客の最初の注文, false=2回目以降）",
    "customer_type": "顧客区分（注文単位、新規=初回注文, 既存=2回目以降の注文）"
}
for column, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,gd_orders 主キー・外部キー設定
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.gd_orders'
PK_CONSTRAINT_NAME = 'pk_gd_orders'
FK_ORDER_CONSTRAINT_NAME = 'fk_gd_orders_sl_order'

columns_to_set_not_null = ['order_id', 'user_id']
for column in columns_to_set_not_null:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} SET NOT NULL;")

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (order_id);')

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_ORDER_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {FK_ORDER_CONSTRAINT_NAME} FOREIGN KEY (order_id) REFERENCES {MY_CATALOG}.{MY_SCHEMA}.sl_orders(order_id);')
print(f"✅ {TABLE_PATH} の主キー・外部キー設定完了")

# COMMAND ----------

# DBTITLE 1,gd_order_items コメント設定
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.gd_order_items'
table_comment = """
テーブル名：`gd_order_items / 注文明細（Gold）`
説明：オンラインスーパー「ブリックスマート」の注文明細（分析用）です。Silver層のsl_order_itemsに日時分析カラムと顧客セグメント情報を付与しています。商品×セグメント分析、時間帯別商品売上分析、カテゴリ×顧客属性分析に活用できます。
付与情報：
* 日時分析: order_date（注文日）, order_hour（注文時間0-23）, order_dayofweek（曜日1=日〜7=土）, is_weekend（週末フラグ）
* RFMスコア: r_score, f_score, m_score, rfm_segment
* behavior_cluster, behavior_cluster_name: 行動クラスター情報（購買履歴から算出）
* is_first_order: 初回注文フラグ
* customer_type: 新規/既存（注文単位）
補足：
* 会計年度は4月1日から翌年3月31日まで
* 四半期の定義: Q1(4-6月), Q2(7-9月), Q3(10-12月), Q4(1-3月)
* 半期の定義: 上半期(4-9月), 下半期(10-3月)
"""
spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

column_comments = {
    "order_item_id": "注文明細ID",
    "order_id": "注文ID",
    "user_id": "会員ID",
    "store_id": "店舗ID",
    "store_name": "店舗名",
    "store_area": "エリア（関東/関西/中部/九州等）",
    "item_id": "商品ID",
    "item_name": "商品名",
    "category_id": "カテゴリID",
    "category_name": "カテゴリ名",
    "is_organic": "オーガニック・無添加フラグ（true=オーガニック/無添加商品, false=通常商品）",
    "is_discount_target": "値引き対象フラグ（true=値引き対象, false=対象外）",
    "is_ready_to_eat": "調理不要フラグ（true=惣菜/冷凍食品/カット野菜等, false=要調理）",
    "is_premium": "高単価・こだわり商品フラグ（true=プレミアム商品, false=通常商品）",
    "quantity": "数量",
    "unit_price": "単価",
    "subtotal": "小計（unit_price × quantity）",
    "cancel_flg": "キャンセルフラグ（true=キャンセル済, false=有効）",
    "order_datetime": "注文日時",
    "order_date": "注文日（DATE型）",
    "order_hour": "注文時間（0-23の整数、0=0時台, 23=23時台）",
    "order_dayofweek": "曜日（1=日曜, 2=月曜, 3=火曜, 4=水曜, 5=木曜, 6=金曜, 7=土曜）",
    "is_weekend": "週末フラグ（true=土日, false=平日）",
    "behavior_cluster": "行動クラスター（1=週末まとめ買い族, 2=夜型値引きハンター, 3=健康・こだわり層, 4=カテゴリジャンキー, 5=平日夕方の時短族, null=ランダム）",
    "behavior_cluster_name": "行動クラスター名（週末まとめ買い族/夜型値引きハンター/健康・こだわり層/カテゴリジャンキー/平日夕方の時短族/ランダム）",
    "r_score": "Recencyスコア（5=7日以内, 4=14日以内, 3=30日以内, 2=60日以内, 1=60日超）",
    "f_score": "Frequencyスコア（5=10回以上, 4=5-9回, 3=3-4回, 2=2回, 1=1回）",
    "m_score": "Monetaryスコア（5=5万円以上, 4=2-5万円, 3=1-2万円, 2=5千-1万円, 1=5千円未満）",
    "rfm_segment": "RFMセグメント（ロイヤル顧客/常連顧客/新規顧客/離脱リスク/休眠顧客/一般顧客）",
    "is_first_order": "初回注文フラグ（true=その顧客の最初の注文, false=2回目以降）",
    "customer_type": "顧客区分（注文単位、新規=初回注文, 既存=2回目以降の注文）"
}
for column, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
print(f"✅ {table_name} のコメント設定完了")

# COMMAND ----------

# DBTITLE 1,gd_order_items 主キー・外部キー設定
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.gd_order_items'
PK_CONSTRAINT_NAME = 'pk_gd_order_items'
FK_ORDER_ITEM_CONSTRAINT_NAME = 'fk_gd_order_items_sl_order_item'

columns_to_set_not_null = ['order_item_id', 'order_id']
for column in columns_to_set_not_null:
    spark.sql(f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} SET NOT NULL;")

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (order_item_id);')

spark.sql(f'ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {FK_ORDER_ITEM_CONSTRAINT_NAME};')
spark.sql(f'ALTER TABLE {TABLE_PATH} ADD CONSTRAINT {FK_ORDER_ITEM_CONSTRAINT_NAME} FOREIGN KEY (order_item_id) REFERENCES {MY_CATALOG}.{MY_SCHEMA}.sl_order_items(order_item_id);')
print(f"✅ {TABLE_PATH} の主キー・外部キー設定完了")

# COMMAND ----------

# DBTITLE 1,認定済みタグの追加
# Silver層・Gold層のテーブルに認定済みタグを付与
certified_tag = 'system.Certified'

try:
    # Silver層テーブル
    silver_tables = ['sl_users', 'sl_items', 'sl_stores', 'sl_orders', 'sl_order_items']
    for table in silver_tables:
        spark.sql(f"ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.{table} SET TAGS ('{certified_tag}')")

    # Gold層テーブル
    gold_tables = ['gd_users', 'gd_orders', 'gd_order_items']
    for table in gold_tables:
        spark.sql(f"ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.{table} SET TAGS ('{certified_tag}')")

    print(f"✅ 認定済みタグ '{certified_tag}' の追加完了")
    print(f"   Silver層: {', '.join(silver_tables)}")
    print(f"   Gold層: {', '.join(gold_tables)}")

except Exception as e:
    print(f"⚠️ 認定済みタグの追加中にエラーが発生しました: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 設定完了

# COMMAND ----------

# DBTITLE 1,設定確認
print("=== ガバナンス設定完了 ===\n")
print("Bronze層:")
print("  - bz_users: コメント設定 + 列レベルマスキング(email, phone_number, name)")
print("  - bz_items: コメント設定")
print("  - bz_stores: コメント設定")
print("  - bz_orders: コメント設定")
print("  - bz_order_items: コメント設定")
print("\nSilver層（認定済みタグ付与）:")
print("  - sl_users: コメント設定 + PK(user_id)")
print("  - sl_items: コメント設定 + PK(item_id)")
print("  - sl_stores: コメント設定 + PK(store_id)")
print("  - sl_orders: コメント設定 + PK(order_id) + FK(store_id)")
print("  - sl_order_items: コメント設定 + PK(order_item_id) + FK(order_id, item_id, store_id)")
print("\nGold層（認定済みタグ付与）:")
print("  - gd_users: コメント設定 + PK(user_id) + FK(user_id→sl_users)")
print("  - gd_orders: コメント設定 + PK(order_id) + FK(order_id→sl_orders)")
print("  - gd_order_items: コメント設定 + PK(order_item_id) + FK(order_item_id→sl_order_items)")
