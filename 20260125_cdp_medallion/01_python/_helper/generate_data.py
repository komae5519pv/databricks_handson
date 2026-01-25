# Databricks notebook source
# MAGIC %md
# MAGIC # 01. サンプルデータ生成
# MAGIC
# MAGIC このノートブックでは、リテール企業のPOSデータを模したサンプルデータを生成し、CSVファイルとしてVolumeに出力します。
# MAGIC
# MAGIC ## 生成するデータ
# MAGIC | ファイル | 説明 |
# MAGIC |---------|------|
# MAGIC | users | 会員マスタ |
# MAGIC | items | 商品マスタ（属性フラグ付き） |
# MAGIC | stores | 店舗マスタ |
# MAGIC | orders | POS注文（会計単位） |
# MAGIC | order_items | POS注文明細（アイテム単位、5%重複含む） |
# MAGIC
# MAGIC ## 顧客クラスター設計（確率分布）
# MAGIC | # | クラスター名 | 確率 | データ上の特徴 |
# MAGIC |---|-------------|------|---------------|
# MAGIC | 1 | 週末まとめ買い族 | 10% | 購入は土日に集中、バスケットサイズが極端に多い |
# MAGIC | 2 | 夜型値引きハンター | 5% | 購入時間は19時以降のみ、値引き商品が高割合 |
# MAGIC | 3 | 健康・こだわり層 | 5% | オーガニック/高単価商品の購入比率が60%以上 |
# MAGIC | 4 | カテゴリジャンキー | 10% | 酒・つまみ or スイーツ・アイスなど特定カテゴリのみ |
# MAGIC | 5 | 平日夕方の時短族 | 10% | 平日16〜18時、惣菜/冷凍食品/カット野菜が中心 |
# MAGIC | - | ランダム（ノイズ） | 60% | 特定の傾向なし |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# %run ../00_config

# COMMAND ----------

# DBTITLE 1,Volumeリセット
# Volume削除
spark.sql(f'DROP VOLUME if exists {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}')

# Volume作成（CSVファイル格納用）
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")

# COMMAND ----------

# DBTITLE 1,ライブラリのインポート
from pyspark.sql.functions import (
    col, when, lit, expr, current_timestamp, current_date, rand,
    concat, lpad, date_sub, monotonically_increasing_id
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    BooleanType, DateType, TimestampType
)
from datetime import datetime, timedelta, date
import random
import numpy as np

# ===========================================
# 基準日設定
# ===========================================
# サンプルデータは「前日」を最終日として過去5年分を生成
# ※実行日ではなく前日を基準にすることで、時間帯による影響を排除
BASE_DATE = (datetime.now() - timedelta(days=1)).date()  # 前日
DATA_YEARS = 5  # データ期間（年）
DATA_DAYS = DATA_YEARS * 365  # データ期間（日数）= 1825日

print(f"基準日（最終購買日の最大値）: {BASE_DATE}")
print(f"データ期間: {DATA_YEARS}年間（{DATA_DAYS}日）")

# COMMAND ----------

print('サンプルデータ生成を開始します！')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 会員マスタ（users）

# COMMAND ----------

# DBTITLE 1,定数定義
# 名前リスト
FIRST_NAMES_MALE = ['次郎', '健太', '翔太', '直樹', '大輔', '拓也', '悠斗', '誠', '亮', '和也', '徹', '一郎', '慎太郎', '裕樹', '哲也']
FIRST_NAMES_FEMALE = ['花子', '美咲', '葵', 'さおり', '結衣', '菜々子', '美優', '玲奈', '愛子', '由美子', '恵美', '真由美', '千春', '陽子', '麻衣']
LAST_NAMES = ['田中', '佐藤', '鈴木', '高橋', '渡辺', '伊藤', '山田', '中村', '小林', '加藤', '吉田', '山口', '松本', '井上', '木村']

# 県リスト
PREFECTURES = [
    "東京都", "神奈川県", "千葉県", "埼玉県", "大阪府", "兵庫県", "愛知県", "北海道",
    "福岡県", "宮城県", "広島県", "京都府", "静岡県", "新潟県", "長野県"
]

# クラスター定義（確率分布）
# 1: 週末まとめ買い族, 2: 夜型値引きハンター, 3: 健康・こだわり層,
# 4: カテゴリジャンキー, 5: 平日夕方の時短族, None: ランダム
CLUSTER_PROBABILITIES = [0.10, 0.05, 0.05, 0.10, 0.10, 0.60]  # 合計1.0
CLUSTER_IDS = ['1', '2', '3', '4', '5', None]

CLUSTER_NAMES = {
    '1': '週末まとめ買い族',
    '2': '夜型値引きハンター',
    '3': '健康・こだわり層',
    '4': 'カテゴリジャンキー',
    '5': '平日夕方の時短族',
    None: 'ランダム'
}

# COMMAND ----------

# DBTITLE 1,会員マスタ生成
def create_single_user(user_id_num, registration_date):
    """単一ユーザーを生成（新規顧客追加用）"""
    gender = 'M' if random.random() < 0.35 else 'F'
    if gender == 'M':
        name = random.choice(LAST_NAMES) + ' ' + random.choice(FIRST_NAMES_MALE)
    else:
        name = random.choice(LAST_NAMES) + ' ' + random.choice(FIRST_NAMES_FEMALE)

    pref = random.choice(PREFECTURES) if random.random() > 0.15 else None
    status = 'active' if random.random() < 0.75 else 'inactive'
    last_login = datetime.combine(BASE_DATE, datetime.min.time()) - timedelta(days=random.randint(0, 90)) if random.random() > 0.30 else None
    cluster = np.random.choice(CLUSTER_IDS, p=CLUSTER_PROBABILITIES)

    return (
        f'U{str(user_id_num).zfill(8)}', name, gender,
        (BASE_DATE - timedelta(days=random.randint(3650, 21900))),  # 生年月日
        pref,
        f'0{random.randint(10,99)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}',
        f'user{user_id_num}@example.com',
        random.random() < 0.35,
        registration_date, status, last_login,
        cluster
    )


def generate_users(num_users):
    """会員マスタを生成（男性35%、女性65%、クラスター割り当て付き）

    注意: この関数は初期シードユーザーを生成します。
    実際の会員マスタは、POS注文データ生成後に新規顧客を含めて再生成されます。
    """
    male_count = int(num_users * 0.35)
    female_count = num_users - male_count

    users_data = []
    user_id = 1

    # クラスター割り当て（確率分布に基づく）
    def assign_cluster():
        return np.random.choice(CLUSTER_IDS, p=CLUSTER_PROBABILITIES)

    # 男性会員（初期ユーザーは5年前から存在）
    for _ in range(male_count):
        name = random.choice(LAST_NAMES) + ' ' + random.choice(FIRST_NAMES_MALE)
        pref = random.choice(PREFECTURES) if random.random() > 0.15 else None  # 15%NULL
        # 初期ユーザーの登録日: データ開始日より前（5年前～5年半前）
        registration_date = BASE_DATE - timedelta(days=DATA_DAYS + random.randint(0, 180))
        status = 'active' if random.random() < 0.75 else 'inactive'
        # 最終ログイン: BASE_DATEから過去90日以内
        last_login = datetime.combine(BASE_DATE, datetime.min.time()) - timedelta(days=random.randint(0, 90)) if random.random() > 0.30 else None
        cluster = assign_cluster()

        users_data.append((
            f'U{str(user_id).zfill(8)}', name, 'M',
            # 生年月日: BASE_DATEから10-60歳の範囲
            (BASE_DATE - timedelta(days=random.randint(3650, 21900))),
            pref,
            f'0{random.randint(10,99)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}',
            f'user{user_id}@example.com',
            random.random() < 0.35,  # メール許諾35%
            registration_date, status, last_login,
            cluster
        ))
        user_id += 1

    # 女性会員（初期ユーザーは5年前から存在）
    for _ in range(female_count):
        name = random.choice(LAST_NAMES) + ' ' + random.choice(FIRST_NAMES_FEMALE)
        pref = random.choice(PREFECTURES) if random.random() > 0.15 else None
        # 初期ユーザーの登録日: データ開始日より前（5年前～5年半前）
        registration_date = BASE_DATE - timedelta(days=DATA_DAYS + random.randint(0, 180))
        status = 'active' if random.random() < 0.75 else 'inactive'
        # 最終ログイン: BASE_DATEから過去90日以内
        last_login = datetime.combine(BASE_DATE, datetime.min.time()) - timedelta(days=random.randint(0, 90)) if random.random() > 0.30 else None
        cluster = assign_cluster()

        users_data.append((
            f'U{str(user_id).zfill(8)}', name, 'F',
            # 生年月日: BASE_DATEから10-60歳の範囲
            (BASE_DATE - timedelta(days=random.randint(3650, 21900))),
            pref,
            f'0{random.randint(10,99)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}',
            f'user{user_id}@example.com',
            random.random() < 0.35,
            registration_date, status, last_login,
            cluster
        ))
        user_id += 1

    return users_data

# スキーマ定義
schema_users = StructType([
    StructField("user_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("birth_date", DateType(), True),
    StructField("pref", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email", StringType(), False),
    StructField("email_permission_flg", BooleanType(), False),
    StructField("registration_date", DateType(), False),
    StructField("status", StringType(), False),
    StructField("last_login_at", TimestampType(), True),
    StructField("behavior_cluster", StringType(), True)
])

# データ生成
users_data = generate_users(NUM_USERS)
random.shuffle(users_data)  # シャッフル
users_df = spark.createDataFrame(users_data, schema=schema_users)

print(f"会員マスタ: {users_df.count()}件")
print(f"  男性: {users_df.filter(col('gender') == 'M').count()}件")
print(f"  女性: {users_df.filter(col('gender') == 'F').count()}件")
display(users_df.limit(10))

# COMMAND ----------

# DBTITLE 1,クラスター分布確認（内部シミュレーション用、CSVには出力しない）
# ※ behavior_clusterは購買パターン生成の制御に使用、Gold層で購買履歴から再計算
display(
    users_df
    .groupBy("behavior_cluster")
    .count()
    .withColumn("ratio", col("count") / users_df.count() * 100)
    .orderBy("behavior_cluster")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 商品マスタ（items）

# COMMAND ----------

# DBTITLE 1,商品カテゴリ定義（属性フラグ付き）
# カテゴリ別商品と属性
# is_organic: オーガニック・無添加
# is_discount_target: 値引き対象になりやすい
# is_ready_to_eat: 調理不要（惣菜/冷凍/カット野菜）
# is_premium: 高単価・こだわり商品
CATEGORIES = {
    100: {
        "name": "野菜・果物",
        "items": [
            {"name": "トマト", "price_range": (150, 400), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 0, "premium_rate": 0.2},
            {"name": "有機きゅうり", "price_range": (200, 350), "organic_rate": 1.0, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0.5},
            {"name": "ほうれん草", "price_range": (150, 300), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 0, "premium_rate": 0.1},
            {"name": "キャベツ", "price_range": (100, 250), "organic_rate": 0.2, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0},
            {"name": "大根", "price_range": (100, 200), "organic_rate": 0.2, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0},
            {"name": "カット野菜ミックス", "price_range": (150, 300), "organic_rate": 0, "discount_rate": 0.3, "ready_rate": 1.0, "premium_rate": 0},
            {"name": "有機りんご", "price_range": (300, 600), "organic_rate": 1.0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.8},
            {"name": "みかん", "price_range": (200, 500), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 0, "premium_rate": 0.2},
            {"name": "バナナ", "price_range": (100, 250), "organic_rate": 0.3, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0},
            {"name": "玉ねぎ", "price_range": (80, 200), "organic_rate": 0.2, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0},
        ]
    },
    200: {
        "name": "肉類",
        "items": [
            {"name": "国産鶏もも肉", "price_range": (400, 800), "organic_rate": 0.4, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0.5},
            {"name": "豚バラ肉", "price_range": (300, 700), "organic_rate": 0.2, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0.3},
            {"name": "黒毛和牛肩ロース", "price_range": (1500, 3000), "organic_rate": 0.5, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 1.0},
            {"name": "合挽き肉", "price_range": (300, 600), "organic_rate": 0.2, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0.2},
            {"name": "銘柄豚ロース", "price_range": (600, 1200), "organic_rate": 0.4, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0.8},
            {"name": "鶏むね肉", "price_range": (200, 500), "organic_rate": 0.3, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0.2},
            {"name": "牛ひき肉", "price_range": (400, 900), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 0, "premium_rate": 0.4},
            {"name": "ササミ", "price_range": (250, 500), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 0, "premium_rate": 0.3},
        ]
    },
    300: {
        "name": "魚介類",
        "items": [
            {"name": "天然鮭切り身", "price_range": (400, 800), "organic_rate": 0.5, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0.6},
            {"name": "本マグロ刺身", "price_range": (800, 2000), "organic_rate": 0.6, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 1.0},
            {"name": "アサリ", "price_range": (300, 600), "organic_rate": 0.4, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0.3},
            {"name": "エビ", "price_range": (500, 1200), "organic_rate": 0.4, "discount_rate": 0.4, "ready_rate": 0, "premium_rate": 0.5},
            {"name": "イカ", "price_range": (400, 800), "organic_rate": 0.3, "discount_rate": 0.5, "ready_rate": 0, "premium_rate": 0.3},
            {"name": "サバ", "price_range": (200, 500), "organic_rate": 0.3, "discount_rate": 0.6, "ready_rate": 0, "premium_rate": 0.2},
            {"name": "北海道産ホタテ", "price_range": (800, 1500), "organic_rate": 0.5, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 1.0},
            {"name": "タコ", "price_range": (400, 900), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 0, "premium_rate": 0.4},
        ]
    },
    400: {
        "name": "乳製品",
        "items": [
            {"name": "有機牛乳", "price_range": (250, 400), "organic_rate": 1.0, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0.7},
            {"name": "プレミアムチーズ", "price_range": (400, 800), "organic_rate": 0.5, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 1.0},
            {"name": "バター", "price_range": (300, 600), "organic_rate": 0.4, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0.5},
            {"name": "無添加ヨーグルト", "price_range": (200, 400), "organic_rate": 1.0, "discount_rate": 0.4, "ready_rate": 0, "premium_rate": 0.6},
            {"name": "生クリーム", "price_range": (250, 500), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 0, "premium_rate": 0.4},
        ]
    },
    500: {
        "name": "パン・ベーカリー",
        "items": [
            {"name": "天然酵母食パン", "price_range": (300, 500), "organic_rate": 1.0, "discount_rate": 0.4, "ready_rate": 1.0, "premium_rate": 0.6},
            {"name": "クロワッサン", "price_range": (150, 300), "organic_rate": 0.3, "discount_rate": 0.5, "ready_rate": 1.0, "premium_rate": 0.3},
            {"name": "ロールパン", "price_range": (100, 250), "organic_rate": 0.2, "discount_rate": 0.5, "ready_rate": 1.0, "premium_rate": 0.1},
            {"name": "フランスパン", "price_range": (200, 400), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 1.0, "premium_rate": 0.3},
            {"name": "全粒粉ベーグル", "price_range": (200, 350), "organic_rate": 0.8, "discount_rate": 0.3, "ready_rate": 1.0, "premium_rate": 0.5},
        ]
    },
    600: {
        "name": "お菓子・スナック",
        "items": [
            {"name": "ポテトチップス", "price_range": (150, 300), "organic_rate": 0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0},
            {"name": "クッキー", "price_range": (200, 400), "organic_rate": 0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.2},
            {"name": "チョコレート", "price_range": (150, 500), "organic_rate": 0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.3},
            {"name": "キャンディー", "price_range": (100, 250), "organic_rate": 0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0},
            {"name": "グミ", "price_range": (100, 250), "organic_rate": 0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0},
        ]
    },
    700: {
        "name": "冷凍食品",
        "items": [
            {"name": "冷凍餃子", "price_range": (300, 500), "organic_rate": 0.2, "discount_rate": 0.4, "ready_rate": 1.0, "premium_rate": 0.2},
            {"name": "冷凍ピザ", "price_range": (400, 700), "organic_rate": 0.2, "discount_rate": 0.3, "ready_rate": 1.0, "premium_rate": 0.3},
            {"name": "冷凍野菜ミックス", "price_range": (200, 400), "organic_rate": 0.4, "discount_rate": 0.4, "ready_rate": 1.0, "premium_rate": 0.2},
            {"name": "プレミアム冷凍チャーハン", "price_range": (350, 600), "organic_rate": 0.3, "discount_rate": 0.3, "ready_rate": 1.0, "premium_rate": 0.5},
            {"name": "冷凍うどん", "price_range": (200, 400), "organic_rate": 0.2, "discount_rate": 0.4, "ready_rate": 1.0, "premium_rate": 0.2},
        ]
    },
    800: {
        "name": "飲料",
        "items": [
            {"name": "ミネラルウォーター", "price_range": (80, 150), "organic_rate": 0.3, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.2},
            {"name": "有機コーヒー", "price_range": (300, 600), "organic_rate": 1.0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.8},
            {"name": "紅茶", "price_range": (200, 400), "organic_rate": 0.4, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.4},
            {"name": "オレンジジュース", "price_range": (150, 300), "organic_rate": 0.3, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0.3},
            {"name": "炭酸飲料", "price_range": (100, 200), "organic_rate": 0, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0},
            {"name": "有機緑茶", "price_range": (250, 500), "organic_rate": 1.0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.7},
            {"name": "スポーツドリンク", "price_range": (120, 250), "organic_rate": 0, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0},
        ]
    },
    900: {
        "name": "調味料",
        "items": [
            {"name": "有機醤油", "price_range": (400, 800), "organic_rate": 1.0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.8},
            {"name": "無添加味噌", "price_range": (350, 700), "organic_rate": 1.0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.7},
            {"name": "マヨネーズ", "price_range": (200, 400), "organic_rate": 0.2, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0.2},
            {"name": "天然塩", "price_range": (300, 600), "organic_rate": 0.8, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.7},
            {"name": "胡椒", "price_range": (200, 500), "organic_rate": 0.3, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.3},
            {"name": "てんさい糖", "price_range": (300, 500), "organic_rate": 0.9, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.6},
            {"name": "有機酢", "price_range": (350, 600), "organic_rate": 1.0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.7},
        ]
    },
    1000: {
        "name": "惣菜",
        "items": [
            {"name": "唐揚げ弁当", "price_range": (400, 700), "organic_rate": 0, "discount_rate": 0.7, "ready_rate": 1.0, "premium_rate": 0},
            {"name": "焼き魚弁当", "price_range": (500, 800), "organic_rate": 0, "discount_rate": 0.7, "ready_rate": 1.0, "premium_rate": 0.2},
            {"name": "コロッケ", "price_range": (100, 200), "organic_rate": 0, "discount_rate": 0.8, "ready_rate": 1.0, "premium_rate": 0},
            {"name": "ポテトサラダ", "price_range": (200, 400), "organic_rate": 0, "discount_rate": 0.7, "ready_rate": 1.0, "premium_rate": 0},
            {"name": "焼き鳥", "price_range": (150, 300), "organic_rate": 0, "discount_rate": 0.8, "ready_rate": 1.0, "premium_rate": 0},
            {"name": "おにぎりセット", "price_range": (300, 500), "organic_rate": 0, "discount_rate": 0.7, "ready_rate": 1.0, "premium_rate": 0},
            {"name": "天ぷら盛り合わせ", "price_range": (500, 900), "organic_rate": 0, "discount_rate": 0.6, "ready_rate": 1.0, "premium_rate": 0.3},
        ]
    },
    1100: {
        "name": "酒類",
        "items": [
            {"name": "プレミアムビール", "price_range": (300, 500), "organic_rate": 0, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.8},
            {"name": "日本酒（純米大吟醸）", "price_range": (1500, 3000), "organic_rate": 0.3, "discount_rate": 0.1, "ready_rate": 0, "premium_rate": 1.0},
            {"name": "赤ワイン", "price_range": (800, 2000), "organic_rate": 0.4, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.8},
            {"name": "白ワイン", "price_range": (800, 1800), "organic_rate": 0.4, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.7},
            {"name": "発泡酒", "price_range": (150, 250), "organic_rate": 0, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0},
            {"name": "ハイボール缶", "price_range": (180, 300), "organic_rate": 0, "discount_rate": 0.3, "ready_rate": 0, "premium_rate": 0.2},
            {"name": "焼酎", "price_range": (800, 1500), "organic_rate": 0.2, "discount_rate": 0.2, "ready_rate": 0, "premium_rate": 0.5},
        ]
    },
    1200: {
        "name": "スイーツ・アイス",
        "items": [
            {"name": "プレミアムアイスクリーム", "price_range": (400, 700), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 1.0, "premium_rate": 0.9},
            {"name": "シュークリーム", "price_range": (150, 300), "organic_rate": 0, "discount_rate": 0.6, "ready_rate": 1.0, "premium_rate": 0.3},
            {"name": "プリン", "price_range": (200, 400), "organic_rate": 0.2, "discount_rate": 0.5, "ready_rate": 1.0, "premium_rate": 0.4},
            {"name": "ショートケーキ", "price_range": (400, 700), "organic_rate": 0.2, "discount_rate": 0.4, "ready_rate": 1.0, "premium_rate": 0.6},
            {"name": "抹茶アイス", "price_range": (300, 500), "organic_rate": 0.3, "discount_rate": 0.4, "ready_rate": 1.0, "premium_rate": 0.5},
            {"name": "フルーツタルト", "price_range": (500, 900), "organic_rate": 0.2, "discount_rate": 0.3, "ready_rate": 1.0, "premium_rate": 0.8},
        ]
    },
}

# COMMAND ----------

# DBTITLE 1,商品マスタ生成
def generate_items(num_items):
    """商品マスタを生成（属性フラグ付き）"""
    items_data = []
    item_id = 1001

    # 全商品リストを作成
    all_items = []
    for cat_id, cat_info in CATEGORIES.items():
        for item in cat_info["items"]:
            all_items.append((cat_id, cat_info["name"], item))

    # 指定数まで繰り返し選択
    selected = (all_items * ((num_items // len(all_items)) + 1))[:num_items]

    for cat_id, cat_name, item in selected:
        price = random.randint(item["price_range"][0], item["price_range"][1])
        # 食品は賞味期限あり（BASE_DATE基準で将来の日付）
        exp_date = (BASE_DATE + timedelta(days=random.randint(7, 180))) if cat_id <= 1200 else None
        # 入荷日（BASE_DATE基準で過去の日付）
        arrival = BASE_DATE - timedelta(days=random.randint(1, 60))

        # 属性フラグを確率で決定
        is_organic = random.random() < item["organic_rate"]
        is_discount_target = random.random() < item["discount_rate"]
        is_ready_to_eat = random.random() < item["ready_rate"]
        is_premium = random.random() < item["premium_rate"]

        items_data.append((
            item_id, cat_id, item["name"], cat_name, price,
            exp_date, arrival,
            is_organic, is_discount_target, is_ready_to_eat, is_premium
        ))
        item_id += 1

    return items_data

# スキーマ定義
schema_items = StructType([
    StructField("item_id", LongType(), False),
    StructField("category_id", LongType(), False),
    StructField("item_name", StringType(), False),
    StructField("category_name", StringType(), False),
    StructField("price", LongType(), False),
    StructField("expiration_date", DateType(), True),
    StructField("arrival_date", DateType(), True),
    StructField("is_organic", BooleanType(), False),
    StructField("is_discount_target", BooleanType(), False),
    StructField("is_ready_to_eat", BooleanType(), False),
    StructField("is_premium", BooleanType(), False)
])

items_data = generate_items(NUM_PRODUCTS)
items_df = spark.createDataFrame(items_data, schema=schema_items)

print(f"商品マスタ: {items_df.count()}件")
display(items_df.limit(10))

# COMMAND ----------

# DBTITLE 1,商品属性フラグ分布確認
print("=== 商品属性フラグ分布 ===")
print(f"is_organic=True: {items_df.filter(col('is_organic')).count()}件")
print(f"is_discount_target=True: {items_df.filter(col('is_discount_target')).count()}件")
print(f"is_ready_to_eat=True: {items_df.filter(col('is_ready_to_eat')).count()}件")
print(f"is_premium=True: {items_df.filter(col('is_premium')).count()}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 店舗マスタ（stores）

# COMMAND ----------

# DBTITLE 1,店舗マスタ生成
STORE_CONFIG = {
    "東京都": {"area": "関東", "cities": ["渋谷区", "新宿区", "中央区", "港区", "品川区"]},
    "神奈川県": {"area": "関東", "cities": ["横浜市", "川崎市"]},
    "大阪府": {"area": "関西", "cities": ["大阪市", "堺市"]},
    "愛知県": {"area": "中部", "cities": ["名古屋市"]},
    "北海道": {"area": "北海道", "cities": ["札幌市"]},
}

def generate_stores(num_stores):
    """店舗マスタを生成"""
    stores_data = []
    store_id = 1

    prefs = list(STORE_CONFIG.keys())
    for i in range(num_stores):
        pref = prefs[i % len(prefs)]
        config = STORE_CONFIG[pref]
        city = random.choice(config["cities"])

        stores_data.append((
            store_id, f'{city}店', config["area"],
            f'{pref}{city}1-{random.randint(1,20)}-{random.randint(1,10)}',
            f'{random.randint(100,999)}-{random.randint(1000,9999)}',
            pref, city,
            f'03-{random.randint(1000,9999)}-{random.randint(1000,9999)}',
            f'store{store_id}@example.com',
            '10:00-21:00', random.choice(['なし', '水曜日', '火曜日']),
            random.randint(100, 999),
            date(2020, random.randint(1,12), random.randint(1,28)),
            'ACTIVE' if random.random() < 0.95 else 'INACTIVE'
        ))
        store_id += 1

    return stores_data

schema_stores = StructType([
    StructField("store_id", LongType(), False),
    StructField("store_name", StringType(), False),
    StructField("store_area", StringType(), False),
    StructField("address", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("prefecture", StringType(), True),
    StructField("city", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email", StringType(), True),
    StructField("business_hours", StringType(), True),
    StructField("closed_days", StringType(), True),
    StructField("manager_id", LongType(), True),
    StructField("opening_date", DateType(), True),
    StructField("status", StringType(), False)
])

stores_data = generate_stores(NUM_STORES)
stores_df = spark.createDataFrame(stores_data, schema=schema_stores)

print(f"店舗マスタ: {stores_df.count()}件")
display(stores_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. POS注文データ生成（クラスターベース）

# COMMAND ----------

# DBTITLE 1,クラスター別購買行動の定義
def get_purchase_frequency(cluster):
    """購買頻度を決定（クラスターごと）

    ドキュメント定義:
    - 1: 週末まとめ買い族: 2-4回/月
    - 2: 夜型値引きハンター: 8-15回/月
    - 3: 健康・こだわり層: 4-8回/月
    - 4: カテゴリジャンキー: 6-12回/月
    - 5: 平日夕方の時短族: 12-20回/月
    - ランダム層: 1-6回/月
    """
    if cluster == '1':  # 週末まとめ買い族：低頻度だが大量購入
        return random.randint(2, 4)
    elif cluster == '2':  # 夜型値引きハンター：中頻度
        return random.randint(8, 15)
    elif cluster == '3':  # 健康・こだわり層：中頻度
        return random.randint(4, 8)
    elif cluster == '4':  # カテゴリジャンキー：中頻度
        return random.randint(6, 12)
    elif cluster == '5':  # 平日夕方の時短族：高頻度
        return random.randint(12, 20)
    else:  # ランダム層
        return random.randint(1, 6)

def get_items_per_order(cluster):
    """1注文あたりの商品点数を決定（クラスターごと）

    ドキュメント定義:
    - 1: 週末まとめ買い族: 15-30点（大量購入）
    - 2: 夜型値引きハンター: 3-8点
    - 3: 健康・こだわり層: 5-12点
    - 4: カテゴリジャンキー: 2-5点（特定カテゴリのみ）
    - 5: 平日夕方の時短族: 3-6点（即食系中心）
    - ランダム層: 1-10点
    """
    if cluster == '1':  # 週末まとめ買い族：大量購入
        return random.randint(15, 30)
    elif cluster == '2':  # 夜型値引きハンター
        return random.randint(3, 8)
    elif cluster == '3':  # 健康・こだわり層
        return random.randint(5, 12)
    elif cluster == '4':  # カテゴリジャンキー：少なめ（特定カテゴリのみ）
        return random.randint(2, 5)
    elif cluster == '5':  # 平日夕方の時短族：少なめ（即食系中心）
        return random.randint(3, 6)
    else:  # ランダム層
        return random.randint(1, 10)

def get_last_order_recency(cluster):
    """最終購買からの経過日数を決定（クラスターごとのリピートタイミング分布）

    analysis_design.md セクション12「リピートタイミング分析定義」に基づく

    クラスター別分布:
    - 1: 週末まとめ買い族: 7日サイクル、21日で離反
    - 2: 夜型値引きハンター: 3-5日サイクル、14日で離反
    - 3: 健康・こだわり層: 7-10日サイクル、21日で離反
    - 4: カテゴリジャンキー: 5-7日サイクル、14日で離反
    - 5: 平日夕方の時短族: 2-3日サイクル、10日で離反
    - ランダム層: 7-14日サイクル、30日で離反

    全体目標: 0-7日:35%, 8-14日:20%, 15-30日:20%, 31-60日:15%, 61日以上:10%
    """
    r = random.random()

    if cluster == '1':  # 週末まとめ買い族
        if r < 0.45:      # アクティブ（0-7日）
            return random.randint(0, 7)
        elif r < 0.70:    # 要介入（8-14日）
            return random.randint(8, 14)
        elif r < 0.85:    # 離反直前（15-21日）
            return random.randint(15, 21)
        else:             # 離反（22日以上）
            return random.randint(22, 90)

    elif cluster == '2':  # 夜型値引きハンター
        if r < 0.55:      # アクティブ（0-5日）
            return random.randint(0, 5)
        elif r < 0.75:    # 要介入（6-10日）
            return random.randint(6, 10)
        elif r < 0.90:    # 離反直前（11-14日）
            return random.randint(11, 14)
        else:             # 離反（15日以上）
            return random.randint(15, 90)

    elif cluster == '3':  # 健康・こだわり層
        if r < 0.40:      # アクティブ（0-10日）
            return random.randint(0, 10)
        elif r < 0.65:    # 要介入（11-14日）
            return random.randint(11, 14)
        elif r < 0.85:    # 離反直前（15-21日）
            return random.randint(15, 21)
        else:             # 離反（22日以上）
            return random.randint(22, 90)

    elif cluster == '4':  # カテゴリジャンキー
        if r < 0.50:      # アクティブ（0-7日）
            return random.randint(0, 7)
        elif r < 0.75:    # 要介入（8-10日）
            return random.randint(8, 10)
        elif r < 0.90:    # 離反直前（11-14日）
            return random.randint(11, 14)
        else:             # 離反（15日以上）
            return random.randint(15, 90)

    elif cluster == '5':  # 平日夕方の時短族
        if r < 0.60:      # アクティブ（0-3日）
            return random.randint(0, 3)
        elif r < 0.80:    # 要介入（4-7日）
            return random.randint(4, 7)
        elif r < 0.92:    # 離反直前（8-10日）
            return random.randint(8, 10)
        else:             # 離反（11日以上）
            return random.randint(11, 90)

    else:  # ランダム層
        if r < 0.30:      # アクティブ（0-7日）
            return random.randint(0, 7)
        elif r < 0.50:    # アクティブ（8-14日）
            return random.randint(8, 14)
        elif r < 0.70:    # 要注意（15-21日）
            return random.randint(15, 21)
        elif r < 0.85:    # 離反リスク（22-30日）
            return random.randint(22, 30)
        else:             # 休眠（31日以上）
            return random.randint(31, 90)


def get_order_datetime(cluster, base_date, is_last_order=False, last_order_recency=None, max_days_ago=None):
    """注文日時を決定（クラスターごとの特徴を反映）

    ドキュメント定義:
    - 1: 週末まとめ買い族: 10-18時、土日のみ
    - 2: 夜型値引きハンター: 19-21時、全曜日
    - 3: 健康・こだわり層: 9-17時、全曜日
    - 4: カテゴリジャンキー: 11-21時、全曜日
    - 5: 平日夕方の時短族: 16-18時、平日のみ
    - ランダム層: 8-21時、全曜日

    is_last_order=True の場合、last_order_recency を使用して最終購買日を決定
    max_days_ago: 注文日の範囲（デフォルトはDATA_DAYS=5年分）
    """
    if max_days_ago is None:
        max_days_ago = DATA_DAYS

    # 最終注文の場合は指定されたrecencyを使用
    if is_last_order and last_order_recency is not None:
        days_ago = last_order_recency
    else:
        # 過去の注文はDATA_DAYS（5年）の範囲で分散
        days_ago = random.randint(1, max_days_ago)

    if cluster == '1':  # 週末まとめ買い族：土日に集中
        order_date = base_date - timedelta(days=days_ago)
        # 土日に調整
        while order_date.weekday() not in [5, 6]:
            order_date -= timedelta(days=1)
        hour = random.randint(10, 18)
    elif cluster == '2':  # 夜型値引きハンター：19-21時
        order_date = base_date - timedelta(days=days_ago)
        hour = random.randint(19, 21)
    elif cluster == '3':  # 健康・こだわり層：9-17時
        order_date = base_date - timedelta(days=days_ago)
        hour = random.randint(9, 17)
    elif cluster == '4':  # カテゴリジャンキー：11-21時
        order_date = base_date - timedelta(days=days_ago)
        hour = random.randint(11, 21)
    elif cluster == '5':  # 平日夕方の時短族：平日16-18時
        order_date = base_date - timedelta(days=days_ago)
        # 平日に調整
        while order_date.weekday() in [5, 6]:
            order_date -= timedelta(days=1)
        hour = random.randint(16, 18)
    else:  # ランダム層：8-21時
        order_date = base_date - timedelta(days=days_ago)
        hour = random.randint(8, 21)

    return datetime(
        order_date.year, order_date.month, order_date.day,
        hour, random.randint(0, 59), random.randint(0, 59)
    )

def get_order_status():
    """注文ステータスを決定"""
    r = random.random()
    if r < 0.92:
        return 'COMPLETED'
    elif r < 0.97:
        return 'CANCELLED'
    else:
        return 'RETURNED'

# COMMAND ----------

# DBTITLE 1,クラスター別商品選択ロジック
def select_item_for_cluster(cluster, items_by_attr, all_items):
    """クラスターに基づいて商品を選択

    ドキュメント定義:
    - 1: 週末まとめ買い族: 全カテゴリ均等 各8.3%
    - 2: 夜型値引きハンター: 惣菜、パン、冷凍食品 各25%（計75%）
    - 3: 健康・こだわり層: 野菜、乳製品、調味料 各28%（計84%）
    - 4: カテゴリジャンキー: 酒類、お菓子、スイーツ 各30%（計90%）
    - 5: 平日夕方の時短族: 惣菜、冷凍食品、パン 各30%（計90%）
    - ランダム層: 全カテゴリ均等 各8.3%
    """
    if cluster == '1':  # 週末まとめ買い族：全カテゴリ均等
        return random.choice(all_items)

    elif cluster == '2':  # 夜型値引きハンター：惣菜、パン、冷凍食品 各25%
        r = random.random()
        if r < 0.25 and items_by_attr['category_1000']:  # 惣菜
            return random.choice(items_by_attr['category_1000'])
        elif r < 0.50 and items_by_attr['category_500']:  # パン
            return random.choice(items_by_attr['category_500'])
        elif r < 0.75 and items_by_attr['category_700']:  # 冷凍食品
            return random.choice(items_by_attr['category_700'])
        return random.choice(all_items)

    elif cluster == '3':  # 健康・こだわり層：野菜、乳製品、調味料 各28%
        r = random.random()
        if r < 0.28 and items_by_attr['category_100']:  # 野菜
            return random.choice(items_by_attr['category_100'])
        elif r < 0.56 and items_by_attr['category_400']:  # 乳製品
            return random.choice(items_by_attr['category_400'])
        elif r < 0.84 and items_by_attr['category_900']:  # 調味料
            return random.choice(items_by_attr['category_900'])
        return random.choice(all_items)

    elif cluster == '4':  # カテゴリジャンキー：酒類、お菓子、スイーツ 各30%
        r = random.random()
        if r < 0.30 and items_by_attr['category_1100']:  # 酒類
            return random.choice(items_by_attr['category_1100'])
        elif r < 0.60 and items_by_attr['category_600']:  # お菓子
            return random.choice(items_by_attr['category_600'])
        elif r < 0.90 and items_by_attr['category_1200']:  # スイーツ
            return random.choice(items_by_attr['category_1200'])
        return random.choice(all_items)

    elif cluster == '5':  # 平日夕方の時短族：惣菜、冷凍食品、パン 各30%
        r = random.random()
        if r < 0.30 and items_by_attr['category_1000']:  # 惣菜
            return random.choice(items_by_attr['category_1000'])
        elif r < 0.60 and items_by_attr['category_700']:  # 冷凍食品
            return random.choice(items_by_attr['category_700'])
        elif r < 0.90 and items_by_attr['category_500']:  # パン
            return random.choice(items_by_attr['category_500'])
        return random.choice(all_items)

    else:  # ランダム層：全カテゴリ均等
        return random.choice(all_items)

# COMMAND ----------

# DBTITLE 1,POS注文データ生成（成長トレンド・新規割合変動版）
import numpy as np
import math

def generate_pos_data(users_df, items_df, stores_df, num_orders):
    """POS注文データを生成（売上成長トレンド + 新規顧客割合変動）

    新規顧客は動的に生成されます。NUM_USERSは初期ユーザー数を表し、
    毎日の新規顧客割合に応じて新しいユーザーが追加されます。
    """

    print(f"データ生成開始: {num_orders}件の注文を生成します...")
    print(f"  売上成長トレンド + 新規顧客割合変動 + 動的ユーザー生成を適用")

    # マスタからデータ取得
    users_list = [(row.user_id, row.gender, row.behavior_cluster)
                  for row in users_df.select("user_id", "gender", "behavior_cluster").collect()]
    items_list = [(row.item_id, row.category_id, row.price,
                   row.is_organic, row.is_discount_target, row.is_ready_to_eat, row.is_premium)
                  for row in items_df.select("item_id", "category_id", "price",
                                             "is_organic", "is_discount_target", "is_ready_to_eat", "is_premium").collect()]
    store_ids = np.array([row.store_id for row in stores_df.select("store_id").collect()])

    # NumPy配列に変換
    item_ids = np.array([i[0] for i in items_list])
    item_categories = np.array([i[1] for i in items_list])
    item_prices = np.array([i[2] for i in items_list])

    # カテゴリ別インデックス（高速選択用）
    category_indices = {}
    for cat_id in [100, 400, 500, 600, 700, 900, 1000, 1100, 1200]:
        indices = np.where(item_categories == cat_id)[0]
        if len(indices) > 0:
            category_indices[cat_id] = indices

    # クラスター別カテゴリ選択確率
    cluster_category_probs = {
        '1': None,  # 全カテゴリ均等
        '2': [(1000, 0.25), (500, 0.25), (700, 0.25)],  # 惣菜、パン、冷凍食品
        '3': [(100, 0.28), (400, 0.28), (900, 0.28)],   # 野菜、乳製品、調味料
        '4': [(1100, 0.30), (600, 0.30), (1200, 0.30)], # 酒類、お菓子、スイーツ
        '5': [(1000, 0.30), (700, 0.30), (500, 0.30)],  # 惣菜、冷凍食品、パン
    }

    # クラスター別アイテム数範囲
    cluster_items_range = {
        '1': (15, 30), '2': (3, 8), '3': (5, 12),
        '4': (2, 5), '5': (3, 6), 'random': (1, 10)
    }

    # クラスター別時間帯と曜日
    cluster_time_config = {
        '1': {'hours': (10, 18), 'weekend_only': True},
        '2': {'hours': (19, 21), 'weekend_only': False},
        '3': {'hours': (9, 17), 'weekend_only': False},
        '4': {'hours': (11, 21), 'weekend_only': False},
        '5': {'hours': (16, 18), 'weekday_only': True},
        'random': {'hours': (8, 21), 'weekend_only': False}
    }

    # ============================================================
    # 日別の注文数と新規割合を計算（成長トレンド + 季節変動）
    # ============================================================

    def get_growth_factor(day_index, total_days):
        """成長トレンド: 5年間で1.0 → 2.0に成長（年率約15%成長）"""
        progress = day_index / total_days  # 0.0 → 1.0
        return 1.0 + progress * 1.0  # 1.0 → 2.0

    def get_seasonal_factor(order_date):
        """季節変動: 年末商戦、GW、お盆などで売上増"""
        month = order_date.month
        day = order_date.day

        # 年末商戦（12月）: +30-50%
        if month == 12:
            if day >= 20:
                return 1.5  # クリスマス前後
            return 1.3

        # 年始（1月1-3日）: -30%（休業的）
        if month == 1 and day <= 3:
            return 0.7

        # GW（5月3-5日）: +20%
        if month == 5 and 3 <= day <= 5:
            return 1.2

        # お盆（8月13-16日）: +15%
        if month == 8 and 13 <= day <= 16:
            return 1.15

        # 夏休み（7-8月）: +5%
        if month in [7, 8]:
            return 1.05

        return 1.0

    def get_new_customer_ratio(day_index, total_days, order_date):
        """新規顧客割合: 初期30% → 成熟期10% + 季節変動"""
        progress = day_index / total_days  # 0.0 → 1.0

        # 基本トレンド: 30% → 10%（事業成熟に伴い新規獲得コスト上昇）
        base_ratio = 0.30 - progress * 0.20  # 0.30 → 0.10

        # 季節変動（キャンペーン期間は新規が増える）
        month = order_date.month
        day = order_date.day

        seasonal_bonus = 0.0
        # 年末商戦: 新規+5%
        if month == 12 and day >= 15:
            seasonal_bonus = 0.05
        # 新生活シーズン（4月）: 新規+8%
        elif month == 4:
            seasonal_bonus = 0.08
        # GW: 新規+3%
        elif month == 5 and 3 <= day <= 5:
            seasonal_bonus = 0.03
        # 夏のセール（7月）: 新規+4%
        elif month == 7:
            seasonal_bonus = 0.04

        # ランダムノイズ（±3%）
        noise = np.random.uniform(-0.03, 0.03)

        return max(0.05, min(0.40, base_ratio + seasonal_bonus + noise))

    # 日別の注文数を計算
    print("  日別注文数を計算中...")
    daily_orders = []
    total_days = DATA_DAYS

    for day_idx in range(total_days):
        order_date = BASE_DATE - timedelta(days=total_days - day_idx - 1)
        growth = get_growth_factor(day_idx, total_days)
        seasonal = get_seasonal_factor(order_date)

        # 基準注文数 × 成長係数 × 季節係数 × ランダムノイズ
        base_daily = num_orders / total_days
        noise = np.random.uniform(0.85, 1.15)
        daily_count = base_daily * growth * seasonal * noise

        # 新規割合
        new_ratio = get_new_customer_ratio(day_idx, total_days, order_date)

        daily_orders.append({
            'date': order_date,
            'day_idx': day_idx,
            'count': daily_count,
            'new_ratio': new_ratio
        })

    # 注文数を正規化（合計がnum_ordersになるように）
    total_raw = sum(d['count'] for d in daily_orders)
    scale_factor = num_orders / total_raw
    for d in daily_orders:
        d['count'] = int(d['count'] * scale_factor)

    # 端数調整
    current_total = sum(d['count'] for d in daily_orders)
    diff = num_orders - current_total
    if diff > 0:
        for i in range(diff):
            daily_orders[i % len(daily_orders)]['count'] += 1

    print(f"  日別注文数計算完了（{len(daily_orders)}日分）")

    # ============================================================
    # 会員プールを作成（既存顧客 + 動的に新規顧客を追加）
    # ============================================================
    # 初期ユーザーは「既存顧客」として扱う（データ開始前から会員）
    existing_users = list(users_list)
    user_cluster_map = {u[0]: u[2] for u in users_list}
    users_first_order = {}  # ユーザーの初回注文日を記録

    # 新規ユーザー生成用カウンター
    next_user_id_num = len(users_list) + 1
    new_users_data = []  # 動的に生成された新規ユーザーを記録

    # ============================================================
    # 日別に注文を生成
    # ============================================================
    orders_data = []
    order_items_data = []
    order_id = 1000001
    order_item_id = 1

    progress_interval = max(1, len(daily_orders) // 10)

    for day_data in daily_orders:
        order_date = day_data['date']
        day_count = day_data['count']
        new_ratio = day_data['new_ratio']
        day_idx = day_data['day_idx']

        if day_idx % progress_interval == 0:
            print(f"  進捗: {day_idx}/{len(daily_orders)}日 ({100*day_idx//len(daily_orders)}%)")

        # この日の新規注文数と既存注文数
        new_count = int(day_count * new_ratio)
        existing_count = day_count - new_count

        # 新規顧客の注文を生成（毎回新しいユーザーを作成）
        for _ in range(new_count):
            # 新しいユーザーを動的に生成
            new_user = create_single_user(next_user_id_num, order_date)
            new_users_data.append(new_user)

            user_id = new_user[0]  # U00000XXX形式
            cluster = new_user[11]  # behavior_cluster
            users_first_order[user_id] = order_date

            # 既存顧客プールにも追加（次回以降は既存顧客として扱う）
            existing_users.append((user_id, new_user[2], cluster))
            user_cluster_map[user_id] = cluster

            next_user_id_num += 1

            # 注文生成
            order_data, items_data, order_id, order_item_id = _generate_single_order(
                user_id, cluster, order_date, order_id, order_item_id,
                store_ids, item_ids, item_categories, item_prices,
                category_indices, cluster_category_probs, cluster_items_range, cluster_time_config
            )
            orders_data.append(order_data)
            order_items_data.extend(items_data)

        # 既存顧客の注文を生成
        for _ in range(existing_count):
            user_id, gender, cluster = existing_users[np.random.randint(0, len(existing_users))]
            if user_id not in users_first_order:
                users_first_order[user_id] = order_date

            order_data, items_data, order_id, order_item_id = _generate_single_order(
                user_id, cluster, order_date, order_id, order_item_id,
                store_ids, item_ids, item_categories, item_prices,
                category_indices, cluster_category_probs, cluster_items_range, cluster_time_config
            )
            orders_data.append(order_data)
            order_items_data.extend(items_data)

    print(f"  完了: 注文 {len(orders_data)}件, 明細 {len(order_items_data)}件")
    print(f"  新規顧客数: {len(new_users_data)}人（動的生成）")
    print(f"  総顧客数: {len(existing_users)}人（初期{len(users_list)}人 + 新規{len(new_users_data)}人）")

    return orders_data, order_items_data, new_users_data


def _generate_single_order(user_id, cluster, order_date, order_id, order_item_id,
                           store_ids, item_ids, item_categories, item_prices,
                           category_indices, cluster_category_probs, cluster_items_range, cluster_time_config):
    """1件の注文を生成"""

    # 店舗選択
    store_id = int(store_ids[np.random.randint(0, len(store_ids))])

    # ステータス決定
    r_status = np.random.random()
    if r_status < 0.92:
        status = 'COMPLETED'
    elif r_status < 0.97:
        status = 'CANCELLED'
    else:
        status = 'RETURNED'

    # 時間帯・曜日決定（クラスター特性を反映）
    config = cluster_time_config.get(cluster, cluster_time_config['random'])
    hour_range = config['hours']
    hour = np.random.randint(hour_range[0], hour_range[1] + 1)
    minute = np.random.randint(0, 60)
    second = np.random.randint(0, 60)

    # 曜日調整（クラスター1は土日のみ、クラスター5は平日のみ）
    adjusted_date = order_date
    if config.get('weekend_only', False):
        # 土日に調整（最大7日前まで探す）
        for _ in range(7):
            if adjusted_date.weekday() in [5, 6]:  # 土曜=5, 日曜=6
                break
            adjusted_date = adjusted_date - timedelta(days=1)
    elif config.get('weekday_only', False):
        # 平日に調整（最大7日前まで探す）
        for _ in range(7):
            if adjusted_date.weekday() < 5:  # 月-金=0-4
                break
            adjusted_date = adjusted_date - timedelta(days=1)

    order_dt = datetime(adjusted_date.year, adjusted_date.month, adjusted_date.day, hour, minute, second)

    # アイテム数決定
    items_range = cluster_items_range.get(cluster, cluster_items_range['random'])
    num_items = np.random.randint(items_range[0], items_range[1] + 1)

    # 明細生成
    total_amount = 0
    total_qty = 0
    items_data = []

    cat_probs = cluster_category_probs.get(cluster)

    for _ in range(num_items):
        selected_idx = None
        if cat_probs:
            r = np.random.random()
            cumsum = 0
            for cat_id, prob in cat_probs:
                cumsum += prob
                if r < cumsum and cat_id in category_indices:
                    cat_indices = category_indices[cat_id]
                    selected_idx = np.random.choice(cat_indices)
                    break

        if selected_idx is None:
            selected_idx = np.random.randint(0, len(item_ids))

        item_id = int(item_ids[selected_idx])
        cat_id = int(item_categories[selected_idx])
        unit_price = int(item_prices[selected_idx])

        qty = np.random.randint(1, 6)
        subtotal = unit_price * qty
        cancel_flg = (status == 'CANCELLED')

        items_data.append((
            order_item_id, user_id, order_id, store_id,
            item_id, cat_id,
            qty, unit_price, subtotal, cancel_flg,
            order_dt
        ))

        total_amount += subtotal
        total_qty += qty
        order_item_id += 1

    # 会計データ
    order_data = (
        order_id, user_id, store_id, order_dt,
        total_amount, total_qty, status
    )

    return order_data, items_data, order_id + 1, order_item_id

# スキーマ定義
schema_orders = StructType([
    StructField("order_id", LongType(), False),
    StructField("user_id", StringType(), False),
    StructField("store_id", LongType(), False),
    StructField("order_datetime", TimestampType(), False),
    StructField("total_amount", LongType(), False),
    StructField("total_quantity", IntegerType(), False),
    StructField("order_status", StringType(), False)
])

schema_order_items = StructType([
    StructField("order_item_id", LongType(), False),
    StructField("user_id", StringType(), False),
    StructField("order_id", LongType(), False),
    StructField("store_id", LongType(), False),
    StructField("item_id", LongType(), False),
    StructField("category_id", LongType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", LongType(), False),
    StructField("subtotal", LongType(), False),
    StructField("cancel_flg", BooleanType(), False),
    StructField("order_datetime", TimestampType(), False)
])

# データ生成
orders_data, order_items_data, new_users_data = generate_pos_data(users_df, items_df, stores_df, NUM_ORDERS)

orders_df = spark.createDataFrame(orders_data, schema=schema_orders)
order_items_df = spark.createDataFrame(order_items_data, schema=schema_order_items)

# 会員マスタを更新（初期ユーザー + 新規獲得ユーザー）
if new_users_data:
    new_users_df = spark.createDataFrame(new_users_data, schema=schema_users)
    users_df = users_df.union(new_users_df)
    print(f"\n会員マスタ更新: 初期{NUM_USERS}人 + 新規{len(new_users_data)}人 = {users_df.count()}人")

print(f"POS注文（会計）: {orders_df.count()}件")
print(f"POS注文明細: {order_items_df.count()}件")
print(f"平均明細数/注文: {order_items_df.count() / orders_df.count():.1f}件")

# COMMAND ----------

# DBTITLE 1,注文ステータス分布確認
display(
    orders_df
    .groupBy("order_status")
    .count()
    .withColumn("ratio", col("count") / orders_df.count() * 100)
)

# COMMAND ----------

# DBTITLE 1,サンプルデータ確認: 会計データ
display(orders_df.limit(10))

# COMMAND ----------

# DBTITLE 1,サンプルデータ確認: 注文明細データ
display(order_items_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 生成データの傾向確認
# MAGIC
# MAGIC 売上成長トレンドと新規顧客割合の変動を確認します。

# COMMAND ----------

# DBTITLE 1,年別売上推移（成長トレンド確認）
from pyspark.sql.functions import year, month, dayofmonth, sum as _sum, count, countDistinct

yearly_trend = (
    orders_df
    .withColumn("order_year", year("order_datetime"))
    .groupBy("order_year")
    .agg(
        _sum("total_amount").alias("total_sales"),
        count("order_id").alias("order_count"),
        countDistinct("user_id").alias("unique_customers")
    )
    .orderBy("order_year")
)

display(yearly_trend)

# COMMAND ----------

# DBTITLE 1,月別売上推移（季節変動確認）
from pyspark.sql.functions import date_format

monthly_trend = (
    orders_df
    .withColumn("order_month", date_format("order_datetime", "yyyy-MM"))
    .groupBy("order_month")
    .agg(
        _sum("total_amount").alias("total_sales"),
        count("order_id").alias("order_count"),
        countDistinct("user_id").alias("unique_customers")
    )
    .orderBy("order_month")
)

display(monthly_trend)

# COMMAND ----------

# DBTITLE 1,新規顧客割合の推移（月別）
from pyspark.sql.window import Window
from pyspark.sql.functions import min as _min, when, lit, to_date, datediff, row_number

# ユーザーの登録日を取得
users_reg_ref = users_df.select(
    col("user_id").alias("reg_user_id"),
    col("registration_date")
)

# 各ユーザーの注文に順番を付与
user_order_win = Window.partitionBy("user_id").orderBy("order_datetime")

# 注文に初回フラグを付与（登録から30日以内の初回注文のみが新規）
orders_with_first = (
    orders_df
    .withColumn("order_rank", row_number().over(user_order_win))
    .withColumn("order_date", to_date("order_datetime"))
    .withColumn("order_month", date_format("order_datetime", "yyyy-MM"))
    # ユーザーの登録日を結合
    .join(users_reg_ref, orders_df["user_id"] == users_reg_ref["reg_user_id"], "left")
    # 初回注文かつ登録から30日以内 = 新規
    .withColumn("is_first_order",
        when(
            (col("order_rank") == 1) &
            (datediff(col("order_date"), col("registration_date")) <= 30),
            lit(1)
        ).otherwise(lit(0))
    )
)

# 月別の新規割合を計算
new_customer_ratio = (
    orders_with_first
    .groupBy("order_month")
    .agg(
        count("order_id").alias("total_orders"),
        _sum("is_first_order").alias("new_customer_orders")
    )
    .withColumn("new_ratio_pct", col("new_customer_orders") / col("total_orders") * 100)
    .orderBy("order_month")
)

display(new_customer_ratio)

# COMMAND ----------

# DBTITLE 1,成長率サマリー
first_year = yearly_trend.orderBy("order_year").first()
last_year = yearly_trend.orderBy(col("order_year").desc()).first()

print("=" * 50)
print("売上成長サマリー")
print("=" * 50)
print(f"初年度（{first_year['order_year']}年）: {first_year['total_sales']:,}円")
print(f"最終年（{last_year['order_year']}年）: {last_year['total_sales']:,}円")
print(f"成長率: {last_year['total_sales'] / first_year['total_sales']:.2f}倍")
print()
print(f"初年度 注文数: {first_year['order_count']:,}件")
print(f"最終年 注文数: {last_year['order_count']:,}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 重複データの追加（Silver体験用）

# COMMAND ----------

# DBTITLE 1,明細に5%の重複を追加
# 5%の重複データを追加（Silverでの重複排除体験用）
duplicate_count = int(order_items_df.count() * 0.05)
duplicates_df = order_items_df.orderBy(rand()).limit(duplicate_count)
order_items_df_with_dup = order_items_df.union(duplicates_df)

print(f"重複追加前: {order_items_df.count()}件")
print(f"重複追加後: {order_items_df_with_dup.count()}件")
print(f"追加した重複: {duplicate_count}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. CSVファイル出力

# COMMAND ----------

# DBTITLE 1,CSV出力
# タイムスタンプを生成（ファイル名の接頭辞用）
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
print(f"出力先: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}")
print(f"タイムスタンプ: {timestamp}")

# 会員マスタ（behavior_clusterはGold層で購買履歴から算出するためCSVには含めない）
users_csv_df = users_df.drop("behavior_cluster")
users_csv = users_csv_df.toPandas().to_csv(index=False)
dbutils.fs.put(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/users/{timestamp}_users.csv", users_csv, True)
print(f"users/{timestamp}_users.csv 出力完了")

# 商品マスタ
items_csv = items_df.toPandas().to_csv(index=False)
dbutils.fs.put(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/items/{timestamp}_items.csv", items_csv, True)
print(f"items/{timestamp}_items.csv 出力完了")

# 店舗マスタ
stores_csv = stores_df.toPandas().to_csv(index=False)
dbutils.fs.put(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/stores/{timestamp}_stores.csv", stores_csv, True)
print(f"stores/{timestamp}_stores.csv 出力完了")

# POS注文（会計単位）
orders_csv = orders_df.toPandas().to_csv(index=False)
dbutils.fs.put(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/orders/{timestamp}_orders.csv", orders_csv, True)
print(f"orders/{timestamp}_orders.csv 出力完了")

# POS注文明細（アイテム単位、5%重複含む）
order_items_csv = order_items_df_with_dup.toPandas().to_csv(index=False)
dbutils.fs.put(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/order_items/{timestamp}_order_items.csv", order_items_csv, True)
print(f"order_items/{timestamp}_order_items.csv 出力完了（5%重複含む）")

print("\n全てのCSV出力が完了しました!")

# COMMAND ----------

# DBTITLE 1,出力確認
display(dbutils.fs.ls(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. データ品質チェック

# COMMAND ----------

# DBTITLE 1,整合性チェック
print("=" * 60)
print("【テーブル間の整合性チェック】")
print("=" * 60)

# 1. 金額の整合性
orders_total = orders_df.groupBy().agg({"total_amount": "sum"}).collect()[0][0]
items_total = order_items_df.groupBy().agg({"subtotal": "sum"}).collect()[0][0]
print(f"\n● 金額の整合性")
print(f"  会計テーブルの合計金額: {orders_total:,}円")
print(f"  明細テーブルの合計金額: {items_total:,}円")
print(f"  結果: {'✅ OK' if orders_total == items_total else '❌ NG'}")

# 2. 外部キー整合性: orders.user_id → users.user_id
orders_user_ids = orders_df.select("user_id").distinct()
users_user_ids = users_df.select("user_id").distinct()
orphan_orders_users = orders_user_ids.subtract(users_user_ids).count()
print(f"\n● 外部キー整合性: orders → users")
print(f"  ordersのユニークuser_id数: {orders_user_ids.count()}")
print(f"  usersに存在しないuser_id数: {orphan_orders_users}")
print(f"  結果: {'✅ OK' if orphan_orders_users == 0 else '❌ NG'}")

# 3. 外部キー整合性: order_items.user_id → users.user_id
items_user_ids = order_items_df.select("user_id").distinct()
orphan_items_users = items_user_ids.subtract(users_user_ids).count()
print(f"\n● 外部キー整合性: order_items → users")
print(f"  order_itemsのユニークuser_id数: {items_user_ids.count()}")
print(f"  usersに存在しないuser_id数: {orphan_items_users}")
print(f"  結果: {'✅ OK' if orphan_items_users == 0 else '❌ NG'}")

# 4. 外部キー整合性: order_items.order_id → orders.order_id
items_order_ids = order_items_df.select("order_id").distinct()
orders_order_ids = orders_df.select("order_id").distinct()
orphan_items_orders = items_order_ids.subtract(orders_order_ids).count()
print(f"\n● 外部キー整合性: order_items → orders")
print(f"  order_itemsのユニークorder_id数: {items_order_ids.count()}")
print(f"  ordersに存在しないorder_id数: {orphan_items_orders}")
print(f"  結果: {'✅ OK' if orphan_items_orders == 0 else '❌ NG'}")

# 5. 外部キー整合性: orders.store_id → stores.store_id
orders_store_ids = orders_df.select("store_id").distinct()
stores_store_ids = stores_df.select("store_id").distinct()
orphan_orders_stores = orders_store_ids.subtract(stores_store_ids).count()
print(f"\n● 外部キー整合性: orders → stores")
print(f"  ordersのユニークstore_id数: {orders_store_ids.count()}")
print(f"  storesに存在しないstore_id数: {orphan_orders_stores}")
print(f"  結果: {'✅ OK' if orphan_orders_stores == 0 else '❌ NG'}")

# 6. 外部キー整合性: order_items.item_id → items.item_id
items_item_ids = order_items_df.select("item_id").distinct()
items_master_ids = items_df.select("item_id").distinct()
orphan_items_items = items_item_ids.subtract(items_master_ids).count()
print(f"\n● 外部キー整合性: order_items → items")
print(f"  order_itemsのユニークitem_id数: {items_item_ids.count()}")
print(f"  itemsに存在しないitem_id数: {orphan_items_items}")
print(f"  結果: {'✅ OK' if orphan_items_items == 0 else '❌ NG'}")

# 総合判定
all_checks_passed = (
    orders_total == items_total and
    orphan_orders_users == 0 and
    orphan_items_users == 0 and
    orphan_items_orders == 0 and
    orphan_orders_stores == 0 and
    orphan_items_items == 0
)
print(f"\n{'=' * 60}")
print(f"総合判定: {'✅ 全チェックOK - テーブル間の整合性が保たれています' if all_checks_passed else '❌ NG - 整合性に問題があります'}")
print(f"{'=' * 60}")

# cancel_flgの分布確認
cancel_count = order_items_df.filter(col("cancel_flg") == True).count()
print(f"\nキャンセル明細: {cancel_count}件 ({cancel_count / order_items_df.count() * 100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,カテゴリ別売上確認
display(
    order_items_df
    .groupBy("category_id")
    .agg({"subtotal": "sum", "quantity": "sum"})
    .withColumnRenamed("sum(subtotal)", "total_sales")
    .withColumnRenamed("sum(quantity)", "total_qty")
    .orderBy(col("total_sales").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. データ分布検証
# MAGIC
# MAGIC analysis_design.md セクション10「データ生成時の分布定義」の通りにデータが生成されていることを検証します。

# COMMAND ----------

# DBTITLE 1,会員データ分布検証
from pyspark.sql.functions import col, count, when, round as spark_round

print("=" * 60)
print("【会員データ (users) の分布検証】")
print("=" * 60)

total_users = users_df.count()

# 性別: 男性 35%, 女性 65%
male_count = users_df.filter(col("gender") == "M").count()
female_count = users_df.filter(col("gender") == "F").count()
male_pct = male_count / total_users * 100
female_pct = female_count / total_users * 100
print(f"\n● 性別 (定義: 男性35%, 女性65%)")
print(f"  男性: {male_count}件 ({male_pct:.1f}%) - 期待値: 35%")
print(f"  女性: {female_count}件 ({female_pct:.1f}%) - 期待値: 65%")

# 都道府県: 15% が NULL
pref_null = users_df.filter(col("pref").isNull()).count()
pref_null_pct = pref_null / total_users * 100
print(f"\n● 都道府県NULL率 (定義: 15%)")
print(f"  NULL: {pref_null}件 ({pref_null_pct:.1f}%) - 期待値: 15%")

# 会員ステータス: active 75%, inactive 25%
active_count = users_df.filter(col("status") == "active").count()
inactive_count = users_df.filter(col("status") == "inactive").count()
active_pct = active_count / total_users * 100
inactive_pct = inactive_count / total_users * 100
print(f"\n● 会員ステータス (定義: active 75%, inactive 25%)")
print(f"  active: {active_count}件 ({active_pct:.1f}%) - 期待値: 75%")
print(f"  inactive: {inactive_count}件 ({inactive_pct:.1f}%) - 期待値: 25%")

# 最終ログイン: 30% が NULL
login_null = users_df.filter(col("last_login_at").isNull()).count()
login_null_pct = login_null / total_users * 100
print(f"\n● 最終ログインNULL率 (定義: 30%)")
print(f"  NULL: {login_null}件 ({login_null_pct:.1f}%) - 期待値: 30%")

# メール許可フラグ: 35% が true
email_true = users_df.filter(col("email_permission_flg") == True).count()
email_pct = email_true / total_users * 100
print(f"\n● メール許可フラグ (定義: true 35%)")
print(f"  true: {email_true}件 ({email_pct:.1f}%) - 期待値: 35%")

# 行動クラスター: 1:10%, 2:5%, 3:5%, 4:10%, 5:10%, NULL:60%
# ※ behavior_clusterはGold層で購買履歴から算出するためCSVには含めないが、
#    シミュレーション用に内部的に保持して購買パターン生成に使用
print(f"\n● 行動クラスター分布 (内部シミュレーション用、CSVには出力しない)")
print(f"  ※ 購買パターン生成の制御に使用、Gold層で購買履歴から再計算")
cluster_counts = users_df.groupBy("behavior_cluster").count().orderBy("behavior_cluster").collect()
for row in cluster_counts:
    cluster = row["behavior_cluster"] if row["behavior_cluster"] else "NULL"
    cnt = row["count"]
    pct = cnt / total_users * 100
    expected = {"1": 10, "2": 5, "3": 5, "4": 10, "5": 10, "NULL": 60}.get(cluster, 0)
    print(f"  クラスター{cluster}: {cnt}件 ({pct:.1f}%) - 期待値: {expected}%")

# COMMAND ----------

# DBTITLE 1,店舗データ分布検証
print("=" * 60)
print("【店舗データ (stores) の分布検証】")
print("=" * 60)

total_stores = stores_df.count()

# 店舗ステータス: ACTIVE 95%, INACTIVE 5%
active_stores = stores_df.filter(col("status") == "ACTIVE").count()
inactive_stores = stores_df.filter(col("status") == "INACTIVE").count()
active_store_pct = active_stores / total_stores * 100
inactive_store_pct = inactive_stores / total_stores * 100
print(f"\n● 店舗ステータス (定義: ACTIVE 95%, INACTIVE 5%)")
print(f"  ACTIVE: {active_stores}件 ({active_store_pct:.1f}%) - 期待値: 95%")
print(f"  INACTIVE: {inactive_stores}件 ({inactive_store_pct:.1f}%) - 期待値: 5%")

# COMMAND ----------

# DBTITLE 1,注文データ分布検証
print("=" * 60)
print("【注文データ (orders) の分布検証】")
print("=" * 60)

total_orders = orders_df.count()

# 注文ステータス: COMPLETED 92%, CANCELLED 5%, RETURNED 3%
completed = orders_df.filter(col("order_status") == "COMPLETED").count()
cancelled = orders_df.filter(col("order_status") == "CANCELLED").count()
returned = orders_df.filter(col("order_status") == "RETURNED").count()
print(f"\n● 注文ステータス (定義: COMPLETED 92%, CANCELLED 5%, RETURNED 3%)")
print(f"  COMPLETED: {completed}件 ({completed/total_orders*100:.1f}%) - 期待値: 92%")
print(f"  CANCELLED: {cancelled}件 ({cancelled/total_orders*100:.1f}%) - 期待値: 5%")
print(f"  RETURNED: {returned}件 ({returned/total_orders*100:.1f}%) - 期待値: 3%")

# COMMAND ----------

# DBTITLE 1,注文明細データ分布検証
print("=" * 60)
print("【注文明細データ (order_items) の分布検証】")
print("=" * 60)

original_count = order_items_df.count()
with_dup_count = order_items_df_with_dup.count()
dup_count = with_dup_count - original_count
dup_rate = dup_count / original_count * 100

print(f"\n● 重複レコード (定義: 5%)")
print(f"  元データ: {original_count}件")
print(f"  重複追加後: {with_dup_count}件")
print(f"  追加重複: {dup_count}件 ({dup_rate:.1f}%) - 期待値: 5%")

# COMMAND ----------

# DBTITLE 1,クラスター別購買パターン検証
from pyspark.sql.functions import hour, dayofweek

print("=" * 60)
print("【クラスター別購買パターン検証】")
print("=" * 60)

# ordersにuser情報をjoin
orders_with_user = orders_df.join(
    users_df.select("user_id", "behavior_cluster"),
    "user_id"
)

# 曜日別・時間別分析用に列追加
orders_analysis = orders_with_user \
    .withColumn("order_hour", hour("order_datetime")) \
    .withColumn("order_dow", dayofweek("order_datetime"))

print("\n● 購買時間帯の検証")
print("  (定義: 1:10-18時土日, 2:19-21時, 3:9-17時, 4:11-21時, 5:16-18時平日)")

for cluster_id in ['1', '2', '3', '4', '5']:
    cluster_orders = orders_analysis.filter(col("behavior_cluster") == cluster_id)
    if cluster_orders.count() > 0:
        hour_stats = cluster_orders.agg({"order_hour": "min"}).collect()[0][0], \
                     cluster_orders.agg({"order_hour": "max"}).collect()[0][0]
        print(f"  クラスター{cluster_id}: 時間帯 {hour_stats[0]}時〜{hour_stats[1]}時")

print("\n● 週末/平日の検証")
print("  (定義: 1:土日のみ, 5:平日のみ)")

# クラスター1: 土日のみ
cluster1_orders = orders_analysis.filter(col("behavior_cluster") == '1')
if cluster1_orders.count() > 0:
    weekend_orders = cluster1_orders.filter(col("order_dow").isin([1, 7])).count()  # 1=日曜, 7=土曜
    weekend_pct = weekend_orders / cluster1_orders.count() * 100
    print(f"  クラスター1（週末まとめ買い族）: 土日率 {weekend_pct:.1f}% - 期待値: 100%")

# クラスター5: 平日のみ
cluster5_orders = orders_analysis.filter(col("behavior_cluster") == '5')
if cluster5_orders.count() > 0:
    weekday_orders = cluster5_orders.filter(~col("order_dow").isin([1, 7])).count()
    weekday_pct = weekday_orders / cluster5_orders.count() * 100
    print(f"  クラスター5（平日夕方の時短族）: 平日率 {weekday_pct:.1f}% - 期待値: 100%")

# COMMAND ----------

# DBTITLE 1,クラスター別バスケットサイズ検証
print("=" * 60)
print("【クラスター別バスケットサイズ検証】")
print("=" * 60)
print("(定義: 1:15-30点, 2:3-8点, 3:5-12点, 4:2-5点, 5:3-6点, ランダム:1-10点)")
print("※ バスケットサイズ = 1注文あたりの明細数（商品種類数）")

# order_itemsから注文ごとの明細数を計算
order_item_counts = order_items_df.groupBy("order_id").agg(count("*").alias("item_count"))

# ordersとjoinしてクラスター情報を取得
orders_with_item_count = orders_df.join(order_item_counts, "order_id") \
    .join(users_df.select("user_id", "behavior_cluster"), "user_id")

# 各クラスターの定義
expected_basket = {
    '1': '15-30点',
    '2': '3-8点',
    '3': '5-12点',
    '4': '2-5点',
    '5': '3-6点',
    None: '1-10点'
}

for cluster_id in ['1', '2', '3', '4', '5', None]:
    cluster_orders = orders_with_item_count.filter(
        col("behavior_cluster") == cluster_id if cluster_id else col("behavior_cluster").isNull()
    )
    if cluster_orders.count() > 0:
        avg_items = cluster_orders.agg({"item_count": "avg"}).collect()[0][0]
        cluster_name = cluster_id if cluster_id else "ランダム"
        print(f"  クラスター{cluster_name}: 平均 {avg_items:.1f}点 (期待範囲: {expected_basket.get(cluster_id, '?')})")

# COMMAND ----------

# DBTITLE 1,クラスター別カテゴリ選択傾向検証
print("=" * 60)
print("【クラスター別カテゴリ選択傾向検証】")
print("=" * 60)

# order_itemsにuser情報をjoin
order_items_with_user = order_items_df.join(
    users_df.select("user_id", "behavior_cluster"),
    "user_id"
)

# カテゴリ名マッピング
category_names = {
    100: "野菜・果物", 200: "肉類", 300: "魚介類", 400: "乳製品",
    500: "パン", 600: "お菓子", 700: "冷凍食品", 800: "飲料",
    900: "調味料", 1000: "惣菜", 1100: "酒類", 1200: "スイーツ"
}

# 各クラスターの期待カテゴリ
expected_categories = {
    '2': [1000, 500, 700],   # 惣菜、パン、冷凍食品
    '3': [100, 400, 900],    # 野菜、乳製品、調味料
    '4': [1100, 600, 1200],  # 酒類、お菓子、スイーツ
    '5': [1000, 700, 500],   # 惣菜、冷凍食品、パン
}

for cluster_id, expected_cats in expected_categories.items():
    cluster_items = order_items_with_user.filter(col("behavior_cluster") == cluster_id)
    if cluster_items.count() > 0:
        total_items = cluster_items.count()
        target_items = cluster_items.filter(col("category_id").isin(expected_cats)).count()
        target_pct = target_items / total_items * 100
        cat_names = ", ".join([category_names[c] for c in expected_cats])
        print(f"\n● クラスター{cluster_id}: {cat_names}")
        print(f"  対象カテゴリ購入率: {target_pct:.1f}% (期待値: 約75-90%)")

# COMMAND ----------

# DBTITLE 1,クラスター別Recency分布検証
from pyspark.sql.functions import max as spark_max, datediff, to_date, current_date

print("=" * 60)
print("【クラスター別Recency分布検証】")
print("=" * 60)
print("(analysis_design.md セクション12「リピートタイミング分析定義」に基づく)")
print()

# 各ユーザーの最終購買日を計算
user_last_order = orders_df.groupBy("user_id").agg(
    spark_max("order_datetime").alias("last_order_datetime")
).withColumn(
    "recency_days",
    datediff(current_date(), to_date("last_order_datetime"))
)

# ユーザー情報とjoin
user_recency = user_last_order.join(
    users_df.select("user_id", "behavior_cluster"),
    "user_id"
)

# クラスター別の臨界点定義
cluster_thresholds = {
    '1': {'active': 7, 'intervention': 14, 'critical': 21, 'name': '週末まとめ買い族'},
    '2': {'active': 5, 'intervention': 10, 'critical': 14, 'name': '夜型値引きハンター'},
    '3': {'active': 10, 'intervention': 14, 'critical': 21, 'name': '健康・こだわり層'},
    '4': {'active': 7, 'intervention': 10, 'critical': 14, 'name': 'カテゴリジャンキー'},
    '5': {'active': 3, 'intervention': 7, 'critical': 10, 'name': '平日夕方の時短族'},
    None: {'active': 14, 'intervention': 21, 'critical': 30, 'name': 'ランダム層'},
}

for cluster_id, thresholds in cluster_thresholds.items():
    cluster_users = user_recency.filter(
        col("behavior_cluster") == cluster_id if cluster_id else col("behavior_cluster").isNull()
    )
    total = cluster_users.count()
    if total > 0:
        active = cluster_users.filter(col("recency_days") <= thresholds['active']).count()
        intervention = cluster_users.filter(
            (col("recency_days") > thresholds['active']) &
            (col("recency_days") <= thresholds['intervention'])
        ).count()
        critical = cluster_users.filter(
            (col("recency_days") > thresholds['intervention']) &
            (col("recency_days") <= thresholds['critical'])
        ).count()
        churned = cluster_users.filter(col("recency_days") > thresholds['critical']).count()

        avg_recency = cluster_users.agg({"recency_days": "avg"}).collect()[0][0]

        print(f"● クラスター{cluster_id or 'NULL'}: {thresholds['name']}")
        print(f"  平均recency: {avg_recency:.1f}日")
        print(f"  アクティブ（0-{thresholds['active']}日）: {active}人 ({active/total*100:.1f}%)")
        print(f"  要介入（{thresholds['active']+1}-{thresholds['intervention']}日）: {intervention}人 ({intervention/total*100:.1f}%)")
        print(f"  離反直前（{thresholds['intervention']+1}-{thresholds['critical']}日）: {critical}人 ({critical/total*100:.1f}%)")
        print(f"  離反（{thresholds['critical']+1}日以上）: {churned}人 ({churned/total*100:.1f}%)")
        print()

# 全体のRecency分布
print("=" * 60)
print("【全体のRecency分布】")
print("=" * 60)
print("(目標: 0-7日:35%, 8-14日:20%, 15-30日:20%, 31-60日:15%, 61日以上:10%)")
print()

total_users_with_orders = user_recency.count()
recency_0_7 = user_recency.filter(col("recency_days") <= 7).count()
recency_8_14 = user_recency.filter((col("recency_days") > 7) & (col("recency_days") <= 14)).count()
recency_15_30 = user_recency.filter((col("recency_days") > 14) & (col("recency_days") <= 30)).count()
recency_31_60 = user_recency.filter((col("recency_days") > 30) & (col("recency_days") <= 60)).count()
recency_61_plus = user_recency.filter(col("recency_days") > 60).count()

print(f"0-7日: {recency_0_7}人 ({recency_0_7/total_users_with_orders*100:.1f}%) - 目標: 35%")
print(f"8-14日: {recency_8_14}人 ({recency_8_14/total_users_with_orders*100:.1f}%) - 目標: 20%")
print(f"15-30日: {recency_15_30}人 ({recency_15_30/total_users_with_orders*100:.1f}%) - 目標: 20%")
print(f"31-60日: {recency_31_60}人 ({recency_31_60/total_users_with_orders*100:.1f}%) - 目標: 15%")
print(f"61日以上: {recency_61_plus}人 ({recency_61_plus/total_users_with_orders*100:.1f}%) - 目標: 10%")

# COMMAND ----------

# DBTITLE 1,Gold層クラスター判定シミュレーション
from pyspark.sql.functions import sum as _sum, count, when, col, hour, dayofweek

print("=" * 60)
print("【Gold層クラスター判定シミュレーション】")
print("=" * 60)
print("※ 購買履歴からGold層と同じロジックでクラスターを判定した場合の分布予測")
print("※ 期待値: 1:10%, 2:5%, 3:5%, 4:10%, 5:10%, NULL:60%")
print()

# --- Step 1: 注文レベルの統計（曜日、時間帯） ---
order_stats = (
    orders_df
    .withColumn("order_hour", hour(col("order_datetime")))
    .withColumn("order_dayofweek", dayofweek(col("order_datetime")))  # 1=日曜, 7=土曜
    .withColumn("is_weekend", when(dayofweek(col("order_datetime")).isin([1, 7]), 1).otherwise(0))
    .withColumn("is_weekday_evening",  # 平日16-18時
        when(
            (~dayofweek(col("order_datetime")).isin([1, 7])) &
            (hour(col("order_datetime")).between(16, 18)),
            1
        ).otherwise(0)
    )
    .withColumn("is_night",  # 19時以降
        when(hour(col("order_datetime")) >= 19, 1).otherwise(0)
    )
    .groupBy("user_id")
    .agg(
        count("*").alias("total_orders"),
        _sum("is_weekend").alias("weekend_orders"),
        _sum("is_weekday_evening").alias("weekday_evening_orders"),
        _sum("is_night").alias("night_orders"),
        _sum("total_quantity").alias("total_items")
    )
    .withColumn("weekend_rate", col("weekend_orders") / col("total_orders"))
    .withColumn("weekday_evening_rate", col("weekday_evening_orders") / col("total_orders"))
    .withColumn("night_rate", col("night_orders") / col("total_orders"))
    .withColumn("avg_basket_size", col("total_items") / col("total_orders"))
)

# --- Step 2: 商品レベルの統計（カテゴリ、属性） ---
# order_items_df と items_df を結合して商品属性を取得
order_items_with_attrs = (
    order_items_df
    .join(
        items_df.select("item_id", "is_organic", "is_discount_target", "is_ready_to_eat", "is_premium"),
        "item_id",
        "left"
    )
)

item_stats = (
    order_items_with_attrs
    .withColumn("is_category_junkie",  # 酒類(1100)、お菓子(600)、スイーツ(1200)
        when(col("category_id").isin([1100, 600, 1200]), 1).otherwise(0)
    )
    .withColumn("is_organic_premium",  # オーガニック or プレミアム
        when((col("is_organic") == True) | (col("is_premium") == True), 1).otherwise(0)
    )
    .withColumn("is_discount",  # 値引き対象
        when(col("is_discount_target") == True, 1).otherwise(0)
    )
    .withColumn("is_ready_to_eat_item",  # 即食系
        when(col("is_ready_to_eat") == True, 1).otherwise(0)
    )
    .groupBy("user_id")
    .agg(
        count("*").alias("total_items_purchased"),
        _sum("is_category_junkie").alias("category_junkie_items"),
        _sum("is_organic_premium").alias("organic_premium_items"),
        _sum("is_discount").alias("discount_items"),
        _sum("is_ready_to_eat_item").alias("ready_to_eat_items")
    )
    .withColumn("category_junkie_rate", col("category_junkie_items") / col("total_items_purchased"))
    .withColumn("organic_premium_rate", col("organic_premium_items") / col("total_items_purchased"))
    .withColumn("discount_rate", col("discount_items") / col("total_items_purchased"))
    .withColumn("ready_to_eat_rate", col("ready_to_eat_items") / col("total_items_purchased"))
)

# --- Step 3: クラスター判定（Gold層と同じ優先順位） ---
# 優先順位: 5(時短族) > 1(週末族) > 2(夜型) > 3(健康) > 4(カテゴリ) > NULL(ランダム)
simulated_cluster = (
    order_stats
    .join(item_stats, "user_id", "left")
    .withColumn("predicted_cluster",
        when(
            # クラスター5: 平日夕方の時短族
            (col("weekday_evening_rate") >= 0.70) & (col("ready_to_eat_rate") >= 0.60),
            "5"
        ).when(
            # クラスター1: 週末まとめ買い族
            (col("weekend_rate") >= 0.80) & (col("avg_basket_size") >= 10),
            "1"
        ).when(
            # クラスター2: 夜型値引きハンター
            (col("night_rate") >= 0.70) & (col("discount_rate") >= 0.50),
            "2"
        ).when(
            # クラスター3: 健康・こだわり層
            (col("organic_premium_rate") >= 0.60),
            "3"
        ).when(
            # クラスター4: カテゴリジャンキー
            (col("category_junkie_rate") >= 0.70),
            "4"
        ).otherwise(None)  # ランダム層
    )
)

# 結果を集計・表示
total_users_sim = simulated_cluster.count()
cluster_distribution = simulated_cluster.groupBy("predicted_cluster").count().orderBy("predicted_cluster")

print("● Gold層判定によるクラスター分布予測:")
expected_rates = {"1": 10, "2": 5, "3": 5, "4": 10, "5": 10, None: 60}
for row in cluster_distribution.collect():
    cluster = row["predicted_cluster"] if row["predicted_cluster"] else "NULL"
    cnt = row["count"]
    pct = cnt / total_users_sim * 100
    expected = expected_rates.get(row["predicted_cluster"], 60)
    diff = pct - expected
    status = "✓" if abs(diff) < 5 else "△" if abs(diff) < 10 else "×"
    print(f"  クラスター{cluster}: {cnt}人 ({pct:.1f}%) - 期待値: {expected}% {status}")

print()
print("● シミュレーション用クラスター vs Gold層判定の一致率:")
# users_dfのbehavior_clusterと比較
comparison = simulated_cluster.join(
    users_df.select("user_id", col("behavior_cluster").alias("original_cluster")),
    "user_id"
)
match_count = comparison.filter(
    (col("predicted_cluster") == col("original_cluster")) |
    (col("predicted_cluster").isNull() & col("original_cluster").isNull())
).count()
match_rate = match_count / total_users_sim * 100
print(f"  一致: {match_count}人 / {total_users_sim}人 ({match_rate:.1f}%)")
print()
print("※ 一致率が低い場合、購買パターン生成ロジックの調整が必要な可能性があります")

# COMMAND ----------

# DBTITLE 1,売上成長トレンド検証
from pyspark.sql.functions import year, sum as _sum, count, countDistinct

print("=" * 60)
print("【売上成長トレンド検証】")
print("=" * 60)
print("(定義: 5年間で約2倍に成長、年率約15%成長)")
print()

yearly_sales = (
    orders_df
    .withColumn("order_year", year("order_datetime"))
    .groupBy("order_year")
    .agg(
        _sum("total_amount").alias("total_sales"),
        count("order_id").alias("order_count")
    )
    .orderBy("order_year")
    .collect()
)

first_year_sales = yearly_sales[0]["total_sales"]
first_year_orders = yearly_sales[0]["order_count"]

for row in yearly_sales:
    year_val = row["order_year"]
    sales = row["total_sales"]
    orders = row["order_count"]
    growth_ratio = sales / first_year_sales
    print(f"{year_val}年: 売上 {sales:,}円 ({growth_ratio:.2f}倍), 注文 {orders:,}件")

last_year_sales = yearly_sales[-1]["total_sales"]
total_growth = last_year_sales / first_year_sales
print()
print(f"初年度→最終年度 成長率: {total_growth:.2f}倍 (期待値: 約2.0倍)")

# COMMAND ----------

# DBTITLE 1,新規顧客割合の推移検証
from pyspark.sql.functions import date_format, min as _min, when, lit, to_date, datediff, row_number
from pyspark.sql.window import Window

print("=" * 60)
print("【新規顧客割合の推移検証】")
print("=" * 60)
print("(定義: 登録から30日以内の初回注文 = 新規)")
print("(期待値: 初年度約30% → 最終年度約10%)")
print()

# ユーザーの登録日を取得
users_reg = users_df.select(
    col("user_id").alias("reg_user_id"),
    col("registration_date")
)

# 各ユーザーの注文に順番を付与（order_datetimeの昇順で1, 2, 3...）
user_order_window = Window.partitionBy("user_id").orderBy("order_datetime")

# 注文に初回フラグを付与（登録から30日以内の初回注文のみが新規）
orders_with_first_flag = (
    orders_df
    .withColumn("order_rank", row_number().over(user_order_window))
    .withColumn("order_date", to_date("order_datetime"))
    .withColumn("order_year", year("order_datetime"))
    # ユーザーの登録日を結合
    .join(users_reg, orders_df["user_id"] == users_reg["reg_user_id"], "left")
    # 初回注文かつ登録から30日以内 = 新規
    .withColumn("is_first_order",
        when(
            (col("order_rank") == 1) &
            (datediff(col("order_date"), col("registration_date")) <= 30),
            lit(1)
        ).otherwise(lit(0))
    )
)

# 年別の新規割合を計算
yearly_new_ratio = (
    orders_with_first_flag
    .groupBy("order_year")
    .agg(
        count("order_id").alias("total_orders"),
        _sum("is_first_order").alias("new_orders")
    )
    .withColumn("new_ratio_pct", col("new_orders") / col("total_orders") * 100)
    .orderBy("order_year")
    .collect()
)

for row in yearly_new_ratio:
    year_val = row["order_year"]
    total = row["total_orders"]
    new = row["new_orders"]
    ratio = row["new_ratio_pct"]
    print(f"{year_val}年: 新規 {new:,}件 / 全体 {total:,}件 = {ratio:.1f}%")

first_ratio = yearly_new_ratio[0]["new_ratio_pct"]
last_ratio = yearly_new_ratio[-1]["new_ratio_pct"]
print()
print(f"初年度: {first_ratio:.1f}% → 最終年度: {last_ratio:.1f}%")
print(f"期待値: 約30% → 約10%")

# COMMAND ----------

# DBTITLE 1,季節変動検証（月別売上）
from pyspark.sql.functions import month

print("=" * 60)
print("【季節変動検証（月別売上）】")
print("=" * 60)
print("(定義: 12月+30-50%, GW/お盆+15-20%)")
print()

# 直近1年のデータで月別売上を集計
monthly_sales = (
    orders_df
    .withColumn("order_month", month("order_datetime"))
    .groupBy("order_month")
    .agg(_sum("total_amount").alias("total_sales"))
    .orderBy("order_month")
    .collect()
)

avg_monthly = sum(row["total_sales"] for row in monthly_sales) / 12

for row in monthly_sales:
    m = row["order_month"]
    sales = row["total_sales"]
    ratio = sales / avg_monthly
    seasonal_note = ""
    if m == 12:
        seasonal_note = " ← 年末商戦（期待: +30-50%）"
    elif m == 5:
        seasonal_note = " ← GW（期待: +15-20%）"
    elif m == 8:
        seasonal_note = " ← お盆（期待: +15-20%）"
    print(f"{m:2d}月: {sales:>15,}円 (平均比 {ratio:.2f}倍){seasonal_note}")

# COMMAND ----------

print('サンプルデータ生成が完了しました！')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証結果サマリー
# MAGIC
# MAGIC 上記の検証結果を確認し、各分布がanalysis_design.mdの定義通りになっていることを確認してください。
# MAGIC
# MAGIC | 項目 | 期待分布 | 許容誤差 |
# MAGIC |------|---------|---------|
# MAGIC | 性別 | 男性35%, 女性65% | ±5% |
# MAGIC | 都道府県NULL | 15% | ±3% |
# MAGIC | 会員ステータス | active 75%, inactive 25% | ±5% |
# MAGIC | 最終ログインNULL | 30% | ±5% |
# MAGIC | メール許可 | true 35% | ±5% |
# MAGIC | クラスター分布 | 定義通り | ±3% |
# MAGIC | 店舗ステータス | ACTIVE 95%, INACTIVE 5% | ±3% |
# MAGIC | 注文ステータス | COMPLETED 92%, CANCELLED 5%, RETURNED 3% | ±3% |
# MAGIC | 重複レコード | 5% | ±1% |
# MAGIC | Recency分布 | セクション12の目標分布 | ±10% |
# MAGIC | 売上成長（5年） | 約2.0倍 | ±0.3倍 |
# MAGIC | 新規割合（初年度） | 約30% | ±5% |
# MAGIC | 新規割合（最終年度） | 約10% | ±3% |
# MAGIC | 季節変動（12月） | +30-50% | ±10% |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 次のステップ
# MAGIC
# MAGIC CSVファイルの出力が完了しました。次は **02_ETL_bronze** ノートブックで、これらのCSVファイルをBronzeテーブルとして取り込みます。
