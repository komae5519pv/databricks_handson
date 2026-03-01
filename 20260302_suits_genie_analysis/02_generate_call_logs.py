# Databricks notebook source
# MAGIC %md
# MAGIC # 通話ログ生成（5,000件）
# MAGIC サーバレスコンピュートで実行してください。
# MAGIC
# MAGIC リアルなコールセンターログをJSONL形式で生成し、Volumeに保存します。
# MAGIC
# MAGIC **10シナリオ:**
# MAGIC | # | シナリオ | 比率 |
# MAGIC |---|---------|------|
# MAGIC | 1 | 割引・クーポンのクレーム | 20% |
# MAGIC | 2 | 返品・交換手続き | 20% |
# MAGIC | 3 | 配送・納期の問い合わせ | 15% |
# MAGIC | 4 | 注文変更・キャンセル | 8% |
# MAGIC | 5 | 在庫確認・取り寄せ | 8% |
# MAGIC | 6 | 商品不良・破損の申告 | 8% |
# MAGIC | 7 | 他社比較 | 5% |
# MAGIC | 8 | お直し（裾上げ）の問い合わせ | 8% |
# MAGIC | 9 | 請求・支払いトラブル | 4% |
# MAGIC | 10 | お手入れ・メンテナンス相談 | 4% |
# MAGIC
# MAGIC **データ相関設計:**
# MAGIC - 割引クレーム → LINE経由の顧客に集中（デモ核心）
# MAGIC - 解決済み通話 → リピート顧客から優先選出（Wow: 解決率×リピート）
# MAGIC - 急ぎ対応 → 高単価顧客から優先選出（Wow: 急ぎ×高単価）

# COMMAND ----------

# DBTITLE 1,変数設定
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,共通インポート・定数定義
import json
import random
from datetime import date, timedelta, datetime
from collections import Counter

random.seed(123)

TODAY = date(2026, 3, 1)
DATE_START = TODAY - timedelta(days=365)
NUM_CALLS = 5000

# COMMAND ----------

# DBTITLE 1,顧客・注文データの読み込み（相関設計用）
from pyspark.sql import functions as F

# 顧客情報
customer_rows = spark.table(f"{catalog}.{schema}.bz_customers").select(
    "customer_id", "customer_name", "age", "gender", "prefecture"
).collect()

customer_map = {
    row.customer_id: {
        "name": row.customer_name,
        "age": row.age,
        "gender": row.gender,
        "prefecture": row.prefecture,
    }
    for row in customer_rows
}
customer_ids = list(customer_map.keys())

# 顧客別の注文統計（相関仕込み用）
order_stats_rows = (
    spark.table(f"{catalog}.{schema}.bz_orders")
    .groupBy("customer_id")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_spend"),
        F.avg("total_amount").alias("avg_amount"),
        F.sum(F.when(F.col("returned"), 1).otherwise(0)).alias("return_count"),
    )
    .collect()
)
order_stats = {
    row.customer_id: {
        "order_count": row.order_count,
        "total_spend": row.total_spend,
        "avg_amount": row.avg_amount,
        "return_count": row.return_count,
    }
    for row in order_stats_rows
}

# 顧客別の主要接触チャネル
exposure_rows = (
    spark.table(f"{catalog}.{schema}.bz_campaign_exposures")
    .select("customer_id", "channel")
    .collect()
)
customer_channels_list = {}
for row in exposure_rows:
    customer_channels_list.setdefault(row.customer_id, []).append(row.channel)

def get_primary_channel(cid):
    chs = customer_channels_list.get(cid, [])
    return Counter(chs).most_common(1)[0][0] if chs else "なし"

# 相関用: 顧客をセグメント化
line_customers = [cid for cid in customer_ids if get_primary_channel(cid) == "LINE"]
line_return_customers = [cid for cid in line_customers
                         if order_stats.get(cid, {}).get("return_count", 0) > 0]
high_spend_customers = [cid for cid in customer_ids
                        if order_stats.get(cid, {}).get("avg_amount", 0) > 30000]
repeat_customers = [cid for cid in customer_ids
                    if order_stats.get(cid, {}).get("order_count", 0) >= 3]
low_order_customers = [cid for cid in customer_ids
                       if order_stats.get(cid, {}).get("order_count", 0) <= 1]

print(f"全顧客: {len(customer_ids)}")
print(f"LINE顧客: {len(line_customers)}")
print(f"LINE+返品顧客: {len(line_return_customers)}")
print(f"高単価顧客: {len(high_spend_customers)}")
print(f"リピート顧客(3件以上): {len(repeat_customers)}")
print(f"低注文顧客(1件以下): {len(low_order_customers)}")

# COMMAND ----------

# DBTITLE 1,共通定数
PRODUCTS = {
    "スーツ": ["ビジネススーツ", "リクルートスーツ", "ストレッチスーツ", "ウォッシャブルスーツ", "フォーマルスーツ", "スリムフィットスーツ"],
    "シャツ": ["ワイシャツ", "ノンアイロンシャツ", "形態安定シャツ"],
    "ネクタイ": ["シルクネクタイ", "ニットタイ"],
    "ベルト": ["ビジネスベルト", "リバーシブルベルト"],
    "シューズ": ["ビジネスシューズ"],
    "コート": ["ステンカラーコート", "トレンチコート", "チェスターコート"],
}
COLORS = ["ネイビー", "チャコール", "ブラック", "グレー"]
SIZES = ["S", "M", "L", "LL", "3L"]
STORES = ["新宿店", "渋谷店", "池袋店", "横浜店", "大阪梅田店", "名古屋店", "博多店", "札幌店"]
OPERATOR_NAMES = ["山田", "鈴木", "佐藤", "田中", "高橋", "伊藤", "渡辺", "中村"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## シナリオ別 transcript 生成

# COMMAND ----------

# DBTITLE 1,シナリオ1: 割引・クーポンのクレーム（20%）
def gen_discount_complaint():
    product = random.choice(PRODUCTS["スーツ"])
    seen_discount = random.choice([20, 25, 30])
    actual_discount = seen_discount - random.choice([5, 10])
    channel = random.choice(["LINE", "Webサイト", "アプリの通知"])
    min_amount = random.choice([20000, 30000, 50000])
    op = random.choice(OPERATOR_NAMES)

    transcripts = [
        f"""オペレーター（{op}）：お電話ありがとうございます。担当の{op}でございます。本日はどのようなご用件でしょうか。
お客様：すみません、{channel}で{seen_discount}%オフのキャンペーンを見て{product}を注文したんですが、届いた明細を見たら{actual_discount}%しか割引されてなくて。
オペレーター（{op}）：大変申し訳ございません。確認いたしますので、少々お待ちください。
お客様：はい。
オペレーター（{op}）：お待たせいたしました。確認いたしましたところ、{channel}のキャンペーンは税込{min_amount:,}円以上のお買い上げが条件となっておりまして、今回のご注文金額が条件に満たなかったため{actual_discount}%の適用となっております。
お客様：えっ、そうなんですか？広告にはそんな条件書いてなかったと思うんですけど…
オペレーター（{op}）：ご不便をおかけして申し訳ございません。広告の表記については社内で確認させていただきます。
お客様：それだと話が違いますよね。{seen_discount}%オフだと思って買ったのに。
オペレーター（{op}）：おっしゃる通りでございます。今回は特別に{seen_discount}%割引を適用させていただきます。差額分は返金処理いたします。
お客様：それならお願いします。でも今後はわかりやすく表示してほしいです。
オペレーター（{op}）：承知いたしました。貴重なご意見ありがとうございます。他にご不明点はございますか。
お客様：大丈夫です。ありがとうございました。
オペレーター（{op}）：お電話ありがとうございました。失礼いたします。""",

        f"""オペレーター（{op}）：お電話ありがとうございます。{op}が承ります。
お客様：あの、先日{channel}の広告で{product}の{seen_discount}%割引って書いてあったので買ったんですけど、請求額が思ってたより高いんです。
オペレーター（{op}）：ご迷惑をおかけして申し訳ございません。お調べいたしますので、ご注文番号をお伺いしてもよろしいでしょうか。
お客様：番号がすぐ出てこないです。名前で調べてもらえますか。
オペレーター（{op}）：かしこまりました。お名前とご登録のお電話番号をお願いいたします。
お客様：（名前と電話番号を伝える）
オペレーター（{op}）：確認できました。こちらのキャンペーンですが、{seen_discount}%割引は{min_amount:,}円以上のまとめ買いが対象で、今回の単品ご購入には{actual_discount}%が適用されております。
お客様：それは知らなかったです。広告にはっきり書いてありましたか？
オペレーター（{op}）：小さい文字で注記がございましたが、わかりにくい表記だったかと思います。大変申し訳ございません。差額の返金対応をさせていただきます。
お客様：お願いします。
オペレーター（{op}）：かしこまりました。3〜5営業日でお戻しいたします。
お客様：わかりました。ありがとうございます。
オペレーター（{op}）：こちらこそ、ご不便をおかけして申し訳ございませんでした。失礼いたします。""",
    ]

    return {
        "inquiry_type": "苦情",
        "inquiry_detail": f"{channel}で表示された割引率（{seen_discount}%OFF）と実際の適用率（{actual_discount}%OFF）が異なる",
        "transcript": random.choice(transcripts),
        "operator_action": f"割引条件の確認・説明。{channel}での広告表記と実際の適用条件の差異を確認",
        "proposed_solution": f"差額分の返金処理（{seen_discount}%割引を特別適用）",
        "info_provided": f"キャンペーン適用条件（税込{min_amount:,}円以上が対象）",
        "resolved": True,
        "follow_up_required": False,
    }

# COMMAND ----------

# DBTITLE 1,シナリオ2: 返品・交換手続き（20%）
def gen_return_exchange():
    product = random.choice(PRODUCTS["スーツ"] + PRODUCTS["シャツ"])
    reason = random.choice(["サイズが合わない", "色がイメージと違う", "生地の感触が思ったのと違う"])
    days_ago = random.randint(2, 10)
    op = random.choice(OPERATOR_NAMES)
    tag_on = random.random() < 0.7

    if tag_on:
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：すみません、{days_ago}日前に買った{product}なんですけど、{reason}ので交換か返品をしたいんです。
オペレーター（{op}）：かしこまりました。当店では商品到着後14日以内、タグ未開封の状態でしたら返品・交換を承っております。タグはお付けのままでしょうか。
お客様：はい、一回着てみただけでタグはまだついてます。
オペレーター（{op}）：それでしたら交換または返品が可能です。交換の場合はサイズ変更のみ無料で承れます。返品の場合は返送料のみご負担いただく形になります。
お客様：交換でお願いします。ワンサイズ上はありますか。
オペレーター（{op}）：確認いたします。…はい、在庫ございます。着払いの返送用伝票をメールでお送りしますので、商品をご返送ください。新しい商品は到着確認後、2〜3営業日でお届けいたします。
お客様：わかりました。ありがとうございました。
オペレーター（{op}）：お電話ありがとうございました。失礼いたします。"""
    else:
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：先日届いた{product}を返品したいんですが。{reason}んです。
オペレーター（{op}）：ご不便をおかけして申し訳ございません。タグは付いたままでしょうか。
お客様：タグは外してしまいました…。返品は無理ですか。
オペレーター（{op}）：大変申し訳ございません。タグを外された場合は原則として返品・交換はお受けできないのですが、到着後間もないということですので、上長に確認させていただきます。少々お待ちください。
お客様：お願いします…。
オペレーター（{op}）：お待たせいたしました。今回は特例として交換のみ対応させていただきます。
お客様：ありがとうございます。助かります。
オペレーター（{op}）：かしこまりました。交換手続きをメールでご案内いたします。
お客様：ありがとうございました。
オペレーター（{op}）：お電話ありがとうございました。失礼いたします。"""

    return {
        "inquiry_type": "要望",
        "inquiry_detail": f"{product}の返品・交換希望（理由：{reason}、購入から{days_ago}日経過）",
        "transcript": transcript,
        "operator_action": f"返品・交換ポリシーの確認。{'通常の交換手続き案内' if tag_on else '上長確認の上、特例交換対応'}",
        "proposed_solution": f"{'サイズ交換（無料）' if tag_on else '特例としてサイズ/カラー交換対応'}",
        "info_provided": "返品・交換ポリシー（14日以内・タグ付き）、返送手続き、到着目安",
        "resolved": True,
        "follow_up_required": False,
    }

# COMMAND ----------

# DBTITLE 1,シナリオ3: 配送・納期の問い合わせ（15%）
def gen_delivery_inquiry():
    op = random.choice(OPERATOR_NAMES)
    scenario_type = random.choices(
        ["urgent", "delay", "wrong_address"],
        weights=[0.4, 0.4, 0.2], k=1
    )[0]

    if scenario_type == "urgent":
        urgency = random.choice(["面接", "結婚式", "葬儀", "急な出張", "入社式"])
        days_until = random.randint(1, 5)
        product = random.choice(["ビジネススーツ", "リクルートスーツ", "フォーマルスーツ"])
        store = random.choice(STORES)
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：すみません、急いでるんですが、{days_until}日後に{urgency}がありまして、{product}を至急手に入れたいんです。ECで注文したら間に合いますか。
オペレーター（{op}）：お急ぎなんですね。通常配送ですと3〜5営業日ほどかかりますので、{days_until}日後ですと少し難しいかもしれません。
お客様：やっぱりそうですか…。何か方法はないですか。
オペレーター（{op}）：お近くの店舗でしたら在庫があればすぐにお渡しできます。{store}でしたら{product}の在庫を確認できますが、いかがでしょうか。
お客様：{store}ですか。在庫はありますか。
オペレーター（{op}）：今確認いたします。…はい、在庫がございます。裾上げも最短で当日お渡し可能です。
お客様：本当ですか！助かります。今日中に行きます。
オペレーター（{op}）：{urgency}のご成功をお祈りしております。お待ちしております。"""
        detail = f"{urgency}（{days_until}日後）に向けた{product}の緊急購入相談"
        action = f"在庫確認、{store}での当日受取りを手配"
        solution = f"{store}での当日受取り（裾上げ含む）"
        info = f"{store}の在庫状況、当日裾上げ対応可否"
    elif scenario_type == "delay":
        days_ago = random.randint(5, 10)
        product = random.choice(PRODUCTS["スーツ"] + PRODUCTS["コート"])
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：{days_ago}日前にオンラインで{product}を注文したんですけど、まだ届かないんです。追跡番号を見ても「出荷準備中」から変わらなくて。
オペレーター（{op}）：お待たせしてしまい大変申し訳ございません。ご注文番号を確認させていただけますか。
お客様：ORD…（注文番号を伝える）
オペレーター（{op}）：確認いたしました。倉庫での出荷処理に遅延が発生しておりまして、本日中に出荷予定となっております。到着は明後日の見込みです。
お客様：あと2日かかるんですか…。来週使いたかったので、まだ間に合いますけどちょっと不安です。
オペレーター（{op}）：ご不便をおかけして申し訳ございません。出荷が完了しましたらメールでお知らせいたします。万が一届かない場合は再度ご連絡ください。
お客様：わかりました。
オペレーター（{op}）：お急ぎのところ申し訳ございません。お電話ありがとうございました。"""
        detail = f"{product}の注文から{days_ago}日経過も未着。追跡ステータスが更新されない"
        action = "配送状況の確認。倉庫での出荷遅延を確認し、到着見込みを案内"
        solution = f"出荷遅延の説明と到着目安（2日後）の案内"
        info = "配送ステータス、到着見込み日"
    else:
        product = random.choice(PRODUCTS["スーツ"])
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：すみません、さっき注文を確定したんですけど、届け先の住所を間違えてしまいまして。変更できますか。
オペレーター（{op}）：かしこまりました。まだ出荷前でしたら変更可能です。ご注文番号をお伺いできますか。
お客様：（注文番号を伝える）
オペレーター（{op}）：確認いたしました。まだ出荷前ですので、届け先の変更が可能です。正しいご住所をお願いいたします。
お客様：（正しい住所を伝える）
オペレーター（{op}）：かしこまりました。届け先を変更いたしました。配送予定日に変更はございません。
お客様：助かりました。ありがとうございます。
オペレーター（{op}）：お電話ありがとうございました。失礼いたします。"""
        detail = "注文確定後の届け先住所の変更依頼"
        action = "出荷前の届け先住所変更処理"
        solution = "届け先住所の変更完了"
        info = "変更可能条件（出荷前であること）、配送予定日"

    return {
        "inquiry_type": "相談" if scenario_type == "urgent" else "確認",
        "inquiry_detail": detail,
        "transcript": transcript,
        "operator_action": action,
        "proposed_solution": solution,
        "info_provided": info,
        "resolved": True,
        "follow_up_required": scenario_type == "delay",
    }

# COMMAND ----------

# DBTITLE 1,シナリオ4: 注文変更・キャンセル（8%）
def gen_order_change():
    op = random.choice(OPERATOR_NAMES)
    product = random.choice(PRODUCTS["スーツ"] + PRODUCTS["シャツ"])
    is_cancel = random.random() < 0.4

    if is_cancel:
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：先ほど{product}を注文したんですけど、やっぱりキャンセルしたいんです。
オペレーター（{op}）：かしこまりました。ご注文番号をお伺いできますか。
お客様：（注文番号を伝える）
オペレーター（{op}）：確認いたしました。まだ出荷前ですのでキャンセルが可能です。キャンセル処理を行ってよろしいでしょうか。
お客様：はい、お願いします。他の店で似たようなものを見つけてしまって。
オペレーター（{op}）：かしこまりました。キャンセル処理が完了いたしました。返金は3〜5営業日でお戻しいたします。
お客様：ありがとうございます。
オペレーター（{op}）：またのご利用をお待ちしております。失礼いたします。"""
        detail = f"{product}の注文キャンセル依頼"
    else:
        new_color = random.choice(COLORS)
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：昨日注文した{product}なんですが、色を{new_color}に変更したいんですけど、まだ間に合いますか。
オペレーター（{op}）：確認いたします。ご注文番号をお願いできますか。
お客様：（注文番号を伝える）
オペレーター（{op}）：確認いたしました。まだ出荷前ですので変更可能です。{product}（{new_color}）に変更でよろしいでしょうか。
お客様：はい、お願いします。
オペレーター（{op}）：かしこまりました。変更処理が完了いたしました。配送予定日に変更はございません。
お客様：よかったです。ありがとうございます。
オペレーター（{op}）：お電話ありがとうございました。失礼いたします。"""
        detail = f"{product}の色変更依頼（→{new_color}）"

    return {
        "inquiry_type": "要望",
        "inquiry_detail": detail,
        "transcript": transcript,
        "operator_action": "出荷前の注文変更/キャンセル処理",
        "proposed_solution": "キャンセル処理・返金" if is_cancel else "色変更処理",
        "info_provided": "変更可能条件（出荷前であること）、返金時期" if is_cancel else "変更可能条件、配送予定日",
        "resolved": True,
        "follow_up_required": False,
    }

# COMMAND ----------

# DBTITLE 1,シナリオ5: 在庫確認・取り寄せ（8%）
def gen_stock_inquiry():
    product = random.choice(PRODUCTS["スーツ"] + PRODUCTS["コート"])
    color = random.choice(COLORS)
    size = random.choice(SIZES)
    store = random.choice(STORES)
    alt_store = random.choice([s for s in STORES if s != store])
    op = random.choice(OPERATOR_NAMES)
    has_stock = random.random() < 0.6

    if has_stock:
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：オンラインストアで{product}（{color}）の{size}サイズが在庫切れなんですが、{store}にはありますか。
オペレーター（{op}）：確認いたします。少々お待ちください。…{product}（{color}）の{size}サイズ、{store}に在庫がございます。
お客様：よかった。取り置きできますか。
オペレーター（{op}）：はい、3日間のお取り置きが可能です。
お客様：明日行きます。ありがとうございます。
オペレーター（{op}）：ご来店をお待ちしております。失礼いたします。"""
    else:
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：ECで{product}（{color}）の{size}サイズが品切れなんですけど、{store}に在庫ありますか。
オペレーター（{op}）：確認いたします。…申し訳ございません、{store}でも品切れです。
お客様：他の店舗にはありますか。
オペレーター（{op}）：お調べいたします。…{alt_store}には在庫がございます。{store}へのお取り寄せも可能ですが、2〜3日ほどかかります。
お客様：取り寄せてもらえるなら{store}でお願いします。
オペレーター（{op}）：かしこまりました。到着次第ご連絡差し上げます。
お客様：ありがとうございました。
オペレーター（{op}）：失礼いたします。"""

    return {
        "inquiry_type": "確認",
        "inquiry_detail": f"{product}（{color}/{size}）の{store}在庫確認",
        "transcript": transcript,
        "operator_action": f"店舗在庫の確認。{'取り置き手配' if has_stock else f'{alt_store}からの取り寄せ手配'}",
        "proposed_solution": f"{'3日間の取り置き' if has_stock else f'{alt_store}から{store}への取り寄せ（2〜3日）'}",
        "info_provided": f"{'在庫あり、取り置き期間' if has_stock else f'{store}品切れ、{alt_store}在庫あり、取り寄せ日数'}",
        "resolved": True,
        "follow_up_required": not has_stock,
    }

# COMMAND ----------

# DBTITLE 1,シナリオ6: 商品不良・破損の申告（8%）
def gen_defect_report():
    # 特定商品に不良を集中させる（品質問題パターン）
    defect_products = random.choices(
        ["ストレッチスーツ", "ウォッシャブルスーツ", "ノンアイロンシャツ", "ビジネスシューズ"],
        weights=[0.35, 0.30, 0.20, 0.15], k=1
    )[0]
    defect = random.choice(["ほつれがある", "ボタンが取れている", "縫い目がほどけている", "ファスナーが壊れている", "シミがついている"])
    days_ago = random.randint(1, 7)
    op = random.choice(OPERATOR_NAMES)

    transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：{days_ago}日前に届いた{defect_products}なんですが、{defect}んです。新品なのにこれはちょっと…
オペレーター（{op}）：大変申し訳ございません。不良品をお届けしてしまい、お詫び申し上げます。状態を確認させてください。具体的にどの部分でしょうか。
お客様：（不良箇所を説明する）
オペレーター（{op}）：ありがとうございます。不良品の交換手続きをさせていただきます。送料は当然こちらで負担いたします。
お客様：同じ商品の新品と交換してもらえますか。
オペレーター（{op}）：はい、同じ商品の新品をお送りいたします。返送用の伝票をメールでお送りしますので、不良品をご返送ください。新品は最短で翌日発送いたします。
お客様：わかりました。初めてこういうことがあったのでちょっとびっくりしました。
オペレーター（{op}）：重ねてお詫び申し上げます。品質管理を強化してまいります。他にご不明点はございますか。
お客様：大丈夫です。
オペレーター（{op}）：お電話ありがとうございました。失礼いたします。"""

    return {
        "inquiry_type": "苦情",
        "inquiry_detail": f"{defect_products}の商品不良（{defect}、購入から{days_ago}日）",
        "transcript": transcript,
        "operator_action": "不良品の状態確認。無料交換手続きの案内",
        "proposed_solution": f"同一商品の新品と無料交換（送料当社負担）",
        "info_provided": "返送手続き、新品発送目安（翌日）",
        "resolved": True,
        "follow_up_required": False,
    }

# COMMAND ----------

# DBTITLE 1,シナリオ7: 他社比較（5%）
def gen_competitor_comparison():
    product = random.choice(PRODUCTS["スーツ"])
    price = random.choice([29800, 34800, 39800])
    competitor_price = price - random.choice([2000, 3000, 5000])
    op = random.choice(OPERATOR_NAMES)

    transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：正直に言うと、同じような{product}が他のお店で{competitor_price:,}円くらいで売ってるんです。御社のは{price:,}円ですよね。この差はなんですか。
オペレーター（{op}）：ご検討いただきありがとうございます。確かに価格差がございますが、当店の{product}は国産生地を使用しておりまして、耐久性やシルエットに自信がございます。
お客様：生地の質が違うということですか。
オペレーター（{op}）：はい。また購入後の裾上げやボタン付けなどのアフターサービスも無料です。
お客様：アフターサービスがあるのはいいですね。他のところだと有料ですから。
オペレーター（{op}）：さらに、現在まとめ買い割引フェアで3点以上のお買い上げで15%オフとなっております。
お客様：そうですか。ちょっと検討してみます。
オペレーター（{op}）：ぜひご検討ください。店舗でも同じキャンペーンを実施しております。
お客様：わかりました。ありがとうございます。
オペレーター（{op}）：お電話ありがとうございました。"""

    return {
        "inquiry_type": "質問",
        "inquiry_detail": f"{product}の価格・品質について他店との比較相談",
        "transcript": transcript,
        "operator_action": "品質・アフターサービスの優位性を説明。まとめ買いキャンペーンを案内",
        "proposed_solution": "まとめ買い割引フェアの提案（3点以上15%OFF）",
        "info_provided": "素材・品質の違い、アフターサービス内容、まとめ買い割引条件",
        "resolved": True,
        "follow_up_required": False,
    }

# COMMAND ----------

# DBTITLE 1,シナリオ8: お直し（裾上げ）の問い合わせ（8%）
def gen_alteration_inquiry():
    op = random.choice(OPERATOR_NAMES)
    is_complaint = random.random() < 0.4

    if is_complaint:
        product = random.choice(["ビジネススーツ", "フォーマルスーツ", "スリムフィットスーツ"])
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：先日店舗で{product}を購入して裾上げをお願いしたんですけど、仕上がりが短すぎるんです。
オペレーター（{op}）：大変申し訳ございません。お直しの仕上がりにご満足いただけなかったとのこと、お詫び申し上げます。
お客様：お店で測ってもらって注文したのに、明らかに短いんですよ。仕事で使うのに困ります。
オペレーター（{op}）：ご不便をおかけして申し訳ございません。再度お直しを無料で対応させていただきます。お近くの店舗にお持ちいただければ、最短で当日お渡し可能です。
お客様：もう一度持っていかないといけないのは面倒ですけど…仕方ないですね。
オペレーター（{op}）：ご面倒をおかけして重ねてお詫びいたします。お越しの際は{op}宛とお伝えいただければスムーズにご案内いたします。
お客様：わかりました。
オペレーター（{op}）：お電話ありがとうございました。失礼いたします。"""
        detail = f"{product}の裾上げ仕上がりが短すぎる（再直し希望）"
        action = "裾上げ仕上がり不備のお詫び。無料再直しの案内"
        solution = "店舗での無料再直し（最短当日仕上げ）"
    else:
        product = random.choice(PRODUCTS["スーツ"])
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：オンラインで{product}を買おうと思ってるんですけど、裾上げってできますか。
オペレーター（{op}）：はい、オンラインでご購入いただいた商品もお近くの店舗にお持ちいただければ裾上げが可能です。
お客様：料金はいくらですか。
オペレーター（{op}）：シングル仕上げで1,100円、ダブル仕上げで1,650円です。ご購入時にまとめ買い割引が適用されている場合、裾上げも無料になります。
お客様：期間はどのくらいかかりますか。
オペレーター（{op}）：通常3〜5日ですが、お急ぎの場合は最短当日仕上げも可能です。
お客様：わかりました。注文してから店舗に持っていきます。
オペレーター（{op}）：かしこまりました。ご不明点がございましたらお気軽にお問い合わせください。失礼いたします。"""
        detail = f"オンライン購入{product}の裾上げ方法・料金・期間の問い合わせ"
        action = "裾上げ料金・期間・店舗持ち込み手順の案内"
        solution = "店舗持ち込みでの裾上げ案内"

    return {
        "inquiry_type": "苦情" if is_complaint else "質問",
        "inquiry_detail": detail,
        "transcript": transcript,
        "operator_action": action,
        "proposed_solution": solution,
        "info_provided": "裾上げ料金（シングル1,100円/ダブル1,650円）、所要期間（通常3〜5日/最短当日）",
        "resolved": True,
        "follow_up_required": is_complaint,
    }

# COMMAND ----------

# DBTITLE 1,シナリオ9: 請求・支払いトラブル（4%）
def gen_billing_issue():
    op = random.choice(OPERATOR_NAMES)
    issue_type = random.choice(["double_charge", "wrong_amount", "receipt"])

    if issue_type == "double_charge":
        amount = random.choice([29800, 34800, 39800])
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：クレジットカードの明細を見たら、同じ金額で{amount:,}円が2回引き落とされてるんですけど。
オペレーター（{op}）：大変申し訳ございません。二重引き落としの可能性がございます。確認いたしますので、ご注文番号とカード下4桁をお伺いできますか。
お客様：（情報を伝える）
オペレーター（{op}）：確認いたしました。システム上、二重決済が発生しておりました。誠に申し訳ございません。直ちに返金処理を行います。
お客様：いつ頃戻りますか。
オペレーター（{op}）：カード会社の処理期間にもよりますが、5〜10営業日でお戻しいたします。
お客様：わかりました。次回から気をつけてください。
オペレーター（{op}）：ご迷惑をおかけして申し訳ございませんでした。失礼いたします。"""
        detail = f"クレジットカードの二重引き落とし（{amount:,}円×2）"
        solution = f"二重決済分（{amount:,}円）の返金処理（5〜10営業日）"
    elif issue_type == "wrong_amount":
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：先日の注文なんですが、割引が適用された金額で注文したはずなのに、明細を見たら定価で引き落とされてるみたいなんです。
オペレーター（{op}）：ご確認ありがとうございます。お調べいたしますので、ご注文番号をお願いいたします。
お客様：（注文番号を伝える）
オペレーター（{op}）：確認いたしました。ご注文時には15%割引が適用されておりますが、カード明細への反映にタイムラグがある場合がございます。最終的な請求額は割引適用後の金額になりますのでご安心ください。
お客様：そうなんですか。じゃあ大丈夫なんですね。
オペレーター（{op}）：はい、ご安心ください。最終確定額は注文確認メールに記載の金額と同じです。
お客様：わかりました。ありがとうございます。
オペレーター（{op}）：お電話ありがとうございました。失礼いたします。"""
        detail = "カード明細の請求額と注文時の割引適用額が異なるように見える"
        solution = "請求タイムラグの説明（最終請求額は割引適用後）"
    else:
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：先月の購入分の領収書を発行してほしいんですけど、オンラインでやり方がわからなくて。
オペレーター（{op}）：かしこまりました。マイページの注文履歴から「領収書発行」ボタンで発行できますが、お電話でも対応可能です。宛名をお伺いできますか。
お客様：（宛名を伝える）
オペレーター（{op}）：かしこまりました。PDFの領収書をご登録メールアドレスにお送りいたします。
お客様：ありがとうございます。助かりました。
オペレーター（{op}）：お電話ありがとうございました。失礼いたします。"""
        detail = "領収書の発行依頼"
        solution = "PDF領収書をメール送付"

    return {
        "inquiry_type": "苦情" if issue_type == "double_charge" else "確認",
        "inquiry_detail": detail,
        "transcript": transcript,
        "operator_action": "請求内容の確認・説明",
        "proposed_solution": solution,
        "info_provided": "請求処理の仕組み、返金時期" if issue_type != "receipt" else "領収書発行方法",
        "resolved": True,
        "follow_up_required": issue_type == "double_charge",
    }

# COMMAND ----------

# DBTITLE 1,シナリオ10: お手入れ・メンテナンス相談（4%）
def gen_care_inquiry():
    op = random.choice(OPERATOR_NAMES)
    care_type = random.choice(["wash", "stain", "storage", "shine"])

    if care_type == "wash":
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：先日購入したウォッシャブルスーツなんですが、本当に洗濯機で洗って大丈夫なんですか？高い買い物だったので失敗したくなくて。
オペレーター（{op}）：ご心配ですよね。ウォッシャブルスーツは洗濯機でお洗濯いただけます。洗濯ネットに入れて、おしゃれ着洗いコースで洗っていただければ問題ございません。
お客様：水温とか洗剤は何がいいですか。
オペレーター（{op}）：水温は30度以下の冷水がおすすめです。洗剤は中性洗剤（おしゃれ着用）をお使いください。柔軟剤は生地を傷める場合がありますのでお控えください。
お客様：乾燥機は使えますか。
オペレーター（{op}）：乾燥機は型崩れの原因になりますので、陰干しをおすすめいたします。ハンガーにかけて形を整えていただければ、アイロンなしでもシワが伸びます。
お客様：わかりました。丁寧にありがとうございます。
オペレーター（{op}）：お役に立てて嬉しいです。お電話ありがとうございました。"""
        detail = "ウォッシャブルスーツの洗濯方法・注意点の問い合わせ"
    elif care_type == "stain":
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：すみません、今朝スーツにコーヒーをこぼしてしまって…明日大事な会議があるんですけど、どうしたらいいですか。
オペレーター（{op}）：お急ぎですね。コーヒーのシミでしたら、まず乾いたタオルで軽く押さえて水分を吸い取ってください。こすらないのがポイントです。
お客様：それはやりました。でもまだ跡が残ってて。
オペレーター（{op}）：その場合、水で薄めた中性洗剤をタオルに含ませて、シミの外側から内側に向かって軽くたたいてください。その後、水拭きで洗剤を取り除きます。
お客様：それで落ちますかね…
オペレーター（{op}）：軽いシミでしたら落ちることが多いです。難しい場合はクリーニング店にお持ちください。明日の会議に間に合わせるなら、お急ぎクリーニングのご利用もおすすめです。
お客様：わかりました、やってみます。ありがとうございます。
オペレーター（{op}）：お電話ありがとうございました。うまくいくことを祈っております。"""
        detail = "スーツのコーヒーシミの応急処置方法の問い合わせ（翌日会議あり）"
    elif care_type == "storage":
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：冬物のコートをしまいたいんですけど、保管方法がわからなくて。防虫剤とか必要ですか。
オペレーター（{op}）：シーズンオフの保管ですね。まずクリーニングに出してから保管していただくのがベストです。汚れが残ったまま保管すると虫食いやカビの原因になります。
お客様：クリーニングの後はどうすればいいですか。
オペレーター（{op}）：クリーニング後のビニールカバーは外してください。通気性のある不織布カバーに入れて、防虫剤と一緒に保管するのがおすすめです。
お客様：ハンガーは普通のでいいですか。
オペレーター（{op}）：肩の形に合った厚みのあるハンガーをおすすめします。細いハンガーだと型崩れの原因になります。
お客様：なるほど。丁寧にありがとうございます。
オペレーター（{op}）：お役に立てて嬉しいです。お電話ありがとうございました。"""
        detail = "冬物コートのシーズンオフ保管方法の問い合わせ"
    else:
        transcript = f"""オペレーター（{op}）：お電話ありがとうございます。{op}でございます。
お客様：ビジネススーツのズボンの膝のあたりがテカってきたんですけど、これって直せますか。
オペレーター（{op}）：テカリですね。スーツの生地は摩擦で繊維が寝てしまうとテカリが出ます。ご自宅でのケア方法と、クリーニングでの対応の2つの方法がございます。
お客様：自分でできる方法を教えてもらえますか。
オペレーター（{op}）：スチームアイロンを生地から浮かせて蒸気を当て、その後ブラシで逆方向にブラッシングしてください。繊維が起き上がってテカリが軽減されます。
お客様：やってみます。ありがとうございます。
オペレーター（{op}）：ひどい場合はクリーニング店でテカリ取りのオプションもございますので、合わせてご検討ください。お電話ありがとうございました。"""
        detail = "ビジネススーツのテカリ（膝部分）の対処方法の問い合わせ"

    return {
        "inquiry_type": "質問",
        "inquiry_detail": detail,
        "transcript": transcript,
        "operator_action": "お手入れ・メンテナンス方法の案内",
        "proposed_solution": "ケア方法の説明",
        "info_provided": "自宅でのケア方法、クリーニングの活用",
        "resolved": True,
        "follow_up_required": False,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 通話ログ生成・保存

# COMMAND ----------

# DBTITLE 1,5,000件の通話ログを生成（データ相関あり）
SCENARIO_FUNCS = [
    (gen_discount_complaint,     0.20),  # 割引クレーム
    (gen_return_exchange,        0.20),  # 返品・交換
    (gen_delivery_inquiry,       0.15),  # 配送・納期
    (gen_order_change,           0.08),  # 注文変更・キャンセル
    (gen_stock_inquiry,          0.08),  # 在庫確認
    (gen_defect_report,          0.08),  # 商品不良
    (gen_competitor_comparison,  0.05),  # 他社比較
    (gen_alteration_inquiry,     0.08),  # お直し
    (gen_billing_issue,          0.04),  # 請求・支払い
    (gen_care_inquiry,           0.04),  # お手入れ
]
scenario_weights = [w for _, w in SCENARIO_FUNCS]

call_logs = []
for i in range(1, NUM_CALLS + 1):
    call_id = f"CALL-{i:05d}"

    # シナリオ選択
    scenario_idx = random.choices(range(len(SCENARIO_FUNCS)), weights=scenario_weights, k=1)[0]
    scenario_func = SCENARIO_FUNCS[scenario_idx][0]

    # --- 相関設計: シナリオに応じた顧客選出 ---
    if scenario_func == gen_discount_complaint:
        # 割引クレーム → LINE顧客を80%の確率で選出
        if random.random() < 0.80 and line_customers:
            pool = line_return_customers if (random.random() < 0.5 and line_return_customers) else line_customers
            cid = random.choice(pool)
        else:
            cid = random.choice(customer_ids)
    elif scenario_func == gen_delivery_inquiry:
        # 配送・納期(急ぎ含む) → 高単価顧客を60%で選出
        if random.random() < 0.60 and high_spend_customers:
            cid = random.choice(high_spend_customers)
        else:
            cid = random.choice(customer_ids)
    else:
        cid = random.choice(customer_ids)

    cust = customer_map[cid]

    # 通話日時（直近1年内、営業時間9:00-20:00）
    call_date = DATE_START + timedelta(days=random.randint(0, 364))
    call_hour = random.randint(9, 19)
    call_min = random.randint(0, 59)
    call_datetime = datetime(call_date.year, call_date.month, call_date.day, call_hour, call_min)
    call_duration = random.randint(120, 600)

    # 発信者番号（マスク済み）
    area = random.choice(["090", "080", "070", "03", "06", "045", "052"])
    caller_number = f"{area}-XXXX-{random.randint(1000, 9999)}"

    # 過去問い合わせ回数
    past_count = random.choices([0, 1, 2, 3], weights=[0.4, 0.3, 0.2, 0.1], k=1)[0]

    # シナリオ生成
    scenario = scenario_func()

    # --- 相関設計: resolved と satisfaction を注文回数に連動 ---
    cust_orders = order_stats.get(cid, {}).get("order_count", 0)
    if cust_orders >= 3:
        # リピート顧客 → 解決率高い（Wow3: 解決→リピートの相関）
        resolved = scenario["resolved"] if scenario["resolved"] else (random.random() < 0.90)
    elif cust_orders <= 1:
        # 低注文顧客 → 解決率低め
        resolved = scenario["resolved"] if scenario["resolved"] else (random.random() < 0.55)
    else:
        resolved = scenario["resolved"]

    # 満足度（30%の通話でのみ取得）
    satisfaction = None
    if random.random() < 0.3:
        if resolved:
            satisfaction = random.choices([1, 2, 3, 4, 5], weights=[0.02, 0.05, 0.15, 0.40, 0.38], k=1)[0]
        else:
            satisfaction = random.choices([1, 2, 3, 4, 5], weights=[0.20, 0.30, 0.30, 0.15, 0.05], k=1)[0]

    log = {
        "call_id": call_id,
        "call_datetime": call_datetime.isoformat(),
        "call_duration_sec": call_duration,
        "caller_number": caller_number,
        "customer_id": cid,
        "customer_name": cust["name"],
        "customer_age": cust["age"],
        "customer_gender": cust["gender"],
        "customer_prefecture": cust["prefecture"],
        "past_inquiry_count": past_count,
        "inquiry_type": scenario["inquiry_type"],
        "inquiry_detail": scenario["inquiry_detail"],
        "transcript": scenario["transcript"],
        "operator_action": scenario["operator_action"],
        "proposed_solution": scenario["proposed_solution"],
        "info_provided": scenario["info_provided"],
        "resolved": resolved,
        "customer_satisfaction": satisfaction,
        "follow_up_required": scenario["follow_up_required"],
    }
    call_logs.append(log)

print(f"生成完了: {len(call_logs)} 件")
print(f"\nサンプル（CALL-00001）:")
sample = call_logs[0].copy()
sample["transcript"] = sample["transcript"][:200] + "..."
print(json.dumps(sample, ensure_ascii=False, indent=2))

# COMMAND ----------

# DBTITLE 1,JSONL形式でVolumeに保存
BATCH_SIZE = 1000
call_logs_dir = f"/Volumes/{catalog}/{schema}/{volume}/call_logs"

# 既存ファイルをクリア
try:
    existing = dbutils.fs.ls(call_logs_dir)
    for f in existing:
        dbutils.fs.rm(f.path)
except:
    pass

for batch_idx in range(0, NUM_CALLS, BATCH_SIZE):
    batch = call_logs[batch_idx:batch_idx + BATCH_SIZE]
    file_name = f"call_logs_{batch_idx:05d}.jsonl"
    file_path = f"{call_logs_dir}/{file_name}"

    content = "\n".join(json.dumps(log, ensure_ascii=False) for log in batch)
    dbutils.fs.put(file_path, content, overwrite=True)
    print(f"保存完了: {file_path} ({len(batch)} 件)")

print(f"\n全 {NUM_CALLS} 件の通話ログを {call_logs_dir} に保存しました。")

# COMMAND ----------

# DBTITLE 1,保存確認・相関チェック
files = dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/{volume}/call_logs")
for f in files:
    print(f"{f.name}  ({f.size:,} bytes)")

# COMMAND ----------

# DBTITLE 1,相関チェック: シナリオ分布
from collections import Counter
scenario_dist = Counter(log["inquiry_type"] for log in call_logs)
for k, v in scenario_dist.most_common():
    print(f"  {k}: {v} ({v/len(call_logs)*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,相関チェック: 割引クレームのLINE集中度
discount_calls = [log for log in call_logs if "割引" in log["inquiry_detail"]]
line_discount = sum(1 for log in discount_calls if get_primary_channel(log["customer_id"]) == "LINE")
print(f"割引クレーム全体: {len(discount_calls)} 件")
print(f"うちLINE顧客: {line_discount} 件 ({line_discount/len(discount_calls)*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,相関チェック: 解決率×注文回数
resolved_orders = []
unresolved_orders = []
for log in call_logs:
    oc = order_stats.get(log["customer_id"], {}).get("order_count", 0)
    if log["resolved"]:
        resolved_orders.append(oc)
    else:
        unresolved_orders.append(oc)

avg_resolved = sum(resolved_orders) / len(resolved_orders) if resolved_orders else 0
avg_unresolved = sum(unresolved_orders) / len(unresolved_orders) if unresolved_orders else 0
print(f"解決済み顧客の平均注文回数: {avg_resolved:.2f}")
print(f"未解決顧客の平均注文回数: {avg_unresolved:.2f}")
print(f"倍率: {avg_resolved/avg_unresolved:.2f}x" if avg_unresolved > 0 else "")
