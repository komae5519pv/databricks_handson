# Databricks notebook source
# MAGIC %md
# MAGIC # アンケートデータ生成
# MAGIC サーバレスコンピュートで実行してください。
# MAGIC
# MAGIC 旅行代理店業務を想定した**5種類のアンケート**をJSON形式で動的に生成し、Volumeに投入します。
# MAGIC
# MAGIC | # | シナリオ | 比率 | ファイル名例 |
# MAGIC |---|---------|------|-------------|
# MAGIC | 1 | 宿泊アンケート | 25% | `stay_0001.json` |
# MAGIC | 2 | ツアー体験アンケート | 20% | `tour_0001.json` |
# MAGIC | 3 | 店舗接客アンケート | 25% | `store_0001.json` |
# MAGIC | 4 | オンラインサービスアンケート | 15% | `online_0001.json` |
# MAGIC | 5 | 法人サービスアンケート | 15% | `corp_0001.json` |
# MAGIC
# MAGIC > `NUM_FILES` を変更するだけで任意の件数を生成できます（デフォルト: 100件）。

# COMMAND ----------

# DBTITLE 1,変数設定
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,生成パラメータ
import random
import json
import os
from datetime import date, timedelta

NUM_FILES = 100          # 生成件数（5000 にすればそのまま5000件生成可能）
SEED = 42

rng = random.Random(SEED)

DATE_START = date(2025, 4, 1)
DATE_END = date(2026, 2, 28)

# シナリオ別比率（合計=1.0）
SCENARIO_WEIGHTS = {
    "stay": 0.25,
    "tour": 0.20,
    "store": 0.25,
    "online": 0.15,
    "corp": 0.15,
}

# COMMAND ----------

# DBTITLE 1,共通データプール
LAST_NAMES = [
    "山田", "鈴木", "佐藤", "田中", "高橋", "伊藤", "渡辺", "山本", "中村", "小林",
    "加藤", "吉田", "松本", "井上", "木村", "林", "清水", "斎藤", "森", "池田",
    "橋本", "石川", "前田", "藤田", "後藤", "岡田", "村上", "近藤", "石井", "上田",
]
FIRST_NAMES_M = ["太郎", "健一", "雄太", "翔太", "大輔", "拓也", "直人", "和也", "亮", "誠",
                 "隆", "浩二", "慎一", "正樹", "修"]
FIRST_NAMES_F = ["花子", "美咲", "陽子", "恵子", "裕子", "明美", "真理", "由美", "智子", "愛",
                 "沙織", "綾", "麻衣", "瞳", "彩"]
STAFF_NAMES = ["佐藤", "田中", "鈴木", "高橋", "伊藤", "渡辺", "山本", "中村", "小林", "加藤",
               "吉田", "松本", "井上", "木村", "林", "西村", "太田", "三浦", "藤井", "岡本"]
PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県", "滋賀県", "京都府",
    "大阪府", "兵庫県", "奈良県", "和歌山県", "鳥取県", "島根県",
    "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
AGE_RANGES = ["20代", "30代", "40代", "50代", "60代", "70代"]
GENDERS = ["男性", "女性"]

COMPETITOR_MENTIONS = [
    "同価格帯の別のホテルも次回は検討するかもしれません",
    "正直なところ、ネット専業の予約サイトの方が安くて便利だと感じています",
    "近隣の別の代理店の方が入りやすい雰囲気でした",
    "他社の旅行管理サービスへの切り替えも社内で話題に上がっています",
    "大手航空系の代理店の方がマイル連携で有利だと聞きました",
    "知人から別の代理店を勧められているので、比較検討中です",
    "以前使っていた別の代理店ではこういうことはなかったです",
    "オンライン専業の競合サービスの方がUIは使いやすかった",
    "同業他社はもっと柔軟なキャンセルポリシーを提供していると聞きます",
    "別のサービスではアプリで完結できたので、それに慣れていると不便に感じます",
]


def random_date(rng):
    delta = (DATE_END - DATE_START).days
    return DATE_START + timedelta(days=rng.randint(0, delta))


def random_respondent(rng):
    gender = rng.choice(GENDERS)
    first = rng.choice(FIRST_NAMES_M if gender == "男性" else FIRST_NAMES_F)
    last = rng.choice(LAST_NAMES)
    return {
        "name": f"{last}{first}",
        "age": rng.choice(AGE_RANGES),
        "gender": gender,
        "prefecture": rng.choice(PREFECTURES),
    }

# COMMAND ----------

# DBTITLE 1,① 宿泊アンケート生成
HOTEL_AREAS = ["箱根", "熱海", "京都", "沖縄", "北海道", "軽井沢", "日光", "伊豆", "鎌倉",
               "奈良", "白浜", "別府", "草津", "富良野", "石垣島", "宮古島", "金沢", "高山"]
ROOM_TYPES = ["シングル", "ツイン", "ダブル", "デラックスツイン", "スイート", "和室", "和洋室",
              "ファミリールーム", "コーナーツイン", "ジュニアスイート"]
STAY_PURPOSES = [
    "家族旅行で利用しました。",
    "夫婦の記念日旅行として予約しました。",
    "友人との旅行で2泊しました。",
    "出張のついでに1泊延泊して観光しました。",
    "両親へのプレゼント旅行として手配しました。",
    "子どもの誕生日祝いを兼ねた旅行です。",
    "一人旅でリフレッシュ目的で利用しました。",
    "卒業旅行で友人4人と利用しました。",
    "介護疲れのリフレッシュで妻と訪れました。",
    "海外旅行の代わりに国内の高級宿を選びました。",
]

CHECKIN_SEGMENTS = {
    "negative": [
        "チェックイン時に{wait}分待たされました。フロントのスタッフが明らかに足りていない印象で、列がなかなか進みませんでした。",
        "15時ちょうどに到着しましたが、部屋の準備ができていないと言われ{wait}分ロビーで待機。事前に連絡が欲しかったです。",
        "チェックインの手続き自体は問題ありませんでしたが、駐車場の案内が不十分で{wait}分ほど迷いました。",
        "到着時にフロントが混雑しており、{wait}分並びました。子連れだったので、せめてキッズスペースがあれば助かったのですが。",
        "チェックイン時、予約内容が反映されておらず確認に{wait}分かかりました。事前決済済みなのに再確認されるのは不安になります。",
    ],
    "positive": [
        "チェックインは非常にスムーズでした。到着すると既に準備ができており、待ち時間ゼロで部屋に案内していただけました。",
        "フロントの方がとても丁寧で、周辺の観光情報まで教えていただきました。第一印象が良かったです。",
        "アーリーチェックインをお願いしたところ、快く対応していただけました。旅の出だしが気持ちよかったです。",
        "到着時にウェルカムドリンクのサービスがあり、旅の疲れが癒されました。スタッフの笑顔も印象的でした。",
    ],
}

ROOM_SEGMENTS = {
    "negative": [
        "部屋は広さは十分でしたが、洗面台の排水が非常に遅く、歯磨き中に水が溜まってしまう状態でした。清掃も行き届いていない部分がありました。",
        "窓からの景色は良かったのですが、エアコンの音がうるさくて夜中に何度か目が覚めました。古い設備を使い続けている印象です。",
        "バスルームのアメニティが一部切れており、フロントに電話しましたが届くまでに15分以上かかりました。",
        "隣の部屋の声がかなり聞こえました。防音対策が不十分だと思います。また、カーテンの遮光性も低く、早朝から明るくて困りました。",
        "テレビのリモコンが反応しにくく、Wi-Fiも頻繁に切れました。この価格帯ならもう少し設備投資してほしいです。",
        "部屋のにおいが少し気になりました。消臭対応をお願いしましたが、完全には取れませんでした。",
    ],
    "positive": [
        "部屋は期待以上に広く、清潔感がありました。窓から見える景色も素晴らしく、非日常を味わえました。",
        "アメニティが充実しており、特にオーガニックのシャンプーが気に入りました。タオルもふかふかで気持ちよかったです。",
        "部屋の設備は新しく、Wi-Fiも高速で快適でした。コンセントの数も十分で、仕事もできる環境でした。",
        "ベッドの寝心地が最高でした。枕も複数種類から選べるサービスがあり、ぐっすり眠れました。",
    ],
}

FOOD_SEGMENTS = {
    "negative": [
        "朝食ビュッフェは種類が少なく、補充も遅かったです。8時台に行ったら主要なメニューが品切れでした。",
        "夕食のコース料理は見た目は良かったのですが、味付けが全体的に薄く、物足りない印象でした。価格を考えると残念です。",
        "レストランが混雑しており、30分以上待ちました。予約制にするか、席数を増やすかの対策が必要だと思います。",
        "アレルギー対応をお願いしていたのですが、一部反映されていないメニューがありました。命に関わることなので徹底してほしいです。",
    ],
    "positive": [
        "朝食ビュッフェが非常に充実していました。地元の食材を使った和食コーナーが特に良く、焼き魚と出汁巻き卵は絶品でした。",
        "夕食のコース料理は一品一品が丁寧に作られており、大変満足しました。特にデザートの盛り付けが美しかったです。",
        "食事のクオリティが高く、このホテルを選んだ最大の理由が食事だったので期待通りでした。",
        "レストランスタッフの方が子どものアレルギーに細やかに対応してくれて、安心して食事を楽しめました。",
        "地元の日本酒のペアリングコースが素晴らしかったです。料理との組み合わせを丁寧に説明していただけました。",
    ],
}

STAFF_SEGMENTS_POSITIVE = [
    "{name}さんという方が特に印象的でした。こちらの要望を先回りして対応してくださり、プロの接客だと感じました。",
    "スタッフの{name}さんが子どもに声をかけてくれて、ちょっとしたサプライズまで用意してくれました。家族全員感動しました。",
    "{name}さんの対応が素晴らしく、この方がいるならまた来たいと思わせるレベルでした。",
    "困ったときに対応してくれた{name}さんがとても親切で、嫌な気分が一気に解消されました。",
    "コンシェルジュの{name}さんに観光プランの相談をしたところ、穴場スポットまで教えていただけました。",
]
STAFF_SEGMENTS_NEGATIVE = [
    "残念だったのはスタッフの{name}さんの対応です。質問に対して面倒くさそうに答えられ、不快でした。",
    "スタッフの{name}さんに頼んだことが30分経っても対応されず、再度お願いしてようやく動いていただけました。",
    "{name}さんの対応は丁寧でしたが、明らかに経験不足で、何度も確認のために奥に引っ込んでいました。",
]

OVERALL_CLOSINGS = [
    "総合的には満足ですが、改善点もいくつかありました。次回の利用を検討する際の参考にしたいと思います。",
    "正直なところ、この価格帯であれば、もう少しサービスの質を上げてほしいと思います。",
    "良い点も残念な点もありましたが、トータルではまた利用したいと思えるホテルでした。",
    "非常に満足しています。ぜひまた利用させていただきたいです。",
    "期待値が高かった分、残念に感じる部分が目立ちました。改善を期待します。",
    "この価格なら他にも選択肢はあると感じましたが、立地は良かったです。",
    "スタッフの対応次第でまた来たいと思えるホテルなので、教育面の改善を期待しています。",
    "子連れには少し不便な部分もありましたが、全体としては楽しい旅行になりました。",
]


def generate_stay_survey(rng, seq):
    d = random_date(rng)
    nights = rng.randint(1, 4)
    area = rng.choice(HOTEL_AREAS)
    room_type = rng.choice(ROOM_TYPES)

    # コメント組み立て
    comment_parts = []
    comment_parts.append(rng.choice(STAY_PURPOSES))

    # チェックイン
    if rng.random() < 0.5:
        wait = rng.choice([10, 15, 20, 25, 30, 40, 45])
        comment_parts.append(rng.choice(CHECKIN_SEGMENTS["negative"]).format(wait=wait))
    else:
        comment_parts.append(rng.choice(CHECKIN_SEGMENTS["positive"]))

    # 客室
    if rng.random() < 0.45:
        comment_parts.append(rng.choice(ROOM_SEGMENTS["negative"]))
    else:
        comment_parts.append(rng.choice(ROOM_SEGMENTS["positive"]))

    # 食事
    if rng.random() < 0.35:
        comment_parts.append(rng.choice(FOOD_SEGMENTS["negative"]))
    else:
        comment_parts.append(rng.choice(FOOD_SEGMENTS["positive"]))

    # スタッフ言及 (80%)
    staff = rng.choice(STAFF_NAMES)
    if rng.random() < 0.8:
        if rng.random() < 0.6:
            comment_parts.append(rng.choice(STAFF_SEGMENTS_POSITIVE).format(name=staff))
        else:
            comment_parts.append(rng.choice(STAFF_SEGMENTS_NEGATIVE).format(name=staff))

    # 競合言及 (25%)
    if rng.random() < 0.25:
        comment_parts.append(rng.choice(COMPETITOR_MENTIONS))

    # 締め
    comment_parts.append(rng.choice(OVERALL_CLOSINGS))

    respondent = random_respondent(rng)

    # 満足度のつけ方をコメントの内容と連動させる
    neg_count = sum(1 for p in comment_parts if any(w in p for w in ["残念", "不満", "待たされ", "うるさ", "不快", "遅", "切れ"]))
    base_score = max(1, 5 - neg_count)
    satisfaction = max(1, min(5, base_score + rng.choice([-1, 0, 0, 0, 1])))

    return {
        "survey_type": "宿泊アンケート",
        "survey_date": str(d),
        "accommodation": {
            "area": area,
            "room_type": room_type,
            "nights": nights,
            "check_in_date": str(d - timedelta(days=nights)),
        },
        "satisfaction_score": satisfaction,
        "free_comment": "\n".join(comment_parts),
        "respondent": respondent,
    }

# COMMAND ----------

# DBTITLE 1,② ツアー体験アンケート生成
TOUR_DESTINATIONS = ["北海道", "沖縄", "京都・奈良", "九州周遊", "東北", "四国", "北陸",
                     "屋久島", "小笠原", "知床", "瀬戸内海", "信州"]
TOUR_TYPES = ["家族向けツアー", "シニア向けゆったりツアー", "アクティブツアー",
              "グルメツアー", "温泉めぐりツアー", "歴史探訪ツアー", "自然満喫ツアー",
              "写真撮影ツアー", "クルーズツアー"]
TOUR_DURATIONS = ["日帰り", "1泊2日", "2泊3日", "3泊4日", "4泊5日"]

GUIDE_SEGMENTS = {
    "negative": [
        "ガイドさんの説明が早口で聞き取りにくかったです。特にシニアの参加者は困っていたように見えました。",
        "ガイドの方の知識は豊富でしたが、一方的に話し続ける感じで、質問する隙がありませんでした。",
        "ガイドさんが時間管理に厳しすぎて、写真を撮る余裕もないスポットがいくつかありました。",
        "ガイドの方が集合時間に遅れてきたのは残念でした。参加者には時間厳守を求めるのに、矛盾していると感じました。",
        "ガイドさんの声が小さく、バスの後方席ではほとんど聞こえませんでした。マイクの使用をお願いしたいです。",
    ],
    "positive": [
        "ガイドの{name}さんが素晴らしかったです。各スポットの歴史的背景から地元のエピソードまで、引き出しの多さに感動しました。",
        "ガイドの{name}さんがとても気さくで、参加者全員に声をかけていて、グループの雰囲気がとても良くなりました。",
        "ガイドさんの臨機応変な対応が光りました。天候が変わった際に、代替プランをすぐに提案してくれました。",
        "ガイドの{name}さんが写真スポットをたくさん教えてくれて、SNS映えする写真がたくさん撮れました。",
    ],
}

SCHEDULE_SEGMENTS = {
    "negative": [
        "スケジュールがかなりタイトで、各観光地での滞在時間が短すぎました。もっとゆっくり見たかった場所がいくつもあります。",
        "自由時間がほとんどなく、常にバスで移動している感覚でした。もう少し余裕のある行程にしてほしいです。",
        "朝が早すぎました。毎日6時集合は体力的にきつかったです。特にシニアの方は辛そうでした。",
        "観光地間の移動時間が予想以上に長く、バスの中で過ごす時間が多かったのが残念です。",
    ],
    "positive": [
        "行程に余裕があり、各観光地をゆっくり楽しめました。詰め込みすぎないバランスが良かったです。",
        "スケジュールが効率的で、限られた時間の中で多くの見どころを回れました。満足度が高いです。",
        "自由時間が適度に設けられていて、個人的に行きたかったお店にも立ち寄れました。",
    ],
}

TOUR_FOOD_SEGMENTS = {
    "negative": [
        "ツアーに含まれていた昼食が期待外れでした。団体向けの画一的なメニューで、地元感がまったくありませんでした。",
        "食事の量が少なく、男性参加者は物足りなそうでした。追加注文もできない状況でした。",
        "アレルギー対応を事前に伝えていたのですが、現地のレストランに伝わっていませんでした。危険です。",
    ],
    "positive": [
        "食事が毎回美味しく、地元の名産品を存分に味わえました。これだけでもツアーに参加した甲斐がありました。",
        "昼食の地元料理が素晴らしく、特に海鮮丼は今まで食べた中で一番でした。",
        "ツアーに含まれない食事も、ガイドさんがおすすめの店を教えてくれて、すべて当たりでした。",
    ],
}

TOUR_CLOSINGS = [
    "全体としては楽しい旅行でした。次回もこのシリーズのツアーに参加したいと思います。",
    "改善点はありますが、コストパフォーマンスは良いと思います。",
    "家族全員満足でした。特に子どもが楽しめるポイントが多かったのが良かったです。",
    "もう少し参加者の体力や年齢に合わせた配慮があると、さらに良くなると思います。",
    "一人参加でしたが、居心地の悪さは感じませんでした。また参加したいです。",
    "期待以上の内容でした。写真を見返しながら、次の旅行先を考えています。",
    "正直なところ、この価格なら個人手配でも良かったかもしれないと感じました。",
    "ツアーならではの安心感があり、特に高齢の母を連れていくには最適でした。",
]


def generate_tour_survey(rng, seq):
    d = random_date(rng)
    dest = rng.choice(TOUR_DESTINATIONS)
    tour_type = rng.choice(TOUR_TYPES)
    duration = rng.choice(TOUR_DURATIONS)
    participants = rng.randint(1, 6)

    comment_parts = []

    # ガイド
    staff = rng.choice(STAFF_NAMES)
    if rng.random() < 0.4:
        comment_parts.append(rng.choice(GUIDE_SEGMENTS["negative"]))
    else:
        comment_parts.append(rng.choice(GUIDE_SEGMENTS["positive"]).format(name=staff))

    # スケジュール
    if rng.random() < 0.45:
        comment_parts.append(rng.choice(SCHEDULE_SEGMENTS["negative"]))
    else:
        comment_parts.append(rng.choice(SCHEDULE_SEGMENTS["positive"]))

    # 食事
    if rng.random() < 0.35:
        comment_parts.append(rng.choice(TOUR_FOOD_SEGMENTS["negative"]))
    else:
        comment_parts.append(rng.choice(TOUR_FOOD_SEGMENTS["positive"]))

    # 競合言及 (20%)
    if rng.random() < 0.20:
        comment_parts.append(rng.choice(COMPETITOR_MENTIONS))

    comment_parts.append(rng.choice(TOUR_CLOSINGS))

    respondent = random_respondent(rng)
    neg_count = sum(1 for p in comment_parts if any(w in p for w in ["残念", "不満", "きつ", "危険", "期待外れ", "遅れ", "聞こえ"]))
    satisfaction = max(1, min(5, 5 - neg_count + rng.choice([-1, 0, 0, 1])))

    return {
        "survey_type": "ツアー体験アンケート",
        "survey_date": str(d),
        "tour_info": {
            "destination": dest,
            "tour_type": tour_type,
            "duration": duration,
            "participants": participants,
        },
        "satisfaction_score": satisfaction,
        "free_comment": "\n".join(comment_parts),
        "respondent": respondent,
    }

# COMMAND ----------

# DBTITLE 1,③ 店舗接客アンケート生成
STORE_NAMES = [
    "新宿南口店", "渋谷マークシティ店", "池袋東口店", "横浜そごう店", "大宮駅前店",
    "千葉ペリエ店", "町田モディ店", "立川グランデュオ店", "吉祥寺店", "藤沢OPA店",
    "名古屋栄店", "梅田阪急店", "神戸三宮店", "京都四条店", "広島本通り店",
    "福岡天神店", "仙台エスパル店", "札幌ステラプレイス店",
]
CUSTOMER_TYPES = ["個人・ファミリー", "個人・カップル", "個人・一人旅", "個人・シニア",
                  "個人・学生", "法人・総務", "法人・秘書", "個人・友人グループ"]
VISIT_PURPOSES = ["海外ツアーの相談", "国内旅行の予約", "社員旅行の見積もり",
                  "新婚旅行の相談", "家族旅行のプラン相談", "出張手配の相談",
                  "パンフレット収集", "予約変更の手続き", "キャンセルの手続き"]

STORE_WAIT_COMPLAINTS = [
    "待ち時間が{wait}分と言われて一度外出しましたが、戻ったら順番が飛ばされていました。番号札の仕組みをちゃんとしてほしいです。",
    "{wait}分待ちと聞いて覚悟はしていましたが、実際は{extra}分超過していました。目安時間の精度を上げてほしいです。",
    "予約なしで行ったので{wait}分待ちは仕方ないですが、待ちスペースに椅子が足りなくて立ったまま待ちました。",
    "繁忙期だったのか{wait}分以上待ちました。ウェブ予約できることを知らなかったので、もっと目立つところに案内してほしいです。",
]
STORE_INFO_COMPLAINTS = [
    "パンフレットが多すぎてどれを見ればいいか全然わからなかったです。棚に数十種類並んでいて、比較もできません。最初からデジタルで絞り込めるようにしてほしいです。",
    "ウェブサイトに載っていない情報を聞きに来たのに、「ウェブで確認できますよ」と言われて、それなら来店した意味がないと思いました。",
    "見積もりが出るまで1週間かかると言われて驚きました。以前お願いした別の代理店は翌日に概算を出してくれましたよ。",
    "毎年お願いしているリピーターなのに、毎回ゼロから要件をヒアリングされるのが正直しんどいです。顧客情報をちゃんと管理しているのか疑問です。",
    "提案されたプランの選択肢が少なかったです。こちらの予算と希望を伝えたのに、定番のプランしか出てこなくて、カスタマイズの余地がない印象でした。",
]
STORE_STAFF_POSITIVE = [
    "担当してくれた{name}さんの説明がすごくわかりやすかったです。子連れの注意点や持ち物リストまで印刷してくれて助かりました。",
    "{name}さんが地図を広げて丁寧に説明してくれて、とても安心して予約できました。こういう対面の良さがありますよね。",
    "{name}さんはこちらの漠然とした希望から的確にプランを提案してくれました。さすがベテランだと感じました。",
    "途中から対応してくれた{name}さんがとても親切で、最終的にはこの方のおかげで申し込みました。",
    "{name}さんが予算内で最大限の提案をしてくれました。無理に高いプランを勧めてこない姿勢が好印象でした。",
]
STORE_STAFF_NEGATIVE = [
    "若いスタッフの方に「ウェブで見てもらった方が早いですよ」と言われて正直カチンときました。こちらはウェブが苦手だから店舗に来てるんです。",
    "担当の方が新人さんだったようで、何を聞いても「確認してきます」と奥に引っ込んでしまい、話が全然進みませんでした。",
    "スタッフの方が忙しそうで、こちらの話を最後まで聞いてくれませんでした。途中で別の対応に行ってしまうこともありました。",
    "担当の方の商品知識が不足していると感じました。現地の気候や治安について聞いても、曖昧な答えしか返ってきませんでした。",
    "笑顔がなく事務的な対応でした。旅行の相談ってワクワクするものなのに、こちらのテンションまで下がりました。",
]

STORE_CLOSINGS = [
    "最終的には予約しましたが、仕組みの部分で損していると思います。",
    "いい人もいるんだけど、最初の対応で「もう来たくない」と思わせるのは致命的です。",
    "次回はオンラインで予約するかもしれません。来店する意味が感じられなかったので。",
    "対面相談の良さを活かすなら、もう少しスタッフの教育に力を入れてほしいです。",
    "全体的に満足です。やっぱり対面で相談できると安心感がありますね。",
    "良い買い物ができました。またこちらの店舗を利用したいです。",
    "引き継ぎの仕組みに問題があると思います。担当が変わるたびに一からやり直しになるのは困ります。",
    "店舗の雰囲気は良かったのですが、提案力にもう一歩踏み込みが欲しいと思いました。",
]


def generate_store_survey(rng, seq):
    d = random_date(rng)
    store = rng.choice(STORE_NAMES)
    ctype = rng.choice(CUSTOMER_TYPES)
    purpose = rng.choice(VISIT_PURPOSES)
    wait_min = rng.choice([5, 10, 15, 20, 25, 30, 40, 50, 60])

    comment_parts = []
    comment_parts.append(f"{purpose}で来店しました。")

    # 待ち時間 (60% で不満あり)
    if rng.random() < 0.6 and wait_min >= 20:
        extra = rng.randint(5, 20)
        comment_parts.append(rng.choice(STORE_WAIT_COMPLAINTS).format(wait=wait_min, extra=wait_min + extra))

    # 情報提供 (40% で不満あり)
    if rng.random() < 0.4:
        comment_parts.append(rng.choice(STORE_INFO_COMPLAINTS))

    # スタッフ
    staff = rng.choice(STAFF_NAMES)
    if rng.random() < 0.55:
        comment_parts.append(rng.choice(STORE_STAFF_POSITIVE).format(name=staff))
    else:
        comment_parts.append(rng.choice(STORE_STAFF_NEGATIVE))

    # 競合言及 (30%)
    if rng.random() < 0.30:
        comment_parts.append(rng.choice(COMPETITOR_MENTIONS))

    comment_parts.append(rng.choice(STORE_CLOSINGS))

    respondent = random_respondent(rng)
    neg_count = sum(1 for p in comment_parts if any(w in p for w in ["カチン", "残念", "不満", "待ち", "疑問", "困り", "損", "致命的"]))
    satisfaction = max(1, min(5, 5 - neg_count + rng.choice([-1, 0, 0, 1])))

    return {
        "survey_type": "店舗接客アンケート",
        "survey_date": str(d),
        "store_info": {
            "store_name": store,
            "visit_purpose": purpose,
            "customer_type": ctype,
            "wait_time_min": wait_min,
        },
        "satisfaction_score": satisfaction,
        "free_comment": "\n".join(comment_parts),
        "respondent": respondent,
    }

# COMMAND ----------

# DBTITLE 1,④ オンラインサービスアンケート生成
ONLINE_CHANNELS = ["公式サイト", "スマホアプリ", "LINE", "メール問い合わせ", "チャットサポート"]
ONLINE_PURPOSES = ["ツアー予約", "ホテル予約", "航空券手配", "予約変更", "キャンセル手続き",
                   "ポイント確認", "会員情報変更", "問い合わせ"]

ONLINE_UI_COMPLAINTS = [
    "サイトの検索機能が使いにくいです。日付と行き先を入れても、関係ない結果が大量に表示されて、目的のプランにたどり着けません。",
    "アプリが頻繁に落ちます。予約の途中で落ちて、最初からやり直しになったことが2回ありました。",
    "スマホで見ると表示が崩れるページがあります。特に料金比較のテーブルが横にはみ出して見にくいです。",
    "マイページから過去の予約履歴が見られなくなっていました。以前は見られたのに、改悪だと思います。",
    "決済画面でエラーが出て、処理が完了したのかどうか分からず不安になりました。結局電話で確認しました。",
    "ログイン後のセッションがすぐ切れるので、比較検討しているうちにログアウトされてしまい、お気に入りリストが消えました。",
]
ONLINE_UI_POSITIVES = [
    "リニューアル後のサイトは以前より見やすくなりました。特に料金カレンダーの表示が便利です。",
    "アプリの予約フローがシンプルで、初めてでも迷わず完了できました。",
    "チャットサポートの応答が早く、10分以内に疑問が解決しました。",
    "LINEでの予約確認通知が便利です。出発前のリマインドも助かります。",
]
ONLINE_RESPONSE_COMPLAINTS = [
    "メールで問い合わせたところ、返信に3営業日かかりました。急ぎの変更だったので、結局電話しました。",
    "チャットサポートに繋がるまで15分待ちました。「ただいま混み合っております」のメッセージだけで、目安時間の表示もありません。",
    "問い合わせフォームから送ったのに、自動返信すら来ませんでした。ちゃんと届いているのか不安です。",
    "電話窓口の営業時間が短すぎます。平日の10-17時だけでは、仕事をしている人間は連絡できません。",
]
ONLINE_RESPONSE_POSITIVES = [
    "問い合わせへの返信が当日中に来て助かりました。内容も丁寧で的確でした。",
    "電話窓口の{name}さんの対応が素晴らしく、複雑な変更手続きもスムーズに完了しました。",
    "チャットボットが予想以上に優秀で、簡単な質問はAIだけで解決できました。",
]

ONLINE_CLOSINGS = [
    "オンラインサービスの利便性は認めますが、もう少し安定性と速度を改善してほしいです。",
    "対面の窓口に行かなくて済むのは助かりますが、困ったときの対応がもう一歩です。",
    "全体的に使いやすくなっていると感じます。今後も改善を期待しています。",
    "急ぎの対応が必要な場面では、まだオンラインだけでは心もとないです。",
    "デジタルに慣れている世代には便利ですが、操作が苦手な人へのサポートが足りないと思います。",
    "便利な時代になりましたが、大切な旅行の手配は対面の安心感も捨てがたいです。",
]


def generate_online_survey(rng, seq):
    d = random_date(rng)
    channel = rng.choice(ONLINE_CHANNELS)
    purpose = rng.choice(ONLINE_PURPOSES)

    comment_parts = []
    comment_parts.append(f"{channel}で{purpose}をしました。")

    # UI/UX (50% で不満)
    if rng.random() < 0.50:
        comment_parts.append(rng.choice(ONLINE_UI_COMPLAINTS))
    else:
        comment_parts.append(rng.choice(ONLINE_UI_POSITIVES))

    # レスポンス (45% で不満)
    staff = rng.choice(STAFF_NAMES)
    if rng.random() < 0.45:
        comment_parts.append(rng.choice(ONLINE_RESPONSE_COMPLAINTS))
    else:
        comment_parts.append(rng.choice(ONLINE_RESPONSE_POSITIVES).format(name=staff))

    # 競合言及 (20%)
    if rng.random() < 0.20:
        comment_parts.append(rng.choice(COMPETITOR_MENTIONS))

    comment_parts.append(rng.choice(ONLINE_CLOSINGS))

    respondent = random_respondent(rng)
    neg_count = sum(1 for p in comment_parts if any(w in p for w in ["落ち", "エラー", "不安", "遅", "崩れ", "使いにく", "待ち"]))
    satisfaction = max(1, min(5, 5 - neg_count + rng.choice([-1, 0, 0, 1])))

    return {
        "survey_type": "オンラインサービスアンケート",
        "survey_date": str(d),
        "channel": channel,
        "purpose": purpose,
        "satisfaction_score": satisfaction,
        "free_comment": "\n".join(comment_parts),
        "respondent": respondent,
    }

# COMMAND ----------

# DBTITLE 1,⑤ 法人サービスアンケート生成
COMPANY_TYPES = ["製造業", "IT", "商社", "金融", "不動産", "建設", "医療", "教育", "小売", "サービス業"]
DEPARTMENTS = ["総務部", "人事部", "経理部", "営業部", "企画部", "情報システム部", "秘書室"]
SERVICES_USED = ["出張手配", "社員旅行", "研修合宿", "海外出張", "会議室・会場手配",
                 "インセンティブ旅行", "接待手配"]
RESPONDENT_ROLES = ["部長", "課長", "主任", "担当者", "マネージャー", "アシスタント"]

CORP_BOOKING_COMPLAINTS = [
    "出張手配のリードタイムが長すぎます。急な出張に対応できず、結局自分で予約することが何度もありました。",
    "法人向け航空券の選択肢が少ないです。直行便の法人割引がないルートが多く、競争力に欠けます。",
    "海外出張の場合、ビザ手配のサポートが不十分です。結局自分で大使館に問い合わせることになりました。",
    "予約の変更・キャンセルポリシーが硬直的です。出発72時間前ルールへの不満が社内で多数上がっています。",
    "繁忙期の宿泊手配で希望のホテルが取れないことが増えています。もっと早い段階でアラートを出してほしいです。",
]
CORP_BOOKING_POSITIVES = [
    "出張手配の対応が迅速で助かっています。特に急な変更にも柔軟に対応いただけるのが強みだと思います。",
    "法人レート交渉で年間のコスト削減を実現していただきました。具体的な数字で効果を示していただけるのも良いです。",
    "海外出張のサポートが手厚く、現地でのトラブル対応も含めて安心感があります。",
    "担当の{name}さんが当社の出張パターンを熟知してくれていて、毎回スムーズです。",
]
CORP_SYSTEM_COMPLAINTS = [
    "経費精算システムとの連携が遅いです。現状CSV手動連携のため、月末の精算作業が大変です。API連携を早急に実現してほしいです。",
    "予約管理画面のUIが古く、使いにくいです。部署ごとの利用状況をダッシュボードで見られるようにしてほしいです。",
    "モバイルアプリの使い勝手が悪く、出先からの予約確認がストレスです。検索が遅い、表示が崩れるなどの声が社内から上がっています。",
    "月次レポートの自動生成機能がなく、毎月手作業で集計しています。データのエクスポート機能を充実させてほしいです。",
    "シングルサインオン(SSO)に対応していないので、社員が個別にアカウント管理する必要があり、セキュリティ上の懸念があります。",
]
CORP_SYSTEM_POSITIVES = [
    "管理画面から部署ごとの利用状況が一目でわかるようになり、予算管理が楽になりました。",
    "経費精算との自動連携が実現して、月末の作業が大幅に削減されました。",
    "レポート機能が充実しており、経営層への報告資料がすぐに作れるのが助かります。",
]
CORP_CLOSINGS = [
    "長年利用していますが、最近は競合のサービスも進化しており、比較検討する機会が増えています。",
    "担当者の質は高いのですが、システム面での遅れが全体の満足度を下げています。",
    "総合的には満足しています。引き続きよろしくお願いします。",
    "コスト面での改善をもう少し期待したいです。年間契約のメリットをもっと感じられるようにしてほしいです。",
    "当社の規模に合ったきめ細かいサービスを今後も期待しています。",
    "他部署からも「使いやすくなった」との声が出ており、導入して良かったと思います。",
    "サービス品質は信頼していますが、価格面で見直しの時期に来ていると感じています。",
]


def generate_corp_survey(rng, seq):
    d = random_date(rng)
    comp_type = rng.choice(COMPANY_TYPES)
    dept = rng.choice(DEPARTMENTS)
    service = rng.choice(SERVICES_USED)
    role = rng.choice(RESPONDENT_ROLES)

    comment_parts = []
    comment_parts.append(f"{service}サービスについてフィードバックします。")

    # 手配に関する意見
    staff = rng.choice(STAFF_NAMES)
    if rng.random() < 0.45:
        comment_parts.append(rng.choice(CORP_BOOKING_COMPLAINTS))
    else:
        comment_parts.append(rng.choice(CORP_BOOKING_POSITIVES).format(name=staff))

    # システムに関する意見
    if rng.random() < 0.50:
        comment_parts.append(rng.choice(CORP_SYSTEM_COMPLAINTS))
    else:
        comment_parts.append(rng.choice(CORP_SYSTEM_POSITIVES))

    # 競合言及 (30%)
    if rng.random() < 0.30:
        comment_parts.append(rng.choice(COMPETITOR_MENTIONS))

    comment_parts.append(rng.choice(CORP_CLOSINGS))

    respondent = random_respondent(rng)
    neg_count = sum(1 for p in comment_parts if any(w in p for w in ["遅", "不満", "足りな", "古", "不十分", "ストレス", "懸念", "硬直"]))
    satisfaction = max(1, min(5, 5 - neg_count + rng.choice([-1, 0, 0, 1])))

    return {
        "survey_type": "法人サービスアンケート",
        "survey_date": str(d),
        "company_info": {
            "industry": comp_type,
            "department": dept,
            "respondent_role": role,
        },
        "service_used": service,
        "satisfaction_score": satisfaction,
        "free_comment": "\n".join(comment_parts),
        "respondent": respondent,
    }

# COMMAND ----------

# DBTITLE 1,メイン: 全シナリオを混合生成
# シナリオ別の件数を計算
scenario_counts = {}
remaining = NUM_FILES
for i, (scenario, weight) in enumerate(SCENARIO_WEIGHTS.items()):
    if i == len(SCENARIO_WEIGHTS) - 1:
        scenario_counts[scenario] = remaining
    else:
        count = round(NUM_FILES * weight)
        scenario_counts[scenario] = count
        remaining -= count

print("=== 生成計画 ===")
for s, c in scenario_counts.items():
    print(f"  {s}: {c}件")
print(f"  合計: {sum(scenario_counts.values())}件")

# 生成関数マッピング
GENERATORS = {
    "stay": generate_stay_survey,
    "tour": generate_tour_survey,
    "store": generate_store_survey,
    "online": generate_online_survey,
    "corp": generate_corp_survey,
}

# 全ファイルのリストを作成（シャッフル用）
file_plan = []
for scenario, count in scenario_counts.items():
    for i in range(count):
        file_plan.append((scenario, i + 1))

rng.shuffle(file_plan)

# COMMAND ----------

# DBTITLE 1,Volumeへの書き込み
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

# 既存ファイルをクリア
existing = dbutils.fs.ls(volume_path)
for f in existing:
    dbutils.fs.rm(f.path)
print(f"既存ファイル {len(existing)} 件を削除しました")

# シナリオ別の連番管理
seq_counters = {s: 0 for s in SCENARIO_WEIGHTS}

generated = 0
for scenario, _ in file_plan:
    seq_counters[scenario] += 1
    seq = seq_counters[scenario]

    data = GENERATORS[scenario](rng, seq)
    fname = f"{scenario}_{seq:04d}.json"

    json_str = json.dumps(data, ensure_ascii=False, indent=2)
    dbutils.fs.put(f"{volume_path}/{fname}", json_str, overwrite=True)

    generated += 1
    if generated % 100 == 0:
        print(f"  {generated}/{NUM_FILES} 件生成済み...")

print(f"\n✅ 全 {generated} 件のアンケートデータを生成しました")

# COMMAND ----------

# DBTITLE 1,生成結果の確認
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
files = dbutils.fs.ls(volume_path)

# シナリオ別集計
from collections import Counter
scenario_counter = Counter()
total_size = 0
for f in files:
    prefix = f.name.split("_")[0]
    scenario_counter[prefix] += 1
    total_size += f.size

print(f"=== Volume内ファイル ===")
print(f"  合計: {len(files)} 件  ({total_size:,} bytes)")
for scenario, count in sorted(scenario_counter.items()):
    print(f"  {scenario}: {count}件")

# COMMAND ----------

# DBTITLE 1,サンプル表示（各シナリオ1件ずつ）
import json

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
for prefix in ["stay", "tour", "store", "online", "corp"]:
    content = dbutils.fs.head(f"{volume_path}/{prefix}_0001.json", 65536)
    data = json.loads(content)
    print(f"\n{'='*60}")
    print(f"📄 {prefix}_0001.json — {data['survey_type']}")
    print(f"{'='*60}")
    # free_comment の先頭200文字だけ表示
    comment = data.get("free_comment", "")
    print(f"コメント冒頭: {comment[:200]}...")
    print(f"満足度: {data.get('satisfaction_score', 'N/A')}")
