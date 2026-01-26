# Databricks notebook source
# MAGIC %md
# MAGIC # AI/BI Genie Research Agent サンプルプロンプト集
# MAGIC
# MAGIC このノートブックでは、Goldテーブルを使ってAI/BI Genie Research Agentに投げるサンプルプロンプトを紹介します。
# MAGIC
# MAGIC **対象テーブル:**
# MAGIC - `sl_users`       - シルバー層/顧客マスタ
# MAGIC - `sl_items`       - シルバー層/ 商品マスタ
# MAGIC - `sl_stores`      - シルバー層/ 店舗マスタ
# MAGIC - `sl_orders`      - シルバー層/ 注文データ（会計単位）
# MAGIC - `sl_order_items` - シルバー層/ 注文詳細（アイテム単位）
# MAGIC
# MAGIC OR
# MAGIC
# MAGIC - `gd_users` - 顧客マスタ（RFM・クラスター情報付き）
# MAGIC - `gd_orders` - 注文データ（会計単位）
# MAGIC - `gd_order_items` - 注文明細（アイテム単位）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 基本的な現状把握
# MAGIC
# MAGIC まずはデータの全体像を把握するための質問です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト例
# MAGIC
# MAGIC ```
# MAGIC 売上の推移を教えて。年別と月別で見たい。
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC うちのお客さんって何人くらいいて、どれくらいリピートしてくれてる？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 一番売れてる商品カテゴリは何？店舗ごとに違いはある？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 平均的なお客さんは1回の買い物でいくら使ってる？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 顧客セグメント発見
# MAGIC
# MAGIC お客様をグループ分けして、それぞれの特徴を理解するための質問です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト例
# MAGIC
# MAGIC ```
# MAGIC お客さんをいくつかのグループに分けて特徴を教えて。
# MAGIC 購買頻度とか金額とか、買い物の仕方で分類してほしい。
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC よく買ってくれるお客さんと、あまり来なくなったお客さんの違いは何？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 土日に買い物する人と平日に買い物する人で、買うものに違いはある？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 夜遅くに買い物する人ってどんな人？何を買ってる？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC まとめ買いする人の特徴を教えて。どんな商品を一緒に買ってる？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 新規・リピーター分析
# MAGIC
# MAGIC 新規顧客の獲得状況やリピート率を把握するための質問です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト例
# MAGIC
# MAGIC ```
# MAGIC 新規のお客さんは増えてる？減ってる？
# MAGIC 新規とリピーターの売上比率はどうなってる？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 初めて来たお客さんが2回目も来てくれる確率はどれくらい？
# MAGIC 何を買った人がリピートしやすい？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 新規のお客さんと常連さんで、買うものに違いはある？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 1回しか来てないお客さんはどれくらいいる？
# MAGIC どうすればもう1回来てもらえる？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 離脱リスク・アクション示唆
# MAGIC
# MAGIC 離脱しそうな顧客を特定し、対策を考えるための質問です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト例
# MAGIC
# MAGIC ```
# MAGIC 最近来なくなったお客さんはどれくらいいる？
# MAGIC どんな特徴がある人が離脱しやすい？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC このままだと来月離脱しそうなお客さんのリストを出して。
# MAGIC どんなアプローチをすればいい？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 以前はよく来てたのに最近来なくなった人を見つけて。
# MAGIC 何がきっかけで来なくなった？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 離脱したお客さんが最後に買った商品は何？共通点ある？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 季節・時間帯分析
# MAGIC
# MAGIC 時期や時間帯による購買傾向を把握するための質問です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト例
# MAGIC
# MAGIC ```
# MAGIC 12月って売上いいけど、何が特に売れてる？
# MAGIC どんなお客さんが12月に買いに来る？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 曜日ごとの売上傾向を教えて。土日と平日で違いはある？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 時間帯別の売上を見たい。ピークはいつ？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC GWやお盆の売上はどう？普段と比べて何が売れる？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 商品・カテゴリ分析
# MAGIC
# MAGIC 商品やカテゴリの売れ行きを分析するための質問です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト例
# MAGIC
# MAGIC ```
# MAGIC オーガニック商品を買う人の特徴を教えて。
# MAGIC 他のお客さんより単価高い？リピート率は？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 値引き商品ばかり買う人って、通常価格の商品も買ってくれるようになる？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 惣菜を買う人は他に何を一緒に買ってる？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 売れてない商品カテゴリはどれ？なぜ売れてない？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 高単価商品を買うお客さんの特徴は？どうやって増やせる？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 店舗分析
# MAGIC
# MAGIC 店舗ごとのパフォーマンスを比較するための質問です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト例
# MAGIC
# MAGIC ```
# MAGIC 店舗ごとの売上ランキングを見せて。
# MAGIC 成績のいい店舗と悪い店舗の違いは何？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 店舗によってお客さんの層は違う？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC エリアごとの売上傾向を教えて。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 深掘り・仮説検証
# MAGIC
# MAGIC より詳細な分析や仮説を検証するための質問です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト例
# MAGIC
# MAGIC ```
# MAGIC まとめ買いする人としない人で、年間の購入金額にどれくらい差がある？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC ロイヤル顧客になる人の初回購入には何か特徴がある？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 購入頻度が高い人は何曜日に買い物することが多い？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 客単価を上げるにはどうすればいい？
# MAGIC 何を一緒に買ってもらえばいい？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. アクション提案を求める
# MAGIC
# MAGIC 具体的な施策提案を求めるための質問です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト例
# MAGIC
# MAGIC ```
# MAGIC 売上を10%上げるには何をすればいい？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 来月のキャンペーン、どの顧客層に何を訴求すべき？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC メール配信するならどのお客さんに送るべき？
# MAGIC 何を訴求すれば反応してもらえそう？
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 新規顧客を増やすにはどうすればいい？
# MAGIC データから見えるヒントはある？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. すぐに試せるサンプル質問（コピペ用）
# MAGIC
# MAGIC 以下の質問をそのままコピーしてGenie Research Agentに投げてみてください。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問1: 全体像の把握
# MAGIC ```
# MAGIC この5年間の売上推移を年別に教えて。
# MAGIC 成長してる？どれくらいのペースで伸びてる？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問2: 顧客セグメントの発見
# MAGIC ```
# MAGIC お客さんを購買行動でグループ分けして、それぞれの特徴を教えて。
# MAGIC どのグループが一番売上に貢献してる？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問3: 週末 vs 平日の比較
# MAGIC ```
# MAGIC 土日に買い物する人と平日に買い物する人を比較して。
# MAGIC 客単価や購入点数に違いはある？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問4: 新規顧客の傾向
# MAGIC ```
# MAGIC 新規顧客の割合は年々どう変わってる？
# MAGIC 新規とリピーターで購入する商品カテゴリに違いはある？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問5: 離脱リスクの特定
# MAGIC ```
# MAGIC 最後の購入から60日以上経ってるお客さんは何人いる？
# MAGIC その人たちに共通する特徴はある？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問6: 時間帯分析
# MAGIC ```
# MAGIC 時間帯別の売上を見せて。
# MAGIC 夜（19時以降）に買い物する人はどんな商品を買ってる？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問7: ロイヤル顧客の分析
# MAGIC ```
# MAGIC 購入金額トップ10%のお客さんの特徴を教えて。
# MAGIC どうすればこういうお客さんを増やせる？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問8: 商品カテゴリの併売分析
# MAGIC ```
# MAGIC 惣菜を買う人は他に何を一緒に買ってることが多い？
# MAGIC おすすめの併売商品を提案して。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問9: 季節性の発見
# MAGIC ```
# MAGIC 12月の売上は他の月と比べてどれくらい高い？
# MAGIC 12月に特に売れる商品カテゴリは何？
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 質問10: アクション提案
# MAGIC ```
# MAGIC 来月の売上を上げるために、どの顧客層にどんなアプローチをすべき？
# MAGIC データに基づいて具体的な施策を3つ提案して。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## プロンプトのコツ
# MAGIC
# MAGIC | コツ | 例 |
# MAGIC |------|-----|
# MAGIC | **比較を求める** | 「〜と〜で違いはある？」 |
# MAGIC | **理由を聞く** | 「なぜ〜なの？」 |
# MAGIC | **アクションを求める** | 「どうすればいい？」 |
# MAGIC | **具体的な数字を求める** | 「どれくらい？」「何%？」 |
# MAGIC | **深掘りする** | 「もう少し詳しく教えて」 |
# MAGIC | **可視化を求める** | 「グラフで見せて」 |
# MAGIC
# MAGIC **ポイント:** 分析手法ではなく「知りたいこと」をそのまま聞くのが効果的です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 注意事項
# MAGIC
# MAGIC - Genie Research Agentは対象テーブルの構造を理解した上で回答します
# MAGIC - 複雑な質問は段階的に聞くと精度が上がります
# MAGIC - 回答に違和感があれば「本当に？」「根拠は？」と確認しましょう
# MAGIC - グラフや表で見たい場合は明示的にリクエストしてください
