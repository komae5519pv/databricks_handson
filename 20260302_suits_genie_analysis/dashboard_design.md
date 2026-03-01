# ダッシュボード帳票設計

## 基本情報

- **タイトル**: コールセンター × 売上データ分析
- **データソース**: `komae_demo_v4.call_center.gd_dashboard_fact`（単一テーブル・クロスフィルタリング対応）

## デモでの役割（2分）

1. KPIで事業規模を見せる
2. 媒体別の売上を見せる →「LINEは売上が多い。良い媒体ですよね。」
3. 返品率を見せる →「ところが返品率を見ると…LINEだけ突出して高い。なぜでしょう？」
4. Genieへ移行 →「深掘りしてみましょう。」

---

## フィルタ

| フィルタ名 | カラム | タイプ |
|-----------|--------|--------|
| 媒体チャネル | `campaign_channel` | マルチセレクト |
| 商品カテゴリ | `product_category` | マルチセレクト |
| 注文月 | `order_month` | 日付範囲 |

---

## レイアウト

### Row 1: KPIカード

| # | タイトル | チャートタイプ | 集計方法 | 備考 |
|---|---------|--------------|---------|------|
| 1 | 合計注文件数 | Counter | `COUNT(order_id)` | — |
| 2 | 合計売上金額 | Counter | `SUM(total_amount)` | — |
| 3 | 返品率 | Counter | 下記カスタム計算式を使用 | 表示フォーマット: パーセント |

**カスタム計算式（返品率）**
- **名前**: `返品率`
- **計算式**: `SUM(CASE WHEN returned THEN 1 ELSE 0 END) / COUNT(*)`
- **コメント**: `返品件数 / 注文件数。全体では約10%、LINEチャネルは15〜18%で突出して高い。`
| 4 | ユニーク顧客数 | Counter | `COUNT_DISTINCT(customer_id)` | — |

---

### Row 2: 媒体比較（★デモの核心）

| # | タイトル | チャートタイプ |
|---|---------|--------------|
| 5 | 媒体別 売上・注文件数 | 棒グラフ |
| 6 | 媒体別 返品率 | 棒グラフ |

#### 5. 媒体別 売上・注文件数

```sql
SELECT
    campaign_channel,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue
FROM gd_dashboard_fact
GROUP BY campaign_channel
ORDER BY total_revenue DESC
```

- X軸: `campaign_channel`
- Y軸: `total_revenue`（棒）、`order_count`（第2軸 or ラベル）
- デモでの役割: **「LINEは売上が多い。良い媒体に見えますよね。」**

#### 6. 媒体別 返品率

- X軸: `campaign_channel`
- Y軸: カスタム計算式 `返品率`
- デモでの役割: **★「ところがLINEだけ返品率が突出して高い。なぜでしょう？」→ Genieへ移行**

---

### Row 3: 返品分析 + 時系列

| # | タイトル | チャートタイプ |
|---|---------|--------------|
| 7 | 媒体別 返品理由の内訳 | 積み上げ棒グラフ |
| 8 | 月次売上推移 | 折れ線グラフ |

#### 7. 媒体別 返品理由の内訳

```sql
SELECT
    campaign_channel,
    return_reason,
    COUNT(*) AS cnt
FROM gd_dashboard_fact
WHERE returned = true
GROUP BY campaign_channel, return_reason
ORDER BY campaign_channel, cnt DESC
```

- X軸: `campaign_channel`
- Y軸: `cnt`
- 色分け: `return_reason`
- デモでの役割: LINEに「割引条件違い」が集中していることを視覚的に確認（Genieへの伏線）

#### 8. 月次売上推移

```sql
SELECT
    order_month,
    campaign_channel,
    SUM(total_amount) AS monthly_revenue
FROM gd_dashboard_fact
GROUP BY order_month, campaign_channel
ORDER BY order_month
```

- X軸: `order_month`
- Y軸: `monthly_revenue`
- 色分け: `campaign_channel`
- デモでの役割: 事業の推移を見せる全体感の補足

---

### Row 4: 地域 + 商品

| # | タイトル | チャートタイプ |
|---|---------|--------------|
| 9 | 都道府県別 売上分布 | MAP |
| 10 | 商品カテゴリ別 売上・返品率 | 棒グラフ |

#### 9. 都道府県別 売上分布

```sql
SELECT
    prefecture,
    latitude,
    longitude,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue
FROM gd_dashboard_fact
GROUP BY prefecture, latitude, longitude
```

- 緯度: `latitude`
- 経度: `longitude`
- サイズ: `total_revenue`
- デモでの役割: 視覚的なインパクト。地域偏在があればQ&Aネタにもなる

#### 10. 商品カテゴリ別 売上・返品率

- X軸: `product_category`
- Y軸: `SUM(total_amount)`（棒）、カスタム計算式 `返品率`（第2軸・折れ線）
- デモでの役割: スーツが主力商品であることを見せる。商品別の返品率差も確認可能

---

### Row 5: 通話ログ一覧

| # | タイトル | チャートタイプ |
|---|---------|--------------|
| 11 | 通話ログ一覧 | テーブル |

#### 11. 通話ログ一覧

**データセット名**: `call_log_detail`（`gd_dashboard_fact` とは別データセットとして追加）

```sql
SELECT
    call_date,            -- 通話日
    purpose,              -- 問い合わせ目的（割引クレーム/返品交換/配送納期 等）
    trouble_type,       -- トラブル要因（割引条件/サイズ/納期/品質/請求/なし）
    urgency_level,        -- 緊急度（高/中/低）
    resolution_status,    -- 解決状況（解決/未解決/エスカレーション）
    operator_action,      -- オペレーターの対応内容
    transcript_summary    -- 通話要約（ai_queryで事前生成、50文字以内）
FROM komae_demo_v4.call_center.sv_call_analysis
ORDER BY call_date DESC
```

**通話ログ用フィルタ**（上記データセットに紐づける）

| フィルタ名 | カラム | タイプ | デモでの使い方 |
|-----------|--------|--------|--------------|
| 問い合わせ目的 | `purpose` | マルチセレクト | 「割引クレーム」で絞ると割引関連の声だけ見える |
| トラブル要因 | `trouble_type` | マルチセレクト | 「割引条件」で絞ると★核心の通話が出る |
| 緊急度 | `urgency_level` | マルチセレクト | 「高」で絞ると面接・結婚式等の切実な声 |
| 解決状況 | `resolution_status` | マルチセレクト | 「未解決」で絞ると改善すべき対応が見える |

- デモでの役割: 数字だけでは見えない「顧客の実際の声」を確認できる。デモ中に「割引条件」でフィルタし、LINEの割引トラブルの通話テキストを見せると効果的
- 注意: `gd_dashboard_fact` とは別データセットのため、Row 1〜4のウィジェットとのクロスフィルタリングは効かない
