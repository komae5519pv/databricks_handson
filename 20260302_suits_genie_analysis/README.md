# コールセンター × 売上データ分析デモ

通話ログという「理由データ」を売上・返品の「結果データ」と掛け合わせ、経営判断が変わる瞬間を体験してもらうデモです。

## 前提条件

- Databricksワークスペース（AWS）
- カタログ `komae_demo_v4` への書き込み権限
- サーバレスコンピュート or SQL Warehouse
- ai_extract を使用する場合: Foundation Model APIs が有効であること

## プロジェクト構成

```
00_config.py                     ← 設定（カタログ・スキーマ・ボリューム）
01_generate_structured_data.py   ← 構造化データ生成（4テーブル）
02_generate_call_logs.py         ← 通話ログJSONL生成（5,000件）→ Volume保存
03_bronze_call_logs.py           ← Bronze: Volume → ターン単位テーブル
04_silver_call_extract.py        ← Silver: ai_extract で構造化抽出
05_gold_analytics.py             ← Gold: 全テーブル結合（Genie分析用）
06_genie_setup.py                ← Genie Space 手動設定ガイド（実行不要）
demo_story.md                    ← デモストーリー・Genie質問集
README.md                        ← このファイル
```

## 実行手順

### 1. ノートブックをワークスペースにアップロード

```bash
databricks workspace import-dir \
  ./コールセンターデータ分析 \
  /Workspace/Users/<your-email>/03_External_Work/20260302_コールセンター分析 \
  --profile aws-profile --overwrite
```

### 2. ノートブックを順番に実行

全てサーバレスコンピュートで実行してください。

| 順番 | ノートブック | 内容 | 所要時間目安 |
|------|------------|------|------------|
| 1 | `00_config` | カタログ・スキーマ・ボリューム作成 | 〜1分 |
| 2 | `01_generate_structured_data` | customers(30K), offers(30), exposures(150K), orders(50K) 生成 | 〜5分 |
| 3 | `02_generate_call_logs` | 5,000件の通話ログJSONL生成・Volume保存 | 〜3分 |
| 4 | `03_bronze_call_logs` | Volume → Bronzeテーブル（ターン単位展開） | 〜2分 |
| 5 | `04_silver_call_extract` | ai_extract でコール単位に構造化抽出 | 〜10分 |
| 6 | `05_gold_analytics` | Goldテーブル2つを作成 | 〜3分 |

### 3. Genie Space を作成

`06_genie_setup` ノートブックのガイドに従って、手動でGenie Spaceを設定してください。

### 4. デモ実施

`demo_story.md` のストーリーラインに沿ってデモを実施してください。

## 生成されるテーブル一覧

| レイヤー | テーブル名 | 件数 | 説明 |
|---------|-----------|------|------|
| Bronze | bz_customers | 30,000 | 顧客マスタ |
| Bronze | bz_offers | 〜30 | キャンペーン・割引オファー |
| Bronze | bz_campaign_exposures | 150,000 | キャンペーン接触ログ |
| Bronze | bz_orders | 50,000 | 注文データ |
| Bronze | bz_call_logs | 〜50,000 | 通話ログ（ターン単位） |
| Silver | sv_call_analysis | 5,000 | 通話ログのAI構造化抽出 |
| Gold | gd_dashboard_fact | 50,000 | ダッシュボード用ファクト（注文粒度） |
| Gold | gd_customer_journey | 30,000 | 顧客ジャーニー横断分析 |
| Gold | gd_channel_performance | 〜40 | 媒体×商品パフォーマンス |

## スキーマの削除（リセット）

`00_config.py` のコメントアウト行を解除して実行してください：

```python
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
```

## 注意事項

- 特定の企業名は一切使用していません
- データは全て自動生成です（実データは含まれません）
- LINEチャネル経由の返品率は意図的に高く設定しています（デモストーリーの核心）
