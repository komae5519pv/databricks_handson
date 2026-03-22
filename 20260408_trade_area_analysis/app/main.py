"""
Trade Area Analysis App - 包括的商圏分析バックエンド
Lv1（基礎分析）→ Lv2（深掘分析）→ Lv3（施策立案）の3段階分析
"""
import os
import json
import time
import threading
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from databricks.sdk import WorkspaceClient
import requests

app = FastAPI(title="Trade Area Analysis App")

# ============================================================
# Configuration (環境変数から読み込み、なければデフォルト値を使用)
# ============================================================
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "01f125245982136a801da22015caab4c")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "4b9b953939869799")
CATALOG = os.environ.get("DATA_CATALOG", "komae_demo_v4")
SCHEMA = os.environ.get("DATA_SCHEMA", "trade_area")

# テーブル名のプレフィックス
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
ANALYSIS_CACHE_DIR = BASE_DIR / "analysis_cache"
ANALYSIS_CACHE_DIR.mkdir(exist_ok=True)

def get_workspace_client() -> WorkspaceClient:
    """WorkspaceClientを取得（Databricks Appsの組み込み認証を使用）"""
    # Databricks Apps では組み込みのサービスプリンシパル認証を使用
    # 明示的なauth_type指定は不要（SDKが自動検出）
    return WorkspaceClient()

w = get_workspace_client()

# ============================================================
# Analysis Storage (In-Memory + File Persistence)
# ============================================================
analysis_store: Dict[str, Dict[str, Any]] = {}
analysis_lock = threading.Lock()

def get_analysis_path(store_id: str) -> Path:
    return ANALYSIS_CACHE_DIR / f"{store_id}_analysis.json"

def save_analysis(store_id: str, data: dict):
    """分析結果を保存"""
    with analysis_lock:
        analysis_store[store_id] = data
        try:
            with open(get_analysis_path(store_id), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            print(f"Failed to save analysis: {e}")

def load_analysis(store_id: str) -> Optional[dict]:
    """分析結果を読み込み"""
    with analysis_lock:
        if store_id in analysis_store:
            return analysis_store[store_id]
    path = get_analysis_path(store_id)
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                with analysis_lock:
                    analysis_store[store_id] = data
                return data
        except:
            pass
    return None

# ============================================================
# Models
# ============================================================
class AnalysisRequest(BaseModel):
    store_id: str
    force_refresh: bool = False

# ============================================================
# Helper Functions
# ============================================================
def execute_query(query: str, return_columns: bool = False):
    """クエリを実行してデータを返す。return_columns=Trueの場合は(columns, data)のタプルを返す"""
    print(f"[DEBUG] Using warehouse_id: {WAREHOUSE_ID}")

    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="50s"
        )

        if result.status and result.status.state:
            if result.status.state.value == "FAILED":
                print(f"[ERROR] Query failed: {result.status.error}")
                return ([], []) if return_columns else []

        columns = []
        if return_columns and hasattr(result, 'manifest') and result.manifest:
            schema = getattr(result.manifest, 'schema', None)
            if schema and hasattr(schema, 'columns') and schema.columns:
                columns = [getattr(c, 'name', f'col{i}') for i, c in enumerate(schema.columns)]

        data = []
        if result.result and result.result.data_array:
            data = result.result.data_array

        if return_columns:
            return (columns, data)
        return data
    except Exception as e:
        print(f"[ERROR] Query execution error: {e}")
        print(f"[ERROR] Query: {query[:200]}...")
        return ([], []) if return_columns else []

def get_foundation_model_analysis(prompt: str) -> str:
    serving_endpoint = os.environ.get("DATABRICKS_SERVING_ENDPOINT", "databricks-claude-sonnet-4-5")
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")

    try:
        token = w.config.token
    except:
        token = os.environ.get("DATABRICKS_TOKEN", "")

    url = f"{host}/serving-endpoints/{serving_endpoint}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 4000,
        "temperature": 0.7
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)

    if response.status_code == 200:
        result = response.json()
        if "choices" in result and len(result["choices"]) > 0:
            return result["choices"][0].get("message", {}).get("content", "")

    raise Exception(f"Foundation Model API error: {response.status_code}")

# ============================================================
# Lv1 基礎分析
# ============================================================
def run_lv1_analysis(store_id: str) -> dict:
    results = {}

    # 店舗基本情報と業績
    query = f"""
    SELECT s.store_id, s.store_name, s.prefecture, s.city, s.size_sqm, s.store_type, s.parking_capacity,
           ROUND(AVG(sm.yoy_change) * 100, 2) as avg_yoy_pct,
           ROUND(SUM(sm.sales_amount) / 10000, 0) as annual_sales_man,
           ROUND(AVG(sm.customer_count), 0) as avg_monthly_customers,
           ROUND(AVG(sm.avg_basket), 0) as avg_basket
    FROM {TABLE_PREFIX}.stores s
    JOIN {TABLE_PREFIX}.sales_monthly sm ON s.store_id = sm.store_id
    WHERE s.store_id = '{store_id}' AND YEAR(sm.month) = 2024
    GROUP BY s.store_id, s.store_name, s.prefecture, s.city, s.size_sqm, s.store_type, s.parking_capacity
    """
    rows = execute_query(query)
    if rows:
        r = rows[0]
        results["store_info"] = {
            "store_id": r[0], "store_name": r[1], "prefecture": r[2], "city": r[3],
            "size_sqm": int(r[4]) if r[4] else 0, "store_type": r[5],
            "parking_capacity": int(r[6]) if r[6] else 0,
            "avg_yoy_pct": float(r[7]) if r[7] else 0,
            "annual_sales_man": float(r[8]) if r[8] else 0,
            "avg_monthly_customers": int(r[9]) if r[9] else 0,
            "avg_basket": int(r[10]) if r[10] else 0
        }

    # 月次トレンド
    query = f"""
    SELECT month, sales_amount, yoy_change, customer_count, avg_basket
    FROM {TABLE_PREFIX}.sales_monthly WHERE store_id = '{store_id}' ORDER BY month
    """
    rows = execute_query(query)
    results["monthly_trend"] = [
        {"month": str(r[0]), "sales": float(r[1]) if r[1] else 0,
         "yoy_change": float(r[2]) if r[2] else 0,
         "customers": int(r[3]) if r[3] else 0, "basket": float(r[4]) if r[4] else 0}
        for r in rows
    ]

    # 商圏人口統計
    query = f"""
    SELECT population_5km, households, avg_income, elderly_rate, young_adult_rate,
           detached_house_rate, day_night_ratio, num_businesses
    FROM {TABLE_PREFIX}.trade_area WHERE store_id = '{store_id}'
    """
    rows = execute_query(query)
    if rows:
        r = rows[0]
        results["demographics"] = {
            "population_5km": int(r[0]) if r[0] else 0,
            "households": int(r[1]) if r[1] else 0,
            "avg_income": int(r[2]) if r[2] else 0,
            "elderly_rate": float(r[3]) if r[3] else 0,
            "young_adult_rate": float(r[4]) if r[4] else 0,
            "detached_house_rate": float(r[5]) if r[5] else 0,
            "day_night_ratio": float(r[6]) if r[6] else 0,
            "num_businesses": int(r[7]) if r[7] else 0
        }

    # 競合
    query = f"""
    SELECT competitor_name, distance_km, size_sqm, open_year, is_new_entry
    FROM {TABLE_PREFIX}.competitors WHERE store_id = '{store_id}' ORDER BY distance_km
    """
    rows = execute_query(query)
    results["competitors"] = [
        {"name": r[0], "distance_km": float(r[1]) if r[1] else 0,
         "size_sqm": int(r[2]) if r[2] else 0, "open_year": int(r[3]) if r[3] else 0,
         "is_new_entry": r[4] == True or r[4] == "true"}
        for r in rows
    ]

    # 全店平均
    query = f"""
    SELECT ROUND(AVG(yoy_change) * 100, 2), ROUND(AVG(sales_amount) / 10000 * 12, 0)
    FROM {TABLE_PREFIX}.sales_monthly WHERE YEAR(month) = 2024
    """
    rows = execute_query(query)
    if rows:
        results["chain_average"] = {
            "avg_yoy_pct": float(rows[0][0]) if rows[0][0] else 0,
            "avg_annual_sales_man": float(rows[0][1]) if rows[0][1] else 0
        }

    return results

# ============================================================
# Lv2 深掘分析
# ============================================================
def run_lv2_analysis(store_id: str, lv1: dict) -> dict:
    results = {}

    # カテゴリ別ギャップ
    query = f"""
    WITH store_sales AS (
        SELECT c.category_id, c.category_name, c.expenditure_mapping, SUM(sc.sales_amount) as sales
        FROM {TABLE_PREFIX}.sales_by_category sc
        JOIN {TABLE_PREFIX}.categories c ON sc.category_id = c.category_id
        WHERE sc.store_id = '{store_id}' AND YEAR(sc.month) = 2024
        GROUP BY c.category_id, c.category_name, c.expenditure_mapping
    ),
    avg_sales AS (
        SELECT c.category_id, c.category_name, SUM(sc.sales_amount) / COUNT(DISTINCT sc.store_id) as avg_sales
        FROM {TABLE_PREFIX}.sales_by_category sc
        JOIN {TABLE_PREFIX}.categories c ON sc.category_id = c.category_id
        WHERE YEAR(sc.month) = 2024 GROUP BY c.category_id, c.category_name
    )
    SELECT ss.category_name, ss.expenditure_mapping,
           ROUND(ss.sales / 10000, 0), ROUND(a.avg_sales / 10000, 0),
           ROUND((ss.sales - a.avg_sales) / 10000, 0),
           ROUND((ss.sales - a.avg_sales) / a.avg_sales * 100, 1)
    FROM store_sales ss JOIN avg_sales a ON ss.category_id = a.category_id ORDER BY 5
    """
    rows = execute_query(query)
    results["category_analysis"] = [
        {"category": r[0], "mapping": r[1],
         "store_sales": float(r[2]) if r[2] else 0, "avg_sales": float(r[3]) if r[3] else 0,
         "gap": float(r[4]) if r[4] else 0, "gap_pct": float(r[5]) if r[5] else 0}
        for r in rows
    ]

    # 消費支出ポテンシャル
    query = f"""
    SELECT expenditure_total, garden_plants, garden_supplies, pet_food, pet_supplies,
           housing_repair_total, furniture_household_total, household_durables,
           household_miscellaneous, construction_services
    FROM {TABLE_PREFIX}.trade_area_expenditure WHERE store_id = '{store_id}'
    """
    rows = execute_query(query)
    if rows:
        r = rows[0]
        results["expenditure"] = {
            "total": int(r[0]) if r[0] else 0,
            "garden_plants": int(r[1]) if r[1] else 0, "garden_supplies": int(r[2]) if r[2] else 0,
            "pet_food": int(r[3]) if r[3] else 0, "pet_supplies": int(r[4]) if r[4] else 0,
            "housing_repair": int(r[5]) if r[5] else 0, "furniture_household": int(r[6]) if r[6] else 0,
            "household_durables": int(r[7]) if r[7] else 0, "household_misc": int(r[8]) if r[8] else 0,
            "construction_services": int(r[9]) if r[9] else 0
        }

    # ハフモデル簡易推定
    query = f"""
    WITH all_stores AS (
        SELECT 'self' as type, size_sqm, 0 as distance_km FROM {TABLE_PREFIX}.stores WHERE store_id = '{store_id}'
        UNION ALL
        SELECT 'competitor', size_sqm, distance_km FROM {TABLE_PREFIX}.competitors WHERE store_id = '{store_id}'
    ),
    attraction AS (
        SELECT type, size_sqm, distance_km,
               CASE WHEN distance_km = 0 THEN size_sqm ELSE size_sqm / POWER(distance_km, 2) END as attr
        FROM all_stores
    )
    SELECT type, size_sqm, ROUND(attr / SUM(attr) OVER () * 100, 1) as share FROM attraction ORDER BY share DESC
    """
    rows = execute_query(query)
    results["huff_model"] = {"self_share": 0, "competitors": []}
    for r in rows:
        if r[0] == "self":
            results["huff_model"]["self_share"] = float(r[2]) if r[2] else 0
        else:
            results["huff_model"]["competitors"].append({"size_sqm": int(r[1]) if r[1] else 0, "share": float(r[2]) if r[2] else 0})

    # 近隣施設
    query = f"""
    SELECT facility_type, facility_name, distance_km, traffic_impact, opening_hours
    FROM {TABLE_PREFIX}.nearby_facilities WHERE store_id = '{store_id}' ORDER BY traffic_impact DESC
    """
    rows = execute_query(query)
    results["facilities"] = [
        {"type": r[0], "name": r[1], "distance": float(r[2]) if r[2] else 0,
         "impact": float(r[3]) if r[3] else 0, "hours": r[4]}
        for r in rows
    ]

    return results

# ============================================================
# Lv3 施策立案
# ============================================================
def run_lv3_analysis(store_id: str, lv1: dict, lv2: dict) -> dict:
    results = {}

    # 類似店舗
    query = f"""
    SELECT ss.similar_store_id, s.store_name, s.prefecture, s.city,
           ss.similarity_rank, ss.similarity_score,
           ROUND(AVG(sm.yoy_change) * 100, 1), ROUND(SUM(sm.sales_amount) / 10000, 0)
    FROM {TABLE_PREFIX}.similar_stores ss
    JOIN {TABLE_PREFIX}.stores s ON ss.similar_store_id = s.store_id
    JOIN {TABLE_PREFIX}.sales_monthly sm ON ss.similar_store_id = sm.store_id
    WHERE ss.store_id = '{store_id}' AND YEAR(sm.month) = 2024
    GROUP BY ss.similar_store_id, s.store_name, s.prefecture, s.city, ss.similarity_rank, ss.similarity_score
    ORDER BY ss.similarity_rank
    """
    rows = execute_query(query)
    results["similar_stores"] = [
        {"store_id": r[0], "store_name": r[1], "location": f"{r[2]} {r[3]}",
         "rank": int(r[4]), "score": float(r[5]) if r[5] else 0,
         "yoy_pct": float(r[6]) if r[6] else 0, "sales_man": float(r[7]) if r[7] else 0}
        for r in rows
    ]

    # ベンチマーク差分
    best_similar = next((s for s in results["similar_stores"] if s["yoy_pct"] > 0), None)
    if best_similar:
        similar_id = best_similar["store_id"]
        query = f"""
        WITH target AS (
            SELECT c.category_name, SUM(sc.sales_amount) / 10000 as sales
            FROM {TABLE_PREFIX}.sales_by_category sc
            JOIN {TABLE_PREFIX}.categories c ON sc.category_id = c.category_id
            WHERE sc.store_id = '{store_id}' AND YEAR(sc.month) = 2024 GROUP BY c.category_name
        ),
        benchmark AS (
            SELECT c.category_name, SUM(sc.sales_amount) / 10000 as sales
            FROM {TABLE_PREFIX}.sales_by_category sc
            JOIN {TABLE_PREFIX}.categories c ON sc.category_id = c.category_id
            WHERE sc.store_id = '{similar_id}' AND YEAR(sc.month) = 2024 GROUP BY c.category_name
        )
        SELECT t.category_name, ROUND(t.sales, 0), ROUND(b.sales, 0), ROUND(b.sales - t.sales, 0)
        FROM target t JOIN benchmark b ON t.category_name = b.category_name ORDER BY 4 DESC
        """
        rows = execute_query(query)
        results["benchmark_gaps"] = [
            {"category": r[0], "target": float(r[1]) if r[1] else 0,
             "benchmark": float(r[2]) if r[2] else 0, "gap": float(r[3]) if r[3] else 0}
            for r in rows
        ]

    # 過去施策
    query = f"""
    SELECT measure_type, description, status, expected_effect, actual_effect,
           start_date, end_date, related_categories, investment_amount
    FROM {TABLE_PREFIX}.store_measures WHERE store_id = '{store_id}' ORDER BY start_date DESC
    """
    rows = execute_query(query)
    results["measures"] = [
        {"type": r[0], "description": r[1], "status": r[2],
         "expected": float(r[3]) if r[3] else 0, "actual": float(r[4]) if r[4] else None,
         "start": str(r[5]) if r[5] else None, "end": str(r[6]) if r[6] else None,
         "categories": r[7].split(",") if r[7] else [], "investment": int(r[8]) if r[8] else 0}
        for r in rows
    ]

    return results

# ============================================================
# AI総合レポート生成
# ============================================================
def synthesize_ai_report(store_id: str, lv1: dict, lv2: dict, lv3: dict) -> str:
    store = lv1.get("store_info", {})
    demo = lv1.get("demographics", {})
    chain = lv1.get("chain_average", {})
    comps = lv1.get("competitors", [])
    cats = lv2.get("category_analysis", [])
    exp = lv2.get("expenditure", {})
    huff = lv2.get("huff_model", {})
    facs = lv2.get("facilities", [])
    similar = lv3.get("similar_stores", [])
    gaps = lv3.get("benchmark_gaps", [])
    measures = lv3.get("measures", [])

    worst_cats = [c for c in cats if c.get("gap", 0) < 0][:3]
    best_cats = [c for c in reversed(cats) if c.get("gap", 0) > 0][:3]
    new_comps = [c for c in comps if c.get("is_new_entry")]
    success_similar = [s for s in similar if s.get("yoy_pct", 0) > 0][:2]

    prompt = f"""あなたはホームセンターチェーンの商圏分析エキスパートです。
以下の包括的な分析データに基づいて、詳細な診断レポートを作成してください。

═══════════════════════════════════════════════════════════════
■ 店舗概要
═══════════════════════════════════════════════════════════════
店舗名: {store.get('store_name', '')}（{store.get('prefecture', '')} {store.get('city', '')}）
売場面積: {store.get('size_sqm', 0):,}㎡ / 駐車場: {store.get('parking_capacity', 0)}台
店舗タイプ: {store.get('store_type', '')}

■ 業績サマリー
- 年間売上: {store.get('annual_sales_man', 0):,.0f}万円（全店平均: {chain.get('avg_annual_sales_man', 0):,.0f}万円）
- 前年比: {store.get('avg_yoy_pct', 0):+.1f}%（全店平均: {chain.get('avg_yoy_pct', 0):+.1f}%）
- 月間平均客数: {store.get('avg_monthly_customers', 0):,}人
- 平均客単価: {store.get('avg_basket', 0):,}円

═══════════════════════════════════════════════════════════════
■ 商圏環境分析
═══════════════════════════════════════════════════════════════
【人口統計】
- 5km圏人口: {demo.get('population_5km', 0):,}人 / 世帯数: {demo.get('households', 0):,}世帯
- 平均世帯年収: {demo.get('avg_income', 0)//10000}万円
- 高齢化率: {demo.get('elderly_rate', 0)*100:.1f}% / 若年層比率: {demo.get('young_adult_rate', 0)*100:.1f}%
- 戸建て比率: {demo.get('detached_house_rate', 0)*100:.1f}%

【消費支出ポテンシャル】（商圏内推計年間支出額）
- 総額: {exp.get('total', 0)//10000:,}万円
- 園芸関連: {(exp.get('garden_plants', 0) + exp.get('garden_supplies', 0))//10000:,}万円
- ペット関連: {(exp.get('pet_food', 0) + exp.get('pet_supplies', 0))//10000:,}万円
- 住宅修繕(DIY): {exp.get('housing_repair', 0)//10000:,}万円
- 工事外注: {exp.get('construction_services', 0)//10000:,}万円

【競合環境】
競合店舗数: {len(comps)}店舗 / 推定市場シェア: {huff.get('self_share', 0):.1f}%
{chr(10).join([f"- {c['name']}: {c['distance_km']:.1f}km, {c['size_sqm']:,}㎡{' ★新規出店' if c.get('is_new_entry') else ''}" for c in comps[:5]])}

【主要近隣施設】
{chr(10).join([f"- {f['name']}（{f['type']}）: {f['distance']:.1f}km, 集客影響度{f['impact']:.0f}" for f in facs[:5]])}

═══════════════════════════════════════════════════════════════
■ カテゴリ別パフォーマンス
═══════════════════════════════════════════════════════════════
【弱いカテゴリ（全店平均比マイナス）】
{chr(10).join([f"- {c['category']}: {c['gap']:+.0f}万円（{c['gap_pct']:+.1f}%）" for c in worst_cats])}

【強いカテゴリ（全店平均比プラス）】
{chr(10).join([f"- {c['category']}: {c['gap']:+.0f}万円（{c['gap_pct']:+.1f}%）" for c in best_cats])}

═══════════════════════════════════════════════════════════════
■ 類似店舗ベンチマーク
═══════════════════════════════════════════════════════════════
【成功している類似店舗】
{chr(10).join([f"- {s['store_name']}（{s['location']}）: 前年比{s['yoy_pct']:+.1f}%, 売上{s['sales_man']:,.0f}万円, 類似度{s['score']:.0%}" for s in success_similar]) if success_similar else "該当店舗なし"}

【ベンチマーク店舗とのカテゴリ別差異（上位5）】
{chr(10).join([f"- {g['category']}: 差{g['gap']:+.0f}万円" for g in gaps[:5]]) if gaps else "データなし"}

═══════════════════════════════════════════════════════════════
■ 過去施策の実績
═══════════════════════════════════════════════════════════════
{chr(10).join([f"- [{m['status']}] {m['type']}: {m['description']} (期待:{m['expected']*100:+.1f}% → 実績:{m['actual']*100 if m['actual'] else '測定中':+.1f}%)" for m in measures[:5]]) if measures else "過去施策データなし"}

═══════════════════════════════════════════════════════════════
上記データを踏まえ、以下の構成で詳細な診断レポートを作成してください。
データに基づいた具体的な数値を引用しながら、実行可能なアクションを提案してください。

---

## 📊 エグゼクティブサマリー
（3-4文で店舗の現状と最重要課題を要約）

## 🔍 詳細診断

### 1. 業績状況の診断
（売上推移、客数・客単価の傾向、全店平均との乖離を分析）

### 2. 商圏環境の評価
（人口動態、住民特性、消費ポテンシャルから見た店舗立地の強み・弱み）

### 3. 競合影響の分析
（競合状況、新規出店の影響、市場シェアの評価）

### 4. カテゴリ別課題
（弱いカテゴリの原因仮説、商圏ポテンシャルとの乖離分析）

## 💡 改善提案

### 🏪 棚割り・品揃え改善
（具体的なカテゴリ名と売場面積・SKU数の提案。例: 「園芸用品を現状120㎡→150㎡に拡大」）

### 💰 価格戦略
（競合対抗価格、プライスライン見直しの提案。例: 「DIY工具の価格帯を競合より5%下げる」）

### 📢 販促施策
（ターゲット顧客層に合わせた具体的な施策。例: 「高齢者向け園芸教室を月2回開催」）

### ⚙️ オペレーション改善
（営業時間、接客、在庫管理などの提案）

## 📈 優先度マトリクス

| 施策 | インパクト | 実現容易性 | 優先度 |
|------|-----------|-----------|-------|
（上位5つの施策を表形式で。インパクト・実現容易性は「高/中/低」で評価）

## 🎯 期待効果
（施策実行による売上改善の見込みを具体的な数値で。例: 「6ヶ月後に前年比+5%、年間売上+XXX万円」）

## ⏭️ 次のステップ
（今後1ヶ月、3ヶ月、6ヶ月のアクションプラン）
"""

    try:
        return get_foundation_model_analysis(prompt)
    except Exception as e:
        return f"""## ⚠️ AI分析エラー
Foundation Model APIでエラーが発生しました: {str(e)}

## 基本分析結果
- 店舗: {store.get('store_name', '')}
- 前年比: {store.get('avg_yoy_pct', 0):+.1f}%（全店平均: {chain.get('avg_yoy_pct', 0):+.1f}%）
- 弱いカテゴリ: {', '.join([c['category'] for c in worst_cats])}
- 推奨: 上記カテゴリの棚割り見直しを検討してください
"""

def run_full_analysis(store_id: str):
    """バックグラウンドで包括的分析を実行"""
    print(f"[Analysis] Starting full analysis for store: {store_id}")
    data = {
        "store_id": store_id, "status": "running", "current_phase": "lv1",
        "progress": 0, "started_at": datetime.now().isoformat(),
        "completed_at": None, "error": None,
        "lv1": None, "lv2": None, "lv3": None, "ai_report": None
    }
    save_analysis(store_id, data)

    try:
        print(f"[Analysis] Phase: lv1")
        data["current_phase"] = "lv1"
        data["progress"] = 10
        save_analysis(store_id, data)
        lv1 = run_lv1_analysis(store_id)
        data["lv1"] = lv1
        data["progress"] = 25
        save_analysis(store_id, data)
        print(f"[Analysis] lv1 completed")

        print(f"[Analysis] Phase: lv2")
        data["current_phase"] = "lv2"
        data["progress"] = 30
        save_analysis(store_id, data)
        lv2 = run_lv2_analysis(store_id, lv1)
        data["lv2"] = lv2
        data["progress"] = 50
        save_analysis(store_id, data)
        print(f"[Analysis] lv2 completed")

        print(f"[Analysis] Phase: lv3")
        data["current_phase"] = "lv3"
        data["progress"] = 55
        save_analysis(store_id, data)
        lv3 = run_lv3_analysis(store_id, lv1, lv2)
        data["lv3"] = lv3
        data["progress"] = 75
        save_analysis(store_id, data)
        print(f"[Analysis] lv3 completed")

        print(f"[Analysis] Phase: ai_synthesis")
        data["current_phase"] = "ai_synthesis"
        data["progress"] = 80
        save_analysis(store_id, data)
        ai_report = synthesize_ai_report(store_id, lv1, lv2, lv3)
        data["ai_report"] = ai_report
        data["progress"] = 100
        data["status"] = "completed"
        data["completed_at"] = datetime.now().isoformat()
        save_analysis(store_id, data)
        print(f"[Analysis] COMPLETED for store: {store_id}")

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"[Analysis] ERROR for store {store_id}: {str(e)}")
        print(f"[Analysis] Traceback: {error_details}")
        data["status"] = "failed"
        data["error"] = f"{str(e)} (Phase: {data.get('current_phase', 'unknown')})"
        data["completed_at"] = datetime.now().isoformat()
        save_analysis(store_id, data)

# ============================================================
# API Endpoints
# ============================================================
@app.get("/api/health")
def health_check():
    return {"status": "healthy"}

@app.get("/api/user")
def get_current_user():
    try:
        user = w.current_user.me()
        return {"user_name": user.user_name, "display_name": user.display_name or user.user_name.split("@")[0]}
    except:
        return {"display_name": "ゲスト"}

@app.get("/api/debug")
def debug_info():
    """デバッグ用エンドポイント"""
    info = {
        "warehouse_id": WAREHOUSE_ID or "NOT_SET",
        "genie_space_id": GENIE_SPACE_ID or "NOT_SET",
        "catalog": CATALOG or "NOT_SET",
        "schema": SCHEMA or "NOT_SET",
        "table_prefix": TABLE_PREFIX,
        "host": os.environ.get("DATABRICKS_HOST", "NOT_SET"),
    }
    # テストクエリ
    try:
        rows = execute_query("SELECT 1 as test")
        info["query_test"] = "OK" if rows else "EMPTY"
        info["query_result"] = rows

        # 店舗クエリテスト
        rows2 = execute_query(f"SELECT store_id, store_name FROM {TABLE_PREFIX}.stores LIMIT 3")
        info["stores_test"] = "OK" if rows2 else "EMPTY"
        info["stores_sample"] = rows2[:3] if rows2 else []

        # 特定店舗クエリテスト
        rows3 = execute_query(f"SELECT store_id, store_name FROM {TABLE_PREFIX}.stores WHERE store_id = 'S025'")
        info["store_filter_test"] = "OK" if rows3 else "EMPTY"
        info["store_filter_result"] = rows3
    except Exception as e:
        info["query_test"] = f"ERROR: {str(e)}"
    return info

@app.get("/api/stores/{store_id}/overview-test")
def get_store_overview_test(store_id: str):
    """テスト用モックデータ"""
    return {
        "store": {
            "store_id": store_id, "store_name": "テスト店舗", "prefecture": "東京都", "city": "渋谷区",
            "size_sqm": 1500, "store_type": "都市型", "parking_capacity": 50,
            "avg_yoy_pct": -5.5, "annual_sales_man": 35000, "avg_monthly_customers": 12000, "avg_basket": 2800
        },
        "chain_avg": {"avg_yoy_pct": -2.0, "avg_annual_sales_man": 40000},
        "demographics": {"population_5km": 150000, "households": 65000, "avg_income": 5500000, "elderly_rate": 0.22, "young_adult_rate": 0.25, "detached_house_rate": 0.15},
        "competitors": [{"name": "カインズ渋谷", "distance_km": 1.2, "size_sqm": 2000, "is_new_entry": False}],
        "top_categories": [{"category": "園芸用品", "sales_man": 5000}, {"category": "DIY用品", "sales_man": 4500}],
        "monthly_trend": [{"month": "2024-01", "sales": 2500000, "yoy_change": -0.03, "customers": 11000}]
    }

@app.get("/api/stores")
def get_stores():
    query = f"""
    SELECT s.store_id, s.store_name, s.prefecture, s.city,
           ROUND(AVG(sm.yoy_change), 3), ROUND(SUM(sm.sales_amount) / 10000, 0),
           s.latitude, s.longitude
    FROM {TABLE_PREFIX}.stores s
    JOIN {TABLE_PREFIX}.sales_monthly sm ON s.store_id = sm.store_id
    WHERE YEAR(sm.month) = 2024
    GROUP BY s.store_id, s.store_name, s.prefecture, s.city, s.latitude, s.longitude ORDER BY 5
    """
    rows = execute_query(query)
    stores = []
    for row in rows:
        yoy = float(row[4]) if row[4] else 0
        if yoy >= 0: alert = "good"
        elif yoy > -0.10: alert = "normal"
        elif yoy > -0.15: alert = "warning"
        else: alert = "critical"
        analysis = load_analysis(row[0])
        stores.append({
            "store_id": row[0], "store_name": row[1], "prefecture": row[2], "city": row[3],
            "yoy_change": yoy, "sales_amount": float(row[5]) if row[5] else 0,
            "latitude": float(row[6]) if row[6] else None,
            "longitude": float(row[7]) if row[7] else None,
            "alert_level": alert, "analysis_status": analysis.get("status") if analysis else None
        })
    return {"stores": stores}

@app.get("/api/business/overview")
def get_business_overview(
    health_filter: str = None,
    prefecture_filter: str = None,
    store_id_filter: str = None
):
    """事業全体ダッシュボード用のデータを取得

    Optional filters for monthly_trend:
    - health_filter: 'healthy', 'caution', 'critical'
    - prefecture_filter: 県名
    - store_id_filter: 特定の店舗ID
    """
    result = {}

    # Build filter conditions for trend query
    trend_filter_conditions = []
    if store_id_filter:
        trend_filter_conditions.append(f"s.store_id = '{store_id_filter}'")
    if prefecture_filter:
        trend_filter_conditions.append(f"s.prefecture = '{prefecture_filter}'")
    # Health filter requires a subquery since it's based on aggregated yoy_change
    health_subquery = None
    if health_filter:
        if health_filter == 'healthy':
            health_subquery = "AVG(yoy_change) >= 0"
        elif health_filter == 'caution':
            health_subquery = "AVG(yoy_change) >= -0.10 AND AVG(yoy_change) < 0"
        elif health_filter == 'critical':
            health_subquery = "AVG(yoy_change) < -0.10"

    # 1. 全店舗KPI集計
    try:
        kpi_query = f"""
        SELECT
            COUNT(DISTINCT s.store_id) as store_count,
            ROUND(SUM(sm.sales_amount) / 100000000, 2) as total_sales_oku,
            ROUND(AVG(sm.yoy_change) * 100, 1) as avg_yoy_pct,
            ROUND(AVG(sm.customer_count), 0) as avg_customers,
            ROUND(AVG(sm.avg_basket), 0) as avg_basket
        FROM {TABLE_PREFIX}.stores s
        JOIN {TABLE_PREFIX}.sales_monthly sm ON s.store_id = sm.store_id
        WHERE YEAR(sm.month) = 2024
        """
        rows = execute_query(kpi_query)
        if rows:
            r = rows[0]
            result["kpi"] = {
                "store_count": int(float(r[0])) if r[0] else 0,
                "total_sales_oku": float(r[1]) if r[1] else 0,
                "avg_yoy_pct": float(r[2]) if r[2] else 0,
                "avg_customers": int(float(r[3])) if r[3] else 0,
                "avg_basket": int(float(r[4])) if r[4] else 0
            }
    except Exception as e:
        print(f"[ERROR] KPI query failed: {e}")
        result["kpi"] = {"store_count": 0, "total_sales_oku": 0, "avg_yoy_pct": 0, "avg_customers": 0, "avg_basket": 0}

    # 2. 健康度別店舗数
    try:
        health_query = f"""
        SELECT
            CASE
                WHEN AVG(yoy_change) >= 0 THEN 'healthy'
                WHEN AVG(yoy_change) >= -0.10 THEN 'caution'
                ELSE 'critical'
            END as health_status,
            COUNT(*) as count
        FROM {TABLE_PREFIX}.sales_monthly
        WHERE YEAR(month) = 2024
        GROUP BY store_id
        """
        # Group by health status
        rows = execute_query(health_query)
        health_counts = {"healthy": 0, "caution": 0, "critical": 0}
        for row in rows:
            status = row[0]
            if status in health_counts:
                health_counts[status] += 1
        result["health_distribution"] = health_counts
    except Exception as e:
        print(f"[ERROR] Health query failed: {e}")
        result["health_distribution"] = {"healthy": 0, "caution": 0, "critical": 0}

    # 3. 月次売上推移（フィルター対応）
    try:
        # フィルターがある場合はstoresテーブルとJOINしてフィルター適用
        if store_id_filter or prefecture_filter or health_filter:
            # 健康度フィルターがある場合はサブクエリで対象店舗を特定
            if health_filter:
                trend_query = f"""
                WITH store_health AS (
                    SELECT store_id, AVG(yoy_change) as avg_yoy
                    FROM {TABLE_PREFIX}.sales_monthly
                    WHERE YEAR(month) = 2024
                    GROUP BY store_id
                    HAVING {health_subquery}
                )
                SELECT sm.month,
                       ROUND(SUM(sm.sales_amount) / 100000000, 2) as total_sales_oku,
                       ROUND(AVG(sm.yoy_change) * 100, 1) as avg_yoy_pct,
                       ROUND(SUM(sm.customer_count), 0) as total_customers
                FROM {TABLE_PREFIX}.sales_monthly sm
                JOIN {TABLE_PREFIX}.stores s ON sm.store_id = s.store_id
                JOIN store_health sh ON sm.store_id = sh.store_id
                WHERE YEAR(sm.month) = 2024
                {' AND ' + ' AND '.join(trend_filter_conditions) if trend_filter_conditions else ''}
                GROUP BY sm.month
                ORDER BY sm.month
                """
            else:
                where_clause = "YEAR(sm.month) = 2024"
                if trend_filter_conditions:
                    where_clause += " AND " + " AND ".join(trend_filter_conditions)
                trend_query = f"""
                SELECT sm.month,
                       ROUND(SUM(sm.sales_amount) / 100000000, 2) as total_sales_oku,
                       ROUND(AVG(sm.yoy_change) * 100, 1) as avg_yoy_pct,
                       ROUND(SUM(sm.customer_count), 0) as total_customers
                FROM {TABLE_PREFIX}.sales_monthly sm
                JOIN {TABLE_PREFIX}.stores s ON sm.store_id = s.store_id
                WHERE {where_clause}
                GROUP BY sm.month
                ORDER BY sm.month
                """
        else:
            trend_query = f"""
            SELECT month,
                   ROUND(SUM(sales_amount) / 100000000, 2) as total_sales_oku,
                   ROUND(AVG(yoy_change) * 100, 1) as avg_yoy_pct,
                   ROUND(SUM(customer_count), 0) as total_customers
            FROM {TABLE_PREFIX}.sales_monthly
            WHERE YEAR(month) = 2024
            GROUP BY month
            ORDER BY month
            """
        rows = execute_query(trend_query)
        result["monthly_trend"] = [{
            "month": str(row[0]),
            "sales_oku": float(row[1]) if row[1] else 0,
            "yoy_pct": float(row[2]) if row[2] else 0,
            "customers": int(float(row[3])) if row[3] else 0
        } for row in rows]
        # フィルター情報も返す
        result["active_filters"] = {
            "health": health_filter,
            "prefecture": prefecture_filter,
            "store_id": store_id_filter
        }
    except Exception as e:
        print(f"[ERROR] Trend query failed: {e}")
        result["monthly_trend"] = []
        result["active_filters"] = {}

    # 4. 店舗ランキング（TOP10 / WORST10）
    try:
        ranking_query = f"""
        SELECT s.store_id, s.store_name, s.prefecture, s.city,
               ROUND(AVG(sm.yoy_change) * 100, 1) as yoy_pct,
               ROUND(SUM(sm.sales_amount) / 10000, 0) as sales_man,
               s.latitude, s.longitude
        FROM {TABLE_PREFIX}.stores s
        JOIN {TABLE_PREFIX}.sales_monthly sm ON s.store_id = sm.store_id
        WHERE YEAR(sm.month) = 2024
        GROUP BY s.store_id, s.store_name, s.prefecture, s.city, s.latitude, s.longitude
        ORDER BY yoy_pct DESC
        """
        rows = execute_query(ranking_query)
        all_stores = [{
            "store_id": row[0], "store_name": row[1], "prefecture": row[2], "city": row[3],
            "yoy_pct": float(row[4]) if row[4] else 0,
            "sales_man": float(row[5]) if row[5] else 0,
            "latitude": float(row[6]) if row[6] else None,
            "longitude": float(row[7]) if row[7] else None
        } for row in rows]

        result["top_stores"] = all_stores[:10]
        result["worst_stores"] = list(reversed(all_stores[-10:]))
        result["all_stores"] = all_stores  # マップ用
    except Exception as e:
        print(f"[ERROR] Ranking query failed: {e}")
        result["top_stores"] = []
        result["worst_stores"] = []
        result["all_stores"] = []

    # 5. 地域別集計
    try:
        region_query = f"""
        SELECT s.prefecture,
               COUNT(DISTINCT s.store_id) as store_count,
               ROUND(SUM(sm.sales_amount) / 100000000, 2) as sales_oku,
               ROUND(AVG(sm.yoy_change) * 100, 1) as avg_yoy_pct
        FROM {TABLE_PREFIX}.stores s
        JOIN {TABLE_PREFIX}.sales_monthly sm ON s.store_id = sm.store_id
        WHERE YEAR(sm.month) = 2024
        GROUP BY s.prefecture
        ORDER BY sales_oku DESC
        """
        rows = execute_query(region_query)
        result["regional_breakdown"] = [{
            "prefecture": row[0],
            "store_count": int(float(row[1])) if row[1] else 0,
            "sales_oku": float(row[2]) if row[2] else 0,
            "avg_yoy_pct": float(row[3]) if row[3] else 0
        } for row in rows]
    except Exception as e:
        print(f"[ERROR] Region query failed: {e}")
        result["regional_breakdown"] = []

    return result

@app.post("/api/analysis/start")
def start_analysis(request: AnalysisRequest, background_tasks: BackgroundTasks):
    existing = load_analysis(request.store_id)
    if existing and existing.get("status") == "running":
        return {"message": "Analysis already running", "status": "running"}
    if existing and existing.get("status") == "completed" and not request.force_refresh:
        return {"message": "Analysis already completed", "status": "completed"}
    background_tasks.add_task(run_full_analysis, request.store_id)
    return {"message": "Analysis started", "status": "running", "store_id": request.store_id}

@app.get("/api/analysis/{store_id}/status")
def get_analysis_status(store_id: str):
    analysis = load_analysis(store_id)
    if not analysis:
        return {"status": "not_started", "store_id": store_id}
    return {
        "store_id": store_id, "status": analysis.get("status"),
        "current_phase": analysis.get("current_phase"), "progress": analysis.get("progress", 0),
        "started_at": analysis.get("started_at"), "completed_at": analysis.get("completed_at"),
        "error": analysis.get("error")
    }

@app.get("/api/analysis/{store_id}/result")
def get_analysis_result(store_id: str):
    analysis = load_analysis(store_id)
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    if analysis.get("status") != "completed":
        return {"status": analysis.get("status"), "progress": analysis.get("progress", 0)}
    return {
        "status": "completed", "store_id": store_id,
        "lv1": analysis.get("lv1"), "lv2": analysis.get("lv2"), "lv3": analysis.get("lv3"),
        "ai_report": analysis.get("ai_report"), "completed_at": analysis.get("completed_at")
    }

@app.get("/api/stores/{store_id}/metrics")
def get_store_metrics(store_id: str):
    query = f"""
    SELECT month, sales_amount, yoy_change FROM {TABLE_PREFIX}.sales_monthly
    WHERE store_id = '{store_id}' ORDER BY month
    """
    rows = execute_query(query)
    return {"store_id": store_id, "metrics": [
        {"month": str(r[0]), "sales": float(r[1]) if r[1] else 0, "yoy_change": float(r[2]) if r[2] else 0}
        for r in rows
    ]}

@app.get("/api/stores/{store_id}/overview")
def get_store_overview(store_id: str):
    """店舗基礎情報を一括取得（分析前のダッシュボード用）"""
    print(f"[DEBUG] ========== Overview requested for store_id: '{store_id}' ==========")

    # 常にDBから取得を試みる（モックフォールバックを最小限に）
    result = {}
    errors = []

    # 1. 店舗基本情報（緯度経度含む）
    try:
        store_query = f"""
        SELECT s.store_id, s.store_name, s.prefecture, s.city, s.size_sqm, s.store_type, s.parking_capacity,
               s.latitude, s.longitude
        FROM {TABLE_PREFIX}.stores s
        WHERE s.store_id = '{store_id}'
        """
        store_rows = execute_query(store_query)
        print(f"[DEBUG] Store query returned {len(store_rows)} rows")
        if store_rows:
            r = store_rows[0]
            result["store"] = {
                "store_id": r[0], "store_name": r[1], "prefecture": r[2], "city": r[3],
                "size_sqm": int(r[4]) if r[4] else 0, "store_type": r[5],
                "parking_capacity": int(r[6]) if r[6] else 0,
                "latitude": float(r[7]) if r[7] else None,
                "longitude": float(r[8]) if r[8] else None,
                "avg_yoy_pct": 0, "annual_sales_man": 0, "avg_monthly_customers": 0, "avg_basket": 0
            }
    except Exception as e:
        errors.append(f"store: {e}")
        print(f"[ERROR] Store query failed: {e}")

    # 2. 売上情報を追加
    try:
        sales_query = f"""
        SELECT ROUND(AVG(yoy_change) * 100, 2), ROUND(SUM(sales_amount) / 10000, 0),
               ROUND(AVG(customer_count), 0), ROUND(AVG(avg_basket), 0)
        FROM {TABLE_PREFIX}.sales_monthly
        WHERE store_id = '{store_id}' AND YEAR(month) = 2024
        """
        sales_rows = execute_query(sales_query)
        if sales_rows and "store" in result:
            r = sales_rows[0]
            # 各値を独立してチェックし、NULLでなければ更新（文字列で返る場合があるのでfloat経由で変換）
            if r[0] is not None:
                result["store"]["avg_yoy_pct"] = float(r[0])
            if r[1] is not None:
                result["store"]["annual_sales_man"] = float(r[1])
            if r[2] is not None:
                result["store"]["avg_monthly_customers"] = int(float(r[2]))
            if r[3] is not None:
                result["store"]["avg_basket"] = int(float(r[3]))
    except Exception as e:
        errors.append(f"sales: {e}")
        print(f"[ERROR] Sales query failed: {e}")

    # 3. 全店平均
    try:
        chain_query = f"""
        SELECT ROUND(AVG(yoy_change) * 100, 2), ROUND(AVG(sales_amount) / 10000 * 12, 0)
        FROM {TABLE_PREFIX}.sales_monthly WHERE YEAR(month) = 2024
        """
        chain_rows = execute_query(chain_query)
        if chain_rows:
            result["chain_avg"] = {
                "avg_yoy_pct": float(chain_rows[0][0]) if chain_rows[0][0] else 0,
                "avg_annual_sales_man": float(chain_rows[0][1]) if chain_rows[0][1] else 0
            }
    except Exception as e:
        errors.append(f"chain_avg: {e}")

    # 4. 商圏人口統計
    try:
        demo_query = f"""
        SELECT population_5km, households, avg_income, elderly_rate, young_adult_rate, detached_house_rate
        FROM {TABLE_PREFIX}.trade_area WHERE store_id = '{store_id}'
        """
        demo_rows = execute_query(demo_query)
        if demo_rows:
            r = demo_rows[0]
            result["demographics"] = {
                "population_5km": int(r[0]) if r[0] else 0, "households": int(r[1]) if r[1] else 0,
                "avg_income": int(r[2]) if r[2] else 0, "elderly_rate": float(r[3]) if r[3] else 0,
                "young_adult_rate": float(r[4]) if r[4] else 0, "detached_house_rate": float(r[5]) if r[5] else 0
            }
    except Exception as e:
        errors.append(f"demographics: {e}")

    # 5. 競合店舗（重要：必ずDBから取得）
    try:
        comp_query = f"""
        SELECT competitor_name, distance_km, size_sqm, is_new_entry
        FROM {TABLE_PREFIX}.competitors WHERE store_id = '{store_id}' ORDER BY distance_km LIMIT 8
        """
        comp_rows = execute_query(comp_query)
        print(f"[DEBUG] Competitors query returned {len(comp_rows)} rows for {store_id}")
        result["competitors"] = [
            {"name": r[0], "distance_km": float(r[1]) if r[1] else 0,
             "size_sqm": int(r[2]) if r[2] else 0, "is_new_entry": r[3] == True or str(r[3]).lower() == "true"}
            for r in comp_rows
        ]
    except Exception as e:
        errors.append(f"competitors: {e}")
        print(f"[ERROR] Competitors query failed: {e}")
        result["competitors"] = []

    # 6. 近隣施設（重要：必ずDBから取得）
    facility_type_map = {
        "駅": "station", "学校": "school", "病院": "hospital", "役所・公共施設": "government",
        "公園": "park", "公園・レジャー": "park", "商業施設": "commercial", "スーパー": "supermarket",
        "コンビニ": "convenience", "銀行": "bank", "郵便局": "post", "ドラッグストア": "commercial",
        "家電量販店": "commercial", "ガソリンスタンド": "commercial"
    }
    try:
        fac_query = f"""
        SELECT facility_name, facility_type, distance_km, traffic_impact
        FROM {TABLE_PREFIX}.nearby_facilities WHERE store_id = '{store_id}' ORDER BY distance_km LIMIT 10
        """
        fac_rows = execute_query(fac_query)
        print(f"[DEBUG] Facilities query returned {len(fac_rows)} rows for {store_id}")
        result["facilities"] = [
            {"name": r[0], "type": facility_type_map.get(r[1], "other"), "type_label": r[1],
             "distance_km": float(r[2]) if r[2] else 0, "traffic_impact": float(r[3]) if r[3] else 0}
            for r in fac_rows
        ]
    except Exception as e:
        errors.append(f"facilities: {e}")
        print(f"[ERROR] Facilities query failed: {e}")
        result["facilities"] = []

    # 7. カテゴリ別売上
    try:
        cat_query = f"""
        SELECT c.category_name, ROUND(SUM(sc.sales_amount) / 10000, 0) as sales_man
        FROM {TABLE_PREFIX}.sales_by_category sc
        JOIN {TABLE_PREFIX}.categories c ON sc.category_id = c.category_id
        WHERE sc.store_id = '{store_id}' AND YEAR(sc.month) = 2024
        GROUP BY c.category_name ORDER BY sales_man DESC LIMIT 5
        """
        cat_rows = execute_query(cat_query)
        result["top_categories"] = [
            {"category": r[0], "sales_man": float(r[1]) if r[1] else 0}
            for r in cat_rows
        ]
    except Exception as e:
        errors.append(f"categories: {e}")
        result["top_categories"] = []

    # 8. 月次トレンド
    try:
        trend_query = f"""
        SELECT month, sales_amount, yoy_change, customer_count
        FROM {TABLE_PREFIX}.sales_monthly WHERE store_id = '{store_id}' ORDER BY month
        """
        trend_rows = execute_query(trend_query)
        result["monthly_trend"] = [
            {"month": str(r[0]), "sales": float(r[1]) if r[1] else 0,
             "yoy_change": float(r[2]) if r[2] else 0, "customers": int(r[3]) if r[3] else 0}
            for r in trend_rows
        ]
    except Exception as e:
        errors.append(f"monthly_trend: {e}")
        result["monthly_trend"] = []

    # 必須フィールドのデフォルト値を設定
    if "store" not in result:
        print(f"[WARN] No store data found for {store_id}, using minimal fallback")
        result["store"] = {"store_id": store_id, "store_name": f"店舗_{store_id}", "prefecture": "", "city": "",
                          "size_sqm": 0, "store_type": "", "parking_capacity": 0,
                          "avg_yoy_pct": 0, "annual_sales_man": 0, "avg_monthly_customers": 0, "avg_basket": 0,
                          "latitude": None, "longitude": None}
    if "chain_avg" not in result:
        result["chain_avg"] = {"avg_yoy_pct": 0, "avg_annual_sales_man": 0}
    if "demographics" not in result:
        result["demographics"] = {"population_5km": 0, "households": 0, "avg_income": 0,
                                  "elderly_rate": 0, "young_adult_rate": 0, "detached_house_rate": 0}

    # デバッグ情報を追加
    result["_debug"] = {"errors": errors, "store_id": store_id}
    print(f"[DEBUG] Returning overview with {len(result.get('competitors', []))} competitors, {len(result.get('facilities', []))} facilities")

    return result

def _get_mock_overview(store_id: str):
    """モックデータを返す（DBエラー時のフォールバック）"""
    return {
        "store": {
            "store_id": store_id, "store_name": f"店舗_{store_id}",
            "prefecture": "東京都", "city": "渋谷区",
            "size_sqm": 2500, "store_type": "都市型", "parking_capacity": 150,
            "avg_yoy_pct": -8.5, "annual_sales_man": 45000,
            "avg_monthly_customers": 12000, "avg_basket": 3200
        },
        "chain_avg": {"avg_yoy_pct": -3.5, "avg_annual_sales_man": 42000},
        "demographics": {
            "population_5km": 180000, "households": 75000, "avg_income": 5800000,
            "elderly_rate": 0.18, "young_adult_rate": 0.28, "detached_house_rate": 0.12
        },
        "competitors": [
            {"name": "カインズ渋谷", "distance_km": 1.2, "size_sqm": 3000, "is_new_entry": False},
            {"name": "コーナン代々木", "distance_km": 2.5, "size_sqm": 2000, "is_new_entry": True},
            {"name": "ビバホーム新宿", "distance_km": 3.1, "size_sqm": 4500, "is_new_entry": False},
            {"name": "島忠世田谷", "distance_km": 2.8, "size_sqm": 2800, "is_new_entry": False}
        ],
        "facilities": [
            {"name": "渋谷駅", "type": "station", "type_label": "駅", "distance_km": 0.8, "traffic_impact": 0.35},
            {"name": "代々木公園", "type": "park", "type_label": "公園", "distance_km": 1.0, "traffic_impact": 0.15},
            {"name": "渋谷区役所", "type": "government", "type_label": "役所・公共施設", "distance_km": 0.5, "traffic_impact": 0.08},
            {"name": "青山学院", "type": "school", "type_label": "学校", "distance_km": 1.2, "traffic_impact": 0.25}
        ],
        "top_categories": [
            {"category": "園芸用品", "sales_man": 8500},
            {"category": "DIY工具", "sales_man": 7200},
            {"category": "住宅設備", "sales_man": 6800},
            {"category": "ペット用品", "sales_man": 5500},
            {"category": "カー用品", "sales_man": 4200}
        ],
        "monthly_trend": [
            {"month": "2024-01", "sales": 3500000, "yoy_change": -0.05, "customers": 11000},
            {"month": "2024-02", "sales": 3200000, "yoy_change": -0.08, "customers": 10500},
            {"month": "2024-03", "sales": 3800000, "yoy_change": -0.03, "customers": 12000},
            {"month": "2024-04", "sales": 4200000, "yoy_change": -0.06, "customers": 13000},
            {"month": "2024-05", "sales": 4500000, "yoy_change": -0.10, "customers": 14000},
            {"month": "2024-06", "sales": 3900000, "yoy_change": -0.12, "customers": 12500}
        ]
    }

def _get_store_overview_from_db(store_id: str):
    """DBから店舗概要を取得"""
    result = {}

    # 店舗基本情報と業績（緯度・経度を追加）
    query = f"""
    SELECT s.store_id, s.store_name, s.prefecture, s.city, s.size_sqm, s.store_type, s.parking_capacity,
           ROUND(AVG(sm.yoy_change) * 100, 2) as avg_yoy_pct,
           ROUND(SUM(sm.sales_amount) / 10000, 0) as annual_sales_man,
           ROUND(AVG(sm.customer_count), 0) as avg_monthly_customers,
           ROUND(AVG(sm.avg_basket), 0) as avg_basket,
           s.latitude, s.longitude
    FROM {TABLE_PREFIX}.stores s
    JOIN {TABLE_PREFIX}.sales_monthly sm ON s.store_id = sm.store_id
    WHERE s.store_id = '{store_id}' AND YEAR(sm.month) = 2024
    GROUP BY s.store_id, s.store_name, s.prefecture, s.city, s.size_sqm, s.store_type, s.parking_capacity, s.latitude, s.longitude
    """
    rows = execute_query(query)
    print(f"[DEBUG] Store query returned {len(rows)} rows")
    if rows:
        r = rows[0]
        result["store"] = {
            "store_id": r[0], "store_name": r[1], "prefecture": r[2], "city": r[3],
            "size_sqm": int(r[4]) if r[4] else 0, "store_type": r[5],
            "parking_capacity": int(r[6]) if r[6] else 0,
            "avg_yoy_pct": float(r[7]) if r[7] else 0,
            "annual_sales_man": float(r[8]) if r[8] else 0,
            "avg_monthly_customers": int(r[9]) if r[9] else 0,
            "avg_basket": int(r[10]) if r[10] else 0,
            "latitude": float(r[11]) if r[11] else None,
            "longitude": float(r[12]) if r[12] else None
        }
    else:
        print(f"[DEBUG] No store data found for {store_id}")

    # 全店平均
    query = f"""
    SELECT ROUND(AVG(yoy_change) * 100, 2), ROUND(AVG(sales_amount) / 10000 * 12, 0)
    FROM {TABLE_PREFIX}.sales_monthly WHERE YEAR(month) = 2024
    """
    rows = execute_query(query)
    if rows:
        result["chain_avg"] = {
            "avg_yoy_pct": float(rows[0][0]) if rows[0][0] else 0,
            "avg_annual_sales_man": float(rows[0][1]) if rows[0][1] else 0
        }

    # 商圏人口統計
    query = f"""
    SELECT population_5km, households, avg_income, elderly_rate, young_adult_rate,
           detached_house_rate, day_night_ratio
    FROM {TABLE_PREFIX}.trade_area WHERE store_id = '{store_id}'
    """
    rows = execute_query(query)
    if rows:
        r = rows[0]
        result["demographics"] = {
            "population_5km": int(r[0]) if r[0] else 0,
            "households": int(r[1]) if r[1] else 0,
            "avg_income": int(r[2]) if r[2] else 0,
            "elderly_rate": float(r[3]) if r[3] else 0,
            "young_adult_rate": float(r[4]) if r[4] else 0,
            "detached_house_rate": float(r[5]) if r[5] else 0,
            "day_night_ratio": float(r[6]) if r[6] else 0
        }

    # 競合
    query = f"""
    SELECT competitor_name, distance_km, size_sqm, is_new_entry
    FROM {TABLE_PREFIX}.competitors WHERE store_id = '{store_id}' ORDER BY distance_km LIMIT 8
    """
    rows = execute_query(query)
    result["competitors"] = [
        {"name": r[0], "distance_km": float(r[1]) if r[1] else 0,
         "size_sqm": int(r[2]) if r[2] else 0, "is_new_entry": r[3] == True or r[3] == "true"}
        for r in rows
    ]

    # 近隣施設
    # facility_typeをフロントエンド用にマッピング
    facility_type_map = {
        "駅": "station",
        "学校": "school",
        "病院": "hospital",
        "役所・公共施設": "government",
        "公園": "park",
        "商業施設": "commercial",
        "スーパー": "supermarket",
        "コンビニ": "convenience",
        "銀行": "bank",
        "郵便局": "post"
    }
    query = f"""
    SELECT facility_name, facility_type, distance_km, traffic_impact
    FROM {TABLE_PREFIX}.nearby_facilities WHERE store_id = '{store_id}' ORDER BY distance_km LIMIT 10
    """
    rows = execute_query(query)
    result["facilities"] = [
        {
            "name": r[0],
            "type": facility_type_map.get(r[1], "other"),
            "type_label": r[1],
            "distance_km": float(r[2]) if r[2] else 0,
            "traffic_impact": float(r[3]) if r[3] else 0
        }
        for r in rows
    ]

    # カテゴリ別売上（上位5）
    query = f"""
    WITH store_sales AS (
        SELECT c.category_name, SUM(sc.sales_amount) as sales
        FROM {TABLE_PREFIX}.sales_by_category sc
        JOIN {TABLE_PREFIX}.categories c ON sc.category_id = c.category_id
        WHERE sc.store_id = '{store_id}' AND YEAR(sc.month) = 2024
        GROUP BY c.category_name
    )
    SELECT category_name, ROUND(sales / 10000, 0) as sales_man FROM store_sales ORDER BY sales DESC LIMIT 5
    """
    rows = execute_query(query)
    result["top_categories"] = [
        {"category": r[0], "sales_man": float(r[1]) if r[1] else 0}
        for r in rows
    ]

    # 月次トレンド
    query = f"""
    SELECT month, sales_amount, yoy_change, customer_count
    FROM {TABLE_PREFIX}.sales_monthly WHERE store_id = '{store_id}' ORDER BY month
    """
    rows = execute_query(query)
    result["monthly_trend"] = [
        {"month": str(r[0]), "sales": float(r[1]) if r[1] else 0,
         "yoy_change": float(r[2]) if r[2] else 0, "customers": int(r[3]) if r[3] else 0}
        for r in rows
    ]

    print(f"[DEBUG] Returning overview with keys: {list(result.keys())}")

    # データが取得できなかった場合はフォールバック
    if "store" not in result:
        print(f"[DEBUG] No store data, returning fallback for {store_id}")
        # storesリストから取得を試みる
        stores_query = f"""
        SELECT s.store_id, s.store_name, s.prefecture, s.city, s.size_sqm, s.store_type, s.parking_capacity,
               AVG(sm.yoy_change) * 100 as avg_yoy_pct,
               SUM(sm.sales_amount) / 10000 as annual_sales_man,
               AVG(sm.customer_count) as avg_monthly_customers,
               AVG(sm.avg_basket) as avg_basket,
               s.latitude, s.longitude
        FROM {TABLE_PREFIX}.stores s
        LEFT JOIN {TABLE_PREFIX}.sales_monthly sm ON s.store_id = sm.store_id
        WHERE s.store_id = '{store_id}'
        GROUP BY s.store_id, s.store_name, s.prefecture, s.city, s.size_sqm, s.store_type, s.parking_capacity, s.latitude, s.longitude
        """
        try:
            rows = execute_query(stores_query)
            print(f"[DEBUG] Fallback query returned {len(rows)} rows")
            if rows:
                r = rows[0]
                result["store"] = {
                    "store_id": r[0] or store_id, "store_name": r[1] or "店舗",
                    "prefecture": r[2] or "", "city": r[3] or "",
                    "size_sqm": int(r[4]) if r[4] else 0, "store_type": r[5] or "",
                    "parking_capacity": int(r[6]) if r[6] else 0,
                    "avg_yoy_pct": float(r[7]) if r[7] else 0,
                    "annual_sales_man": float(r[8]) if r[8] else 0,
                    "avg_monthly_customers": int(r[9]) if r[9] else 0,
                    "avg_basket": int(r[10]) if r[10] else 0,
                    "latitude": float(r[11]) if r[11] else None,
                    "longitude": float(r[12]) if r[12] else None
                }
        except Exception as e:
            print(f"[DEBUG] Fallback query error: {e}")

    # 最終フォールバック：モックデータ
    if "store" not in result:
        print(f"[DEBUG] Using mock data for {store_id}")
        result["store"] = {"store_id": store_id, "store_name": "データ取得中", "prefecture": "", "city": "",
                          "size_sqm": 0, "store_type": "", "parking_capacity": 0,
                          "avg_yoy_pct": 0, "annual_sales_man": 0, "avg_monthly_customers": 0, "avg_basket": 0,
                          "latitude": None, "longitude": None}

    # 必須フィールドのデフォルト値を設定
    if "chain_avg" not in result:
        result["chain_avg"] = {"avg_yoy_pct": 0, "avg_annual_sales_man": 0}
    if "demographics" not in result:
        result["demographics"] = {"population_5km": 0, "households": 0, "avg_income": 0, "elderly_rate": 0,
                                  "young_adult_rate": 0, "detached_house_rate": 0}
    if "competitors" not in result:
        result["competitors"] = []
    if "facilities" not in result:
        result["facilities"] = []
    if "top_categories" not in result:
        result["top_categories"] = []
    if "monthly_trend" not in result:
        result["monthly_trend"] = []

    return result

# ============================================================
# Genie API Endpoints (SDK genie メソッド使用 - AI Dev Kit 方式)
# ============================================================

class GenieMessageRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = None
    store_context: Optional[dict] = None

def _format_genie_response(question: str, genie_message, space_id: str, client: WorkspaceClient) -> dict:
    """Genie SDKレスポンスをフォーマット（SDK互換性対応）"""
    result = {
        "question": question,
        "conversation_id": genie_message.conversation_id,
        "message_id": genie_message.id,
        "status": str(genie_message.status.value) if genie_message.status else "UNKNOWN",
    }

    sql_query = None

    # アタッチメントからデータを抽出
    if genie_message.attachments:
        for attachment in genie_message.attachments:
            # クエリアタッチメント（SQLと結果）
            if hasattr(attachment, 'query') and attachment.query:
                query_obj = attachment.query
                sql_query = getattr(query_obj, 'query', '') or ""
                result["sql"] = sql_query
                result["description"] = getattr(query_obj, 'description', '') or ""

                # row_countは属性があれば取得
                if hasattr(query_obj, 'query_result_metadata') and query_obj.query_result_metadata:
                    result["row_count"] = getattr(query_obj.query_result_metadata, 'row_count', None)

                # 実際のデータを取得（attachment_id経由）
                attachment_id = getattr(attachment, 'attachment_id', None)
                if attachment_id:
                    try:
                        data_result = client.genie.get_message_query_result_by_attachment(
                            space_id=space_id,
                            conversation_id=genie_message.conversation_id,
                            message_id=genie_message.id,
                            attachment_id=attachment_id,
                        )
                        if hasattr(data_result, 'statement_response') and data_result.statement_response:
                            sr = data_result.statement_response
                            # カラム名を取得
                            if hasattr(sr, 'manifest') and sr.manifest:
                                schema = getattr(sr.manifest, 'schema', None)
                                if schema and hasattr(schema, 'columns') and schema.columns:
                                    result["columns"] = [getattr(c, 'name', '') for c in schema.columns]
                            # データを取得
                            if hasattr(sr, 'result') and sr.result:
                                data_array = getattr(sr.result, 'data_array', None)
                                if data_array:
                                    result["data"] = data_array
                    except Exception as e:
                        print(f"[Genie] Failed to get query result via attachment: {e}")

            # テキストアタッチメント（説明）
            if hasattr(attachment, 'text') and attachment.text:
                result["text_response"] = getattr(attachment.text, 'content', '') or ""

    # SQLがあるがデータがない場合、直接実行してデータを取得
    if sql_query and "data" not in result:
        try:
            print(f"[Genie] Executing SQL directly: {sql_query[:100]}...")
            columns, query_result = execute_query(sql_query, return_columns=True)
            if query_result:
                result["data"] = query_result[:50]  # 最大50行
                result["row_count"] = len(query_result)
                if columns:
                    result["columns"] = columns
                    print(f"[Genie] Got columns from query: {columns}")
                print(f"[Genie] Direct SQL execution returned {len(query_result)} rows")
        except Exception as e:
            print(f"[Genie] Failed to execute SQL directly: {e}")

    return result

@app.get("/api/genie/debug")
def genie_debug():
    """Genie API デバッグ情報"""
    debug_info = {
        "env": {
            "DATABRICKS_HOST": os.environ.get("DATABRICKS_HOST", "NOT SET"),
            "DATABRICKS_CLIENT_ID": "SET" if os.environ.get("DATABRICKS_CLIENT_ID") else "NOT SET",
            "DATABRICKS_CLIENT_SECRET": "SET" if os.environ.get("DATABRICKS_CLIENT_SECRET") else "NOT SET",
            "DATABRICKS_TOKEN": "SET" if os.environ.get("DATABRICKS_TOKEN") else "NOT SET",
        },
        "space_id": GENIE_SPACE_ID,
    }

    try:
        client = get_workspace_client()
        debug_info["client_created"] = True
        debug_info["client_host"] = client.config.host
        debug_info["auth_type"] = str(client.config.auth_type) if hasattr(client.config, 'auth_type') else "unknown"

        # 簡単なAPI呼び出しテスト
        try:
            me = client.current_user.me()
            debug_info["current_user"] = me.user_name
            debug_info["current_user_id"] = me.id
        except Exception as e:
            debug_info["current_user_error"] = str(e)

        # Genie Space情報を取得（権限テスト）
        try:
            space = client.genie.get_space(space_id=GENIE_SPACE_ID)
            debug_info["genie_space"] = {
                "title": space.title if hasattr(space, 'title') else "unknown",
                "id": space.id if hasattr(space, 'id') else GENIE_SPACE_ID,
            }
        except Exception as e:
            debug_info["genie_space_error"] = str(e)

    except Exception as e:
        debug_info["client_created"] = False
        debug_info["client_error"] = str(e)
        debug_info["traceback"] = traceback.format_exc()

    return debug_info

@app.get("/api/genie/test")
def genie_test():
    """Genie API テスト - 詳細なレスポンス確認"""
    result_info = {"step": "init"}

    try:
        result_info["step"] = "get_client"
        client = get_workspace_client()
        result_info["client_host"] = client.config.host
        result_info["auth_type"] = str(client.config.auth_type) if hasattr(client.config, 'auth_type') else "unknown"

        result_info["step"] = "start_conversation"
        result_info["space_id"] = GENIE_SPACE_ID

        # シンプルなテスト - 全店舗数を聞く
        test_question = "全店舗数は何店舗ですか？"
        result = client.genie.start_conversation_and_wait(
            space_id=GENIE_SPACE_ID,
            content=test_question,
            timeout=timedelta(seconds=60),
        )

        # 生のレスポンス情報を収集
        raw_info = {
            "conversation_id": result.conversation_id,
            "message_id": result.id,
            "status": str(result.status.value) if result.status else "UNKNOWN",
            "has_attachments": result.attachments is not None,
            "attachments_count": len(result.attachments) if result.attachments else 0,
        }

        # アタッチメントの詳細
        if result.attachments:
            raw_info["attachments"] = []
            for i, att in enumerate(result.attachments):
                att_info = {
                    "index": i,
                    "attachment_id": getattr(att, 'attachment_id', None),
                    "has_query": hasattr(att, 'query') and att.query is not None,
                    "has_text": hasattr(att, 'text') and att.text is not None,
                }
                if att_info["has_query"]:
                    q = att.query
                    att_info["query_info"] = {
                        "query": getattr(q, 'query', None),
                        "description": getattr(q, 'description', None),
                        "has_query_result_metadata": hasattr(q, 'query_result_metadata'),
                    }
                if att_info["has_text"]:
                    t = att.text
                    att_info["text_info"] = {
                        "content": getattr(t, 'content', None),
                    }
                raw_info["attachments"].append(att_info)

        # フォーマット済みレスポンスも返す
        formatted = _format_genie_response(test_question, result, GENIE_SPACE_ID, client)

        return {
            "success": True,
            "test_question": test_question,
            "raw_response": raw_info,
            "formatted_response": formatted,
            "debug": result_info,
        }
    except Exception as e:
        result_info["error"] = str(e)
        result_info["error_type"] = type(e).__name__
        result_info["traceback"] = traceback.format_exc()
        return {
            "success": False,
            **result_info
        }

@app.post("/api/genie/ask")
def ask_genie(body: GenieMessageRequest):
    """Genieに質問して回答を取得（AI Dev Kit方式 - 同期的に待機）"""
    import traceback

    try:
        # 新しいクライアントを取得（OAuth M2M認証）
        print(f"[Genie] Getting workspace client...")
        client = get_workspace_client()
        print(f"[Genie] Client host: {client.config.host}")

        # 店舗コンテキストがある場合、メッセージに追加
        message = body.message
        if body.store_context:
            ctx = body.store_context
            context_prefix = f"[現在選択中の店舗: {ctx.get('store_name', '')} ({ctx.get('prefecture', '')} {ctx.get('city', '')}), 前年比: {ctx.get('yoy_change', 0)*100:.1f}%]\n\n"
            message = context_prefix + message

        print(f"[Genie] Asking: {message[:100]}...")
        print(f"[Genie] Conversation ID: {body.conversation_id}")
        print(f"[Genie] Space ID: {GENIE_SPACE_ID}")

        if body.conversation_id:
            # 既存の会話を続ける
            print(f"[Genie] Calling create_message_and_wait...")
            result = client.genie.create_message_and_wait(
                space_id=GENIE_SPACE_ID,
                conversation_id=body.conversation_id,
                content=message,
                timeout=timedelta(seconds=120),
            )
        else:
            # 新規会話を開始
            print(f"[Genie] Calling start_conversation_and_wait...")
            result = client.genie.start_conversation_and_wait(
                space_id=GENIE_SPACE_ID,
                content=message,
                timeout=timedelta(seconds=120),
            )

        print(f"[Genie] Got result: {result}")
        formatted = _format_genie_response(message, result, GENIE_SPACE_ID, client)
        print(f"[Genie] Response status: {formatted.get('status')}")
        return {"success": True, **formatted}

    except TimeoutError as e:
        print(f"[Genie] Timeout: {e}")
        return {"success": False, "error": "タイムアウト：Genieの応答に時間がかかりすぎています"}
    except Exception as e:
        error_msg = str(e)
        tb = traceback.format_exc()
        print(f"[Genie] Exception: {error_msg}")
        print(f"[Genie] Traceback: {tb}")
        return {"success": False, "error": f"{error_msg}", "traceback": tb}

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/")
def serve_frontend():
    # static/index.html を配信
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        with open(index_path, "r", encoding="utf-8") as f:
            content = f.read()
            return HTMLResponse(
                content=content,
                headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                    "Expires": "0"
                }
            )
    return {"error": "index.html not found"}
