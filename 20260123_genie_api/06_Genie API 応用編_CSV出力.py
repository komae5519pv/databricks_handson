# Databricks notebook source
# MAGIC %md
# MAGIC # Genie API - 設定情報の管理
# MAGIC
# MAGIC このノートブックは、Genie APIを用いて指定したGenie Spaceの設定を管理します。
# MAGIC
# MAGIC **処理フロー:**
# MAGIC 1. Genie Spaceの設定を取得(JSON)
# MAGIC 2. Genie Spaceに設定を反映
# MAGIC 3. DataFrameに変換
# MAGIC 4. CSV形式で出力

# COMMAND ----------

# MAGIC %run ./03_genie_config

# COMMAND ----------

import requests
import json
import time
import pandas as pd
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Genie API関数（最小構成）

# COMMAND ----------

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json",
}

def start_conversation(question: str) -> dict:
    """新しいGenie会話を開始"""
    url = f"{GENIE_API_BASE}/start-conversation"
    payload = {"content": question}
    resp = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    resp.raise_for_status()
    return resp.json()

def get_message(conversation_id: str, message_id: str) -> dict:
    """メッセージの詳細を取得"""
    url = f"{GENIE_API_BASE}/conversations/{conversation_id}/messages/{message_id}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()

def wait_until_completed(conversation_id: str, message_id: str, timeout_sec: int = 120) -> dict:
    """メッセージが完了するまで待機"""
    start = time.time()
    while True:
        msg = get_message(conversation_id, message_id)
        status = msg.get("status")

        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            return msg

        if time.time() - start > timeout_sec:
            raise TimeoutError(f"タイムアウト: {timeout_sec}秒以内に完了しませんでした")

        time.sleep(5)

def get_query_result(conversation_id: str, message_id: str, attachment_id: str) -> dict:
    """クエリ結果をJSON_ARRAY形式で取得"""
    url = (
        f"{GENIE_API_BASE}/conversations/{conversation_id}"
        f"/messages/{message_id}/attachments/{attachment_id}/query-result"
    )
    params = {"format": "JSON_ARRAY"}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json()

def json_to_dataframe(result_json: dict):
    """JSON結果をSpark DataFrameに変換"""
    stmt = result_json.get("statement_response", {})
    
    # カラム情報を取得
    manifest = stmt.get("manifest", {})
    schema = manifest.get("schema", {})
    columns_meta = schema.get("columns", [])
    col_names = [c.get("name") for c in columns_meta]
    
    # データを取得 - JSON_ARRAY形式ではchunksを確認
    result_part = stmt.get("result", {})
    
    # data_arrayが直接ある場合
    data_array = result_part.get("data_array")
    
    # data_arrayがない場合、chunksやexternal_linksを確認
    if not data_array:
        # chunk_indexがある場合は0番目のchunkを取得する必要がある
        chunk_index = result_part.get("chunk_index")
        external_links = result_part.get("external_links")
        
        print(f"\n=== データ取得の詳細 ===")
        print(f"result_part のキー: {list(result_part.keys())}")
        print(f"chunk_index: {chunk_index}")
        print(f"external_links: {external_links}")
        print(f"\n完全なレスポンス:")
        print(json.dumps(result_json, indent=2, ensure_ascii=False))
        
        raise ValueError(
            "data_arrayが見つかりません。"
            "Genie APIはchunk形式でデータを返している可能性があります。"
            "上記のレスポンス構造を確認してください。"
        )
    
    if not col_names or not data_array:
        raise ValueError("カラム名またはデータが空です")
    
    # Spark DataFrame作成
    df = spark.createDataFrame([tuple(row) for row in data_array], schema=col_names)
    return df

print("✓ Genie API関数を読み込みました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Genieに質問して結果を取得

# COMMAND ----------

# 質問を送信（データの実際の期間に合わせてプロンプトを修正）
question = "売上トップ10の商品を教えて"
print(f"質問: {question}\n")

start_resp = start_conversation(question)
conversation_id = start_resp.get("conversation", {}).get("id")
message_id = start_resp.get("message", {}).get("id")

print(f"conversation_id: {conversation_id}")
print(f"message_id: {message_id}")

# COMMAND ----------

# 完了まで待機
print("Genieが処理中...")
final_message = wait_until_completed(conversation_id, message_id)
print(f"✓ ステータス: {final_message.get('status')}")

# COMMAND ----------

# attachment_idを取得
attachments = final_message.get("attachments", [])
attachment_id = None

for att in attachments:
    query_part = att.get("query")
    if query_part:
        attachment_id = att.get("attachment_id") or query_part.get("attachment_id")
        sql = query_part.get("statement") or query_part.get("query")
        description = query_part.get("description")
        break

print(f"✓ attachment_id: {attachment_id}")
print(f"\n生成されたSQL:\n{sql}")
print(f"\n説明:\n{description}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 結果をDataFrameに変換

# COMMAND ----------

# Genie APIから結果を取得
print(f"APIから結果を取得中...\n")
result_data = get_query_result(conversation_id, message_id, attachment_id)

# 生成されたSQLを直接実行（APIが空の結果を返す場合のフォールバック）
print(f"生成されたSQLを実行:\n{sql}\n")
df_result = spark.sql(sql)

print(f"✓ 取得した行数: {df_result.count()}")
print(f"✓ カラム数: {len(df_result.columns)}")
print(f"✓ カラム: {df_result.columns}")

# 結果を表示
display(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. CSV出力

# COMMAND ----------

# ファイル名を生成（タイムスタンプ付き）
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"genie_result_{timestamp}.csv"

# カレントディレクトリを動的取得
import os
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)

# Workspace パスを DBFS パスに変換
output_dir = f"/Workspace{notebook_dir}"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, output_filename)

# CSV出力
pandas_df = df_result.toPandas()
pandas_df.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"✓ CSV出力完了: {output_path}")
print(f"  行数: {len(pandas_df)}")
print(f"  列数: {len(pandas_df.columns)}")
print(f"  サイズ: {os.path.getsize(output_path)} バイト")

# ファイルの内容を確認
print(f"\nファイルの内容（最初の5行）:")
print(pandas_df.head().to_string())
