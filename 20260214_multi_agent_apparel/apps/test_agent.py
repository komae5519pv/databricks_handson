#!/usr/bin/env python3
"""
Agent Bricks接続テスト用の最小限のアプリ
"""

import os
import requests
from databricks.sdk import WorkspaceClient

def test_agent_connection():
    """Agent Bricksへの接続をテスト"""
    
    # 環境変数をチェック
    print("=== 環境変数チェック ===")
    print(f"DATABRICKS_HOST: {os.environ.get('DATABRICKS_HOST')}")
    print(f"DATABRICKS_CLIENT_SECRET exists: {bool(os.environ.get('DATABRICKS_CLIENT_SECRET'))}")
    print(f"PGUSER: {os.environ.get('PGUSER')}")
    print(f"SPACE_ID: {os.environ.get('SPACE_ID')}")
    print(f"SERVING_ENDPOINT_NAME: {os.environ.get('SERVING_ENDPOINT_NAME')}")
    
    # WorkspaceClientでトークンを取得
    print("\n=== WorkspaceClient認証テスト ===")
    try:
        w = WorkspaceClient()
        token = w.config.token
        if token:
            print(f"✅ Token acquired: {token[:20]}...")
        else:
            print("❌ No token from WorkspaceClient")
            return
    except Exception as e:
        print(f"❌ WorkspaceClient error: {e}")
        return
    
    # Agent BricksエンドポイントのURLを構築
    print("\n=== エンドポイントURL構築 ===")
    host = os.environ.get('DATABRICKS_HOST', 'adb-984752964297111.11.azuredatabricks.net')
    if not host.startswith('http'):
        host = f"https://{host}"
    
    # 複数のエンドポイント形式を試す
    endpoint_variants = [
        f"{host}/serving-endpoints/mas-93856fe9-endpoint/invocations",
        f"{host}/serving-endpoints/mas-93856fe9-endpoint/query",
        f"{host}/api/2.0/serving-endpoints/mas-93856fe9-endpoint/invocations",
        f"{host}/api/2.0/serving-endpoints/mas-93856fe9-endpoint/query"
    ]
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # テスト用のペイロード
    test_payloads = [
        {"query": "商品ID 1008 の在庫はありますか？"},
        {"input": [{"role": "user", "content": "商品ID 1008 の在庫はありますか？"}]},
        {"messages": [{"role": "user", "content": "商品ID 1008 の在庫はありますか？"}]},
        {"prompt": "商品ID 1008 の在庫はありますか？"}
    ]
    
    for i, endpoint in enumerate(endpoint_variants):
        print(f"\n=== エンドポイント {i+1}: {endpoint} ===")
        
        for j, payload in enumerate(test_payloads):
            print(f"  Payload {j+1}: {payload}")
            try:
                response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
                print(f"  Status: {response.status_code}")
                print(f"  Response: {response.text[:200]}...")
                
                if response.status_code == 200:
                    result = response.json()
                    print(f"  ✅ Success! Response: {result}")
                    return  # 成功したら終了
                    
            except Exception as e:
                print(f"  ❌ Error: {e}")
    
    print("\n❌ すべてのエンドポイントとペイロード形式で失敗しました")

if __name__ == "__main__":
    test_agent_connection()
