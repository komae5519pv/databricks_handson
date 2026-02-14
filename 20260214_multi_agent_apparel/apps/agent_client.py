"""
Databricks Agent Endpoint Client
エージェントエンドポイントとの通信を管理
"""

import os
import requests
# import streamlit as st  # Not used in this class
from databricks.sdk import WorkspaceClient


class AgentClient:
    def __init__(self, endpoint_url):
        """
        エージェントクライアントの初期化

        Args:
            endpoint_url: エージェントエンドポイントのURL
        """
        self.endpoint_url = endpoint_url
        self.w = WorkspaceClient()

    def get_token(self):
        """認証トークンを取得"""
        print("DEBUG: Starting token acquisition...")
        
        try:
            # Databricks Appsでは自動認証が有効
            print("DEBUG: Checking WorkspaceClient token...")
            token = self.w.config.token
            if token:
                print(f"DEBUG: Token found via WorkspaceClient: {token[:20]}...")
                return token
            else:
                print("DEBUG: No token found via WorkspaceClient")

            # 環境変数をチェック
            print("DEBUG: Checking environment variables...")
            print(f"DEBUG: DATABRICKS_CLIENT_SECRET exists: {bool(os.environ.get('DATABRICKS_CLIENT_SECRET'))}")
            print(f"DEBUG: DATABRICKS_HOST: {os.environ.get('DATABRICKS_HOST')}")
            print(f"DEBUG: PGUSER: {os.environ.get('PGUSER')}")
            
            # OAuth2 Client Credentials フロー
            if os.environ.get('DATABRICKS_CLIENT_SECRET'):
                import requests

                host = os.environ.get('DATABRICKS_HOST', 'adb-984752964297111.11.azuredatabricks.net')
                if not host.startswith('http'):
                    host = f"https://{host}"
                token_url = f"{host}/oidc/v1/token"

                data = {
                    'grant_type': 'client_credentials',
                    'client_id': os.environ.get('PGUSER', '8b835fc5-da75-4654-a7b3-046759e8433e'),
                    'client_secret': os.environ.get('DATABRICKS_CLIENT_SECRET'),
                    'scope': 'all-apis'
                }

                print(f"DEBUG: Attempting OAuth2 token request to: {token_url}")
                response = requests.post(token_url, data=data)
                print(f"DEBUG: OAuth2 response status: {response.status_code}")
                
                if response.status_code == 200:
                    token = response.json().get('access_token')
                    print(f"DEBUG: OAuth2 token acquired: {token[:20]}...")
                    return token
                else:
                    print(f"DEBUG: OAuth2 failed: {response.text}")

        except Exception as e:
            print(f"❌ 認証エラー: {e}")
            import traceback
            print(f"DEBUG: Full traceback: {traceback.format_exc()}")

        print("DEBUG: No token acquired")
        return None

    def query(self, messages, max_tokens=1000, temperature=0.7):
        """
        エージェントにクエリを送信

        Args:
            messages: 会話履歴 (OpenAI形式のリスト)
            max_tokens: 最大トークン数
            temperature: 温度パラメータ

        Returns:
            エージェントからの応答テキスト
        """
        token = self.get_token()
        if not token:
            return "❌ 認証トークンを取得できませんでした"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Databricks Agentエンドポイント形式に変換
        # 複数の形式を試してみる
        payload_variants = [
            {"input": messages},
            {"messages": messages},
            {"dataframe_records": [{"input": messages}]},
            {"query": messages[0]["content"] if messages else ""},
            {"prompt": messages[0]["content"] if messages else ""}
        ]
        
        # デバッグ情報を追加
        print(f"DEBUG: Endpoint URL: {self.endpoint_url}")
        print(f"DEBUG: Messages: {messages}")
        print(f"DEBUG: Will try {len(payload_variants)} different payload formats")

        # デバッグモード：エラーハンドリングを無効化して詳細情報を表示
        print("=" * 50)
        print("DEBUG: STARTING AGENT BRICKS CONNECTION TEST")
        print("=" * 50)
        
        # messagesが空の場合はエラーを返す
        if not messages or len(messages) == 0:
            return "❌ エラー: メッセージが空です。質問を入力してください。"
        
        # Agent Bricksエンドポイントが期待する形式に修正
        # エラーメッセージから、inputフィールド（配列）が必要
        simple_payload = {"input": messages}
        
        debug_info = []
        debug_info.append(f"DEBUG: Trying simple payload: {simple_payload}")
        debug_info.append(f"DEBUG: Messages format: {messages}")
        debug_info.append(f"DEBUG: Messages type: {type(messages)}")
        debug_info.append(f"DEBUG: Messages length: {len(messages) if isinstance(messages, list) else 'N/A'}")
        debug_info.append(f"DEBUG: Endpoint URL: {self.endpoint_url}")
        debug_info.append(f"DEBUG: Headers: {headers}")
        
        print("\n".join(debug_info))
        
        try:
            response = requests.post(
                self.endpoint_url,
                headers=headers,
                json=simple_payload,
                timeout=120  # タイムアウトを120秒に延長
            )
            
            debug_info.append(f"DEBUG: Simple response status: {response.status_code}")
            debug_info.append(f"DEBUG: Simple response headers: {dict(response.headers)}")
            debug_info.append(f"DEBUG: Simple response text: {response.text}")
            
            print("\n".join(debug_info))
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    print(f"DEBUG: Simple response successful: {result}")
                    return self._extract_response(result, debug_info)
                except Exception as e:
                    print(f"DEBUG: JSON parsing failed: {e}")
                    return f"❌ JSON解析エラー: {e}\nレスポンス: {response.text}"
            else:
                debug_info.append(f"DEBUG: Simple response failed: {response.text}")
                print("\n".join(debug_info))
                return f"❌ HTTP {response.status_code}: {response.text}\n\nデバッグ情報:\n" + "\n".join(debug_info)
        
        except requests.Timeout:
            print("DEBUG: Simple payload failed with timeout")
            return f"⏱️ タイムアウト: Agent Bricksの応答に時間がかかっています（120秒）。\n\nデバッグ情報:\n" + "\n".join(debug_info)
        except Exception as e:
            print(f"DEBUG: Simple payload failed with exception: {e}")
            return f"❌ エラー: {str(e)}\n\nデバッグ情報:\n" + "\n".join(debug_info)
        
        # 複数のペイロード形式を試す（上記で成功した場合は実行されない）
        print("DEBUG: Trying other payload formats...")
        
        # Agent Bricksが期待する可能性のある形式を試す
        alternative_payloads = [
            {"input": [{"role": "user", "content": messages[0]["content"] if messages else ""}]},
            {"input": [{"role": "user", "content": messages[0]["content"] if messages else "", "type": "text"}]},
            {"input": [{"role": "user", "content": messages[0]["content"] if messages else "", "type": "text", "text": messages[0]["content"] if messages else ""}]},
            {"input": [{"role": "user", "content": messages[0]["content"] if messages else "", "type": "text", "text": messages[0]["content"] if messages else "", "name": "user"}]}
        ]
        
        for i, payload in enumerate(alternative_payloads):
            print(f"DEBUG: Trying alternative payload format {i+1}: {payload}")
            response = requests.post(
                self.endpoint_url,
                headers=headers,
                json=payload,
                timeout=120  # タイムアウトを120秒に延長
            )
            
            print(f"DEBUG: Response status: {response.status_code}")
            print(f"DEBUG: Response text: {response.text}")
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    print(f"DEBUG: Alternative response format {i+1} successful: {result}")
                    
                    # レスポンスが有効かチェック
                    if self._is_valid_response(result):
                        return self._extract_response(result)
                    else:
                        print(f"DEBUG: Alternative response format {i+1} invalid, trying next...")
                        continue
                except Exception as e:
                    print(f"DEBUG: JSON parsing failed for alternative format {i+1}: {e}")
                    continue
            else:
                print(f"DEBUG: Alternative response format {i+1} failed with status {response.status_code}: {response.text}")
                continue
        
        # すべての形式が失敗した場合
        return "❌ すべてのペイロード形式でエラーが発生しました。Agent Bricksの設定を確認してください。"

    def _is_valid_response(self, result):
        """レスポンスが有効かチェック"""
        if isinstance(result, dict):
            # Databricks Agent形式
            if "output" in result and len(result["output"]) > 0:
                return True
            # 他の形式もチェック
            if "choices" in result and len(result["choices"]) > 0:
                return True
            if "content" in result:
                return True
            if "message" in result:
                return True
            if "predictions" in result and len(result["predictions"]) > 0:
                return True
        elif isinstance(result, str):
            return True
        return False

    def _extract_response(self, result, debug_info=None):
        """レスポンスからテキストを抽出"""
        print(f"DEBUG: _extract_response called with: {result}")
        
        if isinstance(result, dict):
            # Databricks Agent形式
            if "output" in result:
                print(f"DEBUG: Found output field, length: {len(result['output'])}")
                if len(result["output"]) > 0:
                    output = result["output"][0]
                    print(f"DEBUG: First output: {output}")
                    if "content" in output and len(output["content"]) > 0:
                        content = output["content"][0]
                        print(f"DEBUG: First content: {content}")
                        if "text" in content:
                            return content["text"]
                else:
                    # outputが空の場合は、エージェントが応答を生成できなかった
                    print("DEBUG: Output array is empty - Agent Bricks did not generate a response")
                    error_message = f"申し訳ありません。現在、在庫情報を取得できません。Agent Bricksからのレスポンスが空です。\n\nデバッグ情報:\n- レスポンス形式: {result}\n- エージェントが応答を生成できていない可能性があります\n- Agent Bricksの設定を確認してください"
                    
                    if debug_info:
                        error_message += "\n\n追加デバッグ情報:\n" + "\n".join(debug_info)
                    
                    return error_message
            
            # choices形式 (OpenAI互換)
            if "choices" in result and len(result["choices"]) > 0:
                return result["choices"][0]["message"]["content"]
            # 直接content形式
            if "content" in result:
                return result["content"]
            # message形式
            if "message" in result:
                return result["message"]
            # predictions形式 (MLflow)
            if "predictions" in result and len(result["predictions"]) > 0:
                return result["predictions"][0]
        elif isinstance(result, str):
            return result
        return f"⚠️ 予期しないレスポンス形式: {result}"
