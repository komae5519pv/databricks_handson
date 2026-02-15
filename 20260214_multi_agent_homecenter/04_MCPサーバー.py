# Databricks notebook source
# MAGIC %md
# MAGIC # MCPサーバー - Tavily Web Search
# MAGIC
# MAGIC [MCPサーバー接続の作成](https://docs.databricks.com/aws/ja/generative-ai/agent-framework/mcp-server-connections.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tavily MCP Serverをインストールする
# MAGIC
# MAGIC 1. [https://app.tavily.com](https://app.tavily.com) でユーザーアカウント作成後、Tavily API Key を取得
# MAGIC 2. **Marketplace** を開く
# MAGIC 3. 「Tavily」で検索
# MAGIC 4. **Tavily MCP Server** を選択
# MAGIC 5. 「Install」でインストール
# MAGIC 6. MCP接続を作成
# MAGIC | 項目 | 値 | 備考 |
# MAGIC |---|---|---|
# MAGIC | 接続名 | `tavily_search` | Databricks内でこの名前で管理します。わかりやすい文字列にしましょう。 |
# MAGIC | ホスト | `https://mcp.tavily.com` | 固定|
# MAGIC | ベースパス | `/mcp` | 固定|
# MAGIC | ベアラートークン | `<あなたのTavily アカウントのAPI Key>` | Tavily API Key は [https://app.tavily.com](https://app.tavily.com) から取得できます|
# MAGIC ![MCPサーバーの接続作成手順.gif](./_image/MCPサーバーの接続作成手順.gif "MCPサーバーの接続作成手順.gif")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 接続を確認する

# COMMAND ----------

# MAGIC %md
# MAGIC ![MCPサーバーの接続を確認する.gif](./_image/MCPサーバーの接続を確認する.gif "MCPサーバーの接続を確認する.gif")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE CONNECTION tavily_search;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. マルチエージェントスーパーバイザーに追加する
# MAGIC
# MAGIC `05_AgentBricksマルチエージェントスーパーバイザー` のエージェント設定で、MCPサーバーとして `tavily_search` を追加する
