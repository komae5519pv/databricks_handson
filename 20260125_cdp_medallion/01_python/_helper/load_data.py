# Databricks notebook source
# MAGIC %md
# MAGIC # Githubのデータをダウンロード
# MAGIC - ここではデモ用のサンプルデータをGithubからボリュームにロードします  
# MAGIC - サーバレスコンピュート

# COMMAND ----------

# %run ../00_config

# COMMAND ----------

print("CSVをダウンロードします...")

# COMMAND ----------

import requests
import os
from concurrent.futures import ThreadPoolExecutor

class DBDemos:
    @staticmethod
    def download_file_from_git(dest, owner, repo, path):
        def download_file(url, destination):
            local_filename = url.split('/')[-1]
            try:
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    with open(f'{destination}/{local_filename}', 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                print(f'Successfully saved: {local_filename}')
            except Exception as e:
                print(f'Failed to download {local_filename}: {e}')

        if not os.path.exists(dest):
            os.makedirs(dest, exist_ok=True)

        # パスを整形してAPI URLを作成
        clean_path = path.strip("/")
        api_url = f'https://api.github.com/repos/{owner}/{repo}/contents/{clean_path}'
        
        r = requests.get(api_url)
        
        # エラーハンドリング
        if r.status_code == 403:
            print("GitHub API rate limit exceeded.")
            return
        if r.status_code != 200:
            print(f"API Error {r.status_code}: Please check path or repo name.")
            return
        
        files = r.json()
        download_urls = [f['download_url'] for f in files if isinstance(f, dict) and f.get('download_url')]

        print(f"URL: {api_url}")
        print(f"Found {len(download_urls)} files to download.")

        # ThreadPoolExecutorのmapをlist()で囲んで確実に実行させる
        with ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(lambda url: download_file(url, dest), download_urls))

# COMMAND ----------

# DBTITLE 1,CSVダウンロード
# GitHubのリポジトリ構成に合わせてターゲットを指定
targets = [
    ("users", "20260125_cdp_medallion/_data/users/"),
    ("items", "20260125_cdp_medallion/_data/items/"),
    ("stores", "20260125_cdp_medallion/_data/stores/"),
    ("orders", "20260125_cdp_medallion/_data/orders/"),
    ("orders_items", "20260125_cdp_medallion/_data/orders_items/")
]

for folder_name, git_path in targets:
    print(f"--- Processing {folder_name} ---")
    DBDemos.download_file_from_git(
        dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/{folder_name}",
        owner="komae5519pv",        # GitHubのオーナーを指定
        repo="databricks_handson",  # GitHubのリポジトリ名を指定
        path=git_path               # リポジトリ内の対象ディレクトリパスを指定
    )

# COMMAND ----------

print("CSVをダウンロード完了しました！")
