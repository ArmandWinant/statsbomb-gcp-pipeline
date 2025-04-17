from dotenv import load_dotenv
import os
import json
import requests
from datetime import datetime  # Added timedelta import
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta  # Added timedelta import
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed


load_dotenv()

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
SYNC_RECORD_FILE = "synced_files.json"
GITHUB_REPO = "statsbomb/open-data"
BRANCH = "master"
GITHUB_DIR = "data"
GCS_BUCKET_NAME = os.getenv('GCS_DATA_BUCKET_NAME')
GCS_TARGET_PREFIX = "raw/"
MAX_WORKERS = 10  # Adjust based on your network/GCS limits

GITHUB_API_BASE = "https://api.github.com/repos"

HEADERS = {
  "Authorization": f"token {GITHUB_TOKEN}",
  "Accept": "application/vnd.github.v3+json"
}

# Get repo file tree
def get_tree(repo, branch):
  ref_url = f"{GITHUB_API_BASE}/{repo}/git/refs/heads/{branch}"
  sha = requests.get(ref_url, headers=HEADERS).json()['object']['sha']
  tree_url = f"{GITHUB_API_BASE}/{repo}/git/trees/{sha}?recursive=1"
  return requests.get(tree_url, headers=HEADERS).json().get('tree', [])

def load_synced_shas(bucket):
  blob_path = os.path.join(GCS_TARGET_PREFIX, SYNC_RECORD_FILE)
  blob = bucket.blob(blob_path)
  
  if blob.exists():
    return json.loads(blob.download_as_bytes())

  return {}

def save_synced_shas(bucket, shas):
  blob_path = os.path.join(GCS_TARGET_PREFIX, SYNC_RECORD_FILE)
  blob = bucket.blob(blob_path)
  blob.upload_from_string(json.dumps(shas), content_type="application/json")

# Recursive listing of modified JSON files
def get_changed_json_files(**kwargs):
  client = storage.Client(project=os.getenv('PROJECT_ID'))
  bucket = client.bucket(GCS_BUCKET_NAME)

  synced_shas = load_synced_shas(bucket)
  new_synced_shas = {}
  
  tree = get_tree(GITHUB_REPO, BRANCH)
  new_files = []
  
  for item in tree:
    if item['type'] == 'blob' and item['path'].startswith(GITHUB_DIR) and item['path'].endswith(".json"):
      file_path = item['path']
      file_sha = item['sha']

      new_synced_shas[file_path] = file_sha
  
      if synced_shas.get(file_path) == file_sha:
        continue  # File unchanged
  
      new_files.append(file_path)
  
  save_synced_shas(bucket, new_synced_shas)
  kwargs['ti'].xcom_push(key='file_paths', value=new_files)

# Download file content
def download_file(repo_path):
  raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{BRANCH}/{repo_path}"
  response = requests.get(raw_url)
  response.raise_for_status()
  return response.content

# Upload to GCS
def upload_to_gcs(bucket, repo_path, content):
  relative_path = repo_path.replace(GITHUB_DIR + "/", "")
  blob_path = os.path.join(GCS_TARGET_PREFIX, relative_path)
  blob = bucket.blob(blob_path)
  blob.upload_from_string(content, content_type="application/json")
  print(f"Uploaded: {relative_path}")
  return blob_path

# Combined worker function
def download_and_upload(bucket, repo_path):
  try:
    content = download_file(repo_path)
    return upload_to_gcs(bucket, repo_path, content)
  except Exception as e:
    print(f"Failed for {repo_path}: {e}")
    return None

def sync_statsbomb_jsons(**kwargs):
  ti = kwargs['ti']
  json_files = ti.xcom_pull(task_ids='fetch_statsbomb_data', key='file_paths')

  client = storage.Client(project=os.getenv('PROJECT_ID'))
  bucket = client.bucket(GCS_BUCKET_NAME)

  with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(download_and_upload, bucket, path) for path in json_files]
    for future in as_completed(futures):
      future.result()  # Triggers print inside upload_to_gcs or error


# Define the DAG
with DAG(
    dag_id="api_to_gcs",
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Fetch data from API and save as Parquet to GCS",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
  fetch_statsbomb_data = PythonOperator(
    task_id="fetch_statsbomb_data",
    python_callable=get_changed_json_files,
  )

  upload_json_to_gcs = PythonOperator(
    task_id="save_parquet_to_gcs",
    python_callable=sync_statsbomb_jsons,
  )
  
  fetch_statsbomb_data >> upload_json_to_gcs