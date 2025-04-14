from dotenv import load_dotenv
import os
import json
import requests
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
def get_changed_json_files(bucket, repo, branch="main", base_path=""):
  synced_shas = load_synced_shas(bucket)
  new_synced_shas = {}
  
  tree = get_tree(repo, branch)
  new_files = []
  
  for item in tree:
    if item['type'] == 'blob' and item['path'].startswith(base_path) and item['path'].endswith(".json"):
      file_path = item['path']
      file_sha = item['sha']

      new_synced_shas[file_path] = file_sha
  
      if synced_shas.get(file_path) == file_sha:
        continue  # File unchanged
  
      new_files.append(file_path)
  
  save_synced_shas(bucket, new_synced_shas)
  return new_files

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

# === RUN THE PIPELINE ===
def main():
  client = storage.Client(project=os.getenv('PROJECT_ID'))
  bucket = client.bucket(GCS_BUCKET_NAME)
  
  json_files = get_changed_json_files(
    bucket=bucket,
    repo=GITHUB_REPO,
    branch=BRANCH,
    base_path=GITHUB_DIR
  )
  print(f"Found {len(json_files)} new/updated JSON files")
  
  with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(download_and_upload, bucket, path) for path in json_files]
    for future in as_completed(futures):
      future.result()  # Triggers print inside upload_to_gcs or error

if __name__ == "__main__":
  main()