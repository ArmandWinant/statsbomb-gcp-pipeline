import os
import requests
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIG
GITHUB_REPO = "statsbomb/open-data"
BRANCH = "master"
GITHUB_DIR = "data"
GCS_BUCKET_NAME = "statsbomb-gcp-pipeline_data"
GCS_TARGET_PREFIX = "raw/"
MAX_WORKERS = 10  # Adjust based on your network/GCS limits

GITHUB_API_BASE = "https://api.github.com/repos"

# 1. Recursive listing of JSON files
def list_github_files_recursively(path=""):
    url = f"{GITHUB_API_BASE}/{GITHUB_REPO}/contents/{GITHUB_DIR}/{path}?ref={BRANCH}"
    response = requests.get(url)
    response.raise_for_status()
    items = response.json()

    files = []
    for item in items:
        if item["type"] == "file" and item["name"].endswith(".json"):
            files.append(item["path"])
        elif item["type"] == "dir":
            sub_path = item["path"].replace(f"{GITHUB_DIR}/", "")
            files.extend(list_github_files_recursively(sub_path))
    return files

# 2. Download file content
def download_file(repo_path):
    raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{BRANCH}/{repo_path}"
    response = requests.get(raw_url)
    response.raise_for_status()
    return response.content

# 3. Upload to GCS
def upload_to_gcs(repo_path, content):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    relative_path = repo_path.replace(GITHUB_DIR + "/", "")
    blob_path = os.path.join(GCS_TARGET_PREFIX, relative_path)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(content, content_type="application/json")
    print(f"Uploaded: {relative_path}")
    return blob_path

# 4. Combined worker function
def download_and_upload(repo_path):
    try:
        content = download_file(repo_path)
        return upload_to_gcs(repo_path, content)
    except Exception as e:
        print(f"Failed for {repo_path}: {e}")
        return None

# 5. Main function
def main():
    json_files = list_github_files_recursively()
    print(f"Found {len(json_files)} JSON files")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_and_upload, path) for path in json_files]
        for future in as_completed(futures):
            future.result()  # Triggers print inside upload_to_gcs or error

if __name__ == "__main__":
    main()
