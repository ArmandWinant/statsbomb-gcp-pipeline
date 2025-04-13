import os
import requests
from google.cloud import storage

# CONFIG — customize these
GITHUB_REPO = "statsbomb/open-data"
BRANCH = "master"
GITHUB_DIR = "data"
GCS_BUCKET_NAME = "your-gcs-bucket-name"
GCS_TARGET_PREFIX = "raw/statsbomb/"  # destination prefix in GCS

GITHUB_API_BASE = "https://api.github.com/repos"

def list_github_files_recursively(path=""):
    """Recursively lists all JSON files in the GitHub repo under a given directory."""
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

def download_file(repo_path):
    """Download file content from raw GitHub URL."""
    raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{BRANCH}/{repo_path}"
    response = requests.get(raw_url)
    response.raise_for_status()
    return response.content

def upload_to_gcs(repo_path, content):
    """Upload file content to GCS under the same relative path."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)

    relative_path = repo_path.replace(GITHUB_DIR + "/", "")
    blob_path = os.path.join(GCS_TARGET_PREFIX, relative_path)

    blob = bucket.blob(blob_path)
    blob.upload_from_string(content, content_type="application/json")
    print(f"✅ Uploaded: {relative_path} → gs://{GCS_BUCKET_NAME}/{blob_path}")

def main():
    print("🔍 Crawling GitHub repo...")
    json_files = list_github_files_recursively()

    for repo_path in json_files:
        print(f"⬇️ Downloading {repo_path}...")
        content = download_file(repo_path)
        upload_to_gcs(repo_path, content)

if __name__ == "__main__":
    main()