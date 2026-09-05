import os
import sys
import requests

databricks_token = os.environ.get("DATABRICKS_TOKEN")
databricks_workspace_url = os.environ.get("DATABRICKS_WORKSPACE_URL")
volume_path = os.environ.get("DATABRICKS_VOLUME_PATH")

if not databricks_token or not databricks_workspace_url or not volume_path:
    print("ERROR: Missing one of DATABRICKS_TOKEN, DATABRICKS_WORKSPACE_URL, DATABRICKS_VOLUME_PATH.")
    sys.exit(1)

# --- Create tiny fake data ---
fake_data = "col1,col2\nvalue1,value2\n"
filename = "test_upload.csv"

with open(filename, "w") as f:
    f.write(fake_data)

# --- Upload to Databricks Volume ---
upload_url = f"{databricks_workspace_url}/api/2.0/fs/files{volume_path}/{filename}?overwrite=true"
print("Upload URL:", upload_url)

with open(filename, "rb") as f:
    file_bytes = f.read()

headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Content-Type": "application/octet-stream"
}

try:
    response = requests.put(upload_url, headers=headers, data=file_bytes, timeout=30)
    print("Status code:", response.status_code)
    print("Response:", response.text)
    response.raise_for_status()
    print("SUCCESS: file uploaded.")
except requests.exceptions.RequestException as e:
    print("ERROR: Upload failed ->", e)
    sys.exit(1)