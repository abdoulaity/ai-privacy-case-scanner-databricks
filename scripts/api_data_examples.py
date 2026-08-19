import os
import sys
import json
import requests

# --- Setup: Credentials, Auth Headers, API Base URL, and Output Folder ---
api_key = os.environ.get("COURT_LISTENER_KEY")
if not api_key:
    print("ERROR: COURT_LISTENER_KEY environment variable not set.")
    sys.exit(1)

headers = {"Authorization": f"Token {api_key}"}
base_url = "https://www.courtlistener.com/api/rest/v4"
os.makedirs("data/api_data_examples", exist_ok=True)

# --- Step 1: Basic Connectivity Test with One Search :"data protection" ---
try:
    response = requests.get(
        f"{base_url}/search/",
        headers=headers,
        params={"q": "data protection", "type": "o"},
        timeout=10
    )
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print("ERROR: Connectivity test failed ->", e)
    sys.exit(1)

data = response.json()
print("Connectivity Test: Passed")

# --- Step 2: Inspect the Schema of one Result from the Search API and Save it ---
search_example = data["results"][0]

with open("data/api_data_examples/search_example.json", "w") as f:
    json.dump(search_example, f, indent=2)

print("Saved search_example.json")

# --- Step 3: Inspect the Schema of one specific Opinion and Save it ---
opinion_id = search_example["opinions"][0]["id"]

try:
    opinion_response = requests.get(
        f"{base_url}/opinions/{opinion_id}/",
        headers=headers,
        timeout=10
    )
    opinion_response.raise_for_status()
except requests.exceptions.RequestException as e:
    print("ERROR: Opinion fetch failed ->", e)
    sys.exit(1)

opinion_data = opinion_response.json()

with open("data/api_data_examples/opinion_example.json", "w") as f:
    json.dump(opinion_data, f, indent=2)

print("Saved opinion_example.json")