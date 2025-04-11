import subprocess
import requests
import csv
import os
import json

# === Step 1: Resolve accessToken and instanceUrl via Salesforce CLI ===
org_alias = "srcOrg"

print(f"Retrieving credentials for org alias: {org_alias}...")

try:
    result = subprocess.run(
        ["sf", "org", "display", "--target-org", org_alias, "--json"],
        check=True,
        capture_output=True,
        text=True
    )
    org_info = json.loads(result.stdout)["result"]
    access_token = org_info["accessToken"]
    instance_url = org_info["instanceUrl"]
except Exception as e:
    print("Failed to retrieve org info from Salesforce CLI.")
    print(e)
    exit(1)

# === Step 2: Build query ===
query = (
    "SELECT Name, ExpressionSetId, ReferenceObjectId, ConstraintModelTag, ConstraintModelTagType "
    "FROM ExpressionSetConstraintObj"
)

endpoint = f"{instance_url}/services/data/v64.0/query"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}
params = {"q": query}

print("🔍 Querying Salesforce...")
response = requests.get(endpoint, headers=headers, params=params)

if response.status_code != 200:
    print(f"API Error: {response.status_code}")
    print(response.text)
    exit(1)

records = response.json().get("records", [])
print(f"Retrieved {len(records)} records")

# === Step 3: Write to CSV ===
output_path = "data/ExpressionSetConstraintObj.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Name", "ExpressionSetId", "ReferenceObjectId", "ConstraintModelTag", "ConstraintModelTagType"])
    for rec in records:
        writer.writerow([
            rec.get("Name", ""),
            rec.get("ExpressionSetId", ""),
            rec.get("ReferenceObjectId", ""),
            rec.get("ConstraintModelTag", ""),
            rec.get("ConstraintModelTagType", "")
        ])

print(f"Export complete! File saved to {output_path}")
