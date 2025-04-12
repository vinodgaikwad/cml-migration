import requests
import json
import subprocess


TARGET_ALIAS = "tgtOrg"

# === Auth + Org Info ===
def get_auth():
    result = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ALIAS, "--json"],
        check=True,
        capture_output=True,
        text=True
    )
    info = json.loads(result.stdout)["result"]
    return info["accessToken"], info["instanceUrl"]

# Replace these with your actual values
access_token, instance_url = get_auth()

url = f"{instance_url}/services/data/v64.0/sobjects/ExpressionSetDefinitionVersion/describe"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

resp = requests.get(url, headers=headers)
fields = resp.json().get("fields", [])

for field in fields:
    if field["name"] == "ConstraintModel":
        print(json.dumps(field, indent=2))
