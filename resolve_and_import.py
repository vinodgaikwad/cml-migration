import os
import csv
import json
import requests
import subprocess

DATA_DIR = "data"
BLOB_DIR = os.path.join(DATA_DIR, "blobs")
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

# === CSV Loader ===
def read_csv(filename):
    with open(os.path.join(DATA_DIR, filename), newline="") as f:
        return list(csv.DictReader(f))

# === REST: POST ===
def create_record(obj_name, record, access_token, instance_url):
    url = f"{instance_url}/services/data/v64.0/sobjects/{obj_name}/"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    record.pop("Id", None)
    resp = requests.post(url, headers=headers, json=record)
    if resp.status_code == 201:
        print(f"✅ Created {obj_name} → {record.get('Name', record.get('ApiName', '') )}")
        return resp.json()["id"]
    else:
        print(f"❌ Failed {obj_name}: {resp.status_code} - {resp.text}")
        return None

# === REST: PUT blob ===
def upload_blob(record_id, blob_path, access_token, instance_url):
    url = f"{instance_url}/services/data/v64.0/sobjects/ExpressionSetDefinitionVersion/{record_id}/ConstraintModel"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/octet-stream"
    }
    with open(blob_path, "rb") as f:
        resp = requests.put(url, headers=headers, data=f)
    if resp.status_code == 204:
        print(f"📦 Uploaded blob → {record_id}")
    else:
        print(f"⚠️ Blob upload failed → {record_id}: {resp.status_code} - {resp.text}")

# === FK Resolution Helpers ===
def index_by_field(data, key):
    return {row[key]: row for row in data if key in row and row[key]}

def composite_key(row):
    return (
        row.get("ParentProduct.Name", "") + "|" +
        row.get("ChildProduct.Name", "") + "|" +
        row.get("ChildProductClassification.Name", "") + "|" +
        row.get("ProductRelationshipType.Name", "")
    )

# === MAIN ===
def main():
    access_token, instance_url = get_auth()

    # Load all input data
    esdv = read_csv("ExpressionSetDefinitionVersion.csv")[0]
    ess = read_csv("ExpressionSet.csv")[0]
    esv = read_csv("ExpressionSetVersion.csv")[0]
    esc_list = read_csv("ExpressionSetConstraintObj.csv")

    products = index_by_field(read_csv("Product2.csv"), "Name")
    classifications = index_by_field(read_csv("ProductClassification.csv"), "Name")
    components = {composite_key(r): r["Id"] for r in read_csv("ProductRelatedComponent.csv")}

    # === Insert ExpressionSet
    ess.pop("Id", None)
    ess_id = create_record("ExpressionSet", ess, access_token, instance_url)

    # === Insert ExpressionSetVersion
    esv.pop("Id", None)
    esv.pop("IsActive", None)
    esv.pop("IsDeleted", None)
    esv.pop("ExpressionSet.ApiName", None)
    esv.pop("ExpressionSetDefinitionVerId", None)
    esv["ExpressionSetId"] = ess_id

    # Resolve ExpressionSetDefinitionVersion ID by DeveloperName
    devname = esv["ApiName"]
    query_url = f"{instance_url}/services/data/v64.0/query"
    headers = { "Authorization": f"Bearer {access_token}" }
    q = f"SELECT Id FROM ExpressionSetDefinitionVersion WHERE DeveloperName = '{devname}'"
    resp = requests.get(query_url, headers=headers, params={"q": q})

    if resp.status_code != 200 or not resp.json().get("records"):
        print(f"❌ Could not find ExpressionSetDefinitionVersion for {devname}")
        return
    esdv_id = resp.json()["records"][0]["Id"]
    esv["ExpressionSetDefinitionVerId"] = esdv_id

    esv_id = create_record("ExpressionSetVersion", esv, access_token, instance_url)

    # === Insert ExpressionSetConstraintObj
    for row in esc_list:
        row.pop("Id", None)
        row.pop("ExpressionSet.ApiName", None)
        row.pop("Name", None)
        row["ExpressionSetId"] = ess_id
        ref_id = row.get("ReferenceObjectId", "")
        resolved_id = None

        if ref_id.startswith("01t"):  # Product2
            match = next((v["Id"] for v in products.values() if v["Id"] == ref_id), None)
            resolved_id = match
        elif ref_id.startswith("11B"):  # ProductClassification
            match = next((v["Id"] for v in classifications.values() if v["Id"] == ref_id), None)
            resolved_id = match
        elif ref_id.startswith("0dS"):  # ProductRelatedComponent
            match = next((k for k, v in components.items() if v == ref_id), None)
            resolved_id = components.get(match)

        if resolved_id:
            row["ReferenceObjectId"] = resolved_id
            create_record("ExpressionSetConstraintObj", row, access_token, instance_url)
        else:
            print(f"⚠️ Could not resolve ReferenceObjectId: {ref_id}")

    # === Upload Blob
    version = esdv.get("VersionNumber")
    blob_file = os.path.join(BLOB_DIR, f"ESDV_{devname.replace('_V' + version, '')}_V{version}.ffxblob")
    if os.path.exists(blob_file):
        upload_blob(esdv_id, blob_file, access_token, instance_url)
    else:
        print(f"⚠️ Blob file missing: {blob_file}")

if __name__ == "__main__":
    main()
