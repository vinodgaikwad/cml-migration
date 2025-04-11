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
        print(f"✅ Created {obj_name} → {record.get('Name', record.get('ApiName', ''))}")
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
    esd = read_csv("ExpressionSetDefinition.csv")[0]
    esdv = read_csv("ExpressionSetDefinitionVersion.csv")[0]
    ess = read_csv("ExpressionSet.csv")[0]
    esv = read_csv("ExpressionSetVersion.csv")[0]
    esc_list = read_csv("ExpressionSetConstraintObj.csv")

    products = index_by_field(read_csv("Product2.csv"), "Name")
    classifications = index_by_field(read_csv("ProductClassification.csv"), "Name")
    components = {composite_key(r): r["Id"] for r in read_csv("ProductRelatedComponent.csv")}

    # === Insert ExpressionSetDefinition
    esd_id = create_record("ExpressionSetDefinition", esd, access_token, instance_url)

    # === Insert ExpressionSetDefinitionVersion
    esdv["ExpressionSetDefinitionId"] = esd_id
    esdv.pop("ExpressionSetDefinition.DeveloperName", None)
    esdv.pop("Status", None)
    blob_url = esdv.pop("ConstraintModel", "")
    esdv_id = create_record("ExpressionSetDefinitionVersion", esdv, access_token, instance_url)

    # === Insert ExpressionSet
    ess["ExpressionSetDefinitionId"] = esd_id
    ess_id = create_record("ExpressionSet", ess, access_token, instance_url)

    # === Insert ExpressionSetVersion
    esv["ExpressionSetId"] = ess_id
    esv["ExpressionSetDefinitionVerId"] = esdv_id
    esv.pop("IsActive", None)
    esv.pop("IsDeleted", None)
    esv.pop("ExpressionSet.ApiName", None)
    esv_id = create_record("ExpressionSetVersion", esv, access_token, instance_url)

    # === Insert ExpressionSetConstraintObj
    for row in esc_list:
        row["ExpressionSetId"] = ess_id
        ref_id = row.get("ReferenceObjectId", "")
        resolved_id = None

        if ref_id.startswith("01t"):  # Product2
            name = products.get(ref_id, {}).get("Name", "")
            resolved_id = next((v["Id"] for k, v in products.items() if v["Name"] == name), None)

        elif ref_id.startswith("11B"):  # ProductClassification
            name = classifications.get(ref_id, {}).get("Name", "")
            resolved_id = next((v["Id"] for k, v in classifications.items() if v["Name"] == name), None)

        elif ref_id.startswith("0dS"):  # ProductRelatedComponent
            # Find component by composite key
            for key, comp_id in components.items():
                if comp_id == ref_id:
                    resolved_id = comp_id
                    break

        if resolved_id:
            row["ReferenceObjectId"] = resolved_id
        else:
            print(f"⚠️ Could not resolve ReferenceObjectId: {ref_id}")
            continue

        create_record("ExpressionSetConstraintObj", row, access_token, instance_url)

    # === Upload Blob
    devname = esdv["DeveloperName"]
    version = esdv["VersionNumber"]
    blob_file = os.path.join(BLOB_DIR, f"ESDV_{devname.replace('_V' + version, '')}_V{version}.ffxblob")
    if os.path.exists(blob_file):
        upload_blob(esdv_id, blob_file, access_token, instance_url)
    else:
        print(f"⚠️ Blob file missing: {blob_file}")

if __name__ == "__main__":
    main()
