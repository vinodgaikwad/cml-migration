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

def update_constraint_model_blob(record_id, blob_path, access_token, instance_url):
    url = f"{instance_url}/services/data/v64.0/sobjects/ExpressionSetDefinitionVersion/{record_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    with open(blob_path, "rb") as f:
        blob_data = f.read()

    payload = {
        "ConstraintModel": blob_data.decode("latin1")  # base64 bytes as string
    }

    resp = requests.patch(url, headers=headers, json=payload)
    if resp.status_code == 204:
        print(f"📦 ConstraintModel updated successfully → {record_id}")
    else:
        print(f"⚠️ Failed to update ConstraintModel → {record_id}: {resp.status_code} - {resp.text}")

import base64

def upload_blob_via_patch(record_id, blob_path, access_token, instance_url):
    # Build the endpoint for the record (omitting the /ConstraintModel sub-path)
    url = f"{instance_url}/services/data/v64.0/sobjects/ExpressionSetDefinitionVersion/{record_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    # Read blob as binary and base64 encode it
    with open(blob_path, "rb") as f:
        blob_data = f.read()
    encoded_blob = base64.b64encode(blob_data).decode("utf-8")
    # Prepare payload; the ConstraintModel field expects a base64 string.
    payload = {
        "ConstraintModel": encoded_blob
    }
    # Use PATCH to update the record
    resp = requests.patch(url, headers=headers, json=payload)
    if resp.status_code == 204:
        print(f"📦 Uploaded blob via PATCH → {record_id}")
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
    esdcd = read_csv("ExpressionSetDefinitionContextDefinition.csv")[0]
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

    #esv_id = create_record("ExpressionSetVersion", esv, access_token, instance_url)
    
    
    # === Insert ExpressionSetDefinitionContextDefinition
    apiname = ess["ApiName"]
    cd_apiname = esdcd["ContextDefinitionApiName"]
    esdcd.pop("ContextDefinitionApiName", None)
    esdcd.pop("ExpressionSetApiName", None)
    
    # Resolve ContextDefinition ID by DeveloperName
    q = f"SELECT Id FROM ContextDefinition WHERE DeveloperName = '{cd_apiname}'"
    resp = requests.get(query_url, headers=headers, params={"q": q})

    if resp.status_code != 200 or not resp.json().get("records"):
        print(f"❌ Could not find ContextDefinition for {cd_apiname}")
        return
    cd_id = resp.json()["records"][0]["Id"]
    esdcd["ContextDefinitionId"] = cd_id
    
    # Resolve ExpressionSetDefinition ID by DeveloperName
    q = f"SELECT Id FROM ExpressionSetDefinition WHERE DeveloperName = '{apiname}'"
    resp = requests.get(query_url, headers=headers, params={"q": q})

    if resp.status_code != 200 or not resp.json().get("records"):
        print(f"❌ Could not find ExpressionSetDefinition for {apiname}")
        return
    esd_id = resp.json()["records"][0]["Id"]
    esdcd["ExpressionSetDefinitionId"] = esd_id
	
    create_record("ExpressionSetDefinitionContextDefinition", esdcd, access_token, instance_url)


    # === Build lookup maps for ReferenceObjectId resolution ===
    print("🔁 Building legacy ID to Unique Key (UK) maps...")

    legacy_to_uk = {}
    product_names = set()
    classification_names = set()
    prc_parent_names = set()

    # Product2
    for row in read_csv("Product2.csv"):
        legacy_id = row["Id"]
        name = row["Name"]
        product_names.add(name)
        legacy_to_uk[legacy_id] = name  # UK for Product2 is just Name

    # ProductClassification
    for row in read_csv("ProductClassification.csv"):
        legacy_id = row["Id"]
        name = row["Name"]
        classification_names.add(name)
        legacy_to_uk[legacy_id] = name  # UK for Classification is just Name

    # ProductRelatedComponent
    for row in read_csv("ProductRelatedComponent.csv"):
        legacy_id = row["Id"]
        uk = (
            row["ParentProduct.Name"] + "|" +
            (row.get("ChildProduct.Name") or "") + "|" +
            (row.get("ChildProductClassification.Name") or "") + "|" +
            (row.get("ProductRelationshipType.Name") or "") + "|" +
            (row.get("Sequence") or "")
        )
        prc_parent_names.add(row["ParentProduct.Name"])
        legacy_to_uk[legacy_id] = uk

    print("📡 Querying target org for new IDs...")

    headers = {"Authorization": f"Bearer {access_token}"}
    query_url = f"{instance_url}/services/data/v64.0/query"

    # Query target org for Product2
    prod_filter = ",".join(f"'{n}'" for n in product_names)
    q1 = f"SELECT Id, Name FROM Product2 WHERE Name IN ({prod_filter})"
    resp1 = requests.get(query_url, headers=headers, params={"q": q1})
    uk_to_targetId_prod = {r["Name"]: r["Id"] for r in resp1.json().get("records", [])}

    # Query target org for ProductClassification
    class_filter = ",".join(f"'{n}'" for n in classification_names)
    q2 = f"SELECT Id, Name FROM ProductClassification WHERE Name IN ({class_filter})"
    resp2 = requests.get(query_url, headers=headers, params={"q": q2})
    uk_to_targetId_class = {r["Name"]: r["Id"] for r in resp2.json().get("records", [])}

    # Query target org for ProductRelatedComponent
    prc_filter = ",".join(f"'{n}'" for n in prc_parent_names)
    q3 = f"""
    SELECT Id,
        ParentProduct.Name,
        ChildProduct.Name,
        ChildProductClassification.Name,
        ProductRelationshipType.Name, Sequence
    FROM ProductRelatedComponent
    WHERE ParentProduct.Name IN ({prc_filter})
    """
    resp3 = requests.get(query_url, headers=headers, params={"q": q3})
    uk_to_targetId_prc = {
    (
        r["ParentProduct"]["Name"] + "|" +
        (r["ChildProduct"]["Name"] if r.get("ChildProduct") else "") + "|" +
        (r["ChildProductClassification"]["Name"] if r.get("ChildProductClassification") else "") + "|" +
        (r["ProductRelationshipType"]["Name"] if r.get("ProductRelationshipType") else "") + "|" +
        (str(r["Sequence"]) if r.get("Sequence") is not None else "")
    ): r["Id"]
    for r in resp3.json().get("records", [])
    if r.get("ParentProduct")
    }

    print("🔁 Maps ready. Resolving ReferenceObjectIds...")

    # === Insert ExpressionSetConstraintObj
    for row in esc_list:
        row.pop("Id", None)
        row.pop("ExpressionSet.ApiName", None)
        row.pop("Name", None)
        row["ExpressionSetId"] = ess_id
        ref_id = row.get("ReferenceObjectId", "")
        resolved_id = None

        uk = legacy_to_uk.get(ref_id)
        if ref_id.startswith("01t"):
            resolved_id = uk_to_targetId_prod.get(uk)
        elif ref_id.startswith("11B"):
            resolved_id = uk_to_targetId_class.get(uk)
        elif ref_id.startswith("0dS"):
            resolved_id = uk_to_targetId_prc.get(uk)

        if resolved_id:
            row["ReferenceObjectId"] = resolved_id
            create_record("ExpressionSetConstraintObj", row, access_token, instance_url)
        else:
            print(f"⚠️ Could not resolve ReferenceObjectId: {ref_id} → UK: {uk}")

    # === Upload Blob
    version = esdv.get("VersionNumber")
    blob_file = os.path.join(BLOB_DIR, f"ESDV_{devname.replace('_V' + version, '')}_V{version}.ffxblob")
    if os.path.exists(blob_file):
        upload_blob_via_patch(esdv_id, blob_file, access_token, instance_url)
    else:
        print(f"⚠️ Blob file missing: {blob_file}")

if __name__ == "__main__":
    main()
