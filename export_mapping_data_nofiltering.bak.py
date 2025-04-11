import subprocess
import requests
import csv
import os
import json
import argparse

# === Parse Arguments ===
parser = argparse.ArgumentParser(description="Export metadata/data for one Expression Set Definition & Version")
parser.add_argument("--developerName", type=str, required=True, help="DeveloperName of the Expression Set Definition (e.g. ProductQualification)")
parser.add_argument("--version", type=str, required=True, help="Version number (e.g. 1)")
args = parser.parse_args()

dev_name = args.developerName.strip()
version_num = args.version.strip()
api_name_versioned = f"{dev_name}_V{version_num}"

# === Export CSV Helper ===
def export_to_csv(query, filename, fields, alias="srcOrg"):
    print(f"📦 Exporting: {filename.replace('data/', '')}")

    try:
        result = subprocess.run(
            ["sf", "org", "display", "--target-org", alias, "--json"],
            check=True,
            capture_output=True,
            text=True
        )
        org_info = json.loads(result.stdout)["result"]
        access_token = org_info["accessToken"]
        instance_url = org_info["instanceUrl"]
    except Exception as e:
        print("❌ Failed to retrieve org info from Salesforce CLI.")
        print(e)
        return

    endpoint = f"{instance_url}/services/data/v64.0/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(endpoint, headers=headers, params={"q": query})
    if response.status_code != 200:
        print(f"❌ API Error ({filename}): {response.status_code}")
        print(response.text)
        return

    records = response.json().get("records", [])
    print(f"✅ {len(records)} records fetched for {filename}")

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(fields)
        for rec in records:
            writer.writerow([rec.get(f, "") for f in fields])

    print(f"📄 Saved to {filename}\n")

# === Blob Download Helper ===
def download_constraint_model_blobs(alias="srcOrg", input_csv="data/ExpressionSetDefinitionVersion.csv"):
    print("📥 Downloading ConstraintModel blobs...")

    try:
        result = subprocess.run(
            ["sf", "org", "display", "--target-org", alias, "--json"],
            check=True,
            capture_output=True,
            text=True
        )
        org_info = json.loads(result.stdout)["result"]
        access_token = org_info["accessToken"]
        instance_url = org_info["instanceUrl"]
        print(f"🔑 Auth success - instance: {instance_url}")
    except Exception as e:
        print("❌ Failed to get org info")
        print(e)
        return

    headers = { "Authorization": f"Bearer {access_token}" }
    os.makedirs("data/blobs", exist_ok=True)

    with open(input_csv, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            print(f"🧪 Row: DeveloperName={row.get('DeveloperName')}, Version={row.get('VersionNumber')}")

            if row.get("DeveloperName") != api_name_versioned:
                print("⏭️ Skipped (not matching filter)")
                continue

            blob_url = row.get("ConstraintModel", "")
            if not blob_url.startswith("/services"):
                print(f"⚠️ Invalid or empty blob URL: {blob_url}")
                continue

            full_url = instance_url + blob_url
            print(f"🌐 Fetching blob from: {full_url}")

            resp = requests.get(full_url, headers=headers)
            if resp.status_code == 200:
                file_path = f"data/blobs/ESDV_{dev_name}_V{version_num}.ffxblob"
                with open(file_path, "wb") as out_file:
                    out_file.write(resp.content)
                print(f"✅ Saved blob: {file_path}")
            else:
                print(f"❌ Failed to fetch blob: {resp.status_code} - {resp.text}")

# === Begin Export Tasks ===

export_to_csv(
    query=f"""
        SELECT DeveloperName, ExecutionScale, Id, Language, MasterLabel, NamespacePrefix
        FROM ExpressionSetDefinition
        WHERE DeveloperName = '{dev_name}'
    """,
    filename="data/ExpressionSetDefinition.csv",
    fields=["DeveloperName", "ExecutionScale", "Id", "Language", "MasterLabel", "NamespacePrefix"]
)

export_to_csv(
    query=f"""
        SELECT ConstraintModel, DeveloperName, ExpressionSetDefinitionId, Id, Language,
               MasterLabel, NamespacePrefix, Status, VersionNumber
        FROM ExpressionSetDefinitionVersion
        WHERE ExpressionSetDefinition.DeveloperName = '{dev_name}'
          AND VersionNumber = {version_num}
    """,
    filename="data/ExpressionSetDefinitionVersion.csv",
    fields=[
        "ConstraintModel", "DeveloperName", "ExpressionSetDefinitionId", "Id", "Language",
        "MasterLabel", "NamespacePrefix", "Status", "VersionNumber"
    ]
)

export_to_csv(
    query=f"""
        SELECT ApiName, Description, ExecutionScale, ExpressionSetDefinitionId, Id,
               InterfaceSourceType, Name, ResourceInitializationType, UsageType
        FROM ExpressionSet
        WHERE ExpressionSetDefinition.DeveloperName = '{dev_name}'
    """,
    filename="data/ExpressionSet.csv",
    fields=[
        "ApiName", "Description", "ExecutionScale", "ExpressionSetDefinitionId", "Id",
        "InterfaceSourceType", "Name", "ResourceInitializationType", "UsageType"
    ]
)

export_to_csv(
    query=f"""
        SELECT ApiName, DecimalScale, Description, EndDateTime, ExpressionSetDefinitionVerId,
               ExpressionSetId, Id, IsActive, IsDeleted, Name, Rank, StartDateTime, VersionNumber
        FROM ExpressionSetVersion
        WHERE ApiName = '{api_name_versioned}'
    """,
    filename="data/ExpressionSetVersion.csv",
    fields=[
        "ApiName", "DecimalScale", "Description", "EndDateTime", "ExpressionSetDefinitionVerId",
        "ExpressionSetId", "Id", "IsActive", "IsDeleted", "Name", "Rank", "StartDateTime", "VersionNumber"
    ]
)

export_to_csv(
    query=f"""
        SELECT Name, ExpressionSetId, ReferenceObjectId, ConstraintModelTag, ConstraintModelTagType
        FROM ExpressionSetConstraintObj
        WHERE ExpressionSet.ApiName = '{dev_name}'
    """,
    filename="data/ExpressionSetConstraintObj.csv",
    fields=["Name", "ExpressionSetId", "ReferenceObjectId", "ConstraintModelTag", "ConstraintModelTagType"]
)

# === Supporting Objects (No filtering) ===

export_to_csv(
    query="SELECT Id, Name FROM Product2",
    filename="data/Product2.csv",
    fields=["Id", "Name"]
)

export_to_csv(
    query="SELECT Id, Name FROM ProductClassification",
    filename="data/ProductClassification.csv",
    fields=["Id", "Name"]
)

export_to_csv(
    query="""
        SELECT Id, Name, ParentProductId, ChildProductId, ChildProductClassificationId,
               ProductRelationshipTypeId
        FROM ProductRelatedComponent
    """,
    filename="data/ProductRelatedComponent.csv",
    fields=[
        "Id", "Name", "ParentProductId", "ChildProductId", "ChildProductClassificationId",
        "ProductRelationshipTypeId"
    ]
)

# === Download Blob ===
download_constraint_model_blobs()
