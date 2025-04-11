import subprocess
import requests
import csv
import os
import json

# === Utility: Query + Save ===
def export_to_csv(query, filename, fields, alias="srcOrg"):
    print(f"📦 Exporting: {filename.replace('data/', '')}")

    # Step 1: Get accessToken + instanceUrl from SF CLI
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
        print("Failed to retrieve org info from Salesforce CLI.")
        print(e)
        return

    # Step 2: Query Salesforce REST API
    endpoint = f"{instance_url}/services/data/v64.0/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(endpoint, headers=headers, params={"q": query})

    if response.status_code != 200:
        print(f"API Error ({filename}): {response.status_code}")
        print(response.text)
        return

    records = response.json().get("records", [])
    print(f"{len(records)} records fetched for {filename}")

    # Step 3: Save to CSV
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(fields)
        for rec in records:
            writer.writerow([rec.get(f, "") for f in fields])

    print(f"📄 Saved to {filename}\n")


# === Object-specific exports ===

export_to_csv(
    query="""
        SELECT Name, ExpressionSetId, ReferenceObjectId, ConstraintModelTag, ConstraintModelTagType
        FROM ExpressionSetConstraintObj
    """,
    filename="data/ExpressionSetConstraintObj.csv",
    fields=["Name", "ExpressionSetId", "ReferenceObjectId", "ConstraintModelTag", "ConstraintModelTagType"]
)

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
        SELECT Id, Name, ParentProductId, ChildProductId, ChildProductClassificationId, ProductRelationshipTypeId
        FROM ProductRelatedComponent
    """,
    filename="data/ProductRelatedComponent.csv",
    fields=[
        "Id",
        "Name",
        "ParentProductId",
        "ChildProductId",
        "ChildProductClassificationId",
        "ProductRelationshipTypeId"
    ]
)

# === Core Config Export ===

export_to_csv(
    query="""
        SELECT DeveloperName, ExecutionScale, Id, Language, MasterLabel, NamespacePrefix
        FROM ExpressionSetDefinition
    """,
    filename="data/ExpressionSetDefinition.csv",
    fields=["DeveloperName", "ExecutionScale", "Id", "Language", "MasterLabel", "NamespacePrefix"]
)

export_to_csv(
    query="""
        SELECT ConstraintModel, DeveloperName, ExpressionSetDefinitionId, Id, Language,
               MasterLabel, NamespacePrefix, Status, VersionNumber
        FROM ExpressionSetDefinitionVersion
    """,
    filename="data/ExpressionSetDefinitionVersion.csv",
    fields=[
        "ConstraintModel", "DeveloperName", "ExpressionSetDefinitionId", "Id", "Language",
        "MasterLabel", "NamespacePrefix", "Status", "VersionNumber"
    ]
)

export_to_csv(
    query="""
        SELECT ApiName, Description, ExecutionScale, ExpressionSetDefinitionId, Id,
               InterfaceSourceType, Name, ResourceInitializationType, UsageType
        FROM ExpressionSet
    """,
    filename="data/ExpressionSet.csv",
    fields=[
        "ApiName", "Description", "ExecutionScale", "ExpressionSetDefinitionId", "Id",
        "InterfaceSourceType", "Name", "ResourceInitializationType", "UsageType"
    ]
)

export_to_csv(
    query="""
        SELECT ApiName, DecimalScale, Description, EndDateTime, ExpressionSetDefinitionVerId,
               ExpressionSetId, Id, IsActive, IsDeleted, Name, Rank, StartDateTime, VersionNumber
        FROM ExpressionSetVersion
    """,
    filename="data/ExpressionSetVersion.csv",
    fields=[
        "ApiName", "DecimalScale", "Description", "EndDateTime", "ExpressionSetDefinitionVerId",
        "ExpressionSetId", "Id", "IsActive", "IsDeleted", "Name", "Rank", "StartDateTime", "VersionNumber"
    ]
)
