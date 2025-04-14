### 🔄 Constraint Expression Set Migration Tool

This tool exports and imports **Constraint ExpressionSet metadata**, including blob files (CML), from one Salesforce org to another using Salesforce CLI + REST API. It support migration of a new CML as well as update of the existing one, but it assumes target org already has relevant Context Definition and PCM data like Attributes, Product Classifications, Products and Product Related Components. 

---

#### 🔐 Authenticate Orgs

```bash
sf auth:web:login --instance-url https://<source-instance>.salesforce.com -a srcOrg
sf auth:web:login --instance-url https://<target-instance>.salesforce.com -a tgtOrg
```

---

#### 📤 Export from Source Org

```bash
python export_cml.py --developerName Laptop_Pro_Bundle
```

Exports CSVs and blob files into the `data/` folder.

---

#### 📥 Import into Target Org

```bash
python import_cml.py
```

Loads metadata, resolves references, and uploads blob to the target org.

---

#### 📁 Output Structure

- `data/ExpressionSet.csv`
- `data/ExpressionSetDefinitionVersion.csv`
- `data/ExpressionSetDefinitionContextDefinition.csv`
- `data/ExpressionSetConstraintObj.csv`
- `data/Product2.csv`
- `data/ProductClassification.csv`
- `data/ProductRelatedComponent.csv`
- `data/blobs/*.ffxblob`

---

#### 🔧 Requirements

- Python 3.9+
- Salesforce CLI (`sf`)
- Connected orgs with accessible metadata API
