Auth commands:

sf auth:web:login --instance-url https://orgfarm-bed9164559.test1.my.pc-rnd.salesforce.com -a srcOrg
sf auth:web:login --instance-url https://orgfarm-06c7bc6b75.test1.my.pc-rnd.salesforce.com -a tgtOrg


Execution commands:

python3 export_cml.py --developerName Laptop_Pro_Bundle

python3 import_cml.py
