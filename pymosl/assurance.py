# Get TP Site ID
# Get TP Drive ID
# Check for Data Assurance folder on TP site and drive
# Check for new templates in Data Assurance folder
# Get Item Id(s) for new templates
# Download template file from Data Assurance New folder in Trading Party Sharepoint site
# Copy raw templates to Blob Raw container
# Validate templates, add validation columns and copy to Blob New container
# Move template from Data Assurance New to Processed and rename with MOSL-Reviewed

import pymosl.connect as pmc
import pymosl.share as pms
import requests
import pandas as pd

config = pmc.get_config()
headers = pmc.get_graph_headers()
graph_endpoint = config["GraphAPI"]["Endpoint"]

# What is the right table to get all relevant wholesaler ids from?

site_name = "MOSL-TEST-TP"
drive_name = "Data"
folder_path = "Data Share"
folder_name = "TEST-FOLDER-CREATE"

drive_id = pms.get_id_from_aztable(config=config, site_name=site_name, drive_name=drive_name)

folder_id = pms.get_or_create_folder_id(
    folder_path=folder_path, folder_name = folder_name,
    config=config, headers=headers, drive_id=drive_id
    )

result = requests.get(f"{graph_endpoint}/drives/{drive_id}/items/{folder_id}/children", headers=headers)
result.raise_for_status()
children = result.json()["value"]
for item in children:
    print(item.keys())


file_name = children[0]["name"]
download_url = children[0]["@microsoft.graph.downloadUrl"]
r = requests.get(download_url)
r.raise_for_status()

updated_name = "MOSL-processed_" + file_name

item_id = children[0]["id"]

new_folder_id = pms.get_or_create_folder_id(
    folder_path=folder_path, folder_name = "TEST-TEST",
    config=config, headers=headers, drive_id=drive_id
    )

result = requests.put(
    f'{graph_endpoint}/drives/{drive_id}/items/{item_id}',
    headers=headers,
    json={
        "name": updated_name,
        "parentReference": {
            "id": new_folder_id
        }
    }
)
result.raise_for_status()
result.json()


# list headers of r
r.headers.keys()
r.headers["Content-Disposition"]
r.headers

file_names = []
for item in children:
    file_name = item["name"]
    file_names.append(file_name)
    download_url = item["@microsoft.graph.downloadUrl"]
    r = requests.get(download_url)
    r.raise_for_status()
    with open(file_name, "wb") as f:
        f.write(r.content)
    print("Downloaded file: {}".format(file_name))
for file_name in file_names:
    print(file_name)


# convert r content csv to pandas dataframe
df = pd.read_csv(download_url)

df.head()

config = pmc.get_config()
from azure.storage.blob import BlobServiceClient
import os

connection_string = config["data-assurance-test"]["ConnectionString"]
endpoint = config["data-assurance-test"]["EndPoint"]
container_name = config["data-assurance-test"]["Container"]
sas_token = config["data-assurance-test"]["SAS"]

blob_service_client = BlobServiceClient(account_url = endpoint, credential = sas_token)
container_client = blob_service_client.get_container_client(container_name)

# Get all blobs in container
blob_list = container_client.list_blobs()
for blob in blob_list:
    print(blob.name)

# get list of csv files in current directory
for file in os.listdir("."):
    if file.endswith(".csv"):
        print(file)

# upload csv files in current directory to blob container
for file in os.listdir("."):
    if file.endswith(".csv"):
        print(file)
        blob_client = blob_service_client.get_blob_client(
            container = container_name, blob = "data/raw/" + file
        )
        with open(file, "rb") as data:
            blob_client.upload_blob(data)

blob_list = container_client.list_blobs(name_starts_with = "raw/")
for blob in blob_list:
    print("\t" + blob.name)
    container_client.delete_blob(blob.name)