import pymosl.connect as pmc
from azure.storage.blob import BlobServiceClient
import os


def upload_to_blob(
    config=None, blob_service_client=None, storage_name=None, blob_path="", 
    blob_prefix="", file_list=None, file_dir=None, 
    included_extensions=[".csv", ".xlsx"]
    ):
    if config is None:
        config = pmc.get_config()
    if blob_service_client is None:
        endpoint = config[storage_name]["EndPoint"]
        container_name = config[storage_name]["Container"]
        sas_token = config[storage_name]["SAS"]
        blob_service_client = BlobServiceClient(account_url=endpoint, credential=sas_token)
    if file_list is None:
        if file_dir is None:
            file_dir = "."
        file_list = [file for file in os.listdir(file_dir) if file.endswith(tuple(included_extensions))]
    print("Found {} files to upload.".format(len(file_list)))
    for file in file_list:
        print("Uploading file: {}".format(file))
        blob = blob_path + blob_prefix + file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob)
        with open(file, "rb") as data:
            blob_client.upload_blob(data)


