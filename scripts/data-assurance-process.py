
import pymosl.connect as pmc
import pymosl.share as pms
import pymosl.blob as pmb
import pymosl.create as pmcr
import os

wrkdir = ""
os.chdir(wrkdir)

config = pmc.get_config()
headers = pmc.get_graph_headers()
conn = pmc.get_synapse_connection(config=config)

site_ids = pmcr.get_all_site_ids(config=config, headers=headers)

drive_name = "Market Performance"
folder_path = "Data Assurance"
folder_name = "New"
new_folder_name = "Processed"
storage_name = "data-assurance"

# Get list of trading parties
tp_list = site_ids["RowKey"].values

for site_name in tp_list:
    print("trying: " + site_name)
    try:
        file_names = pms.sp_file_download(
        config=config, headers=headers, site_name=site_name, drive_name=drive_name,
        folder_path=folder_path, folder_name=folder_name, new_folder=new_folder_name,
        new_filename_prefix = "MOSL-PROCESSED__", move_file=True
        )
        for name in file_names:
            print("downloaded " + name)
        prefix = site_name + "_" + folder_path + "_" + folder_name + "__" 
        pmb.upload_to_blob(
            config=config, blob_service_client=None, storage_name=storage_name, blob_path="data/raw/", 
            blob_prefix=prefix, blob_suffix="", file_list=None, 
            file_dir=None, included_extensions=[".csv", ".xlsx"]
            )
    except:
        print(f"No {folder_path} folder or files to download for {site_name}")