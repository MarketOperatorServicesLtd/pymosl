import requests
import os.path
import urllib.parse
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
import re
import json
import os
import pymosl.connect as pmc
import pymosl.create as pmcr
import pymosl.blob as pmb


def get_id_from_aztable(site_name, drive_name, return_field="Id", config=None):
    """
    Get Site or Drive Id from Azure Table Storage using spDriveIds Azure table
    """
    if config is None:
        config = pmc.get_config()
    # Access config for Azure Table Storage
    storage_account_name = config["Storage"]["AccountName"]
    storage_access_key = config["Storage"]["AccessKey"]
    storage_endpoint = config["Storage"]["Endpoint"]
    credential = AzureNamedKeyCredential(storage_account_name, storage_access_key)
    table_service_client = TableServiceClient(endpoint=storage_endpoint, credential=credential)
    table_client = table_service_client.get_table_client(table_name="spDriveIds")
    try:
        row_key = site_name.lower() + "-" + drive_name.lower()
        return_value = table_client.get_entity(partition_key="1", row_key = row_key)[return_field]
    except:
        print("Drive Id not found in Azure Table Storage for {}".format(row_key))
    return return_value


def get_or_create_folder_id(
    folder_path, folder_name, headers=None, drive_id=None, config=None, 
    site_name=None, drive_name=None
    ):
    """
    Upload some files to a Sharepoint folder:
    Checks if folder exists and if not Creates folder and otherwise proceed to get folder Id
    """
    if config is None:
        config = pmc.get_config()
    if drive_id is None:
        drive_id = get_id_from_aztable(
            site_name=site_name, drive_name=drive_name, 
            config=config
            )
    if headers is None:
        headers = pmc.get_graph_headers(config)
    graph_endpoint = config["GraphAPI"]["Endpoint"]
    if folder_path is None:
        folder_url = urllib.parse.quote(folder_name)
    else:
        folder_url = urllib.parse.quote(folder_path + "/" + folder_name)
    result = requests.get(f'{graph_endpoint}/drives/{drive_id}/root:/{folder_url}', headers=headers)
    if result.status_code == 200:
        result.raise_for_status()
        folder_info = result.json()
        folder_id = folder_info['id']
        print(f"Folder {folder_name} FOUND with Id {folder_id}")
    elif folder_path is None:
        site_id = get_id_from_aztable(site_name=site_name, drive_name=drive_name, return_field="SiteId", config=config)
        result = requests.post(
            f'{graph_endpoint}/sites/{site_id}/drives/{drive_id}/root/children', 
            headers=headers,
            json={
                "name": folder_name,
                "folder": { },
                "@microsoft.graph.conflictBehavior": "fail"
                }
            )
        result.raise_for_status()
        folder_info = result.json()
        folder_id = folder_info['id']
        print(f"Folder {folder_name} CREATED with Id {folder_id}")
    else:
        result = requests.post(
            f'{graph_endpoint}/drives/{drive_id}/root:/{folder_path}:/children', 
            headers=headers,
            json={
                "name": folder_name,
                "folder": { },
                "@microsoft.graph.conflictBehavior": "fail"
                }
            )
        result.raise_for_status()
        folder_info = result.json()
        folder_id = folder_info['id']
        print(f"Folder {folder_name} CREATED with Id {folder_id}")
    return folder_id


def upload_file_to_sp(
    filename, site_name, drive_name, folder_path, 
    folder_name, config=None, headers=None
    ):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config)
    graph_endpoint = config["GraphAPI"]["Endpoint"]
    drive_id = get_id_from_aztable(site_name=site_name, drive_name=drive_name, config=config)
    folder_id = get_or_create_folder_id(
        folder_path=folder_path, folder_name=folder_name, headers=headers, 
        drive_id=drive_id, config=config, site_name=site_name, drive_name=drive_name
        )
    file_url = urllib.parse.quote(filename)
    result = requests.post(
        f'{graph_endpoint}/drives/{drive_id}/items/{folder_id}:/{file_url}:/createUploadSession',
        headers=headers,
        json={
            '@microsoft.graph.conflictBehavior': 'replace',
            'description': "Uploaded using Python script and Graph API",
            'fileSystemInfo': {'@odata.type': 'microsoft.graph.fileSystemInfo'},
            'name': filename
            }
        )
    result.raise_for_status()
    upload_session = result.json()
    upload_url = upload_session['uploadUrl']
    st = os.stat(filename)
    size = st.st_size
    CHUNK_SIZE = 10485760
    chunks = int(size / CHUNK_SIZE) + 1 if size % CHUNK_SIZE > 0 else 0
    with open(filename, 'rb') as fd:
        start = 0
        for chunk_num in range(chunks):
            chunk = fd.read(CHUNK_SIZE)
            bytes_read = len(chunk)
            upload_range = f'bytes {start}-{start + bytes_read - 1}/{size}'
            print(f'chunk: {chunk_num} bytes read: {bytes_read} upload range: {upload_range}')
            result = requests.put(
                upload_url,
                headers={
                    'Content-Length': str(bytes_read),
                    'Content-Range': upload_range
                },
                data=chunk
            )
            result.raise_for_status()
            start += bytes_read


def get_tp_list(source_tables=None, ref_columns=None, config=None, conn=None):
    if config is None:
        config = pmc.get_config()
    if conn is None:
        conn = pmc.get_synapse_connection(config)
    if source_tables is None:
        source_tables = ("[dm].[TP_MeterData_MonthEnd]", "[dm].[TP_PremisesData_MonthEnd]", "[dm].[TP_VacancyData_MonthEnd]")
    if ref_columns is None:
        ref_columns = ("RetailerId", "WholesalerId")
    tp_list = []
    for source in source_tables:
        for col in ref_columns:
            sql_query = "SELECT DISTINCT ([{}]) FROM {}".format(col, source)
            tp_list_temp = pd.read_sql(sql_query, con=conn)[col].tolist()
            for tp in tp_list_temp:
                tp_list.append(tp)
    tp_list = np.unique(tp_list)
    return tp_list


def sp_data_upload_all(
    drive_name="Market Performance", folder_path="Data Share", config=None, 
    source_tables=None, ref_columns=None, data_date=None, folder_name=None, 
    tp_list=None, headers=None, conn=None, save_log=True, delete_temp = True
    ):
    overall_start = datetime.now()
    if config is None:
        config = pmc.get_config()
    if data_date is None:
        data_date = date(date.today().year, date.today().month, 1).strftime("%Y-%m-%d")
    if folder_name is None:
        folder_name = data_date
    if headers is None:
        headers = pmc.get_graph_headers(config)
    if conn is None:
        conn = pmc.get_synapse_connection(config)
    log_book = pd.DataFrame(
        columns=[
            "Table", "DataType", "TradingParty", "SQLquery", 
            "RowCount", "StartTime", "EndTime", "Message", "Filename", 
            "Status", "FilePath"
            ]
        )
    if source_tables is None:
        source_tables = ("[dm].[TP_MeterData_MonthEnd]", "[dm].[TP_PremisesData_MonthEnd]", "[dm].[TP_VacancyData_MonthEnd]")
    if ref_columns is None:
        ref_columns = ("RetailerId", "WholesalerId")
    for source in source_tables:
        log_book_temp = pd.DataFrame()
        data_type = re.search("._(.*)_.", source).group(1)
        if tp_list is None:
            tp_list = get_tp_list(
                config=config, conn=conn, source_tables=source_tables, 
                ref_columns=ref_columns
                )
        for col in ref_columns:
            for tp in tp_list:
                log_book_temp["Table"] = [source]
                log_book_temp["DataType"] = [data_type]
                log_book_temp["TradingParty"] = [tp]
                log_book_temp["StartTime"] = [datetime.now()]
                try:
                    sql_query = "SELECT * FROM {} WHERE {} = ?;".format(source, col)
                    log_book_temp["SQLquery"] = [sql_query]
                    df = pd.read_sql(sql_query, conn, params=[tp])
                    df_row_count = len(df)
                    log_book_temp["RowCount"] = [df_row_count]
                    filename = "{}_{}_{}.csv".format(tp, data_date, data_type)
                    log_book_temp["Filename"] = [filename]
                    if df_row_count > 0:
                        df.to_csv(filename, index=False)
                        upload_file_to_sp(
                            filename=filename, site_name=tp, drive_name=drive_name, 
                            folder_path=folder_path, folder_name=folder_name, headers=headers
                            )
                    else:
                        message = "No data for {} in column {} for table {}".format(tp, col, source)
                        print(message)
                        log_book_temp["Message"] = [message]
                except:
                    message = "Error uploading to SharePoint: {}".format(filename)
                    print(message)
                    log_book_temp["Status"] = ["Error"]
                    log_book_temp["FilePath"] = ["{}/{}/{}/{}".format(
                        drive_name, folder_path, folder_name, filename)
                        ]
                else:
                    print("Successfully uploaded to SharePoint: {}".format(filename))
                    log_book_temp["Status"] = ["Success"]
                    log_book_temp["FilePath"] = ["{}/{}/{}/{}".format(
                        drive_name, folder_path, folder_name, filename)
                        ]
                finally:
                    if os.path.exists(filename) and delete_temp:
                        os.remove(filename)
                        print("Removed file: {}".format(filename))
                    log_book_temp["EndTime"] = [datetime.now()]
                    log_book = log_book.append(log_book_temp, ignore_index=True)
                    print("Moving onto next trading party...")
                if save_log:
                    log_book.to_csv("log_book_{}.csv".format(data_date), index=False)
    overall_finish = datetime.now()
    print("Total process time: {}".format(overall_finish - overall_start))
    return log_book


def sp_update_file(
    drive_id, item_id, new_folder_id, new_name, 
    config=None, headers=None
    ):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config)
    graph_endpoint = config["GraphAPI"]["Endpoint"]
    url = f"{graph_endpoint}/drives/{drive_id}/items/{item_id}"
    data = {
        "name": new_name,
        "parentReference": {
            "id": new_folder_id
            }
        }
    response = requests.put(url, headers=headers, json=data)
    response.raise_for_status()
    return response


def sp_file_download(
    site_name, drive_name, folder_path, folder_name, new_folder, filename_prefix="",
    new_filename_prefix="", download=True, move_file=True, file_name=None, 
    config=None, headers=None
    ):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config)
    graph_endpoint = config["GraphAPI"]["Endpoint"]
    drive_id = get_id_from_aztable(site_name=site_name, drive_name=drive_name, config=config)
    folder_id = get_or_create_folder_id(
        folder_path=folder_path, folder_name=folder_name, headers=headers, 
        drive_id=drive_id, config=config, site_name=site_name, drive_name=drive_name
        )
    result = requests.get(f"{graph_endpoint}/drives/{drive_id}/items/{folder_id}/children", headers=headers)
    result.raise_for_status()
    file_names = []
    children = result.json()["value"]
    file_count = len(children)
    if file_count > 0:
        print("{} files found...".format(file_count))
        for item in children:
            file_name = item["name"]
            item_id = item["id"]
            if download:
                #filename_prefix = site_name + "-" + drive_name + "-" + folder_name + "__"
                amended_file_name = filename_prefix + file_name
                download_url = item["@microsoft.graph.downloadUrl"]
                response = requests.get(download_url)
                response.raise_for_status()
                with open(amended_file_name, "wb") as f:
                    f.write(response.content)
                print("Downloaded file: {}".format(amended_file_name))
            if move_file:
                new_folder_id = get_or_create_folder_id(
                    folder_path=folder_path, folder_name=new_folder, headers=headers,
                    drive_id=drive_id, config=config
                    )
                new_name = new_filename_prefix + file_name
                sp_update_file(
                    drive_id=drive_id, item_id=item_id, 
                    new_folder_id=new_folder_id, new_name=new_name, 
                    config=config, headers=headers
                    )
            file_names.append(file_name)
    else:
        print("No files found...")
    return file_names


def data_assurance_process(
    config=None, headers=None, drive_name="Market Performance", folder_path="Data Assurance", 
    folder_name="New", new_folder_name="Processed", storage_name="data-assurance", tp_list=None,
    new_file_prefix="MOSL-PROCESSED__", move_file=True, download=True, save_log=True,
    blob_path="data/raw/", included_extensions=[".csv", ".xlsx"], run_date=None
    ):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config)
    if tp_list is None:
        site_ids = pmcr.get_all_site_ids(config=config, headers=headers) # Get list of trading parties
        tp_list = site_ids["RowKey"].values 
    log_book = pd.DataFrame(
        columns=[
            "SiteName", "DriveName", "FileCount", "FolderPath", "FileNames", "Moved",
            "Downloaded", "StartTime", "EndTime", "Status", "FilePath"
            ]
        )
    overall_start = datetime.now()
    if run_date is None:
        run_date = date(date.today().year, date.today().month, 1).strftime("%Y-%m-%d")
    for site_name in tp_list:
        log_book_temp = pd.DataFrame()
        print("trying: " + site_name)
        log_book_temp["SiteName"] = [site_name]
        log_book_temp["DriveName"] = [drive_name]
        log_book_temp["FolderPath"] = [folder_path]
        log_book_temp["FolderName"] = [folder_name]
        log_book_temp["StartTime"] = [datetime.now()]
        try:
            file_names = sp_file_download(
            config=config, headers=headers, site_name=site_name, drive_name=drive_name,
            folder_path=folder_path, folder_name=folder_name, new_folder=new_folder_name,
            new_filename_prefix=new_file_prefix, move_file=move_file, download=download
            )
            log_book_temp["FileNames"] = [file_names]
            for name in file_names:
                print("downloaded " + name)
            prefix = site_name + "_" + folder_path + "_" + folder_name + "__" 
            pmb.upload_to_blob(
                config=config, blob_service_client=None, storage_name=storage_name, blob_path=blob_path, 
                blob_prefix=prefix, file_list=None, file_dir=None, included_extensions=included_extensions
                )
            log_book_temp["Status"] = ["Success"]
            log_book_temp["Downloaded"] = [download]
            log_book_temp["Moved"] = [move_file]
            log_book_temp["FileCount"] = [len(file_names)]
        except:
            log_book_temp["Status"] = ["Failed"]
            log_book_temp["Downloaded"] = [False]
            log_book_temp["Moved"] = [False]
            log_book_temp["FileCount"] = [None]
            log_book_temp["FileNames"] = [None]
            print(f"No {folder_path} folder or files to download for {site_name}")
        finally:
            log_book_temp["EndTime"] = [datetime.now()]
            log_book = log_book.append(log_book_temp, ignore_index=True)
    if save_log:
        log_book.to_csv("log_book_{}.csv".format(run_date), index=False)
    overall_finish = datetime.now()
    print("Total process time: {}".format(overall_finish - overall_start))
    return log_book


def get_item_id(
    folder_name, site_name=None, drive_name=None, site_id=None, 
    drive_id=None, item_path=None, config=None, headers=None
    ):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config)
    if site_id is None:
        site_id = get_id_from_aztable(
            site_name=site_name, drive_name=drive_name, 
            return_field="SiteId", config=config
            )
    if drive_id is None:
        drive_id = get_id_from_aztable(
            site_name=site_name, drive_name=drive_name, 
            return_field="Id", config=config
            )
    if item_path is None:
        item_id = get_or_create_folder_id(
            folder_path=None, folder_name=folder_name, drive_id=drive_id, 
            headers=headers, config=config
            )
    else:
        item_path = urllib.parse.quote(item_path)
        graph_endpoint = config["GraphAPI"]["Endpoint"]
        result = requests.get(f'{graph_endpoint}/drives/{drive_id}/items/root:/{item_path}', headers=headers)
        result.raise_for_status()
        item_info = result.json()
        item_id = item_info['id']
    return item_id


# Delete a file or folder
def delete_item_or_folder(
    site_name=None, drive_name=None, site_id=None, drive_id=None, item_id=None, 
    item_path=None, folder_name=None, headers=None, config=None
    ):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config)
    if site_id is None:
        site_id = get_id_from_aztable(
            site_name=site_name, drive_name=drive_name, 
            return_field="SiteId", config=config
            )
    if drive_id is None:
        drive_id = get_id_from_aztable(
            site_name=site_name, drive_name=drive_name, 
            return_field="Id", config=config
            )
    graph_endpoint = config["GraphAPI"]["Endpoint"]
    if item_id is None:
        item_id = get_item_id(
            site_name=site_name, drive_name=drive_name, site_id=site_id, 
            drive_id=drive_id, item_path=item_path, folder_name=folder_name, 
            config=config, headers=headers
            )
    result = requests.delete(f'{graph_endpoint}/drives/{drive_id}/items/{item_id}', headers=headers)
    result.raise_for_status()
    if result.status_code == 204:
        print(f"Successfully deleted {item_id}")
    else:
        print(f"Failed to delete {item_id}")
    return None