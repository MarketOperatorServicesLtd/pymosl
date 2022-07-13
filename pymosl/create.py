import requests
import pandas as pd
import numpy as np
import pymosl.connect as pmc
from datetime import datetime
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
from azure.data.tables import UpdateMode


# Get all site ids:
def get_all_site_ids(config=None, headers=None, save_as_csv=False):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config=config)
    api_endpoint = config["GraphAPI"]["Endpoint"]
    try:
        print("Getting all site ids...")
        result = requests.get(f'{api_endpoint}sites?search=*', headers=headers)
        result.raise_for_status()
        root_info = result.json()
    except:
        print("Error getting all site ids")
    else:
        print("Obtained site ids, now processing dataframe...")
    site_ids_df = pd.json_normalize(root_info["value"])
    site_ids_df.columns = site_ids_df.columns.str.replace(pat = '(?<!^)(?=[A-Z])', repl = ' ', regex = True).str.title().str.replace(pat = '[\.," "]', repl = '', regex = True) # Converts columns to pascal case
    site_ids_df["CreatedDateTime"] = pd.to_datetime(site_ids_df["CreatedDateTime"], format = "%Y-%m-%dT%H:%M:%SZ").dt.strftime("%Y-%m-%d %H:%M:%S")
    #site_ids_df["CreatedDateTime"] = site_ids_df["CreatedDateTime"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    site_ids_df["LastModifiedDateTime"] = pd.to_datetime(
        site_ids_df["LastModifiedDateTime"]
        .replace("0001-01-01T08:00:00Z", "2015-01-01T08:00:00Z", inplace=False), # Replacing irregular date time values
        format = "%Y-%m-%dT%H:%M:%SZ"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")
    site_ids_df.replace(np.nan, None, inplace=True)
    site_ids_df["ReportDateTime"] = pd.to_datetime(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    site_ids_df.rename(columns={"CreatedDateTime": "SiteIdCreatedDateTime"}, inplace = True)
    site_ids_df["RowKey"] = site_ids_df["Name"].str.lower()
    print("Returned ", site_ids_df.shape[0], " site ids")
    if save_as_csv:
        site_ids_df.to_csv("site_ids.csv", index=False)
        print("Site ids dataframe saved as site_ids.csv")
    else:
        print("Site ids dataframe not saved")
    return site_ids_df


# Use site ids to gets all drive ids:
def get_all_drive_ids(config=None, headers=None, site_ids_df=None, save_as_csv=False):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config=config)
    api_endpoint = config["GraphAPI"]["Endpoint"]
    if site_ids_df is None:
        site_ids_df = get_all_site_ids(config=config, headers=headers, save_as_csv=save_as_csv)
    print("Getting all drive ids...this may take a couple of minutes...")
    drive_ids_df = []
    for index, row in site_ids_df.iterrows():
        try:
            print("Getting drive ids for site id: ", row["Id"])
            site_id = row["Id"]
            site_name = row["Name"]
            result = requests.get(f'{api_endpoint}/sites/{site_id}/drives', headers=headers)
            result.raise_for_status()
            drive_info = result.json()
            drive_info_df = pd.json_normalize(drive_info["value"])
            drive_info_df["SiteId"] = site_id
            drive_info_df["SiteName"] = site_name
            drive_ids_df.append(drive_info_df)
        except:
            print("Error getting drive ids for site id: ", row["Id"])
    print("Obtained drive ids, now processing dataframe...")
    drive_ids_df = pd.concat(drive_ids_df)
    drive_ids_df.columns = drive_ids_df.columns.str.replace(pat = "(?<!^)(?=[A-Z])", repl = " ", regex = True).str.title().str.replace(pat = "[\.,' ']", repl = "", regex = True) # Converts columns to pascal case
    drive_ids_df["CreatedDateTime"] = pd.to_datetime(drive_ids_df["CreatedDateTime"], format = "%Y-%m-%dT%H:%M:%SZ").dt.strftime("%Y-%m-%d %H:%M:%S")
    drive_ids_df["LastModifiedDateTime"] = pd.to_datetime(drive_ids_df["LastModifiedDateTime"], format = "%Y-%m-%dT%H:%M:%SZ").dt.strftime("%Y-%m-%d %H:%M:%S")
    drive_ids_df.replace(np.nan, None, inplace=True)
    drive_ids_df["ReportDateTime"] = pd.to_datetime(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    drive_ids_df.rename(columns={"CreatedDateTime": "DriveIdCreatedDateTime"}, inplace = True)
    drive_ids_df["RowKey"] = drive_ids_df["SiteName"].str.lower() + "-" + drive_ids_df["Name"].str.lower()
    print("Returned ", drive_ids_df.shape[0], " drive ids")
    if save_as_csv:
        site_ids_df.to_csv("drive_ids.csv", index=False)
        print("Drive ids dataframe saved as drive_ids.csv")
    else:
        print("Drive ids dataframe not saved as csv")
    return drive_ids_df


# INSERT site ids dataframe values into GRAPH_SiteIds:
def site_ids_df_to_sql(
    drop_table=True, insert_data=True, create_table=True,
    return_df=False, site_ids_df=None, conn=None, config=None, 
    headers=None
    ):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config=config)
    if conn is None:
        conn = pmc.get_synapse_connection(config=config, type="pyodbc")
    cursor = conn.cursor()    
    if drop_table:
        print("Dropping table...")
        sql = """
            IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dm].[GRAPH_SiteIds]') AND type in (N'U'))
            DROP TABLE [dm].[GRAPH_SiteIds]
            """
        cursor.execute(sql)
    if create_table:
        print("Creating table...")
        sql = """
            CREATE TABLE [dm].[GRAPH_SiteIds]
            (
                [ID] [int] IDENTITY(1,1) NOT NULL,
                ReportDateTime [datetime] NOT NULL,
                SiteIdCreatedDateTime [datetime] NULL,
                Description [varchar](250) NULL,
                SiteId [char](114) NOT NULL,
                LastModifiedDateTime [datetime] NULL,
                Name [varchar](100) NOT NULL,
                WebUrl [varchar](155) NULL,
                DisplayName [varchar](100) NOT NULL,
                SiteCollectionHostname [varchar](100) NULL
            )
            WITH
            (
                DISTRIBUTION = REPLICATE,
                CLUSTERED INDEX ([Id])
            )
        """
        cursor.execute(sql)
    if insert_data:
        if site_ids_df is None:
            print("Getting site ids data frame...")
            site_ids_df = get_all_site_ids(config=config, headers=headers, save_as_csv=False)
        print("Inserting values into table...")
        sql = """
            INSERT INTO dm.GRAPH_SiteIds (ReportDateTime, SiteIdCreatedDateTime, Description, SiteId, 
            LastModifiedDateTime, Name, WebUrl, DisplayName, SiteCollectionHostname
            ) 
            values(?,?,?,?,?,?,?,?,?)
            """
        for index, row in site_ids_df.iterrows():
            cursor.execute(
                sql,
                row.ReportDateTime, row.SiteIdCreatedDateTime, row.Description, row.Id, 
                row.LastModifiedDateTime, row.Name, row.WebUrl, row.DisplayName, 
                row.SiteCollectionHostname
                )
        cursor.close()
    print("Inserted ", site_ids_df.shape[0], " site ids into dm.GRAPH_SiteIds")
    if return_df:
        return site_ids_df
    else:
        return None


# INSERT drive ids dataframe values into GRAPH_DriveIds:
def drive_ids_df_to_sql(
    drop_table=True, insert_data=True, create_table=True,
    return_df=False, site_ids_df=None, drive_ids_df=None, 
    conn=None, config=None, headers=None
    ):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config=config)
    if conn is None:
        conn = pmc.get_synapse_connection(config=config, type="pyodbc")
    cursor = conn.cursor()    
    if drop_table:
        print("Dropping table...")
        sql = """
            IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dm].[GRAPH_DriveIds]') AND type in (N'U'))
            DROP TABLE [dm].[GRAPH_DriveIds]
            """
        cursor.execute(sql)
    if create_table:
        print("Creating table...")
        sql = """
            CREATE TABLE [dm].[GRAPH_DriveIds]
            (
                [ID] [int] IDENTITY(1,1) NOT NULL,
                ReportDateTime [datetime] NOT NULL,
                DriveIdCreatedDateTime [datetime] NULL,
                Description [varchar](250) NULL,
                DriveId [char](66) NOT NULL,
                LastModifiedDateTime [datetime] NULL,
                Name [varchar](100) NOT NULL,
                WebUrl [varchar](155) NULL,
                DriveType [varchar](100) NULL,
                CreatedByUserEmail [varchar](100) NULL,
                CreatedByUserId [char](36) NULL,
                CreatedByUserDisplayName [varchar](100) NULL,
                LastModifiedByUserDisplayName [varchar](100) NULL,
                OwnerUserEmail [varchar](100) NULL,
                OwnerUserId [char](36) NULL,
                OwnerUserDisplayName [varchar](100) NULL,
                LastModifiedByUserEmail [varchar](100) NULL,
                LastModifiedByUserId [char](36) NULL,
                SiteId [char](114) NOT NULL,
                SiteName [varchar](100) NULL,
                QuotaDeleted [bigint] NULL,
                QuotaRemaining [bigint] NULL,
                QuotaState [varchar](20) NULL,
                QuotaTotal [bigint] NULL,
                QuotaUsed [bigint] NULL,
                OwnerGroupId [char](36) NULL,
                OwnerGroupDisplayName [varchar](100) NULL,
                OwnerGroupEmail [varchar](100) NULL
            )
            WITH
            (
                DISTRIBUTION = REPLICATE,
                CLUSTERED INDEX ([Id])
            )
        """
        cursor.execute(sql)
    if insert_data:
        if drive_ids_df is None:
            print("Getting drive ids data frame...")
            drive_ids_df = get_all_drive_ids(site_ids_df=site_ids_df, config=config, headers=headers, save_as_csv=False)
        print("Inserting values into table...this may take a while...")
        cursor = conn.cursor()
        sql = """
                INSERT INTO dm.GRAPH_DriveIds (ReportDateTime, DriveIdCreatedDateTime, Description,
                DriveId, LastModifiedDateTime, Name, WebUrl, DriveType, CreatedByUserEmail, CreatedByUserId,
                CreatedByUserDisplayName, LastModifiedByUserDisplayName, OwnerUserEmail, OwnerUserId,
                OwnerUserDisplayName, LastModifiedByUserEmail, LastModifiedByUserId, SiteId, SiteName,
                QuotaDeleted, QuotaRemaining, QuotaState, QuotaTotal, QuotaUsed, OwnerGroupId, 
                OwnerGroupDisplayName, OwnerGroupEmail
                ) 
                values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
        for index, row in drive_ids_df.iterrows():
            cursor.execute(
                sql,
                row.ReportDateTime, row.DriveIdCreatedDateTime, row.Description, row.Id, row.LastModifiedDateTime, 
                row.Name, row.WebUrl, row.DriveType, row.CreatedByUserEmail, row.CreatedByUserId, row.CreatedByUserDisplayName,
                row.LastModifiedByUserDisplayName, row.OwnerUserEmail, row.OwnerUserId, row.OwnerUserDisplayName,
                row.LastModifiedByUserEmail, row.LastModifiedByUserId, row.SiteId, row.SiteName, row.QuotaDeleted,
                row.QuotaRemaining, row.QuotaState, row.QuotaTotal, row.QuotaUsed, row.OwnerGroupId, row.OwnerGroupDisplayName,
                row.OwnerGroupEmail
                )
        cursor.close()
    print("Inserted ", drive_ids_df.shape[0], " drive ids into dm.GRAPH_DriveIds")
    if return_df:
        return drive_ids_df
    else:
        return None


# Upsert to Azure Table service and optionally create necessary tables
def df_to_azure_table(df, table_name, create_table=True, upload_data=True, config=None):
    if config is None:
        config = pmc.get_config()
    storage_account_name = config["Storage"]["AccountName"]
    storage_access_key = config["Storage"]["AccessKey"]
    storage_endpoint = config["Storage"]["Endpoint"]
    credential = AzureNamedKeyCredential(storage_account_name, storage_access_key)
    table_service_client = TableServiceClient(endpoint=storage_endpoint, credential=credential)
    # Create table if it doesn't exist
    if create_table:
        try:
            print("Trying to create table if it does not already exist...")
            table_service_client.create_table(table_name=table_name)
        except:
            print("Table already exists:", table_name)
        else:
            print("Created table:", table_name)
    if upload_data:
        print("Uploading data to table...")
        table_client = table_service_client.get_table_client(table_name=table_name)
        df_dict = df.to_dict("records")
        for row in df_dict:
            row["PartitionKey"] = "1"
            try:
                table_client.upsert_entity(entity=row, mode=UpdateMode.REPLACE)
                print("Inserted entity: ", row["RowKey"])
            except:
                print("Error creating entity: ", row["RowKey"])
        print("Finished uploading data to ", table_name)
    return None