import pymosl.connect as pmco
import pymosl.create as pmcr
import pymosl.datashare as pmds

if __name__ == "__main__":
    config = pmco.get_config()
    headers = pmco.get_headers()
    conn = pmco.get_conn(config)

    site_ids_df = pmds.get_all_site_ids(config=config, headers=headers, save_as_csv=False)
    drive_ids_df = pmds.get_all_drive_ids(site_ids_df=site_ids_df, config=config, headers=headers, save_as_csv=False)

    pmcr.site_ids_df_to_sql(site_ids_df=site_ids_df, config=config, headers=headers, conn=conn)
    pmcr.drive_ids_df_to_sql(drive_ids_df=drive_ids_df, config=config, headers=headers, conn=conn)

    pmcr.df_to_azure_table(df=site_ids_df, table_name="spSiteIds", create_table=True, upload_data=True, config=config)
    pmcr.df_to_azure_table(df=drive_ids_df, table_name="spDriveIds", create_table=True, upload_data=True, config=config)
