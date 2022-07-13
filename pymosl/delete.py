import pymosl.connect as pmc
import pymosl.datashare as pmd
import requests
import urllib.parse


# Get item id from item path
def get_item_id(
    folder_name, site_name=None, drive_name=None, site_id=None, 
    drive_id=None, item_path=None, config=None, headers=None
    ):
    if config is None:
        config = pmc.get_config()
    if headers is None:
        headers = pmc.get_graph_headers(config)
    if site_id is None:
        site_id = pmd.get_id_from_aztable(
            site_name=site_name, drive_name=drive_name, 
            return_field="SiteId", config=config
            )
    if drive_id is None:
        drive_id = pmd.get_id_from_aztable(
            site_name=site_name, drive_name=drive_name, 
            return_field="Id", config=config
            )
    if item_path is None:
        item_id = pmd.get_or_create_folder_id(
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
        site_id = pmd.get_id_from_aztable(
            site_name=site_name, drive_name=drive_name, 
            return_field="SiteId", config=config
            )
    if drive_id is None:
        drive_id = pmd.get_id_from_aztable(
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