import json
from sqlalchemy import  create_engine, event
from sqlalchemy.engine import URL
import pyodbc
import msal
import atexit
import os


# Get config file utility function
def get_config(config_name=None):
    if config_name is None:
        config_name = "local.config.json"
    with open(config_name) as read_file:
        config = json.load(read_file)
    return config


# Connection details for synapse database
def get_synapse_connection(config=None, type="pyodbc"):
    if config is None:
        config = get_config()
    server = config["Synapse"]["Server"]
    database = config["Synapse"]["Database"]
    username = config["Synapse"]["Username"]
    authentication = config["Synapse"]["Authentication"]
    driver = config["Synapse"]["Driver"]
    connection_string = "DRIVER={};SERVER=tcp:{};PORT=1433;DATABASE={};UID={};AUTHENTICATION={};".format(driver, server, database, username, authentication)
    if type == "sa":
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        conn = create_engine(connection_url)
    elif type == "pyodbc":
        conn = pyodbc.connect(connection_string)
    return conn


# Obtain access token for Graph API:
def get_graph_headers(config=None):
    if config is None:
        config = get_config()
    # Graph API connection details
    TENANT_ID = config["GraphAPI"]["TenantId"]
    CLIENT_ID = config["GraphAPI"]["ClientId"]
    AUTHORITY = 'https://login.microsoftonline.com/' + TENANT_ID
    SCOPES = [
        'Files.ReadWrite.All',
        'Sites.ReadWrite.All',
        'User.Read',
        'User.ReadBasic.All'
        ]
    cache = msal.SerializableTokenCache()
    if os.path.exists('token_cache.bin'):
        cache.deserialize(open('token_cache.bin', 'r').read())
    atexit.register(lambda: open('token_cache.bin', 'w').write(cache.serialize()) if cache.has_state_changed else None)
    app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)
    accounts = app.get_accounts()
    result = None
    if len(accounts) > 0:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
    if result is None:
        flow = app.initiate_device_flow(scopes=SCOPES)
        if 'user_code' not in flow:
            raise Exception('Failed to create device flow')
        print(flow['message'])
        result = app.acquire_token_by_device_flow(flow)
    if 'access_token' in result:
        headers={'Authorization': 'Bearer ' + result['access_token']}
    return headers