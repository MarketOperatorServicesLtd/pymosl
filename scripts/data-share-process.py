
import pymosl.connect as pmc
import pymosl.datashare as pmd

config = pmc.get_config()
headers = pmc.get_graph_headers()
conn = pmc.get_synapse_connection()

log_book = pmd.sp_data_upload_all(config=config, headers=headers, conn=conn)