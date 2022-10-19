
import pymosl.connect as pmc
import pymosl.share as pms

# CREATE CONFIGURATION OBJECTS

config = pmc.get_config()
headers = pmc.get_graph_headers()


# INITIAL SCREENING WITHOUT DOWNLOADS OR UPLOADS

log_book = pms.data_assurance_process(
    config=config, headers=headers, move_file=False, save_log=False,
    download=False
    )

# quick check log book
log_book.head()
# count rows with Status is Failed in log_book
log_book[log_book['Status'] == 'Failed'].shape[0]
# quick check of non-failed 
log_book[log_book["Status"] != "Failed"].head()
# get sum of FileCount
log_book["FileCount"].sum()
# get list of filenames
for filename in log_book["FileNames"]:
    if filename is not None and filename != []:
        print(filename)
# get list of trading parties for full process
tp_list = log_book[log_book["FileCount"] > 0]["SiteName"]


# IF EVERYTHING OK THEN RUN FULL PROCESS

log_book = pms.data_assurance_process(config=config, headers=headers, tp_list=tp_list)