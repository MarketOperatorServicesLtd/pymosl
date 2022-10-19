
import pymosl.share as pms
import pymosl.connect as pmc
import pandas as pd

config = pmc.get_config()
headers=pmc.get_graph_headers(config)
conn = pmc.get_synapse_connection(config)
tp_list = pms.get_tp_list(config=config, conn=conn)

tp_drive = "Market Performance"
tp_folder = "Data Assurance"
tp_new_folder = "Rejections"

#create empty list for log books
log_books = []

# loop through tp_list
for tp in tp_list:
    log_book_temp = pd.DataFrame(
        columns=['tp_name', 'tp_drive', 'tp_folder', 'tp_new_folder', 'folder_id', 'status']
        )
    tp_site = tp
    try:
        folder_id = pms.get_or_create_folder_id(
            folder_path=tp_folder, folder_name=tp_new_folder,
            headers=headers, config=config, site_name=tp_site,
            drive_name=tp_drive
            )
        status = "ok"
    except:
        folder_id = None
        status = "Error: Could not create folder"
    finally:
        log_book_temp = log_book_temp.append(
            {'tp_name': tp, 'tp_drive': tp_drive,
             'tp_folder': tp_folder, 'tp_new_folder': tp_new_folder,
                'folder_id': folder_id, 'status': status
                }, ignore_index=True
            )
        log_books.append(log_book_temp)
    print(tp, folder_id, status)

log_book = pd.concat(log_books, ignore_index=True)
log_book.head()