
import pymosl.connect as pmc
import pymosl.share as pms

# import os
# wrkdir = ""
# os.chdir(wrkdir)

config = pmc.get_config()
headers = pmc.get_graph_headers()
log_book = pms.data_assurance_process(config=config, headers=headers)

# drive_name = "Data"
# folder_path = "Test"
# folder_name = "Rico-Test"
# new_folder_name = "TEST-TEST"
# storage_name = "data-assurance-test"
# tp_list = ["MOSL-TEST-TP"]

# log_book = pms.data_assurance_process(
#     config=config, headers=headers,
#     drive_name=drive_name, folder_path=folder_path,
#     folder_name=folder_name, new_folder_name=new_folder_name,
#     storage_name=storage_name, tp_list=tp_list
#     )

# log_book.head()

# for filename in log_book["FileNames"]:
#     print(filename)