'''
Author : Rhuta Joshi (RJoshi5@slb.com)
------------------------------------------------------------------------------------------------------------------------
Title            : s2h_config_tablewise_generator.py
Description      : This utility is developed to generate configurations table-wise from DR, required for S2H using NiFi.
Pre-requisites   :  1. DR name,format and path should be valid and existing
                    2. Output path entered should be valid. If path does not exist it will be created.
                    3. "Table/View/Collection" column in Collections tab of DR should be sorted
                    4. "Table/View/Collection" column in Attributes tab of DR should be sorted
Python Version   : 3.8
------------------------------------------------------------------------------------------------------------------------
Date                    Change Name             User              Description
May 08, 2020            Initial Draft           rjoshi5           Created initial version
------------------------------------------------------------------------------------------------------------------------
'''

import xlrd
import os

def get_unique(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

dr_location = input("Enter full path of DR with filename and extension : ").replace('\\','/')

work_book_xlsx = xlrd.open_workbook(dr_location)
collections_sheet = work_book_xlsx.sheet_by_name('Collections')
attributes_sheet = work_book_xlsx.sheet_by_name('Attributes')

config_system_name = collections_sheet.cell_value(3, 0).lower().strip()
config_system_name = config_system_name.replace(" ", "-")
config_schedule_group = "DAILY-FULL-LOAD"
config_ssr_id = collections_sheet.cell_value(3, 1).strip()
config_src_db_type = collections_sheet.cell_value(3, 8).upper().strip()

tbl_names_list = collections_sheet.col_values(2)[3:]
tbl_names_list = [i for i in tbl_names_list if i]   #cleaning column list off blank spaces

db_name_list = collections_sheet.col_values(9)[3:]
db_name_dict = {}
schema_name_list = collections_sheet.col_values(10)[3:]
schema_name_dict = {}
for j in range(len(tbl_names_list)):
    db_name_dict[tbl_names_list[j]] = db_name_dict.get(tbl_names_list[j], []) + [db_name_list[j]]
    schema_name_dict[tbl_names_list[j]] = schema_name_list[j]
config_src_db_name = ""
config_src_schema_name = ""
config_tbl_name_override = ""
config_vol_category = "LOW"                    #hardcoded
config_src_access_type = "PRIVATE"                   

tbl_names_list = get_unique(tbl_names_list)
print("TableNames: " + str(tbl_names_list))

print("Table_count: "+str(len(tbl_names_list)))

json_save = input("Enter path to save configurations : ").replace('\\','/')
json_save = os.path.join(json_save, config_ssr_id)
config_column_list = ""
itr = 3
for config_tbl_name in tbl_names_list:
    for db_type in db_name_dict[config_tbl_name]:
        config_column_list = ""
        try:
            while (str(attributes_sheet.cell_value(itr, 0)).strip() == str(config_tbl_name).strip()) and \
                    (str(attributes_sheet.cell_value(itr, 19)).strip() == str(db_type).strip()):
                config_column_list += str(attributes_sheet.cell_value(itr, 1)) + ","
                itr += 1
        except IndexError:
            pass
        config_src_db_name = db_type
        config_src_schema_name = schema_name_dict[config_tbl_name]
        if len(db_name_dict[config_tbl_name]) == 1:
            config_tbl_name_override = ""
            if not os.path.exists(json_save):
                os.makedirs(json_save)
            json_save_file = os.path.join(json_save, config_tbl_name + ".json")
            file_config = open(json_save_file, "w+")
        else:
            config_tbl_name_override = config_src_schema_name + "_" + config_tbl_name
            if not os.path.exists(json_save):
                os.makedirs(json_save)
            json_save_file = os.path.join(json_save, config_tbl_name_override + "-Config.json")
            file_config = open(json_save_file, "w+")
        json_config = "{ \n" \
                        "\t\"id\": \"\", \n" \
                        "\t\"ssrId\": \"" + config_ssr_id + "\", \n" \
                        "\t\"systemName\": \"" + config_system_name + "\", \n" \
                        "\t\"scheduleGroupSchedule\": \"MON:SAT\", \n" \
                        "\t\"scheduleGroup\": \"" + config_schedule_group + "\", \n" \
                        "\t\"sourceDbType\": \"" + config_src_db_type + "\", \n" \
                        "\t\"sourceObjectName\": \"" + config_tbl_name + "\", \n" \
                        "\t\"sourceDatabaseName\": \"" + config_src_db_name + "\", \n" \
                        "\t\"sourceSchemaName\": \"" + config_src_schema_name + "\", \n" \
                        "\t\"sourceDataKey\": \"\", \n" \
                        "\t\"sourceChangeLogIdentifier\": \"\", \n" \
                        "\t\"activeFlag\": \"Y\", \n" \
                        "\t\"tableSchedule\": \"MON:SAT\", \n" \
                        "\t\"fullFilterCondition\": \"TRUE\", \n" \
                        "\t\"incrFilterCondition\": \"\", \n" \
                        "\t\"nullColList\": \"\", \n" \
                        "\t\"ingestionOrder\": \"\", \n" \
                        "\t\"runtimeColumnLinking\": \"\", \n" \
                        "\t\"selectColumnList\": \"" + config_column_list.rstrip(',') + "\", \n" \
                        "\t\"createdDatetime\": \"\", \n" \
                        "\t\"createdBy\": \"\", \n" \
                        "\t\"effectiveStartdate\": \"\", \n" \
                        "\t\"effectiveEnddate\": \"\", \n" \
                        "\t\"ingestionType\": \"FULL\", \n" \
                        "\t\"closingFilterCondition\": \"\", \n" \
                        "\t\"sourceAccessType\": \"" + config_src_access_type + "\", \n" \
                        "\t\"volumeCategory\": \"" + config_vol_category + "\", \n" \
                        "\t\"tableNameOverride\": \"" + config_tbl_name_override + "\" \n" \
                        "}"
        file_config.write(json_config)
        file_config.close()
print("end")