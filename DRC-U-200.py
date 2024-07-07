


'''
Author : Ronak Chhajed(RChhajed@slb.com), Shashank Gupta(SGupta88@slb.com), Rhuta Joshi (RJoshi5@slb.com)
------------------------------------------------------------------------------------------------------------------------
Title            : DR-Comparator-utility-2.0.0.py
Description      : This utility is developed to generate DR from any source and to compare any two standard Data Registries.
                   It simplifies previous utility version to generate selective differences, and handle exceptions.
Pre-requisites   :  1. DR name,format and path should be valid and existing
                    2. Output path entered should be valid. If path does not exist it will not be created.
                    3. sourcedata-registry-generator-1.1.0.jar should be present in location Z:/jars/
Python Version   : 3.8
------------------------------------------------------------------------------------------------------------------------
Date                    Change Name             Authors                             Description
October 31, 2019        Initial Draft           ajadhav9, ggunjan                   Created initial version
October 15, 2020        Modularised version     rchhajed,sgupta88,rjoshi5           Enhanced code maintainability
------------------------------------------------------------------------------------------------------------------------
'''


import os
import subprocess
import logging
import xlrd
import xlwt
from tabulate import tabulate


## Global variables
DR1_WORK_BOOK = ''
DR1_COLLECTIONS_SHEET = ''
DR1_ATTRIBUTES_SHEET = ''
DR2_WORK_BOOK = ''
DR2_COLLECTIONS_SHEET = ''
DR2_ATTRIBUTES_SHEET = ''
DIFF_WORK_BOOK = ''
DR1_NAME = ''
DR2_NAME = ''
DEFAULT_SAVE_PATH=''
HEADER_STYLE=''

DR1_MISSING_COLLECTIONS_KEYS = []
DR2_MISSING_COLLECTIONS_KEYS = []
DR1_MISSING_ATTRIBUTE_KEYS = []
DR2_MISSING_ATTRIBUTE_KEYS = []


## Open file function : Reads sheets from given paths
def open_file(dr1_path, dr2_path):
    global DR1_WORK_BOOK
    global DR1_COLLECTIONS_SHEET
    global DR1_ATTRIBUTES_SHEET

    global DR2_WORK_BOOK
    global DR2_COLLECTIONS_SHEET
    global DR2_ATTRIBUTES_SHEET
    
    global DR1_NAME
    global DR2_NAME
    global DEFAULT_SAVE_PATH

    global HEADER_STYLE

    try:
        DR1_WORK_BOOK = xlrd.open_workbook(dr1_path)
        DR2_WORK_BOOK = xlrd.open_workbook(dr2_path)
    except FileNotFoundError:
        print("ERROR: You have entered wrong file path. Please try again with correct file path")
        print('EXITING WITH STATUS CODE 1')
        exit(1)

    DR1_COLLECTIONS_SHEET = DR1_WORK_BOOK.sheet_by_name('Collections')
    DR1_ATTRIBUTES_SHEET = DR1_WORK_BOOK.sheet_by_name('Attributes')
    
    DR2_COLLECTIONS_SHEET = DR2_WORK_BOOK.sheet_by_name('Collections')
    DR2_ATTRIBUTES_SHEET = DR2_WORK_BOOK.sheet_by_name('Attributes')

    DR1_NAME=dr1_path.split("/")[-1]
    DR2_NAME=dr2_path.split("/")[-1]

    DEFAULT_SAVE_PATH=os.path.dirname(dr1_path)
    HEADER_STYLE = xlwt.XFStyle()
    # font 
    font = xlwt.Font()
    font.bold = True
    HEADER_STYLE.font = font


## Create and initialize Difference file
def init_diff_file():
    global DIFF_WORK_BOOK
    DIFF_WORK_BOOK = xlwt.Workbook()


## Verify matching headers between 2 DRs
def verify_headers(header_type):
    if header_type == 'Collections':
        #remove the empty cell values from the header and calculate length
        dr1_headers_length=len(list(filter(None,DR1_COLLECTIONS_SHEET.row_values(2))))
        dr2_headers_length=len(list(filter(None,DR2_COLLECTIONS_SHEET.row_values(2))))
        if (dr1_headers_length == 13 and dr2_headers_length == 13):
            invalid_collection_headers = {}
            for column in range(0,13):
                dr1_header = str(DR1_COLLECTIONS_SHEET.cell_value(2,column))
                dr2_header = str(DR2_COLLECTIONS_SHEET.cell_value(2,column))
                if dr1_header != dr2_header:
                    invalid_collection_headers[dr1_header] = dr2_header
            if bool(invalid_collection_headers):
                print('ERROR: Following header names in Collections tab are not matching. Please verify and keep headers same.')
                print(tabulate(invalid_collection_headers.items(),[DR1_NAME,DR2_NAME]))
                print('\nEXITING WITH STATUS CODE 1')
                exit(1)
        else:
            print('ERROR: Number of collection headers found: DR1 : {0} , DR2 {1}'.format(dr1_headers_length,dr2_headers_length))
            print('Number of headers in COLLECTIONS sheets of both DRs must be 13.')
            print('DR1 Collections Headers: {0}'.format(list(filter(None,DR1_COLLECTIONS_SHEET.row_values(2)))))
            print('DR2 Collections Headers: {0}'.format(list(filter(None,DR2_COLLECTIONS_SHEET.row_values(2)))))
            print('\nEXITING WITH STATUS CODE 1')
            exit(1)
    elif header_type == 'Attributes':
        #remove the empty cell values from the header and calculate length
        dr1_headers_length=len(list(filter(None,DR1_ATTRIBUTES_SHEET.row_values(2))))
        dr2_headers_length=len(list(filter(None,DR2_ATTRIBUTES_SHEET.row_values(2))))
        if dr1_headers_length == 20 and dr2_headers_length == 20:
            invalid_attribute_headers = {}
            for column in range(0,20):
                dr1_header = str(DR1_ATTRIBUTES_SHEET.cell_value(2,column))
                dr2_header = str(DR2_ATTRIBUTES_SHEET.cell_value(2,column))
                if dr1_header != dr2_header:
                    invalid_attribute_headers[dr1_header] = dr2_header
            if bool(invalid_attribute_headers):
                print('ERROR: Following header names in Attributes tab are not matching. Please verify and keep headers same.')
                print(tabulate(invalid_attribute_headers.items(),[DR1_NAME,DR2_NAME]))
                print('\nEXITING WITH STATUS CODE 1')
                exit(1)
        else:
            print('ERROR: Number of attributes headers found: DR1 : {0} , DR2 {1}'.format(dr1_headers_length,dr2_headers_length))
            print('Number of headers in ATTRIBUTES sheets of both DRs must be 20')
            print('DR1 Attributes Headers: {0}'.format(list(filter(None,DR1_ATTRIBUTES_SHEET.row_values(2)))))
            print('DR2 Attributes Headers: {0}'.format(list(filter(None,DR2_ATTRIBUTES_SHEET.row_values(2)))))
            print('\nEXITING WITH STATUS CODE 1')
            exit(1)


## Generate and fill collections diff sheet
def fill_sheet_collections(column_name, diff_dict):
    diff_sheet=DIFF_WORK_BOOK.add_sheet(column_name)
    diff_sheet.write(0, 0, 'Table/View/Collection',style=HEADER_STYLE)
    diff_sheet.write(0, 1, "{0} in {1}".format(column_name,DR1_NAME),style=HEADER_STYLE)
    diff_sheet.write(0, 2, "{0} in {1}".format(column_name,DR2_NAME),style=HEADER_STYLE)

    row_itr = 1
    max_table_length=21
    for key in diff_dict.keys():
        key_write = key
        if max_table_length<len(key_write): max_table_length=len(key_write)
        diff_sheet.write(row_itr, 0, str(key_write))
        diff_sheet.write(row_itr, 1, str(diff_dict[key][0]))
        diff_sheet.write(row_itr, 2, str(diff_dict[key][1]))
        row_itr += 1

    #setting column width
    diff_sheet.col(0).width=(max_table_length+2)*300
    diff_sheet.col(1).width=(len(column_name)+4+len(DR1_NAME))*260
    diff_sheet.col(2).width=(len(column_name)+4+len(DR2_NAME))*260


## Generate and fill attribute diff sheet
def fill_sheet_attribute(column_name, diff_dict):
    diff_sheet=DIFF_WORK_BOOK.add_sheet(column_name)
    diff_sheet.write(0, 0, 'Table/View/Collection',style=HEADER_STYLE)
    diff_sheet.write(0, 1, 'Column Name',style=HEADER_STYLE)
    diff_sheet.write(0, 2, "{0} in {1}".format(column_name,DR1_NAME),style=HEADER_STYLE)
    diff_sheet.write(0, 3, "{0} in {1}".format(column_name,DR2_NAME),style=HEADER_STYLE)

    row_itr = 1
    max_table_length=21
    max_column_length=11
    for key in diff_dict.keys():
        key_write = key
        if max_table_length<len(key_write[0]): max_table_length=len(key_write[0])
        if max_column_length<len(key_write[1]): max_column_length=len(key_write[1])
        diff_sheet.write(row_itr, 0, str(key_write[0]))
        diff_sheet.write(row_itr, 1, str(key_write[1]))
        diff_sheet.write(row_itr, 2, str(diff_dict[key][0]))
        diff_sheet.write(row_itr, 3, str(diff_dict[key][1]))
        row_itr += 1

    #setting column width
    diff_sheet.col(0).width=(max_table_length+2)*300
    diff_sheet.col(1).width=(max_column_length+2)*300
    diff_sheet.col(2).width=(len(column_name)+4+len(DR1_NAME))*260
    diff_sheet.col(3).width=(len(column_name)+4+len(DR2_NAME))*260


## Compare Collections sheets between 2 DRs
def compare_collections(column_index):
    global DR1_MISSING_COLLECTIONS_KEYS
    global DR2_MISSING_COLLECTIONS_KEYS
    dr1_collection_dict = {}
    dr2_collection_dict = {}
    collection_diff_dict = {}
    
    dr1_collection_tbl_names = DR1_COLLECTIONS_SHEET.col_values(2)[3:]
    # dr1_collection_ssr_id = DR1_COLLECTIONS_SHEET.col_values(1)[3:]
    dr2_collection_tbl_names = DR2_COLLECTIONS_SHEET.col_values(2)[3:]
    # dr2_collection_ssr_id = DR2_COLLECTIONS_SHEET.col_values(1)[3:]
    
    for i in range(len(dr1_collection_tbl_names)):
        key_tuple = (dr1_collection_tbl_names[i])
        dr1_collection_dict[key_tuple] = DR1_COLLECTIONS_SHEET.cell_value(i+3, column_index)
    for i in range(len(dr2_collection_tbl_names)):
        key_tuple = (dr2_collection_tbl_names[i])
        dr2_collection_dict[key_tuple] = DR2_COLLECTIONS_SHEET.cell_value(i+3, column_index)
    for key in dr2_collection_dict.keys():
        if (key not in dr1_collection_dict.keys()) and (key not in DR1_MISSING_COLLECTIONS_KEYS) :
            DR1_MISSING_COLLECTIONS_KEYS.append(key)
    for key in dr1_collection_dict.keys():
        if key not in dr2_collection_dict.keys():
            if key not in DR2_MISSING_COLLECTIONS_KEYS:
                DR2_MISSING_COLLECTIONS_KEYS.append(key)
        else:
            if dr1_collection_dict[key] != dr2_collection_dict[key]:
                collection_diff_dict[key] = [dr1_collection_dict[key],dr2_collection_dict[key]]

    column_name = str(DR1_COLLECTIONS_SHEET.cell_value(2, column_index))
    fill_sheet_collections(column_name, collection_diff_dict)


## Compare Attributes sheets between 2 DRs
def compare_attributes(column_index):
    global DR1_MISSING_ATTRIBUTE_KEYS
    global DR2_MISSING_ATTRIBUTE_KEYS
    dr1_attrib_dict = {}
    dr2_attrib_dict = {}
    attrib_diff_dict = {}
    
    dr1_attrib_tbl_names = DR1_ATTRIBUTES_SHEET.col_values(0)[3:]
    dr1_attrib_col_names = DR1_ATTRIBUTES_SHEET.col_values(1)[3:]
    dr2_attrib_tbl_names = DR2_ATTRIBUTES_SHEET.col_values(0)[3:]
    dr2_attrib_col_names = DR2_ATTRIBUTES_SHEET.col_values(1)[3:]
    
    for i in range(len(dr1_attrib_tbl_names)):
        key_tuple = (dr1_attrib_tbl_names[i] , dr1_attrib_col_names[i])
        dr1_attrib_dict[key_tuple] = DR1_ATTRIBUTES_SHEET.cell_value(i+3, column_index)
    for i in range(len(dr2_attrib_tbl_names)):
        key_tuple = (dr2_attrib_tbl_names[i] , dr2_attrib_col_names[i])
        dr2_attrib_dict[key_tuple] = DR2_ATTRIBUTES_SHEET.cell_value(i+3, column_index)
    for key in dr2_attrib_dict.keys():
        if (key not in dr1_attrib_dict.keys()) and (key not in DR1_MISSING_ATTRIBUTE_KEYS) :
            DR1_MISSING_ATTRIBUTE_KEYS.append(key)
    for key in dr1_attrib_dict.keys():
        if (key not in dr2_attrib_dict.keys()) and (key not in DR2_MISSING_ATTRIBUTE_KEYS) :
            DR2_MISSING_ATTRIBUTE_KEYS.append(key)
        else:    
            if dr1_attrib_dict[key] != dr2_attrib_dict[key]:
                attrib_diff_dict[key] = [dr1_attrib_dict[key],dr2_attrib_dict[key]]

    column_name = str(DR1_ATTRIBUTES_SHEET.cell_value(2, column_index))
    fill_sheet_attribute(column_name, attrib_diff_dict)


## Add missing tables and columns to the workbook
def add_missing_tab_col():
    missing_attributes = DIFF_WORK_BOOK.add_sheet('Missing Attributes Columns')
    missing_collections = DIFF_WORK_BOOK.add_sheet('Missing Collections Tables')
    
    missing_attributes_header_dr1='Columns Missing in {0}'.format(DR1_NAME)
    missing_attributes_header_dr2='Columns Missing in {0}'.format(DR2_NAME)
    missing_attributes.write_merge(0,0,0,1,missing_attributes_header_dr1,style=HEADER_STYLE)
    missing_attributes.write(1,0,'Table/View/Collection',style=HEADER_STYLE)
    missing_attributes.write(1,1,'Column',style=HEADER_STYLE)
    missing_attributes.write(1,2,'Table/View/Collection',style=HEADER_STYLE)
    missing_attributes.write(1,3,'Column',style=HEADER_STYLE)

    missing_attributes.write_merge(0,0,2,3,missing_attributes_header_dr2,style=HEADER_STYLE)
    missing_attributes.col(0).width=missing_attributes.col(1).width=len(missing_attributes_header_dr1)*140
    missing_attributes.col(2).width=missing_attributes.col(3).width=len(missing_attributes_header_dr2)*140
    missing_collections_header_dr1='Table/View/Collection Missing in {0}'.format(DR1_NAME)
    missing_collections_header_dr2='Table/View/Collection Missing in {0}'.format(DR2_NAME)
    missing_collections.write(0,0,missing_collections_header_dr1,style=HEADER_STYLE)
    missing_collections.write(0,1,missing_collections_header_dr2,style=HEADER_STYLE)
    missing_collections.col(0).width=len(missing_collections_header_dr1)*260
    missing_collections.col(1).width=len(missing_collections_header_dr2)*260
   
    for i in range(len(DR1_MISSING_ATTRIBUTE_KEYS)):
        key_write = DR1_MISSING_ATTRIBUTE_KEYS[i]
        missing_attributes.write(i+2,0,key_write[0])
        missing_attributes.write(i+2,1,key_write[1])

    for i in range(len(DR2_MISSING_ATTRIBUTE_KEYS)):
        key_write = DR2_MISSING_ATTRIBUTE_KEYS[i]
        missing_attributes.write(i+2,2,key_write[0])
        missing_attributes.write(i+2,3,key_write[1])

    for i in range(len(DR1_MISSING_COLLECTIONS_KEYS)):
        key_write = DR1_MISSING_COLLECTIONS_KEYS[i]
        missing_collections.write(i+1,0,key_write)
    for i in range(len(DR2_MISSING_COLLECTIONS_KEYS)):
        key_write = DR2_MISSING_COLLECTIONS_KEYS[i]
        missing_collections.write(i+1,1,key_write)



## Save workbook
def save_workbook(save_path):
    if save_path.strip()=='':
        save_path=DEFAULT_SAVE_PATH
        print("Path not provided. Saving to default path")
    try:
        diff_file = os.path.join(save_path, 'DR_Difference.xls')
        DIFF_WORK_BOOK.save(diff_file) #throws PermissionError if file is already open
        print("DR_Difference.xls file saved at {0}".format(save_path))
        if input("Do you want to open the DR_Difference.xls file? (Y/N): ").upper()=='Y': os.startfile(diff_file)
        exit(0)
    except FileNotFoundError as file_error:
        print("ERROR: Directory path is wrong. \n{0}".format( file_error.strerror))
    except PermissionError:
        print("ERROR: Difference file is already open please close the file and try again.")

if __name__ == "__main__":
    generate_flag = input('Do you want to generate Source DR? (Y/N) : ')
    compare_flag = input('Do you want to compare two DRs? (Y/N) : ')
    generate_flag = generate_flag.upper()
    compare_flag = compare_flag.upper()
    
    if generate_flag == 'Y':
        try:
            subprocess.call(['java', '-jar', 'C:/jars/sourcedata-registry-generator-1.2.0.jar'])
        except subprocess.CalledProcessError:
            print('sourcedata-registry-generator-1.1.0.jar is either not present or corrupted at location C:/jars/')
            print('Unable to generate source DR. Please check jar file.')
            print('\nEXITING WITH STATUS CODE 1')
            exit(1)
    if compare_flag == 'Y':
        dr1_path = input("Enter full path of first DR with filename and extension : ").replace('\\','/')
        dr2_path = input("Enter full path of second DR with filename and extension : ").replace('\\','/')
        open_file(dr1_path,dr2_path)
        init_diff_file()
        verify_headers('Collections')
        verify_headers('Attributes')
        compare_collections(11) #Delivery
        compare_collections(12) #Domain
        compare_attributes(7)   #Security Classification
        add_missing_tab_col()
        save_path=input("Enter path where you want save the difference file: ").replace('\\','/')
        save_workbook(save_path)
    if generate_flag!='Y' and compare_flag!='Y':
        print("You have not selected any of the above option.\nExiting utility.")