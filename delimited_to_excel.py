'''tun
Author : Ronak Chhajed(RChhajed@slb.com)
------------------------------------------------------------------------------------------------------------------------
Title            : delimited_to_excel.py
Description      : This utility is developed to generate excelfile from a delimited file based on the given delimiter.
Pre-requisites   :  1. Delimited file with known delimiter and null character ( e.g. \\N).
                    2. Below python libraries installed
                        a. pandas - ( pip install pandas==1.1.3)
                        b. xlsxwriter - (pip install xlsxwriter==1.3.7)
Python Version   : 3.8
Usage            : python delimited_to_excel.py
------------------------------------------------------------------------------------------------------------------------
Date                    Change Name             Authors                             Description
March 1, 2021        Initial Draft           RChhajed@slb                        Developed initial version
------------------------------------------------------------------------------------------------------------------------
'''

import os
import pandas as pd
import time

def get_input_file():
    delimited_file_path = input("Enter full path of delimited file with filename and extension: ").replace('\\','/')
    delimited_file_path=delimited_file_path.strip().strip('"').strip("'")
    if not os.path.isfile(delimited_file_path):
        raise FileNotFoundError
    return delimited_file_path
    
def get_delimiter():
    delimiter=input("Enter delimiter (leave blank to use default - ^^`` ): ")
    if delimiter == '':
        print("INFO: No delimiter provided. Using ^^`` as delimiter.")
        delimiter='^^``'
    return delimiter

def get_null_character():
    null_char=input("Enter a null character if applicable:  ")
    return null_char    

def get_output_file():
    output_file_path=input("Enter path where you want save the excel file: ").replace('\\','/')
    output_file_path=output_file_path.strip().strip('"').strip("'")
    if  output_file_path== '':
        print("ERROR: You have not provided output path. Please try again.")

    if not os.path.exists(output_file_path):
        os.makedirs(output_file_path)
    return output_file_path

def process_and_save_file(delimited_file_path,delimiter,null_char,output_file_path):
    filename=delimited_file_path.split('/')[-1].split('.')[0]
    output_file=os.path.join(output_file_path,"{0}.xlsx".format(filename))
    with open(delimited_file_path,'r',encoding="utf8") as delimited_file:
        data_list=[]
        for line in delimited_file:
            line=line.rstrip('\n')
            split_line=line.split(delimiter)
            if null_char != "":
                split_line=[field if field.strip()!=null_char.strip() else "" for field in split_line]

            data_list.append(split_line)

        df = pd.DataFrame(data_list)
        writer= pd.ExcelWriter(output_file,engine='xlsxwriter')
        df.to_excel(writer, index=False , header=False)
        writer.save()
        print("Success: File processed and saved as {}".format(output_file))
        
    


if __name__ == "__main__":
    play_again="Y"
    while(play_again.lower() == "y"):
        try:
            process_and_save_file(get_input_file(),get_delimiter(),get_null_character(),get_output_file())
        except FileNotFoundError as fnfe:
            print("ERROR: You have entered wrong file path. Please try again with correct file path\n{}".format(fnfe))
        except OSError as ose:
            print("ERROR: Invalid path. Remove \" & ' from path, if you have any\n{}".format(ose))
        except Exception as e:
            print("ERROR: Something went wrong\n{}".format(e))

        play_again=input("\nDo you want to do it again? (Y/N): ")
    
    for i in range(1,4):print("\rExiting{}".format("."*i), end="");time.sleep(0.5)