import requests
import os
import sys
import json
import logging
import calendar
import datetime
import time
import shutil

with open(sys.argv[1], 'r') as argument_file:
    table_data = json.load(argument_file)

logging.basicConfig(level = logging.INFO)
logging.info('Python Version: ' + sys.version)
logging.info('Executing DEGREED Ingestion')
SRC_TABLE = table_data['table']
logging.info('Source table Name: ' + SRC_TABLE)
DATA_BASE_URL = table_data['api_url']
LOAD_TYPE = table_data['load_type']
LOAD_TYPE = LOAD_TYPE.upper()
FULL_LOAD_DAY = table_data['full_load_day']
X_API_KEY = table_data['apikey']['key']
X_API_KEY_VAL = table_data['apikey']['value']
LIMIT = table_data['limit']
OUTPUT_DIR = table_data['output_dir']
ERROR_DIR = table_data['error_dir']
THREAD_SLEEP_TIME = table_data['thread_sleep_time']
LAST_MOD_PATH = table_data['last_mod_path']



page_num = 1
DATA_PAYLOAD = {}
data_json = {}
next_date_set = ""

DATA_HEADERS = {
        'Content-Type': 'application/json;charset=utf-8;odata.metadata=minimal;odata.streaming=true',
    X_API_KEY : X_API_KEY_VAL
    }


output_folder = str(OUTPUT_DIR) + '/' + str(SRC_TABLE)

if not os.path.exists(output_folder):
    os.makedirs(output_folder)
    logging.info('Output directory created:--> ' + str(output_folder))
else:
    shutil.rmtree(output_folder)
    os.makedirs(output_folder)
    logging.info('Local directory cleaned:--> ' + str(output_folder))



def find_day(date):
    weekday = datetime.datetime.strptime(date, '%d %m %Y').weekday()
    return (calendar.day_name[weekday])

def write_json_to_file(data_json,page_num):
    global next_key_fr_url
    data_file_name = os.path.join(output_folder, str(SRC_TABLE)+'_' + str(page_num) + '.json')
    try:
        with open(data_file_name, 'w') as file:
            file.write(json.dumps(data_json))
        print("Data write completed")
        next_key_fr_url = ""
        next_key_substring = 'next='
        #print(next_key_substring)
        line_start = str(data_json).find(next_key_substring)
        line_end = str(data_json).find(' ', str(data_json).find(next_key_substring) + 8)
        #logging.info("starting of next_key line : " + str(line_start) + ", ending of next_key line : " + str(line_end))
        if (line_start > 0 and line_end > 0):
            next_key_line = str(data_json)[line_start:line_end].strip()
            next_key_fr_url = next_key_line[next_key_line.find("next=") + 5: len(next_key_line) - 3]
            logging.info("next key line " + next_key_line + " , key for next page : " + next_key_fr_url)
        else:
            logging.info("No key found for next page so download will end after this page")
    except:
        logging.error("Error while writing data into file:", sys.exc_info()[0])
        logging.error('Exiting the program.')
        sys.exit(1)


def get_json_responses(base_url):
    print("\n\n\n\n")
    global page_num
    global data_json
    next_date_set="true"

    #print("Parent : Call for data download, next_date_set : "+next_date_set)
    while data_json != []:
        print("\n\n")
       # print("while:Start - Call for data download, next_date_set flag : " + next_date_set + ", page_num : " + str(page_num))
        if (page_num > 1):
            if (next_date_set == "true"):
                data_url = base_url
            else:
                if(len(next_key_fr_url) > 0):
                    data_url = base_url + "&next=" + next_key_fr_url
                    print("next_key_fr_url : " + next_key_fr_url + ".")
                else:
                    logging.info("We've reached last page. Exiting download!")
                    break
        else:
            data_url = base_url
        next_date_set = "false"
        #print("while:end - Call for data download, next_date_set flag : " + next_date_set+", page_num : "+str(page_num))
        logging.info('FETCHING URL: '+data_url)
        try:
            data_response = requests.get(data_url, headers=DATA_HEADERS, data=DATA_PAYLOAD,
                                        verify=False)
            status = data_response.status_code
            logging.info('Status code for data ingestion: ' + str(status))
            if status != 200:
                error_time = datetime.datetime.utcnow()
                error_file_name = os.path.join(ERROR_DIR, '{0}_{1}.err'.format(SRC_TABLE,error_time.strftime('%Y%m%d%H%M%S')))
            retry_cnt = 3
            while status != 200 and retry_cnt != 0:
                if str(data_response.content) != "":
                    data_json = data_response.json()
                    error_time = datetime.datetime.utcnow()
                    error_file_name = os.path.join(ERROR_DIR, '{0}_{1}.err'.format(SRC_TABLE,error_time.strftime('%Y%m%d%H%M%S')))
                    try:
                        with open(error_file_name, 'w') as file:
                            file.write(json.dumps(data_json))
                            logging.info("Error logs generated in : {0}".format(ERROR_DIR))
                    except:
                        logging.error("Error while logging error to file for {0} response code:".format(status), sys.exc_info()[0])
                        logging.error('Exiting the program.')
                        sys.exit(1)
                else:
                    logging.info('Error code {0}. No response content found for this code for error file logging.'.format(status))
                    logging.info('Sleep ' + THREAD_SLEEP_TIME + ' seconds')
                    logging.info('Time ' + datetime.datetime.utcnow().strftime("%H:%M:%S"))
                    time.sleep(float(THREAD_SLEEP_TIME))
                    logging.info('Retrying after '+THREAD_SLEEP_TIME+' seconds')
                    logging.info('Time ' + datetime.datetime.utcnow().strftime("%H:%M:%S"))
                    data_response = requests.get(data_url, headers=DATA_HEADERS, data=DATA_PAYLOAD,
                                        verify=False)
                    status = data_response.status_code
                    retry_cnt = retry_cnt-1
            if status != 200 and retry_cnt == 0:
                logging.error('Error while ingestion. Please check the API connection and try again.')
                logging.error('Exiting the program.')
                sys.exit(1)
            data_json = data_response.json()
        except Exception as e:
            logging.error("Error while processing:{0}".format(e))
            sys.exit(1)
        if data_json != []:
            write_json_to_file(data_json,page_num)
            page_num +=1
    logging.info("Data fetch successfully completed.")

def run_incr_load(start_value):
    global end_value
    end_value = datetime.date.today() - datetime.timedelta(days=1)
    if SRC_TABLE == "completions":
        logging.info('Running INCR load')
        start_value = datetime.datetime.strptime(lastmoddt_dict[SRC_TABLE],'%Y-%m-%d').date()+ datetime.timedelta(days=1)
        print("Start date as : " + str(start_value) + " , and End date as :" + str(end_value))
        if start_value>end_value:
           print("Start date is greater than end date. We have latest Data from API. Exiting!")
           sys.exit(1)
        data_url = str(DATA_BASE_URL) + '?filter[start_date]='+str(start_value)+ '&' + 'filter[end_date]= '+str(end_value)+ '&' + 'limit='+LIMIT
        get_json_responses(data_url)
    else:
        run_full_load()

def run_full_load():
    global start_value
    global end_value
    global next_date_set
    today_date = datetime.date.today() - datetime.timedelta(days=1)
    start_value = datetime.datetime.strptime('2021-01-01', '%Y-%m-%d').date()
    end_value = start_value + datetime.timedelta(days=7)
    if end_value >= today_date:
        end_value = today_date
    if SRC_TABLE == "completions":
        while end_value <= today_date:
            print("Start date as : " + str(start_value) + " , and End date as :" + str(end_value))
            data_url = str(DATA_BASE_URL) + '?filter[start_date]='+str(start_value)+ '&' + 'filter[end_date]= '+str(end_value)+ '&' + 'limit='+LIMIT
            next_date_set = "true"
            get_json_responses(data_url)
            if end_value == today_date:
                break
            start_value = end_value + datetime.timedelta(days=1)
            end_value = start_value + datetime.timedelta(days=7)
            if end_value >= today_date:
               end_value = today_date
    else:
        data_url = str(DATA_BASE_URL) + '?limit='+LIMIT
        get_json_responses(data_url)


if __name__ == "__main__":
    today = datetime.datetime.utcnow()
    date = today.strftime("%d %m %Y")
    logging.info('Ingestion date: ' + str(date))
    todays_day = find_day(date)
    logging.info('Ingestion day: ' + str(todays_day))
    last_mod_file_nm = str(LAST_MOD_PATH) + '/lastModDate.json'
    lastmoddt_dict = {}
    if os.path.isfile(last_mod_file_nm):
        with open(last_mod_file_nm,'r') as lastmoddt_file:
            lastmoddt_dict = json.load(lastmoddt_file)
        if SRC_TABLE in lastmoddt_dict:
            logging.info('Found last mod date '+ str(lastmoddt_dict[SRC_TABLE]) +' for ' + str(SRC_TABLE))
            try:
                datetime_obj = datetime.datetime.strptime(lastmoddt_dict[SRC_TABLE],'%Y-%m-%d')
                start_value = str(lastmoddt_dict[SRC_TABLE])

                if todays_day == '{0}'.format(FULL_LOAD_DAY):
                    logging.info('Running FULL load based on full load day ' + str(todays_day))
                    run_full_load()
                else:
                    if LOAD_TYPE == 'FULL':
                        logging.info('Running '+ str(LOAD_TYPE) +' load')
                        run_full_load()
                    elif LOAD_TYPE == 'INCR':
                        logging.info('Running '+ str(LOAD_TYPE) +' load')
                        run_incr_load(start_value)
                    else:
                        logging.error('Incorrect load_type. Please check.')
            except ValueError:
                logging.info('lastModDate.json has been tampered. Last modified date not in format %m/%d/%Y.')
                logging.info('Switching to FULL ingestion')
                run_full_load()
        else:
            logging.info('Source table not found in lastModDate.json. Running full load')
            run_full_load()
    else:
        logging.info('lastModDate.json file not found. Running full load.')
        run_full_load()
if SRC_TABLE == "completions":
    lastmoddt_dict[SRC_TABLE] = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    with open(last_mod_file_nm, 'w') as lastmoddt_file:
        lastmoddt_json = json.dumps(lastmoddt_dict)
        lastmoddt_file.write(lastmoddt_json)
    logging.info('END')

