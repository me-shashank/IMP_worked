#!/bin/sh
exitBadArgument()
{
        echo "sh esm_wrapper -trunc_day <TRUNCATE_DATE> -tbl <TABLE_NAME> -l <LAYER_NAME>"
        exit 1

}

exitScriptOnErrorESMwrapper()
{
        echo "`PrErrD` : Exit function called"
        echo "`PrInfD` : Error description at below path"
        echo "`PrInfD` : ${LOG_DIR}/${g_APP_SYSTM_NM}_${APP_SSR_ID}_${TIME_STAMP}_${PID}.errormsg.txt"
        echo `cat ${LOG_DIR}/${g_APP_SYSTM_NM}_${APP_SSR_ID}_${TIME_STAMP}_${PID}.errormsg.txt`
        echo ""
        exit 1
}

invokeAppConfigFile() {

APPLICATION=ESM
TIME_STAMP=`date +%d%m%y_%H%M%S%3N`
APP_ROOT=/iics/app/custom_ingestion/ESM/

INBOUND_DIR=/iics/data/inbound/ESM/

LOG_DIR=/iics/app/custom_ingestion/ESM/scriptslog

SOURCE_DIR=/iics/data/source
ERROR_DIR=/iics/app/custom_ingestion/ESM/scriptslog
APP_LOG_NAME=/iics/app/custom_ingestion/ESM/scriptslog/ESM_${TIME_STAMP}.log

POLL_SLEEP_INTERVAL=60
POLL_DURATION=1800
POLL_DURATION_SCHEDULE=10000

TRUNC_IND=Y
MDM_LOAD_TRUNC_DB_FLG=ORA_MDM
}


getLastIngestionDate(){

   t_QUERY_ff="select top 1 trim(LATEST_INGESTION_DATE)  from  IICS_AUDIT.JOB_AUDIT_DETAILS AS JAD WITH (nolock) INNER JOIN IICS_CONFIG.VW_APPLICATION_LOADLIST AS vall WITH (nolock) ON JAD.LOADLIST_ID= vall.LOADLIST_ID where APP_SSR_ID='$APP_SSR_ID' and vall.SOURCE_OBJECT_NAME='$TABLE_NAME' and activity='Data_Extraction'  and JAD.TASK_STATUS=1 and ORIGINAL_AS_OF_DATE=(select max(ORIGINAL_AS_OF_DATE) from IICS_AUDIT.JOB_AUDIT where TABLE_NAME_OVERRIDE = UPPER('$TABLE_NAME') and  ACTIVITY='Transfer_GC' and task_status = 1 and APP_SSR_ID='EAR-AA-7809') order by as_of_date desc;"

   t_QUERY_RESULT=`python3.6 /iics/app/shared/scripts/ConnectDB_IICS.py "${t_QUERY_ff}" "sgupta88@slb.com"`


                RC=$?
                if [[ $RC -ne 0 ]];then
#                       echo " : Error while fetching Max_DATE from Audit table" | tee -a $ERROR_MSG_FILE
                                echo ""
#                       exitScriptOnErrorARC_SKORU
                fi

v_MAX_DT_frm_ADT=$(echo "$t_QUERY_RESULT" | tr "results[()],'n" "\n"| sed '/^\s*$/d' |sed 's|\\||g')

                echo "`PrInfD` : Query details from audit as : '"${v_MAX_DT_frm_ADT}"'"

#          v_MAX_DT_frm_ADT=`echo "${v_ADT_QUERY_RESULT}"|awk -F'~' '{print $4}'`
                if [[ -z "${v_MAX_DT_frm_ADT}"  ]];then
                                echo "`PrInfD` : No Max date found in audit table for latest successful run :( "
                else
                                echo "`PrInfD` : Max date fetched from audit as ${v_MAX_DT_frm_ADT}"
                                l_DATE=${v_MAX_DT_frm_ADT}

                fi

}



cleanOutputDirectory(){
echo "`PrInfD` : Checking for previous file - /iics/data/Inbound/ESM/$TABLE_NAME.txt"
if [ -f "/iics/data/Inbound/ESM/$TABLE_NAME.txt" ]; then
                echo "`PrInfD` : File - /iics/data/Inbound/ESM/$TABLE_NAME.txt exists "
                echo "`PrInfD` : Removing Previous file for $TABLE_NAME from Inbound"
                rm -rf /iics/data/Inbound/ESM/$TABLE_NAME.txt
                RC=$?
                if [ $RC -ne 0 ];then
                                                echo "`PrInfD` : Issue in removing the file"
                                                exit 1
                else
                                                echo "`PrInfD` : Previous file removed - /iics/data/Inbound/ESM/$TABLE_NAME.txt"
                fi
else
                echo "`PrInfD` : Old file  not there in Inbound"

fi
}




updtAdtTblForLtstIngstionDate(){
SRC=/iics/data/inbound/ESM/
STG=/iics/app/custom_ingestion/ESM/prmfiles/last_ingestion/
TGT=/iics/app/scriptslog/

#/iics/data/inbound/ESM/kb_submission.txt -- Data file
#/iics/app/custom_ingestion/ESM/prmfiles/last_ingestion/kb_submission_maxCLIDate.txt
#/iics/app/scriptslog/kb_submission.txt

#Search the source DATA File name and extract table name out of file name

ff=$(find $SRC -iname "${TABLE_NAME}.txt")
echo "`PrInfD` : We found the file name with PATH... $ff";
#FNAME=$(basename "$ff" ??????????????.txt)
FNAME=$"${TABLE_NAME}.txt"


echo "`PrInfD` : And the FILE basename is $FNAME";

if [ -f $STG"${TABLE_NAME}_maxCLIDate.txt" ]
then

        echo " `PrInfD` : Cleaning OLD ${TGT}${TABLE_NAME}*.txt from scriptlog directory : ${TGT}"
        echo " `PrInfD` : rm -rf ${TGT}${TABLE_NAME}.txt"
        rm -rf ${TGT}${TABLE_NAME}.txt
        touch $TGT$FNAME
        echo "`PrInfD` : Zero byte file is created in scriptslog dir with same name as actual source file"

        for file in "${FNAME}";
        do id=${file:0:-4}
        echo "`PrInfD` : Extracting Table Name from the filename: $id"

        find $STG -iname "${id}_maxCLIDate.txt"  -print0 | while read -d $'\0' j
        do echo "`PrInfD` : We found the file name containing maxCLIDate... $j";
        cp $j $TGT$FNAME

        done
        done
fi

echo "`PrInfD` : MAXDATE for CLI is copied into scriptlogs successfully with same as source file name"


}
#Updates audit table


##########################################################################################################################################
###############################                                                                                                   #####################################################
###############################         Actual Execution of Script starts here          #####################################################
###############################                                                                                                   #####################################################
##########################################################################################################################################


PID=$$
PrInf="INFO"
PrErr="ERROR"
PrWrn="WARNING"
alias DATE='echo `date +%Y-%m-%d_%H:%M:%S`'
alias PrInfD='echo $PrInf : `DATE` : `basename $0`'
alias PrErrD='echo $PrErr : `DATE` : `basename $0`'
alias PrWrnD='echo $PrWrn : `DATE` : `basename $0`'


###################### Accept and validate the input parameters ############################################
if [ $# -eq 0 ]
  then
        echo -e "\n No arguments supplied"
   exitBadArgument
fi

while [[ $1 = -* ]]; do
        case $1 in
        -rt ) INCR_FLAG=$2
                        shift;;
        -tbl ) TABLE_NAME=$2
                        shift;;
        -l ) LAYER_NAME=$2
                        shift;;
        * ) echo -e "\nBad argument!"; exitBadArgument
        esac
shift
done


#echo "Truncate date : $trunc_date"

if [ "$INCR_FLAG" == "" ]
        then
        echo -e "\nInvalid refresh type."
        exitBadArgument
fi

if [ "$TABLE_NAME" == "" ]
        then
        echo -e "\nTable name can not be empty."
        exitBadArgument
fi




APP_SSR_ID="EAR-AA-7809"
g_APP_SYSTM_NM="ESM"
g_CALLER='esm_pre_script_wrapper'
PARAM_FILE=/iics/app/custom_ingestion/ESM/prmfiles
INBOUND_PATH=/iics/data/inbound/ESM/
LOG_FILE_PATH=/iics/app/custom_ingestion/ESM/scriptslog
OUTPUT_DIR=/iics/data/inbound/ESM/

invokeAppConfigFile

################################### New LOG file for this wrapper ########################################################
TIME_STAMP=`date +%d%m%y_%H%M%S%3N`
NEW_LOG_FILE=`echo $APP_LOG_NAME | sed "s/.[^.]*$//"`
LOG_FILE_NAME=${NEW_LOG_FILE}_${g_CALLER}_${TABLE_NAME}_${PID}.log
ERROR_MSG_FILE=${NEW_LOG_FILE}_${PID}_ERROR_MSG.txt
echo "`PrInfD` : LOG_FILE_NAME : $LOG_FILE_NAME"
exec >> ${LOG_FILE_NAME} 2>&1
RC=100


echo "`PrInfD` : Starting the script.. "
echo "`PrInfD` : Table name: $TABLE_NAME"
echo "`PrInfD` : Layer name: $LAYER_NAME"


FULL_DAY_CFG=/iics/app/custom_ingestion/ESM/prmfiles/full_load_day.cfg
DATE=$(date -d "$D" '+%d')

echo -e "\n`PrInfD` : Reading Config File to check for FULL LOAD DAY : $FULL_DAY_CFG"
FULL_DAY_FROM_FILE=`grep -iw $TABLE_NAME $FULL_DAY_CFG | cut -d '=' -f 2`
echo "`PrInfD` : Today's date: $DATE , Full load date received from file: $FULL_DAY_FROM_FILE"

if [ $DATE == $FULL_DAY_FROM_FILE ]
then
echo "`PrInfD` : Since dates are matching, Table will be running FULL ..."
INCR_FLAG="FULL"

elif [ $FULL_DAY_FROM_FILE == "FULL" ]
then
echo "`PrInfD` : FULL has been given in file, Table will be running FULL ... "
INCR_FLAG="FULL"

else
echo "`PrInfD` : Since dates are not matching, Table will be running INCREMENTAL ..."
INCR_FLAG="INCR"
fi

if [ $TABLE_NAME == 'sn_hr_le_case_hr' ]
then
TABLE_NAME="sn_hr_le_case"
echo "`PrInfD` : This is HR flow, table being fetched from ESM API is: $TABLE_NAME "
fi



CONFIG_FILE=/iics/app/custom_ingestion/ESM/prmfiles/.esm.conf
echo -e "\n`PrInfD` : Reading Config File for Script from : $CONFIG_FILE"
if [[ -f $CONFIG_FILE ]];then
        source $CONFIG_FILE
else
        echo "`PrErrD` : Config file is missing : "$CONFIG_FILE | tee -a $ERROR_MSG_FILE
        exitScriptOnErrorESMwrapper
fi


#clearing previous files if present at inbound
cleanOutputDirectory

FULL="FULL"
INCR="INCR"


if [ $INCR_FLAG == $FULL ];then
        echo "`PrInfD` : Java call for FULL LOAD for table $TABLE_NAME"
        java -Xss2m -D -DESM_CONFIG_FILE=$APP_CONFIG_FILE -Dlog4j2.configurationFile=file:$APP_LOG_FILE -jar $JAVA_JAR $FULL $TABLE_NAME
        #echo "`PrInfD` : java -Xss2m -D -DESM_CONFIG_FILE=$APP_CONFIG_FILE -Dlog4j2.configurationFile=file:$APP_LOG_FILE -jar $JAVA_JAR $FULL $TABLE_NAME"

elif [ $INCR_FLAG == $INCR ];then
        getLastIngestionDate
        echo "`PrInfD` : Java call for INCR LOAD for table $TABLE_NAME"
        java -Xss2m -D -DESM_CONFIG_FILE=$APP_CONFIG_FILE -Dlog4j2.configurationFile=file:$APP_LOG_FILE -jar $JAVA_JAR $INCR $TABLE_NAME $l_DATE
        #echo "`PrInfD` : java -Xss2m -D -DESM_CONFIG_FILE=$APP_CONFIG_FILE -Dlog4j2.configurationFile=file:$APP_LOG_FILE -jar $JAVA_JAR $INCR $TABLE_NAME $l_DATE"
fi


RC=$?
echo "`PrInfD` : Execution completed for table : "$TABLE_NAME
echo "`PrInfD` : Return code for API execution : "$RC

if [ $RC -ne 0 ];then
        echo "`PrErrD` : Error occurred while fetching data from the ESM API"
        echo "`PrErrD` : Error in API Fetch Script"
        echo "`PrErrD` : Error occurred while fetching data from the ESM API" >> ${LOG_DIR}/${g_APP_SYSTM_NM}_${APP_SSR_ID}_${TIME_STAMP}_${PID}.errormsg.txt
        exitScriptOnErrorESMwrapper
else
        echo " `PrInfD` : API Fetch Script Successful. Data Fetch completed."
        echo " `PrInfD` : ********************** End of pre-script *******************************************"
        if [ $FULL_DAY_FROM_FILE != "FULL" ];then
                echo " `PrInfD` : Updating LATEST_INGESTION_DATE as maxCLIDate value obtained from pre-script, in Audit ..."
                updtAdtTblForLtstIngstionDate
                echo -e "\n`PrInfD` : Successfully updated Ingestion dates"
        fi
        ##in case table is everyday FULL, we are skipping latest_date_ingestion update in audit
fi
