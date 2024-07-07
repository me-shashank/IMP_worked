
from datetime import datetime, timedelta

import requests
import logging
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators import bash_operator, python_operator
from create_compute_machine_plugin import CreateVMOperatorHook
from trigger_dispatcher_api_plugin import TriggerDispatcherAPIHook
from audit_capture_plugin import CaptureInfraAuditHook
from delete_compute_machine_plugin import DeleteVMOperatorHook

import subprocess
import psycopg2

# from airflow.operators import PythonOperator
                        
source_details= {'ssrId': 'ear-aa-8', 'sourceName': 'swps', 'sourceType': 'oracle', 'scheduleGroup': 'daily-full-load'}
low_instance_params= {'category': 'low', 'custom-cpu': '4', 'custom-memory': '16GB', 'boot-disk-size': '100GB', 'boot-disk-type': 'pd-ssd', 'num-instances': 3}
medium_instance_params= {'category': 'medium', 'custom-cpu': '4', 'custom-memory': '25GB', 'boot-disk-size': '200GB', 'boot-disk-type': 'pd-ssd', 'num-instances': 1}
high_instance_params= {'category': 'high', 'custom-cpu': '8', 'custom-memory': '32GB', 'boot-disk-size': '300GB', 'boot-disk-type': 'pd-ssd', 'num-instances': 5}

default_dag_args = {
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'owner': 'dataplatform'
}
 
env = Variable.get('INSTANCE_ENV')


restart_dag = 'False'
def convert_timestamp(item_date_object):
    if isinstance(item_date_object, (datetime.date, datetime.datetime)):
        return item_date_object.timestamp()


# Load the DAG configuration, setting a default if none is present
if restart_dag == "True":
    dag_id = source_details['ssrId'] + '-' + source_details['sourceName'] + '-' + source_details[
    'scheduleGroup'] + '-restart'
else:
    dag_id = source_details['ssrId'] + '-' + source_details['sourceName'] + '-' + source_details[
        'scheduleGroup']

dag = DAG(dag_id=dag_id,default_args=default_dag_args,catchup=False,schedule_interval='0 19 * * *') 

def branch_func(**kwargs):
    # request = curl_request
    # for key, value in kwargs.items():
    #     print("%s == %s" % (key, value))
    category = kwargs['category']
    dispatcher_task_id = 'trigger_dispatcher_' + category + '_category'

    dispatcher_key = kwargs['ssrId'] + '-' + kwargs['scheduleGroup'] + '-' + category  + '-dispatch'
    logging.info(dispatcher_task_id)
    ti = kwargs['ti']
    logging.info('---------------' + str(kwargs['ti']))
    # logging.info(kwargs[taskid])
    dispatcher_return_code = int(ti.xcom_pull(task_ids=dispatcher_task_id, key=dispatcher_key))
    if dispatcher_return_code == 200:
        return 'ingestion_schedule_success_' + kwargs['category']
    else:
        return 'delete_infrastructure_' + kwargs['category']


def branch_func_instance_creation(**kwargs):
    category = kwargs['category']
    instance_task_id = 'create_compute_engine_machines_with_' + category + '_volume'
    logging.info(instance_task_id)
    ti = kwargs['ti']
    logging.info('---------------' + str(kwargs['ti']))

    instance_key = kwargs['ssrId'] + '-' + kwargs['scheduleGroup'] + '-' + category
    xcom_value_intance_creation = ti.xcom_pull(task_ids=instance_task_id, key=instance_key)
    for xcom_result in xcom_value_intance_creation:
        logging.info(xcom_result)
        if xcom_result.get('instance_count') is not None:
            return 'skip_dispatcher_trigger_task_' + kwargs['category']
        else:
            return 'capture_infrastructure_audit_' + kwargs['category']
    
set_project = bash_operator.BashOperator(
    task_id='set_gcp_project',
    bash_command='gcloud config set project ' + Variable.get('GCP_PROJECT_ID'),
    dag=dag)

create_compute_engine_machines_with_low_volume = CreateVMOperatorHook(
    task_id='create_compute_engine_machines_with_low_volume',
    provide_context=True,
    op_kwargs={**source_details, **low_instance_params},
    sinet_flag=True,
    #env=env,
    dag=dag,
    xcom_push=True,
    retries=1)

create_compute_engine_machines_with_medium_volume = CreateVMOperatorHook(
    task_id='create_compute_engine_machines_with_medium_volume',
    provide_context=True,
    op_kwargs={**source_details, **medium_instance_params},
    xcom_push=True,
    sinet_flag=True,
    #env=env,
    dag=dag,
    retries=1)
create_compute_engine_machines_with_high_volume = CreateVMOperatorHook(
    task_id='create_compute_engine_machines_with_high_volume',
    provide_context=True,
    op_kwargs={**source_details, **high_instance_params},
    xcom_push=True,
    sinet_flag=True,
    #env=env,
    dag=dag,
    retries=1)

trigger_dispatcher_low = TriggerDispatcherAPIHook(
    task_id='trigger_dispatcher_low_category',
    provide_context=True,
    xcom_push=True,
    op_kwargs={**source_details, **low_instance_params},
    restart_flag=restart_dag,
    dag=dag,
    retries=1)
trigger_dispatcher_medium = TriggerDispatcherAPIHook(
    task_id='trigger_dispatcher_medium_category',
    provide_context=True,
    xcom_push=True,
    restart_flag=restart_dag,
    dag=dag,
    op_kwargs={**source_details, **medium_instance_params},
    retries=1)
trigger_dispatcher_high = TriggerDispatcherAPIHook(
    task_id='trigger_dispatcher_high_category',
    provide_context=True,
    xcom_push=True,
    restart_flag=restart_dag,
    op_kwargs={**source_details, **high_instance_params},
    dag=dag,
    retries=1)

delete_instance_on_dispatcher_failure_low = DeleteVMOperatorHook(
    task_id='delete_infrastructure_low',
    op_kwargs={**source_details, **low_instance_params},
    #env=env,
    dag=dag)
delete_instance_on_dispatcher_failure_medium = DeleteVMOperatorHook(
    task_id='delete_infrastructure_medium',
    op_kwargs={**source_details, **medium_instance_params},
    #env=env,
    dag=dag)
delete_instance_on_dispatcher_failure_high = DeleteVMOperatorHook(
    task_id='delete_infrastructure_high',
    op_kwargs={**source_details, **high_instance_params},
    #env=env,
    dag=dag)

call_dispatcher_api_task_low = python_operator.BranchPythonOperator(
    task_id='call_dispatcher_api_task_low',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={**source_details, **low_instance_params},
    dag=dag)
call_dispatcher_api_task_medium = python_operator.BranchPythonOperator(
    task_id='call_dispatcher_api_task_medium',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={**source_details, **medium_instance_params},
    dag=dag)
call_dispatcher_api_task_high = python_operator.BranchPythonOperator(
    task_id='call_dispatcher_api_task_high',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={**source_details, **high_instance_params},
    dag=dag)

success_op_low = bash_operator.BashOperator(
    task_id='ingestion_schedule_success_low',
    bash_command="echo successfully dispatched messages",
    dag=dag)
success_op_medium = bash_operator.BashOperator(
    task_id='ingestion_schedule_success_medium',
    bash_command="echo successfully dispatched messages",
    dag=dag)
success_op_high = bash_operator.BashOperator(
    task_id='ingestion_schedule_success_high',
    bash_command="echo successfully dispatched messages",
    dag=dag)

capture_infrastructure_audit_low = CaptureInfraAuditHook(
    task_id='capture_infrastructure_audit_low',
    provide_context=True,
    op_kwargs={**source_details, **low_instance_params},
    delete_flag=False,
    dag=dag)
capture_infrastructure_audit_medium = CaptureInfraAuditHook(
    task_id='capture_infrastructure_audit_medium',
    provide_context=True,
    op_kwargs={**source_details, **medium_instance_params},
    delete_flag=False,
    dag=dag)
capture_infrastructure_audit_high = CaptureInfraAuditHook(
    task_id='capture_infrastructure_audit_high',
    provide_context=True,
    op_kwargs={**source_details, **high_instance_params},
    delete_flag=False,
    dag=dag)
capture_infrastructure_audit_delete_low = CaptureInfraAuditHook(
    task_id='capture_infrastructure_audit_delete_low',
    provide_context=True,
    op_kwargs={**source_details, **low_instance_params},
    delete_flag=True,
    dag=dag)
capture_infrastructure_audit_delete_medium = CaptureInfraAuditHook(
    task_id='capture_infrastructure_audit_delete_medium',
    provide_context=True,
    op_kwargs={**source_details, **medium_instance_params},
    delete_flag=True,
    dag=dag)
capture_infrastructure_audit_delete_high = CaptureInfraAuditHook(
    task_id='capture_infrastructure_audit_delete_high',
    provide_context=True,
    op_kwargs={**source_details, **high_instance_params},
    delete_flag=True,
    dag=dag)
delay_dispatcher_task_low = bash_operator.BashOperator(task_id="delay_dispatcher_task_low",
                                                       dag=dag,
                                                       bash_command="sleep 3m")
delay_dispatcher_task_medium = bash_operator.BashOperator(task_id="delay_dispatcher_task_medium",
                                                          dag=dag,
                                                          bash_command="sleep 3m")
delay_dispatcher_task_high = bash_operator.BashOperator(task_id="delay_dispatcher_task_high",
                                                        dag=dag,
                                                        bash_command="sleep 3m")

skip_dispatcher_trigger_task_low = bash_operator.BashOperator(
    task_id='skip_dispatcher_trigger_task_low',
    bash_command="echo dispatcher_not_triggered as instance count is 0 for this category",
    dag=dag)
skip_dispatcher_trigger_task_medium = bash_operator.BashOperator(
    task_id='skip_dispatcher_trigger_task_medium',
    bash_command="echo dispatcher_not_triggered as instance count is 0 for this category",
    dag=dag)
skip_dispatcher_trigger_task_high = bash_operator.BashOperator(
    task_id='skip_dispatcher_trigger_task_high',
    bash_command="echo dispatcher_not_triggered as instance count is 0 for this category",
    dag=dag)

decide_dispatcher_trigger_low = python_operator.BranchPythonOperator(
    task_id='decide_dispatcher_trigger_low',
    provide_context=True,
    python_callable=branch_func_instance_creation,
    op_kwargs={**source_details, **low_instance_params},
    dag=dag)
decide_dispatcher_trigger_medium = python_operator.BranchPythonOperator(
    task_id='decide_dispatcher_trigger_medium',
    provide_context=True,
    python_callable=branch_func_instance_creation,
    op_kwargs={**source_details, **medium_instance_params},
    dag=dag)
decide_dispatcher_trigger_high = python_operator.BranchPythonOperator(
    task_id='decide_dispatcher_trigger_high',
    provide_context=True,
    python_callable=branch_func_instance_creation,
    op_kwargs={**source_details, **high_instance_params},
    dag=dag)

set_project >> create_compute_engine_machines_with_low_volume >> decide_dispatcher_trigger_low >> [
    skip_dispatcher_trigger_task_low, capture_infrastructure_audit_low]
capture_infrastructure_audit_low >> delay_dispatcher_task_low >> trigger_dispatcher_low >> call_dispatcher_api_task_low >> [
    success_op_low, delete_instance_on_dispatcher_failure_low]

set_project >> create_compute_engine_machines_with_medium_volume >> decide_dispatcher_trigger_medium >> [
    skip_dispatcher_trigger_task_medium, capture_infrastructure_audit_medium]
capture_infrastructure_audit_medium >> delay_dispatcher_task_medium >> trigger_dispatcher_medium >> call_dispatcher_api_task_medium >> [
    success_op_medium, delete_instance_on_dispatcher_failure_medium]

set_project >> create_compute_engine_machines_with_high_volume >> decide_dispatcher_trigger_high >> [
    skip_dispatcher_trigger_task_high, capture_infrastructure_audit_high]
capture_infrastructure_audit_high >> delay_dispatcher_task_high >> trigger_dispatcher_high >> call_dispatcher_api_task_high >> [
    success_op_high, delete_instance_on_dispatcher_failure_high]

delete_instance_on_dispatcher_failure_low >> capture_infrastructure_audit_delete_low
delete_instance_on_dispatcher_failure_medium >> capture_infrastructure_audit_delete_medium
delete_instance_on_dispatcher_failure_high >> capture_infrastructure_audit_delete_high
    
