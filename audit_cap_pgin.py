import logging
import subprocess
import psycopg2
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class CaptureInfraAuditHook(BaseOperator):

    def __init__(self, delete_flag: bool, *args, **kwargs):
        self.ssr_id = kwargs['op_kwargs']['ssrId']
        self.source_name = kwargs['op_kwargs']['sourceName']
        self.schedule_group = kwargs['op_kwargs']['scheduleGroup']
        self.category = kwargs['op_kwargs']['category']
        self.delete_flag = delete_flag
        super(CaptureInfraAuditHook, self).__init__(*args, **kwargs)

    def execute(self, context):

        ssr_id = self.ssr_id
        category = self.category
        delete_flag = self.delete_flag
        schedule_group = ssr_id + '-' + self.schedule_group
        task_id_create_machine = 'create_compute_engine_machines_with_' + category + '_volume'
        create_instances_key = schedule_group + '-' + category
        infra_details = context['ti'].xcom_pull(task_ids=task_id_create_machine,key=create_instances_key, include_prior_dates=True)
        connection = None
        try:
            POSTGRES_CONNECTION = 'POSTGRES_AUDIT'
            AUDIT_SCHEMA = Variable.get('AUDIT_DB')
            postgres_insert_query = """insert into s2hingestionaudits.composer_infra_audit_details (id,ssr_id,source_name,schedule_group,instance_name,instance_creation_time,status,remarks) values (%s,%s,%s,%s,%s,%s, %s,%s)
                                    """

            connection = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION, schema=AUDIT_SCHEMA).get_conn()
            dest_cursor = connection.cursor()

            for record in infra_details:
                if delete_flag:
                    dispatcher_key = schedule_group + '-' + category + '-dispatch'
                    dispatcher_task_id= 'trigger_dispatcher_' + category + '_category'
                    dispatcher_return_code = int(context['ti'].xcom_pull(task_ids=dispatcher_task_id,key=dispatcher_key))
                    record_to_insert = (
                        record['id'], record['ssrid'], record['sourcename'], record['schedulegroup'],
                        record['instancename'],
                        record['creationtime'], 'DELETED',
                        'Instance deleted as dispatcher response was : ' + str(dispatcher_return_code))
                else:
                    logging.info('====inserting records to infra audit====')
                    logging.info(str(record))
                    record_to_insert = (
                        record['id'], record['ssrid'], record['sourcename'], record['schedulegroup'],
                        record['instancename'],
                        record['creationtime'], record['status'], record['remarks'])
                dest_cursor.execute(postgres_insert_query, record_to_insert)
                logging.info('====insert query executed====')
                connection.commit()
                logging.info('====connection commited ====')
                count = dest_cursor.rowcount
                logging.info(str(count) + '  ===Infrastructure audit records inserted=== ')

        except (Exception, psycopg2.DatabaseError) as error:
            logging.info(error)
        finally:
            # closing database connection.
            if connection:
                dest_cursor.close()
                connection.close()
                logging.info("=== Cloud sql connection closed ===")
