import requests
import logging
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class TriggerDispatcherAPIHook(BaseOperator):

    def __init__(self,  restart_flag: str, *args, **kwargs):
        self.ssr_id = kwargs['op_kwargs']['ssrId']
        self.source_name = kwargs['op_kwargs']['sourceName']
        self.schedule_group=kwargs['op_kwargs']['scheduleGroup']
        self.category = kwargs['op_kwargs']['category']
        if restart_flag is None:
            restart_flag = "False"
        self.restart_flag = restart_flag
        super(TriggerDispatcherAPIHook, self).__init__(*args, **kwargs)

    def execute(self, context):
        ssr_id = self.ssr_id
        source_name = self.source_name
        schedule_group = self.schedule_group
        schedule_group_dispatcher =  schedule_group
        category = self.category
        restart_flag = self.restart_flag
        dispatcher_key = ssr_id+'-'+schedule_group_dispatcher + '-' + category +  '-dispatch'
        DISPATCHER_HOST = Variable.get('DISPATCHER_HOST')
        DISPATCHER_TOKEN = Variable.get('DISPATCHER_AUTHORIZATION_TOKEN')

        url = DISPATCHER_HOST + '/api/v1/dispatch?ssrId=' + ssr_id + '&scheduleGroup=' + schedule_group_dispatcher + '&volumeCategory=' + category + '&restartFlag=' + restart_flag
        # defining a headers dict for the parameters to be sent to the API
        headers = {'authorization': DISPATCHER_TOKEN, 'Content-Type': 'application/json'}
        # sending get request and saving the response as response object
        logging.info(url)
        r = requests.get(url=url, headers=headers)
        logging.info("Dispacther Response : "+str(r.text))
        logging.info("Dispacther Response Code : "+str(r.status_code))
        context['ti'].xcom_push(key=dispatcher_key, value=r.status_code)