from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import subprocess
from airflow.models import Variable


class DeleteVMOperatorHook(BaseOperator):

    def __init__(self, *args, **kwargs):
        self.ssr_id = kwargs['op_kwargs']['ssrId']
        self.source_name = kwargs['op_kwargs']['sourceName']
        self.source_type = kwargs['op_kwargs']['sourceType']
        self.instance_count = kwargs['op_kwargs']['num-instances']
        self.category = kwargs['op_kwargs']['category']
        self.schedule_group = kwargs['op_kwargs']['scheduleGroup']
        super(DeleteVMOperatorHook, self).__init__(*args, **kwargs)

    def execute(self, context):
        ssr_id = self.ssr_id
        schedule_group = self.schedule_group
        category = self.category
        instance_count = self.instance_count
        schedule_group = ssr_id + '-' + schedule_group
        instance_name = schedule_group + '-' + category + \
                        '-instance'
        zone = Variable.get("GCP_ZONE")
        logging.info("Deleting created compute machines")
        if instance_count > 0:
            i = 1
            while i <= instance_count:
                instance = instance_name + '-' + str(i)
                delete_command_gcloud = 'gcloud compute instances delete ' + instance + ' --zone=' + zone
                try:
                    output = subprocess.check_output(['bash', '-c', delete_command_gcloud])
                except subprocess.CalledProcessError as e:
                    logging.error(e)
                i = i + 1