from airflow.models import BaseOperator
from airflow.models import Variable
import logging
import json
import subprocess
from datetime import datetime, timedelta


class CreateVMOperatorHook(BaseOperator):

    def __init__(self, sinet_flag: bool, *args, **kwargs):
        self.ssr_id = kwargs['op_kwargs']['ssrId']
        self.source_name = kwargs['op_kwargs']['sourceName']
        self.source_type = kwargs['op_kwargs']['sourceType']
        self.instance_count = kwargs['op_kwargs']['num-instances']
        self.category = kwargs['op_kwargs']['category']
        self.boot_disk_size = kwargs['op_kwargs']['boot-disk-size']
        self.memory = kwargs['op_kwargs']['custom-memory']
        self.disk_type = kwargs['op_kwargs']['boot-disk-type']
        self.cpu = kwargs['op_kwargs']['custom-cpu']
        self.schedule_group = kwargs['op_kwargs']['scheduleGroup']
        self.sinet_flag = sinet_flag
        super(CreateVMOperatorHook, self).__init__(*args, **kwargs)

    def execute(self, context):

        ssr_id = self.ssr_id
        source_name = self.source_name
        source_type = self.source_type
        sinet_flag = self.sinet_flag
        schedule_group = ssr_id + '-' + self.schedule_group
        container_registry_host = Variable.get("CONTAINER_REGISTRY_HOST")
        gcp_project = Variable.get("GCP_PROJECT_ID")
        image_tag = Variable.get("CONTAINER_IMAGE_TAG")
        startup_script_bucket = Variable.get("STARTUP_SCRIPT_BUCKET")

        zone = Variable.get("GCP_ZONE")
        if sinet_flag:
            subnet = Variable.get("CONTAINER_SUBNET_SINET")
        else:
            subnet = Variable.get("CONTAINER_SUBNET")
        container_mount_path = Variable.get("CONTAINER_MOUNT_PATH")
        container_host_path = Variable.get("CONTAINER_HOST_PATH")
        mode = Variable.get("CONTAINER_MODE")
        scope = Variable.get("CONTAINER_SCOPES")
        service_account = Variable.get("CONTAINER_SERVICE_ACCOUNT")

        instance_count = self.instance_count
        category = self.category

        logging.info(" ====== Creating compute engine machines ====== ")
        a = []
        if instance_count > 0:
            i = 1
            category = self.category
            disk_size = self.boot_disk_size
            memory = self.memory
            disk_type = self.disk_type
            cpu = self.cpu

            # instance_name = ssr_id + '-' + source_name + '-' + load_schedule + '-' + load_type + '-' + category + env + '-instance'
            instance_name = schedule_group + '-' + category + '-instance'
            startup_script_url = startup_script_bucket + '/' + ssr_id + '-' + source_name + '.sh'
            container_image = container_registry_host + '/' + \
                              gcp_project + '/' +  schedule_group + '-' + \
                              source_type + '-' + category + ':' + \
                              image_tag

            while i <= instance_count:
                instance = instance_name + '-' + str(i)
                create_command_gcloud = 'gcloud compute instances create-with-container ' + instance + ' --zone=' + \
                                        zone + ' --subnet=' + \
                                        subnet + ' --metadata=startup-script-url=' + startup_script_url + ' --maintenance-policy=MIGRATE --service-account=' + \
                                        service_account + ' --container-image=' + container_image + \
                                        ' --container-restart-policy=always --container-mount-host-path=mount-path=' + \
                                        container_mount_path + ',host-path=' + \
                                        container_host_path + ',mode=' + \
                                        mode + '  --custom-cpu=' + cpu + ' --custom-memory=' + memory + ' --tags=' + \
                                        ssr_id + "," + source_name + "," + schedule_group + \
                                        '  --boot-disk-size=' + disk_size + ' --boot-disk-type=' + disk_type + ' --scopes=' + \
                                        scope + ' --format="json"' + ' --no-address'
                try:
                    output_response = subprocess.check_output(['bash', '-c', create_command_gcloud])
                    metadata_dict = json.loads(
                        str(output_response.strip().decode().splitlines()).replace("['[", "").replace("', '",
                                                                                                      "").replace(
                            "]']", ""))
                    logging.info("===========" + str(metadata_dict))
                    creation_time = self.convert_pacific_time(metadata_dict['creationTimestamp'])
                    logging.info(creation_time)
                    info = {"id": metadata_dict['id'], "creationtime": str(creation_time),
                            "instancename": metadata_dict['name'], "ssrid": ssr_id
                        , "sourcename": source_name,
                            "schedulegroup": schedule_group,
                            "status": metadata_dict['status'], "remarks": "Compute machine created successfully"}
                    logging.info("---------------" + str(info))
                    a.append(info)

                except subprocess.CalledProcessError as e:
                    time_create = str(datetime.now())
                    info = {"id": "", "creationtime": time_create, "instancename": instance
                        , "ssrid": ssr_id,
                            "sourcename": source_name, "schedulegroup": schedule_group,
                            "status": 'FAILED', "remarks": (e.__getattribute__('output')).strip().decode()}
                    a.append(info)
                    if not (e.__getattribute__('returncode')) == 0:
                        logging.info("---- Creation of compute engine machine failed with return code :  " + str(
                            e.__getattribute__('returncode')))
                        raise ValueError("---- Creation of compute engine machine failed with return code :  " + str(
                            e.__getattribute__('returncode')))
                i = i + 1
        else:
            info = {'instance_count': 0}
            a.append(info)
        context['ti'].xcom_push(key=schedule_group + '-' + category,
                                value=a)

    def convert_pacific_time(self,pacific_time):
        str_pacific_time = pacific_time[::-1].replace(':', '', 1)[::-1]
        try:
            offset = int(str_pacific_time[-5:])
        except:
            logging.info("Error converting pacific time")
        delta = timedelta(hours=offset / 100)
        time = datetime.strptime(str_pacific_time[:-5], "%Y-%m-%dT%H:%M:%S.%f")
        time -= delta  # reduce the delta from this time object
        utc_time = time.strftime("%Y-%m-%d %H:%M:%S")
        return utc_time
