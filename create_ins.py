"""
# Pupose: Create instance utility
# Author: Indrajeet(igour)
"""
import json
import subprocess
import sys
from datetime import datetime, timedelta

import prepare_infra_audit


class CreateInstance:
    def __init__(self, **kwargs):
        self.ssr_id = kwargs['source_infra_info']['ssr_id']
        self.source_name = kwargs['source_infra_info']['source_name']
        self.source_type = kwargs['source_infra_info']['source_type']
        self.instance_count = kwargs['source_infra_info']['num_of_instances']
        self.category = kwargs['source_infra_info']['category']
        self.boot_disk_size = kwargs['source_infra_info']['disk_size']
        self.memory = kwargs['source_infra_info']['memory']
        self.disk_type = kwargs['source_infra_info']['disk_type']
        self.cpu = kwargs['source_infra_info']['num_of_cpu']
        self.schedule_group = kwargs['source_infra_info']['schedule_group']
        self.postgre_info_list = kwargs['postgre_info_list']
        self.infra_dtls = kwargs['infra_dtls']

    def execute(self):
        ssr_id = self.ssr_id
        source_name = self.source_name
        source_type = self.source_type
        schedule_group = ssr_id + '-' + self.schedule_group
        instance_count = self.instance_count

        infra_dtls = self.infra_dtls
        container_registry_host = infra_dtls['container_registry_host']
        gcp_project = "slb-it-hdf-" + infra_dtls['env_type']
        image_tag = infra_dtls['image_tag']
        startup_script_bucket = infra_dtls['startup_script_bucket']

        zone = infra_dtls['zone']
        subnet = infra_dtls['container_subnet_sinet']

        container_mount_path = infra_dtls['container_mount_path']
        container_host_path = infra_dtls['container_host_path']
        scope = infra_dtls['scope']
        service_account = infra_dtls['service_account']
        impersonate_srv_acc = infra_dtls['impersonate_srv_acc']
        global_vm_labels = infra_dtls['global_vm_labels']

        print(" ====== Creating compute engine machines ====== ")
        if instance_count > 0:
            curr_instance_num = 1
            category = self.category
            disk_size = self.boot_disk_size
            memory = self.memory
            disk_type = self.disk_type
            cpu = self.cpu
            memory = memory.replace("GB","")
            memory = 1024 * int(memory)                        #converting into MBs

            instance_name = schedule_group + '-' + category + '-instance'
            startup_script_url = startup_script_bucket + \
                                 '/' + ssr_id + '-' + source_name + '.sh'
            container_image = container_registry_host + '/' + \
                              gcp_project + '/' + schedule_group + '-' + \
                              source_type + '-' + category + ':' + \
                              image_tag

            print("startup_script_url: ", startup_script_url)
            print("container_image: ", container_image)

            while curr_instance_num <= instance_count:
                instance = instance_name + '-' + str(curr_instance_num)
                create_command_gcloud = 'gcloud compute instances create-with-container ' + instance + ' --zone=' + \
                                        zone + ' --subnet=' + subnet + ' --metadata=startup-script-url=' + startup_script_url + \
                                        ',serial-port-logging-enable=true' + ' --maintenance-policy=MIGRATE --service-account=' + \
                                        service_account + ' --container-image=' + container_image + \
                                        ' --container-restart-policy=always --container-mount-host-path=mount-path=' + \
                                        container_mount_path + ',host-path=' + container_host_path + ',mode=rw' + \
                                        '  --machine-type=' + 'n2d-custom-'+ str(cpu) + '-'+ str(memory) + ' --tags=' + \
                                        ssr_id + "," + source_name + "," + schedule_group + '  --boot-disk-size=' + \
                                        disk_size + ' --boot-disk-type=' + disk_type + ' --scopes=' + scope + \
                                        ' --labels ' + global_vm_labels + ',ssr_id=' + ssr_id + ',source_name=' + source_name + ',schedule_group=' + schedule_group + \
                                        ' --format="json" --no-address' + ' --impersonate-service-account=' + impersonate_srv_acc
                try:
                    output_response = subprocess.check_output(
                        ['bash', '-c', create_command_gcloud])

                    # formatting the response
                    metadata_dict = json.loads(str(output_response.strip().decode(
                    ).splitlines()).replace("['[", "").replace("', '", "").replace("]']", ""))
                    creation_time = convert_pacific_time(
                        metadata_dict['creationTimestamp'])

                    print(
                        "Compute machine created successfully, with status: ", metadata_dict['status'])
                    print("Extra details are as follows:")
                    print("instance id: ", metadata_dict['id'])
                    print("creation time: ", str(creation_time))
                    print("instance name: ", metadata_dict['name'])
                    print("for ssr: ", ssr_id, ", source name: ",
                          source_name, " and schedule group: ", schedule_group)

                    # Calling Infra Audit method
                    self.call_infra_audit(metadata_dict, creation_time)

                except subprocess.CalledProcessError as e:
                    time_create = str(datetime.now())
                    print(
                        "Creation of compute engine machine failed with status: FAILED")
                    print("Extra details are as follows:")
                    print("creation time: ", time_create)
                    print("instance name: ", instance)
                    print("for ssr: ", ssr_id, ", source name: ",
                          source_name, " and schedule group: ", schedule_group)
                    print("Remarks: ", (e.__getattribute__(
                        'output')).strip().decode())

                    if not (e.__getattribute__('returncode')) == 0:
                        print("---- Creation of compute engine machine failed with return code :  " + str(
                            e.__getattribute__('returncode')))
                        raise ValueError("---- Creation of compute engine machine failed with return code :  " + str(
                            e.__getattribute__('returncode')))
                    exit(-1)

                curr_instance_num = curr_instance_num + 1

        print("Current instance count:")
        print("instance_count = ", instance_count)

    def call_infra_audit(self, metadata_dict, creation_time):
        value_to_insert = []
        value_to_insert.insert(0, metadata_dict['id'])
        value_to_insert.insert(1, self.ssr_id)
        value_to_insert.insert(2, self.source_name)
        value_to_insert.insert(3, self.schedule_group)
        value_to_insert.insert(4, metadata_dict['name'])
        value_to_insert.insert(5, creation_time)
        value_to_insert.insert(6, metadata_dict['status'])
        value_to_insert.insert(7, 'Compute machine created successfully')

        print("Values inserting into infra audit tbl: ", value_to_insert)
        prepare_infra_audit.insert_infra_audit(
            value_to_insert, self.postgre_info_list)


def convert_pacific_time(pacific_time):
    str_pacific_time = pacific_time[::-1].replace(':', '', 1)[::-1]
    try:
        offset = int(str_pacific_time[-5:])
    except Exception as e:
        print("Error converting pacific time", e)
    delta = timedelta(hours=offset / 100)
    time = datetime.strptime(str_pacific_time[:-5], "%Y-%m-%dT%H:%M:%S.%f")
    time -= delta  # reduce the delta from this time object
    utc_time = time.strftime("%Y-%m-%d %H:%M:%S")
    return utc_time


# Set properties from Rundeck options values
source_infra_info = json.loads(sys.argv[1].replace("'", "\""))

postgre_user = sys.argv[2]
postgre_pass = sys.argv[3]
postgre_host = sys.argv[4]
postgre_port = sys.argv[5]
postgre_dbname = sys.argv[6]
postgre_info_list = dict(postgre_user=postgre_user, postgre_pass=postgre_pass, postgre_host=postgre_host,
                         postgre_port=postgre_port, postgre_dbname=postgre_dbname)

zone = sys.argv[7]
container_registry_host = sys.argv[8]
image_tag = sys.argv[9]
container_subnet = sys.argv[10]
container_mount_path = sys.argv[11]
container_host_path = sys.argv[12]
scope = sys.argv[13]
# dispatcher_host = sys.argv[14]
subnet_sinet = sys.argv[15]
service_account = sys.argv[16]
env_type = sys.argv[17]
startup_script_bucket = sys.argv[18]
impersonate_srv_acc = sys.argv[19]
global_vm_labels = sys.argv[20]

infra_dtls = dict(zone=zone, container_registry_host=container_registry_host, image_tag=image_tag,
                  container_subnet=container_subnet, container_mount_path=container_mount_path,
                  container_host_path=container_host_path, scope=scope, container_subnet_sinet=subnet_sinet,
                  service_account=service_account, env_type=env_type, startup_script_bucket=startup_script_bucket,
                  impersonate_srv_acc=impersonate_srv_acc, global_vm_labels=global_vm_labels)

if __name__ == '__main__':
    print("=======================| Create Instance: START |======================")
    # execute the create instance command
    create_compute_machine_hook = CreateInstance(
        source_infra_info=source_infra_info,
        postgre_info_list=postgre_info_list,
        infra_dtls=infra_dtls
    )

    create_compute_machine_hook.execute()
    print("=======================| Create Instance: END |======================")

