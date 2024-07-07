""" 
# Pupose: Utility to call Dispatcher API
# Author: Indrajeet(igour)
"""
import sys
import json
import requests


class TriggerDispatcherAPI:
    def __init__(self, **kwargs):
        self.ssr_id = kwargs['source_infra_info']['ssr_id']
        self.schedule_group = kwargs['source_infra_info']['schedule_group']
        self.category = kwargs['source_infra_info']['category']
        self.dispatcher_authorization_token = kwargs['dispatcher_authorization_token']
        self.dispatcher_host = kwargs['dispatcher_host']

        self.restart_flag = kwargs['restart_flag']
        if self.restart_flag is None:
            self.restart_flag = "False"

    def execute(self):
        print("=======================| Trigger Dispatcher: START |======================")
        ssr_id = self.ssr_id
        schedule_group = self.schedule_group
        category = self.category
        rflag = self.restart_flag
        host = self.dispatcher_host
        dispatcher_token = self.dispatcher_authorization_token

        dispatcher_endpoint = host + '/api/v1/dispatch?ssrId=' + ssr_id + '&scheduleGroup=' \
            + schedule_group + '&volumeCategory=' + \
            category + '&restartFlag=' + rflag
        print("Dispatcher endpoint used: ", dispatcher_endpoint)
        headers = {'Authorization': 'Basic %s' % dispatcher_token,
                   'Content-Type': 'application/json'}

        # Call dispatcher endpoint
        dispatcher_response = requests.get(
            url=dispatcher_endpoint, headers=headers)
        # todo: throw exception for api call
        print("Response body from Dispatcher API: " +
              str(dispatcher_response.text))
        print("With response code: ", str(dispatcher_response.status_code))
        print("=======================| Trigger Dispatcher: END |======================")
        print(dispatcher_response.status_code)


# Set properties from rundeck options values
source_infra_info = json.loads(sys.argv[1].replace("'", "\""))
restart_flag = sys.argv[2]
dispatcher_authorization_token = sys.argv[3]
dispatcher_host = sys.argv[4]

if __name__ == '__main__':
    trigger_dispatcher = TriggerDispatcherAPI(
        source_infra_info=source_infra_info,
        restart_flag=restart_flag,
        dispatcher_authorization_token=dispatcher_authorization_token,
        dispatcher_host=dispatcher_host
    )

    trigger_dispatcher.execute()

