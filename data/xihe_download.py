#  Copyright (c) 2022.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import base64
import json

import requests

from data.common import ESClient

API_URL = "https://xihebackend.mindspore.cn/api/base/statistics/"


class XiheDown(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.token = config.get('token')
        self.count_type = config.get('count_type')

        self.esClient = ESClient(config)
        self.session = requests.Session()
        self.headers = {'Content-Type': 'application/json'}

    def run(self, start=None):
        if self.count_type:
            # types = self.count_type.split(',')
            for count_type in self.count_type.split(','):
                self.get_download(count_type)

    def get_download(self, count_type):
        pw = base64.b64decode(self.token).decode('utf-8')
        payload = {
            "password": pw,
            "request_type": count_type
        }
        actions = ''
        response = requests.post(API_URL, data=json.dumps(payload), headers=self.headers, verify=False)
        if response.status_code == 200:
            data = response.json().get('data').get(count_type)
            update_time = response.json().get('data').get('update_time')
            action = {
                count_type: data,
                'update_time': update_time
            }

            index_data = {"index": {"_index": self.index_name, "_id": count_type + update_time}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
            print(actions)
            self.esClient.safe_put_bulk(actions)
