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

        self.esClient = ESClient(config)
        self.session = requests.Session()
        self.headers = {'Content-Type': 'application/json'}

    def run(self, start=None):
        self.get_download()

    def get_download(self):
        pw = base64.b64decode(self.token).decode('utf-8')
        payload = {"password": pw}
        actions = ''
        response = requests.post(API_URL, data=json.dumps(payload), headers=self.headers, verify=False)
        if response.status_code == 200:
            data = response.json().get('data')
            update_time = response.json().get('data').get('update_time')

            index_data = {"index": {"_index": self.index_name, "_id": update_time}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(data) + '\n'
            self.esClient.safe_put_bulk(actions)
