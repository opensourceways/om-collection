import json
import requests
import urllib3
import time

from collect.gitee_v8 import GiteeClient
from data.common import ESClient

urllib3.disable_warnings()

GITEE_API_URL = "https://gitee.com/oauth"
EXPIRES_IN = 86400


class GiteeToken(object):

    def __init__(self, config=None):
        self.config = config
        self.index_name_token = config.get('index_name_token')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.ssl_verify = True
        self.session = requests.Session()
        self.esClient = ESClient(config)
        self.headers = {'Content-Type': 'application/json;charset=UTF-8'}

        self.org = config.get('org')
        self.refresh_token = config.get('refresh_token')
        self.access_token = config.get('access_token')
        self.create_time = config.get('create_time')
        self.gitee_v8 = GiteeClient(self.org, self.access_token)

    def run(self, from_time):
        while True:
            self.is_refresh_token()
            time.sleep(10)
            token = self.esClient.get_access_token(self.index_name_token)
            print(token)

    def refresh_access_token(self):
        """Send a refresh post access to the Gitee Server"""
        self.refresh_token = self.esClient.get_access_token(self.index_name_token)[1]
        if self.refresh_token:
            res = self.gitee_v8.refresh_token(self.refresh_token)
            if 'access_token' in res.json():
                self.create_time = str(res.json().get('created_at'))
                self.access_token = res.json().get('access_token')
                self.refresh_token = res.json().get('refresh_token')
                time_array = time.localtime(int(self.create_time))
                str_date = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time_array)
                action = res.json()
                action.update({'created_date': str_date})
                indexData = {"index": {"_index": self.index_name_token, "_id": 'access_token'}}
                actions = json.dumps(indexData) + '\n'
                actions += json.dumps(action) + '\n'
                self.esClient.safe_put_bulk(actions)
                print("refresh ok!")

    def is_refresh_token(self):
        # 60s bias
        if time.time() > (int(self.create_time) + EXPIRES_IN - 60):
            print('refresh_access_token')
            self.refresh_access_token()
