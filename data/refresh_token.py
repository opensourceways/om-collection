import json
import requests
import urllib3
import time

from collect.gitee_v8 import GiteeClient
from data.common import ESClient
from collect.baidutongji import BaiDuTongjiClient


urllib3.disable_warnings()

EXPIRES_IN = 86400


class RefreshToken(object):

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
        self.service_refresh_token = json.loads(config.get('service_refresh_token'))
        self.create_time = config.get('create_time')
        

    def run(self, from_time):
        while True:
            self.is_refresh_token()
            time.sleep(10)
            services = self.esClient.get_access_token(self.index_name_token)
            print(".....services=", services)

    def refresh_access_token(self):
        """Send a refresh post access to the Gitee Server"""
        services_tokens = self.esClient.get_access_token(self.index_name_token)
        if not services_tokens:
            services_tokens = self.service_refresh_token

        for i in range(len(services_tokens)):
            st = services_tokens[i]
            access_token = st.get("access_token")
            rt = st.get("refresh_token")
            service_name = st.get("service")
            if rt is None:
                continue

            if "giteev8" in service_name:
                gitee_v8 = GiteeClient(self.org, access_token)
                res = gitee_v8.refresh_token(rt)
            elif "baidutongji" in service_name:
                baiduClient = BaiDuTongjiClient(self.config)
                res = baiduClient.refresh_access_token(rt, st.get("client_id"), st.get("client_secret"))
                print("..baidutongji..res=", res)

            if 'access_token' in res.json():
                self.create_time = time.time()
                time_array = time.localtime(int(self.create_time))
                str_date = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time_array)

                action = res.json()
                action.update({'created_at': str_date})
                action.update({'service': service_name})
                action.update({'client_id': st.get("client_id")})
                action.update({'client_secret': st.get("client_secret")})
                indexData = {"index": {"_index": self.index_name_token, "_id": service_name}}
                actions = json.dumps(indexData) + '\n'
                actions += json.dumps(action) + '\n'

                self.esClient.safe_put_bulk(actions)

                print("refresh ok!")

    def is_refresh_token(self):
        # 60s bias
        if time.time() > (int(self.create_time) + EXPIRES_IN - 60):
            print('star to refresh access token ...')
            self.refresh_access_token()
