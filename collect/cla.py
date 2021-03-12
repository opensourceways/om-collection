import json

import requests

AUTH = 'auth'


class ClaClient(object):

    def __init__(self, config):
        self.api_url = config.get('cla_api_url', 'https://clasign.osinfra.cn/api/v1')
        self.platform = config.get('platform')
        self.username = config.get('cla_username')
        self.password = config.get('cla_password')
        self.timeout = config.get('timeout', 60)

    def get_token_cla(self):
        data = json.dumps({'password': self.password, 'username': self.username})
        auth_url = f'{self.api_url}/{AUTH}/{self.platform}'
        token_info = self.fetch_cla(method='post', url=auth_url, data=data)
        token = token_info['data']['access_token']
        return token

    def fetch_cla(self, method='get', url=None, headers=None, data=None):
        req = requests.request(method, url=url, data=data, headers=headers, timeout=self.timeout)
        if req.status_code != 200:
            print("cla api error: ", req.text)
        res = json.loads(req.text)
        return res
