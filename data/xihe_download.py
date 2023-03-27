#  Copyright (c) 2022.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import json
import requests
from data.common import ESClient

retry_times = 3


class XiheDown(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.count_type = config.get('count_type')
        self.type = config.get('type')
        self.model_name = config.get('model_name')
        self.api_url = config.get('api_url')

        self.esClient = ESClient(config)
        self.session = requests.Session()
        self.headers = {'Content-Type': 'application/json'}
        self.retry_cnt = 0

    def run(self, start=None):
        actions = ''
        if self.count_type:
            for count_type in self.count_type.split(','):
                actions += self.get_download(self.api_url, count_type)
        if self.type:
            for t in self.type.split(','):
                base_url = self.api_url + 'd1/'
                actions += self.get_download(base_url, t)
        if self.model_name:
            for name in self.model_name.split(','):
                base_url = self.api_url + 'd1/bigmodel/'
                actions += self.get_download(base_url, name)
        print(actions)
        self.esClient.safe_put_bulk(actions)

    def get_download(self, base_url, count_type):
        url = base_url + count_type
        actions = ''
        # response = requests.get(url=url, headers=self.headers, verify=False)
        response = self.get_api(url)
        try:
            if response.status_code == 200:
                res = response.json().get('data')
                update_time = res.get('update_at')
                action = {
                    'update_time': update_time,
                    count_type: res.get('counts'),
                    'is_project_internal_user': 0
                }
                if count_type in self.model_name:
                    action.update({'model': count_type})
                    action.update({'counts': res.get('counts')})

                index_data = {"index": {"_index": self.index_name, "_id": count_type + update_time}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'
        except AttributeError as e:
            print('Get api: ' + url + 'error')
        return actions

    def get_api(self, url):
        try:
            response = requests.get(url=url, headers=self.headers, verify=False, timeout=60)
            if response.status_code != 200 and self.retry_cnt < retry_times:
                self.retry_cnt += 1
                print({'url': url, 'code': response.status_code, 'retry': self.retry_cnt})
                response = self.get_api(url)
        except requests.exceptions.RequestException as e:
            while self.retry_cnt < retry_times:
                try:
                    self.retry_cnt += 1
                    print('Retry ' + str(self.retry_cnt) + ' times: ' + url)
                    return self.get_api(url)
                finally:
                    pass
        except Exception as e:
            raise e
        else:
            self.retry_cnt = 0
            return response


