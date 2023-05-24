#  Copyright (c) 2023.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import json
import re
import time

from data.common import ESClient


class EurOpenEulerDownload(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.query = config.get('query')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.url_base_regex = config.get('url_base_regex')
        self.spider_base_regex = config.get('spider_base_regex')
        self.log_url = config.get('log_es_url')
        self.log_authorization = config.get('log_authorization')
        self.esClient = ESClient(config)

    def run(self, start):
        startTime = time.time()
        self.collect_download()
        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S", time.gmtime(endTime - startTime))
        print("Collect eur download data finished after %s" % spent_time)

    def deal_log(self, data):
        actions = ''
        for json_line in data:
            log = json_line.get("_source").get("log")
            logs = log.split('"')
            try:
                action = self.is_valid_url(logs[1].strip())
                if action is None:
                    continue
                status = logs[2].strip()
                if status.startswith('404'):
                    continue
                agent = logs[7].strip()
                if not self.is_valid_agent(agent):
                    continue
                created_at = json_line.get("_source").get("@timestamp")
                action.update({'created_at': created_at})
                index_id = json_line.get("_id")
                index_data = {"index": {"_index": self.index_name, "_id": index_id + created_at}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'
            except IndexError as e:
                print(e)
        self.esClient.safe_put_bulk(actions)

    def is_valid_url(self, url):
        rpm_url_regex = re.compile(self.url_base_regex, re.IGNORECASE)
        if not rpm_url_regex.match(url):
            return
        urls = url.split('/')
        info = {
            'owner': urls[2],
            'project': urls[3],
            'rpm': urls[6].split('.rpm')[0],
        }
        return info

    def is_valid_agent(self, agent):
        spider_regex = re.compile(self.spider_base_regex, re.IGNORECASE)
        if agent.startswith('Mock'):
            return False
        if spider_regex.match(agent):
            return False
        return True

    def scroll_download_log(self, url, _headers, search=None, scroll_duration='1m'):
        es_url = url + '/_search?scroll=' + scroll_duration
        res = self.esClient.request_get(url=es_url, headers=_headers, data=search.encode('utf-8'))
        if res.status_code != 200:
            print('requests error')
            return
        res_data = res.json()
        data = res_data['hits']['hits']
        print('scroll data count: %s' % len(data))
        self.deal_log(data)

        scroll_id = res_data['_scroll_id']
        while scroll_id is not None and len(data) != 0:
            es_url = url + '/_search/scroll'
            search = '''{
                "scroll": "%s",
                "scroll_id": "%s"
            }''' % (scroll_duration, scroll_id)
            res = self.esClient.request_get(url=es_url, headers=_headers, data=search.encode('utf-8'))
            if res.status_code != 200:
                print('requests error')
            res_data = res.json()
            scroll_id = res_data['_scroll_id']
            data = res_data['hits']['hits']
            print('scroll data count: %s' % len(data))
            self.deal_log(data)
        print('scroll over')

    def collect_download(self):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.log_authorization
        }
        data_json = '''{
            "size": 2000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "kubernetes.labels.component.keyword": "copr-backend"
                            }
                        },
                        {
                            "term": {
                                "kubernetes.container_name.keyword": "httpd"
                            }
                        },
                        {
                            "term": {
                                "kubernetes.namespace_name.keyword": "fedora-copr-prod"
                            }
                        }
                    ]
                }
            }
        }'''
        self.scroll_download_log(url=self.log_url, _headers=headers, search=data_json)
