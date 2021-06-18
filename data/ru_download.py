import datetime
import json
import time

from data.common import ESClient


class RuDownload(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.before_day = int(config.get('before_day', 1))
        self.source_index_name_head = config.get('source_index_name_head')
        self.target_es_url = config.get('target_es_url')
        self.target_authorization = config.get('target_authorization')
        self.target_index_name = config.get('target_index_name')
        self.vhost = config.get('vhost')
        self.last_run_date = None

    def run(self, from_time):
        print(time.localtime())
        date_now = time.strftime("%Y.%m.%d", time.localtime())
        if self.last_run_date == date_now:
            print("has been executed today")
            return
        self.last_run_date = date_now
        date_yesterday = (datetime.datetime.now() - datetime.timedelta(days=self.before_day)).strftime("%Y.%m.%d")
        source_index_name = self.source_index_name_head + '-' + date_yesterday
        print(source_index_name)

        search = '''{
                      "size":10,
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "vhost.keyword": "%s"
                              }
                            }
                          ]
                        }
                      }
                    }''' % self.vhost
        self.esClient.scrollSearch(index_name=source_index_name, search=search, scroll_duration='2m',
                                   func=self.processingHits)

    def processingHits(self, hits):
        actions = ''
        for data in hits:
            id = data['_id']
            source_data = data['_source']
            path = str(source_data['path'])
            if path.endswith('.iso') or path.endswith('.rpm'):
                log = json.loads(source_data['log'])

                data_res = {
                    "created_at": log['time'],
                    "http_range": log['http_range'],
                    "bytes_sent": log['bytes_sent'],
                    "status": log['status'],
                    "hostname": log['vhost'],
                    "path": path
                }
                if path.endswith('.iso'):
                    data_res.update({"is_iso_download": 1})
                else:
                    data_res.update({"is_rpm_download": 1})

                indexData = {"index": {"_index": self.target_index_name, "_id": id}}
                actions += json.dumps(indexData) + '\n'
                actions += json.dumps(data_res) + '\n'

        header = {
            "Content-Type": 'application/x-ndjson',
            'Authorization': self.target_authorization
        }
        url = self.target_es_url
        self.esClient.safe_put_bulk(bulk_json=actions, header=header, url=url)
