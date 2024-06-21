#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2020-05
#
import datetime
import json
import time

import dateutil.relativedelta

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
        self.collect_from_time = config.get('collect_from_time', 'false')

    def run(self, from_time):
        date_now = time.strftime("%Y.%m.%d", time.localtime())
        if self.last_run_date == date_now:
            print("has been executed today")
            return
        self.last_run_date = date_now
        if self.collect_from_time == 'true':
            self.get_data_from_time(from_time)
        else:
            date_yesterday = (datetime.datetime.now() - datetime.timedelta(days=self.before_day)).strftime("%Y.%m.%d")
            source_index_name = self.source_index_name_head + '-' + date_yesterday
            self.get_data_by_day(source_index_name)

    def get_data_by_day(self, source_index_name):
        search = '''{
                    "size": 1000,
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

    def get_data_from_time(self, from_time):
        start_time = datetime.datetime.strptime(from_time, "%Y%m%d")
        to = datetime.datetime.today().strftime("%Y.%m.%d")
        while start_time.strftime("%Y.%m.%d") < to:
            source_index_name = self.source_index_name_head + '-' + start_time.strftime("%Y.%m.%d")
            self.get_data_by_day(source_index_name)
            start_time += dateutil.relativedelta.relativedelta(days=1)

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
