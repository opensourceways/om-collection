#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2024/10/28
import json
from datetime import datetime, timedelta

from data.common import ESClient


class ImageDownload(object):
    def __init__(self, config):
        self.config = config
        self.index_name = config.get('index_name')
        self.collections = config.get('collections')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.from_date = config.get('from_date')
        self.func_map = {
            'dockerhub': self.reindex_docker,
            'swr': self.reindex_swr,
            'combine': self.reindex_combine
        }

    def run(self, start):
        collections = json.loads(self.collections)
        for coll in collections['collections']:
            print('start: ', coll['origin'])
            func = self.func_map.get(coll['origin'])
            self.combine_image_download(coll['query_index_name'], coll['time_field'], func)
            print('over: ', coll['origin'])

    def combine_image_download(self, query_index_name, time_field, func):
        from_date = datetime.strptime(self.from_date, "%Y%m%d") if self.from_date else datetime.today()
        while from_date.strftime("%Y%m%d") <= datetime.today().strftime("%Y%m%d"):
            from_time = from_date.strftime("%Y-%m-%d")
            query = '''{
                "size": 1000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "%s": {
                                        "gte": "%s",
                                        "lte": "%s"
                                    }
                                }
                            },
                            {
                                "exists": {
                                    "field": "repo.keyword"
                                }
                            }
                        ]
                    }
                }
            }''' % (time_field, from_time, from_time)
            from_date += timedelta(days=1)
            self.esClient.scrollSearch(query_index_name, search=query, func=func)

    def reindex_docker(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            action = {
                'created_at': source.get('metadata__updated_on'),
                'repo': source.get('repo'),
                'namespace': source.get('owner'),
                'download': source.get('pull_count'),
                'origin': 'dockerhub'
            }
            action['unique'] = action['repo'] + '-' + action['namespace'] + '-' + action['origin']
            doc_id = action['origin'] + hit['_id']
            index_data = {'index': {'_index': self.index_name, '_id': doc_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def reindex_swr(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            action = {
                'created_at': source.get('created_at'),
                'repo': source.get('repo'),
                'namespace': source.get('namespace'),
                'download': source.get('repo_download'),
                'origin': 'swr'
            }
            action['unique'] = action['repo'] + '-' + action['namespace'] + '-' + action['origin']
            doc_id = action['origin'] + hit['_id']
            index_data = {'index': {'_index': self.index_name, '_id': doc_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def reindex_combine(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            action = {
                'created_at': source.get('created_at'),
                'repo': source.get('repo'),
                'namespace': source.get('namespace'),
                'download': source.get('repo_download'),
                'origin': source.get('origin')
            }
            action['unique'] = action['repo'] + '-' + action['namespace'] + '-' + action['origin']
            index_data = {'index': {'_index': self.index_name, '_id': hit['_id']}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)