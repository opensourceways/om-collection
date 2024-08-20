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
# Create: 2024/7/18
import json
import queue
import threading
import time

import requests

from collect.api import request_url
from data.common import ESClient


class SoftwareRepoMaintain(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.sig_index_name = config.get('sig_index_name')
        self.query = config.get('query')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.exists_ids = []
        self.now_ids = queue.Queue()
        self.repo_url = config.get('repo_url')

    def run(self, start):
        start_time = time.time()
        self.get_all_id()
        self.write_data()
        end_time = time.time()
        spent_time = time.strftime("%H:%M:%S", time.gmtime(end_time - start_time))
        self.mark_removed_ids()
        print(f'cost time: {spent_time}')

    def get_repo_committer(self, repo):
        query = '''{
            "_source": ["user_login", "repo_name", "created_at", "email", "name", "owner_type", "tag_user_company", "organization", "sig_name"],
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "is_sig_repo_committer:1 AND !is_removed:1 AND repo_name.keyword:\\"src-openeuler/%s\\" AND !email:na"
                            }
                        }
                    ]
                }
            }
        }''' % repo
        url = self.url + '/' + self.sig_index_name + '/_search'
        response = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        if response.status_code != 200:
            print('error', response.text)
            return

        hits = response.json()['hits']['hits']
        maintainer_list, committer_list = [], []
        for hit in hits:
            source = hit['_source']
            owner_type = source.get('owner_type')
            if owner_type == 'maintainers':
                maintainer_list.append(hit)
            else:
                committer_list.append(hit)
        if not committer_list:
            actions = self.get_action(maintainer_list)
        else:
            actions = self.get_action(committer_list)
        self.esClient.safe_put_bulk(actions)

    def get_action(self, users):
        actions = ''
        for user in users:
            self.now_ids.put(user['_id'])
            index_data = {"index": {"_index": self.index_name, "_id": user['_id']}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(user['_source']) + '\n'
        return actions

    def get_repo_list(self):
        response = request_url(self.repo_url)
        if response.status_code != 200:
            return []
        buckets = response.json().get('data')
        repos = []
        for bucket in buckets:
            repos.extend(list(bucket.keys()))
        return repos

    def write_data(self):
        thread_pool_num = 20
        threads = []
        thread_max_num = threading.Semaphore(thread_pool_num)
        repos = self.get_repo_list()

        for repo in repos:
            with thread_max_num:
                t = threading.Thread(target=self.get_repo_committer, args=(repo,))
                t.start()
                threads.append(t)
        for t in threads:
            t.join()

    def get_all_id(self):
        search = '''{
            "size": 1000,
            "_source": {
                "includes": [
                    "user_login"
                ]
            }
        }'''
        self.exists_ids = []

        def func(hit):
            for data in hit:
                self.exists_ids.append(data['_id'])

        self.esClient.scrollSearch(self.index_name, search=search, func=func)

    def mark_removed_ids(self):
        now_id = []
        while not self.now_ids.empty():
            now_id.append(self.now_ids.get())

        print('remove maintainers: ', len(self.exists_ids) - len(now_id))
        for exist_id in self.exists_ids:
            if exist_id not in now_id:
                mark = '''{
                    "script": {
                        "source":"ctx._source['is_removed']=1"
                    },
                    "query": {
                        "term": {
                            "_id":"%s"
                        }
                    }
                }''' % exist_id
                url = self.url + '/' + self.index_name + '/_update_by_query'
                requests.post(url, headers=self.esClient.default_headers, verify=False, data=mark)

