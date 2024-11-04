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
# Create: 2024/10/21
import json

from collections import OrderedDict
from datetime import datetime, timedelta

from data.common import ESClient
from data.common_client.tag_removed_data import TagRemovedData


class DownloadCompute(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.esClient.initLocationGeoIPIndex()
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.index_name = config.get('index_name')
        self.src_index_name = config.get('src_index_name')
        self.download_index_name = config.get('download_index_name')
        self.ip_locations = OrderedDict()
        self.max_size = 1000

    def run(self, from_data):
        new_repos = self.query_repo_info()
        old_repos = self.query_all_repo_id()
        self.tag_removed_data(old_repos, new_repos)
        self.query_download_info(new_repos)

    def query_repo_info(self):
        search = '''{
            "size": 1000,
            "_source": {
                "includes": [
                    "id", "type", "frameworks", "visibility"
                ]
            },
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 AND owner.keyword:*"
                            }
                        }
                    ]
                }
            }
        }'''

        repos = {}

        def func(data):
            for item in data:
                repo = item['_source']
                repos[repo['id']] = repo

        self.esClient.scrollSearch(self.src_index_name, search=search, func=func)
        return repos

    def query_download_info(self, repos):
        yesterday = datetime.now() - timedelta(days=1)
        created_at = yesterday.strftime("%Y-%m-%d")
        search = '''{
            "size": 2000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "created_at": {
                                    "gte": "%s"
                                }
                            }
                        }
                    ]
                }
            }
        }''' % created_at

        def func(data):
            actions = ''
            for item in data:
                doc_id = item['_id']
                action = item['_source']
                if action.get('request_IP'):
                    ip = action.get('request_IP').split(':')[0]
                    action['ip'] = ip
                    location = self.get_location_by_ip(ip)
                    action.update(location)
                repo_info = repos.get(action['repo_id'])
                if not repo_info:
                    continue
                action['repo_type'] = repo_info.get('type')
                action['frameworks'] = repo_info.get('frameworks')
                action['visibility'] = repo_info.get('visibility')
                index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'
            self.esClient.safe_put_bulk(actions)

        self.esClient.scrollSearch(self.download_index_name, search=search, func=func)

    def query_all_repo_id(self):
        search = '''{
            "size": 2000,
            "_source": {
                "includes": [
                    "repo_id"
                ]
            },
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1"
                            }
                        }
                    ]
                }
            }
        }'''

        repos = []

        def func(data):
            for item in data:
                repo = item['_source']
                if repo['repo_id'] not in repos:
                    repos.append(repo['repo_id'])

        self.esClient.scrollSearch(self.index_name, search=search, func=func)
        return repos

    def tag_removed_data(self, old_repos: list, new_repos: dict):
        tag_client = TagRemovedData(self.config)
        last_repo = new_repos.keys()
        removed = set(old_repos) - set(last_repo)
        for repo_id in removed:
            tag_client.tag_removed_term('repo_id', repo_id)
            print(f"tag repo {repo_id} removed over")

    def get_location_by_ip(self, ip):
        if not ip:
            return {}
        if ip in self.ip_locations:
            location = self.ip_locations.pop(ip)
        else:
            location = self.esClient.getLocationByIP(ip)

        self.ip_locations[ip] = location
        if len(self.ip_locations) > self.max_size:
            self.ip_locations.popitem(last=False)

        return location
