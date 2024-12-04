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
# Create: 2024/11/26
import json

from data.common import ESClient, convert_to_date_str


class ModelCi(object):
    def __init__(self, config=None):
        self.config = config
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.ci_info_url = config.get('ci_info_url')
        self.repo_index_name = config.get('repo_index_name')

    def run(self, from_data):
        ci_info = self.get_ci_info(self.ci_info_url)
        self.write_data(ci_info)

    def get_ci_info(self, ci_info_url):
        """
        Get ci info from an api.
        :param ci_info_url: an api url
        :return: a list of ci info
        """
        ci_info = self.esClient.request_get(ci_info_url)
        if ci_info.status_code != 200:
            return []
        return ci_info.json().get('data')

    def write_data(self, ci_info: list):
        """
        Save ci info into es.
        :param ci_info: a list of ci info
        """
        actions = ''
        all_repos = self.query_repo_info()
        for info in ci_info:
            created_at = convert_to_date_str(info.pop('created_at'))
            updated_at = convert_to_date_str(info.pop('updated_at'))
            action = {
                'id': info.get('id'),
                'repo_id': info.pop('model_id'),
                'repo_name': info.pop('model_name'),
                'created_at': created_at,
                'updated_at': updated_at,
                'repo_type': 'model',
                'visibility': 'private',
            }
            all_repos.pop(action.get('repo_id'), None)
            action.update(info)
            doc_id = action.get('repo_id')
            index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

        self.write_no_ci_model(all_repos)

    def write_no_ci_model(self, repos):
        """
        Save model info into es.
        :param repos: a dict of model info
        """
        actions = ''
        for repo_id, repo, in repos.items():
            action = {
                'repo_id': repo_id,
                'repo_name': repo.get('name'),
                'owner': repo.get('owner'),
                'repo_type': 'model',
                'created_at': repo.get('created_at'),
                'visibility': repo.get('visibility'),
                'status': 'Not Ci'
            }
            index_data = {"index": {"_index": self.index_name, "_id": repo_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def query_repo_info(self):
        """
        Get all models from es.
        """
        search = '''{
            "size": 1000,
            "_source": {
                "includes": [
                    "id", "owner", "name", "visibility", "created_at"
                ]
            },
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 AND type.keyword:model AND owner.keyword:*"
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

        self.esClient.scrollSearch(self.repo_index_name, search=search, func=func)
        return repos
