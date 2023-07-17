#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2023 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2023/7/13
import json
from datetime import datetime

import yaml

from data.common import ESClient
from collect.github import GithubClient


class BigModelResearch(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.yaml_path = config.get('yaml_path')
        self.github_authorization = config.get('github_authorization')

    def run(self, start):
        self.get_data()

    def get_swf(self, path):
        action = {}
        try:
            owner, repo = path.strip().split('/')[-2:]
        except ValueError:
            return action
        client = GithubClient(owner, repo, self.github_authorization)
        res = client.get_repo_info(owner, repo)
        for info in res:
            watch = info.get('subscribers_count')
            star = info.get('stargazers_count')
            fork = info.get('forks')
            action = {
                'watch': watch,
                'fork': fork,
                'star': star,
            }
        contributors = client.get_swf(owner, repo, 'contributors')
        if contributors:
            action.update({'contributor': len(contributors)})
        return action

    def get_data(self):
        yaml_response = self.esClient.request_get(self.yaml_path)
        if yaml_response.status_code != 200:
            print('Cannot fetch online yaml file.')
            return
        try:
            contents = yaml.safe_load(yaml_response.text)
        except yaml.YAMLError as e:
            print(f'Error parsing YAML: {e}')
            return
        actions = ''
        now = datetime.today()
        created_at = now.strftime("%Y-%m-%dT08:00:00+08:00")
        for content in contents.get('list'):
            action = content.copy()
            if action.get('github_path'):
                action.update(self.get_swf(action.get('github_path')))
            action.update({'created_at': created_at})
            doc_id = f"{content['name']}_{content['type']}"
            index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)
