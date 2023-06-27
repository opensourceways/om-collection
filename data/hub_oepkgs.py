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
# Create: 2023/6/15

import datetime
import json
from data.common import ESClient


class HubOepkgs(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.query = config.get('query')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.api_url = config.get('api_url')
        self.owner = config.get('owner')

    def run(self, start):
        actions = self.write_pull_count(self.owner)
        self.esClient.safe_put_bulk(actions)

    def write_pull_count(self, repo):
        actions = ''
        created_at = datetime.datetime.now().strftime("%Y-%m-%dT08:00:00+08:00")
        url = self.api_url % self.owner
        res = self.esClient.request_get(url)
        repos = res.json().get('repository')
        for repo in repos:
            if repo.get('project_name') != self.owner:
                continue
            repo.update({'created_at': created_at})
            indexData = {"index": {"_index": self.index_name, "_id": repo.get('repository_name') + created_at}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(repo) + '\n'
        return actions
