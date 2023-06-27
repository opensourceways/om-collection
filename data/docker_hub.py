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

BASE_URL = "https://hub.docker.com/v2"


class DockerHub(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.query = config.get('query')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.owner = config.get('owner')
        self.repos = config.get('repos')

    def run(self, start):
        repos = self.repos.split(',')
        actions = ''
        for repo in repos:
            actions += self.write_pull_count(repo)
        self.esClient.safe_put_bulk(actions)

    def write_pull_count(self, repo):
        actions = ''
        created_at = datetime.datetime.now().strftime("%Y-%m-%dT08:00:00+08:00")
        url = self.urijoin(BASE_URL, 'repositories', self.owner, repo)
        res = self.esClient.request_get(url)
        data = res.json()
        id_str = repo + '-' + self.owner
        action = {
            'id': id_str,
            'pull_count': data.get('pull_count'),
            'repo': repo,
            'metadata__updated_on': created_at,
            'owner': self.owner
        }
        indexData = {"index": {"_index": self.index_name, "_id": id_str + created_at}}
        actions += json.dumps(indexData) + '\n'
        actions += json.dumps(action) + '\n'
        return actions

    @staticmethod
    def urijoin(*args):
        return '/'.join(map(lambda x: str(x).strip('/'), args))
