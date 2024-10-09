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
# Create: 2023/8/7
import datetime
import json

import requests

from data.common import ESClient


class Swr(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.token_url = self.config.get('token_url')
        self.repo_list_url = self.config.get('repo_list_url')
        self.base_url = self.config.get('base_url')
        self.username = self.config.get('username')
        self.password = self.config.get('password')
        self.domain = self.config.get('domain')
        self.region = self.config.get('region')
        self.namespaces = self.config.get('namespaces')
        self.token = self.generate_token()
        self.repos = self.config.get('repos')

    def run(self, start):
        print('start collect swr download data')
        if self.repos:
            self.get_data()
        else:
            self.get_namespace_repo()
        print('collect over')

    def get_data(self):
        self.refresh_token()
        _header = {
            "Content-Type": 'application/json',
            'X-Auth-Token': self.token.get('token')
        }
        download = 0
        repos = self.repos.split(',')
        for repo in repos:
            response = requests.get(url=self.base_url + repo, headers=_header, timeout=60)
            if response.status_code != 200:
                print(f'get repo {repo} download data failed')
                continue
            download += response.json().get('num_download')

        now = datetime.datetime.today()
        created_at = now.strftime("%Y-%m-%dT08:00:00+08:00")
        action = {
            'download': download,
            'created_at': created_at
        }
        index_data = {"index": {"_index": self.index_name, "_id": created_at}}
        actions = json.dumps(index_data) + '\n'
        actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_namespace_repo(self):
        self.refresh_token()
        _header = {
            "Content-Type": 'application/json',
            'X-Auth-Token': self.token.get('token')
        }
        download = 0
        actions = ''
        now = datetime.datetime.today()
        created_at = now.strftime("%Y-%m-%dT08:00:00+08:00")
        for namespace in self.namespaces.split(','):
            param = {'namespace': namespace} if namespace else None
            response = requests.get(url=self.repo_list_url, params=param, headers=_header, timeout=60)
            if response.status_code != 200:
                print('get namespace repo error: ', response.text)
                continue

            for repo in response.json():
                action = {
                    'repo': repo.get('name'),
                    'namespace': namespace,
                    'repo_download': repo.get('num_download'),
                    'created_at': created_at
                }
                index_data = {"index": {"_index": self.index_name, "_id": namespace + '-' + repo.get('name') + created_at}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'
                download += repo.get('num_download')

        action = {
            'download': download,
            'created_at': created_at
        }
        index_data = {"index": {"_index": self.index_name, "_id": created_at}}
        actions += json.dumps(index_data) + '\n'
        actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def generate_token(self):
        headers = {
            "Content-Type": "application/json;charset=utf-8",
        }
        token_data = {
            "auth": {
                "identity": {
                    "methods": [
                        "password"
                    ],
                    "password": {
                        "user": {
                            "name": self.username,
                            "password": self.password,
                            "domain": {
                                "name": self.domain
                            }
                        }
                    }
                },
                "scope": {
                    "project": {
                        "name": self.region
                    }
                }
            }
        }
        response = requests.post(url=self.token_url, data=json.dumps(token_data), headers=headers)
        token = response.headers.get("X-Subject-Token")
        token_map = {
            'token': token,
            'created_at': datetime.datetime.now()
        }
        return token_map

    def refresh_token(self):
        now = datetime.datetime.now()
        if (now - self.token.get('created_at')).seconds > 7200:
            print('...refresh token...')
            self.token = self.generate_token()

