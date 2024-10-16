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
# Create: 2024/10/15
import datetime
import json

from constant.platform_constant import GITEE_BASE
from data.common import ESClient
from data.common_client.platform_file import PlatformFile


class DownloadData(object):
    def __init__(self, config=None):
        self.config = config
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.gitee_access_token = config.get('gitee_access_token')
        self.github_access_token = config.get('github_access_token')
        self.download_data_url = config.get('download_data_url')

    def run(self, from_data):
        download_json = self.get_download_from_yaml(self.download_data_url)
        self.write_data(download_json)

    def get_download_from_yaml(self, yaml_file):
        if GITEE_BASE in yaml_file:
            token = self.gitee_access_token
        else:
            token = self.github_access_token
        download_json = PlatformFile.get_yaml_file(yaml_file, token)
        return download_json

    def write_data(self, download_json: dict):
        actions = ''
        now = datetime.datetime.today()
        created_at = now.strftime("%Y-%m-%dT08:00:00+08:00")
        for origin, repos in download_json.items():
            download = 0
            for repo in repos:
                action = {
                    'repo': repo.get('image_name'),
                    'namespace': repo.get('organization'),
                    'repo_download': repo.get('download_num'),
                    'created_at': created_at,
                    'origin': origin
                }
                index_data = {
                    "index": {
                        "_index": self.index_name,
                        "_id": origin + repo.get('organization') + repo.get('image_name') + created_at
                    }
                }
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'
                download += repo.get('download_num')

            action = {
                'download': download,
                'created_at': created_at,
                'origin': origin
            }
            index_data = {"index": {"_index": self.index_name, "_id": origin + created_at}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)
