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

    def run(self, from_data):
        ci_info = self.get_ci_info(self.ci_info_url)
        self.write_data(ci_info)

    def get_ci_info(self, ci_info_url):
        '''
        Get ci info from an api.
        :param ci_info_url: an api url
        :return: a list of ci info
        '''
        ci_info = self.esClient.request_get(ci_info_url)
        if ci_info.status_code != 200:
            return []
        return ci_info.json().get('data')

    def write_data(self, ci_info: list):
        '''
        Save ci info into es.
        :param ci_info: a list of ci info
        '''
        actions = ''
        for info in ci_info:
            created_at = convert_to_date_str(info.get('created_at'))
            updated_at = convert_to_date_str(info.get('updated_at'))
            action = {
                'id': info.get('id'),
                'repo_id': info.get('model_id'),
                'repo_name': info.get('model_name'),
                'owner': info.get('owner'),
                'status': info.get('status'),
                'created_at': created_at,
                'updated_at': updated_at,
                'repo_type': 'model'
            }
            doc_id = info.get('model_id')
            index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)
