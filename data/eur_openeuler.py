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
# Create: 2023
#
import json
from datetime import datetime

from data.common import ESClient

PROJECT_API = 'https://eur.openeuler.openatom.cn/api_3/project/list'


class EurOpenEuler(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.query = config.get('query')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)

    def run(self, start):
        res = self.esClient.request_get(PROJECT_API)
        if res.status_code != 200:
            return
        items = res.json().get('items')
        actions = ''
        for item in items:
            now = datetime.today()
            created_at = now.strftime("%Y-%m-%dT00:00:00+08:00")
            item.update({'created_at': created_at})
            id_str = item.get('ownername') + '_' + item.get('name')
            indexData = {"index": {"_index": self.index_name, "_id": id_str + created_at}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(item) + '\n'
        self.esClient.safe_put_bulk(actions)
