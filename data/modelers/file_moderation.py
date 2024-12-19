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
# Create: 2024/12/19

import datetime
import json
from collect.api import request_url
from data.common import ESClient


class FileModeration(object):
    def __init__(self, config=None):
        self.config = config
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.api_url = config.get('api_url')
        self.page_size = config.get('page_size', 100)


    def run(self, from_data):
        self.get_file_moderation(self.api_url, self.page_size)

    def get_file_moderation(self, url, page_size=100):
        """
        Get info from an api.
        :param url: an api url
        :return: a list of request info
        """
        cur = 1
        while True:
            params = {"PageNum": cur, "CountPerPage": page_size}
            response = request_url(url, payload=params)
            cur += 1
            objs = response.json().get('data')
            if not objs:
                print(f'Get info over')
                break
            self.write_data(objs)

    def write_data(self, infos: list):
        """
        Save info into es.
        :param infos: a list of request info
        """
        actions = ''
        for info in infos:
            now = datetime.datetime.today()
            created_at = now.strftime("%Y-%m-%dT08:00:00+08:00")
            action = {
                'created_at': created_at,
            }
            action.update(info)
            doc_id = action.get('Id')
            index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)