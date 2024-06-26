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
# Create: 2024/6/25
import json
import time

from collect.api import request_url
from data.common import ESClient


class OpenmindOwner(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.api_base = config.get('api_base')
        self.kind = config.get('kind')

    def run(self, from_data=None):
        created_at = time.strftime("%Y-%m-%d", time.localtime())
        owners = self.get_owners()
        kinds = self.kind.split(',')
        for kind in kinds:
            actions = ''
            for owner in owners:
                action = self.get_repo_by_owner(kind=kind, owner=owner)
                doc_id = created_at + owner + kind
                index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'
            self.esClient.safe_put_bulk(actions)

    def get_owners(self):
        owner_url = f'{self.api_base}/organization'
        page = 1
        owners = []
        while True:
            params = {"page_num": page}
            resp = request_url(owner_url, payload=params)
            page += 1
            if resp.json().get('data').get('total') == 0:
                break
            items = resp.json().get('data').get('Labels')
            for item in items:
                if item.get('type') == 1:
                    owners.append(item.get('account'))
        return owners

    def get_repo_by_owner(self, kind, owner):
        repo_url = f'{self.api_base}/{kind}/{owner}'
        params = {"count": 1}
        resp = request_url(repo_url, payload=params)
        if resp.status_code != 200:
            print(f'get repo by {owner} error', resp.text)
            return ''
        nums = resp.json().get('data').get('total')
        created_at = time.strftime("%Y-%m-%d", time.localtime())
        action = {
            'owner': owner,
            'count': nums,
            'created_at': created_at,
            'type': kind
        }
        return action

