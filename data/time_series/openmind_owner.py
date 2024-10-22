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
import datetime
import json
import time

from collect.api import request_url
from data.common import ESClient
from data.common_client.tag_removed_data import TagRemovedData


class OpenmindOwner(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.api_base = config.get('api_base')
        self.private_api_base = config.get('private_api_base')
        self.private_token = config.get('private_token')
        self.kind = config.get('kind')
        self.last_ids = []

    def run(self, from_data=None):
        self.get_owners()
        kinds = self.kind.split(',')
        for kind in kinds:
            self.get_public_repos(kind)
            self.get_private_repos(kind)
        self.tag_removed_data()
        self.last_ids = []

    def get_owners(self):
        owner_url = f'{self.api_base}/organization'
        page = 1
        orgs, users = 0, 0
        while True:
            params = {"page_num": page}
            resp = request_url(owner_url, payload=params)
            page += 1
            if resp.json().get('data').get('total') == 0:
                break
            items = resp.json().get('data').get('labels')
            if not items:
                print('Get organization info error')
                break
            for item in items:
                if item.get('type') == 1:
                    orgs += 1
                else:
                    users += 1

        actions = self.write_owner_count(orgs, 1)
        actions += self.write_owner_count(users, 0)
        self.esClient.safe_put_bulk(actions)

    def get_public_repos(self, kind):
        repo_url = f'{self.api_base}/{kind}'
        cur = 1
        actions = ''
        field = f'{kind}s'
        org_count, user_count = 0, 0
        while True:
            params = {"page_num": cur, "count": 1}
            response = request_url(repo_url, payload=params)
            cur += 1
            objs = response.json().get('data').get(field)
            if not objs:
                print(f'Get {kind} info error')
                break
            for obj in objs:
                ts_obj = datetime.datetime.fromtimestamp(obj['updated_at'])
                created_at = ts_obj.strftime('%Y-%m-%dT%H:%M:%S+08:00')
                obj['created_at'] = created_at
                obj['type'] = kind
                obj['visibility'] = 'public'
                self.last_ids.append(kind + obj['id'])
                index_data = {"index": {"_index": self.index_name, "_id": kind + obj['id']}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(obj) + '\n'
                if obj.get('owner_type') == 1:
                    org_count += 1
                else:
                    user_count += 1

        actions += self.write_project_count(kind, org_count, 1)
        actions += self.write_project_count(kind, user_count, 0)
        self.esClient.safe_put_bulk(actions)

    def get_private_repos(self, kind):
        repo_url = f'{self.private_api_base}/{kind}/list'
        cur = 1
        actions = ''
        field = f'{kind}s'
        org_count, user_count = 0, 0
        while True:
            headers = {"token": self.private_token}
            params = {"visibility": "private", "count": 1, "page_num": cur}
            response = request_url(repo_url, headers=headers, payload=params)

            cur += 1
            try:
                objs = response.json().get('data').get(field)
            except Exception as e:
                print(f'Get {kind} info error')
                break

            if not objs:
                print(f'Get {kind} info error')
                break
            for obj in objs:
                ts_obj = datetime.datetime.fromtimestamp(obj['updated_at'])
                created_at = ts_obj.strftime('%Y-%m-%dT%H:%M:%S+08:00')
                obj['created_at'] = created_at
                obj['type'] = kind
                obj['visibility'] = 'private'
                self.last_ids.append(kind + obj['id'])
                index_data = {"index": {"_index": self.index_name, "_id": kind + obj['id']}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(obj) + '\n'
                if obj.get('owner_type') == 1:
                    org_count += 1
                else:
                    user_count += 1

        actions += self.write_project_count(kind, org_count, 1, 'private')
        actions += self.write_project_count(kind, user_count, 0, 'private')
        self.esClient.safe_put_bulk(actions)

    def write_project_count(self, kind, count, owner_type, visibility='public'):
        created_at = time.strftime("%Y-%m-%d", time.localtime())
        action = {
            'all_count': count,
            'created_at': created_at,
            'type': kind,
            'owner_type': owner_type,
            'visibility': visibility
        }
        doc_id = created_at + kind + visibility + f'_{owner_type}'
        index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
        actions = json.dumps(index_data) + '\n'
        actions += json.dumps(action) + '\n'
        return actions

    def write_owner_count(self, count, owner_type, visibility='public'):
        created_at = time.strftime("%Y-%m-%d", time.localtime())
        action = {
            'all_count': count,
            'created_at': created_at,
            'is_owner': 1,
            'owner_type': owner_type,
            'visibility': visibility
        }
        doc_id = created_at + visibility + f'owner_{owner_type}'
        index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
        actions = json.dumps(index_data) + '\n'
        actions += json.dumps(action) + '\n'
        return actions

    def tag_removed_data(self):
        query = 'AND owner.keyword:*'
        tag_client = TagRemovedData(self.config)
        id_list = tag_client.query_id(self.last_ids, query)
        tag_client.tag_removed_data(id_list)
