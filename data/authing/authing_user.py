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
# Create: 2024/6/26

import json

from collect.api import request_url
from data.common import ESClient


class AuthingUser(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.token_url = config.get('token_url')
        self.user_pool_id = config.get('user_pool_id')
        self.pool_secret = config.get('pool_secret')
        self.users_url = config.get('users_url')
        self.app_id = config.get('app_id')
        self.last_run_date = None

    def run(self, from_date):
        self.get_users()

    def get_access_token(self):
        body = {
            "accessKeyId": self.user_pool_id,
            "accessKeySecret": self.pool_secret
        }
        headers = {
            'Content-Type': 'application/json'
        }
        response = request_url(self.token_url, headers=headers, payload=json.dumps(body), method='POST')
        if response.status_code != 200:
            print('get token error:', response.status_code, response.text)
            return ''
        token = response.json().get('data').get('access_token')
        return token

    def get_users(self):
        auth = 'Bearer ' + self.get_access_token()
        headers = {
            'authorization': auth,
            'x-authing-userpool-id': self.user_pool_id
        }
        cur = 1
        actions = ''
        while True:
            payload = {'page': cur, 'limit': 50}
            response = request_url(self.users_url, headers=headers, payload=payload)
            cur += 1
            users = response.json().get('data').get('list')
            if not users:
                break
            for user in users:
                if user.get('userSourceId') == self.app_id:
                    index_data = {"index": {"_index": self.index_name, "_id": user['userId']}}
                    actions += json.dumps(index_data) + '\n'
                    actions += json.dumps(user) + '\n'
        self.esClient.safe_put_bulk(actions)
