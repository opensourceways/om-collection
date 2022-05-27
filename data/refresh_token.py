#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2020-05
#
import json
import requests
import urllib3
import time

from collect.gitee_v8 import GiteeClient
from data.common import ESClient
from collect.baidutongji import BaiDuTongjiClient

urllib3.disable_warnings()


class RefreshToken(object):

    def __init__(self, config=None):
        self.config = config
        self.index_name_token = config.get('index_name_token')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.ssl_verify = True
        self.session = requests.Session()
        self.esClient = ESClient(config)
        self.headers = {'Content-Type': 'application/json;charset=UTF-8'}

        self.org = config.get('org')
        self.service_refresh_token = json.loads(config.get('service_refresh_token'))
        self.expires_in = int(config.get('expires_in'))

    def run(self, from_time):
        self.is_refresh_token()
        time.sleep(10)
        services = self.esClient.get_access_token(self.index_name_token)
        for service in services:
            print("...service = %s, created_at = %s, access_token = %s***..."
                  % (service.get("service"), service.get("created_at"), service.get("access_token")[:8]))

    def refresh_access_token(self, valid_token):
        rt = valid_token.get("refresh_token")
        service_name = valid_token.get("service")
        if rt is None:
            return

        if "giteev8" in service_name:
            gitee_v8 = GiteeClient(self.org, rt)
            res = gitee_v8.refresh_token(rt)
        elif "baidutongji" in service_name:
            baiduClient = BaiDuTongjiClient(self.config)
            res = baiduClient.refresh_access_token(rt, valid_token.get("client_id"), valid_token.get("client_secret"))
        else:
            return

        if 'access_token' in res.json():
            created_time = time.time()
            time_array = time.localtime(int(created_time))
            str_date = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time_array)

            action = res.json()
            action.update({'created_at': str_date})
            action.update({'created_time': created_time})
            action.update({'service': service_name})
            action.update({'client_id': valid_token.get("client_id")})
            action.update({'client_secret': valid_token.get("client_secret")})
            indexData = {"index": {"_index": self.index_name_token, "_id": service_name}}
            actions = json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
            self.esClient.safe_put_bulk(actions)
            print("refresh ok!")
        else:
            print("refresh failed! The refresh token may have be used... %s ..." % service_name)
            print("Try to update config and use config to refresh token...")
            for new_token in self.service_refresh_token:
                if service_name == new_token.get("service"):
                    self.refresh_access_token(new_token)

    def get_valid_token(self, service):
        # 取可用于刷新的有效token
        # 使用数据库的值；数据库没有，使用配置文件的值
        services_stored = self.esClient.get_access_token(self.index_name_token)
        vt = service
        for ss in services_stored:
            if service.get("service") == ss.get("service"):
                vt = ss
                break
        return vt

    def is_refresh_token(self):
        # 60s bias
        for service in self.service_refresh_token:
            valid_token = self.get_valid_token(service)
            if time.time() > (int(valid_token.get("created_time")) + self.expires_in - 60):
                print('Star to refresh access token for %s ...' % valid_token.get("service"))
                self.refresh_access_token(valid_token)
