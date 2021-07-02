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
import base64
import os
import sys

import pytz
import requests

import json
import time

import datetime
from data import common
from data.common import ESClient
from collect.github import GithubClient


class HUAWEISWR(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.org = config.get('github_org')
        self.headers = {}
        self.account_name = config.get("account_name")
        self.password = config.get("password")
        self.url = config.get('es_url')
        self.from_date = config.get("from_data")
        self.esClient = ESClient(config)
        self.token_site = config.get("token_site")
        self.repo_site = config.get("repo_site")

    def run(self, from_date):
        startTime = time.time()
        self.token = self.get_subject_token(site=self.token_site)
        self.headers = {'Content-Type': 'application/json', 'Authorization': "Bearer " + self.token}

        print("Starting collect data...")
        self.process_ListRepoDetails(site=self.repo_site)

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S", time.gmtime(endTime - startTime))
        print(f"Total cost time is: {spent_time}")

    def get_subject_token(self, site):
        path = "/v3/auth/tokens"
        base_url = 'https://iam.' + site + ".myhuaweicloud.com"
        url = self.urijoin(base_url, path)
        payload = self.getPayload()
        method = "POST"

        response = requests.request(method=method, url=url, data=payload)
        if response.status_code != 201:
            return None

        return response.headers.get("X-Subject-Token")

    def getPayload(self):
        payload = {}
        auth = {};
        domain = {};
        password = {};
        user = {};
        identity = {}

        domain['name'] = base64.b64decode(self.account_name).decode("ascii")
        user["domain"] = domain
        user["name"] = base64.b64decode(self.account_name).decode("ascii")
        user["password"] = base64.b64decode(self.password).decode("ascii")
        password["user"] = user

        identity["methods"] = ["password"]
        identity["password"] = password
        auth["identity"] = identity
        payload["auth"] = auth
        return str(payload)

    def urijoin(self, *args):
        """Joins given arguments into a URI.
        """
        return '/'.join(map(lambda x: str(x).strip('/'), args))

    def getListNamespaces(self, site):
        path = "v2/manage/repos/namespaces"
        base_url = 'https://swr-api.' + site + ".myhuaweicloud.com"
        url = self.urijoin(base_url, path)

        namespaces = []
        headers = self.headers

        response = requests.request(method="GET", url=url, headers=headers)
        if response.status_code != 200:
            return None

        res = json.loads(response.text)
        namespace_list = res.get("namespaces")
        for np in namespace_list:
            namespaces.append(np["name"])
        return namespaces

    def process_ListRepoDetails(self, site):
        path = "v2/manage/repos"
        base_url = 'https://swr-api.' + site + ".myhuaweicloud.com"
        url = self.urijoin(base_url, path)

        headers = self.headers
        res = requests.request(method="GET", url=url, headers=headers)

        if res.status_code != 200:
            print("Cannot fetch data")
            return None

        result_list = json.loads(res.text)

        # Assemble result data
        mirror_results = []
        for mirror_content in result_list:
            mirror_result = {}
            now = time.time()
            now_time = datetime.datetime.fromtimestamp(now).strftime("%Y-%m-%dT%H:%M:%S+08:00")
            mirror_result["namespace"] = mirror_content["namespace"]
            mirror_result["name"] = mirror_content['name']
            mirror_result["is_swr"] = 1
            mirror_result["num_images"] = mirror_content["num_images"]
            mirror_result["num_download"] = mirror_content["num_download"]
            mirror_result["created_at"] = now_time
            mirror_result["timestamp"] = now
            mirror_results.append(mirror_result)
            time.sleep(0.000001)  # To make timestamp difference, so create a time gap

        # Write data into ES
        actions = ""
        for mirror_result in mirror_results:
            # Generate id, with timestamp
            id = mirror_result["namespace"] + "_" + mirror_result["name"] + "_" + str(int(
                round(mirror_result["timestamp"] * 1000000)))
            mirror_result.pop("timestamp")
            action = common.getSingleAction(self.index_name, id, mirror_result)
            actions += action
        self.esClient.safe_put_bulk(actions)
