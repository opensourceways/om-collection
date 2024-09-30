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
# Create: 2024/9/28
import datetime
import json
import time

from collect.api import request_url
from data.common import ESClient


class OpenmindPrivateModel(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get("index_name")
        self.esClient = ESClient(config)
        self.api_base = config.get("api_base")
        self.token = config.get("token")
        self.kind = config.get("kind")
        self.visibility = config.get("visibility")
        self.count = config.get("count")

    def run(self, from_data=None):
        self.get_private_models(self.kind)

    def get_private_models(self, kind):
        repo_url = f"{self.api_base}/{kind}/list"
        cur = 1
        actions = ""
        field = f"{kind}s"
        org_count, user_count = 0, 0
        while True:
            headers = {"token": self.token}
            params = {"visibility": self.visibility, "count": 1, "page_num": cur}
            
            response = request_url(repo_url, headers=headers, payload=params)
            
            cur += 1
            objs = response.json().get("data").get(field)
            if not objs:
                print(f"Get {kind} info error")
                break
            for obj in objs:
                ts_obj = datetime.datetime.fromtimestamp(obj["updated_at"])
                created_at = ts_obj.strftime("%Y-%m-%dT%H:%M:%S+08:00")
                obj["created_at"] = created_at
                obj["type"] = kind
                index_data = {
                    "index": {"_index": self.index_name, "_id": kind + obj["id"]}
                }
                actions += json.dumps(index_data) + "\n"
                actions += json.dumps(obj) + "\n"
                if obj.get("owner_type") == 1:
                    org_count += 1
                else:
                    user_count += 1

        actions += self.write_project_count(kind, org_count, 1)
        actions += self.write_project_count(kind, user_count, 0)
        self.esClient.safe_put_bulk(actions)

    def write_project_count(self, kind, count, owner_type):
        created_at = time.strftime("%Y-%m-%d", time.localtime())
        action = {
            "all_count": count,
            "created_at": created_at,
            "type": kind,
            "owner_type": owner_type,
        }
        doc_id = created_at + kind + f"_{owner_type}"
        index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
        actions = json.dumps(index_data) + "\n"
        actions += json.dumps(action) + "\n"
        return actions

    def write_owner_count(self, count, owner_type):
        created_at = time.strftime("%Y-%m-%d", time.localtime())
        action = {
            "all_count": count,
            "created_at": created_at,
            "is_owner": 1,
            "owner_type": owner_type,
        }
        doc_id = created_at + f"owner_{owner_type}"
        index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
        actions = json.dumps(index_data) + "\n"
        actions += json.dumps(action) + "\n"
        return actions
