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
# Create: 2024/9/14
import json
import time

from data.common import ESClient


class GiteeFilter(object):

    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.filter = config.get('filter')
        self.user_filter = config.get('user_filter')
        self.gitee_index_name = config.get('gitee_index_name')

    def run(self, from_time):
        print("Collect gitee data: staring")
        start_time = time.time()
        self.get_filter_issue(self.filter)
        end_time = time.time()
        spent_time = time.strftime("%H:%M:%S", time.gmtime(end_time - start_time))
        print("Collect gitee data finished after %s" % spent_time)

    def get_filter_issue(self, issue_filter):
        search = '''{
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 AND is_gitee_issue:1 AND %s"
                            }
                        }
                    ]
                }
            }
        }''' % issue_filter

        self.esClient.scrollSearch(self.gitee_index_name, search, '1m', self.write_issue_data)

    def write_issue_data(self, items):
        actions = ''
        for item in items:
            doc_id = item['_id']
            source = item['_source']
            body = source.get('body')
            user_id = self.get_user_id(body)
            source['userId'] = user_id
            index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(source) + '\n'

        self.esClient.safe_put_bulk(actions)

    def get_user_id(self, item):
        if item and self.user_filter in item:
            items = item.split(self.user_filter)[-1].split('>')
            return items[-1] if items[-1].isdigit() else None
        return None
