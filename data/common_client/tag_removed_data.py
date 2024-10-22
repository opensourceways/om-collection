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
# Create: 2024/10/21
import json

from data.common import ESClient


class TagRemovedData(object):
    def __init__(self, config):
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')

    @staticmethod
    def chunk_list(input_list, chunk_size):
        return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

    def tag_removed_id(self, remove_list):
        for remove in remove_list:
            query = {
                "script": {
                    "source": "ctx._source.is_removed=1"
                },
                "query": {
                    "terms": {
                        "_id": remove
                    }
                }
            }
            self.esClient.updateByQuery(json.dumps(query))

    def query_id(self, last_ids, query):
        search = '''{
            "size": 1000,
            "_source": {
                "includes": [
                    "created_at"
                ]
            },
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 %s"
                            }
                        }
                    ]
                }
            }
        }''' % query

        id_list = []

        def func(data):
            for item in data:
                if item['_id'] not in last_ids:
                    id_list.append(item['_id'])

        self.esClient.scrollSearch(self.index_name, search=search, func=func)

        return id_list

    def tag_removed_data(self, id_list):
        id_lists = self.chunk_list(id_list, 1000)
        self.tag_removed_id(id_lists)

    def tag_removed_term(self, field, remove):
        query = {
            "script": {
                "source": "ctx._source.is_removed=1"
            },
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                f"{field}.keyword": remove
                            }
                        }
                    ]
                }
            }
        }
        self.esClient.updateByQuery(json.dumps(query))