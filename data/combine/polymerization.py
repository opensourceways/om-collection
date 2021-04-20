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

import time
from datetime import timedelta, datetime

import threading
import json

from data import common
from data.common import ESClient



class Polymerization(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.query_index_name = config.get('query_index_name')
        self.query = config.get('query')
        self.key_prefix = config.get('key_prefix')
        self.from_d = config.get('polymerization_from_time')
        self.count_key = config.get('count_key')
        self.is_get_total_count = config.get('is_get_total_count')
        self.is_tag_first_doc = config.get('is_tag_first_doc')
        self.esClient = ESClient(config)

    def run(self, from_time):
        startTime = time.time()

        querys, key_prefixs, count_keys = None, None, None
        if self.query:
            querys = self.query.split(";")
        if self.key_prefix:
            key_prefixs = self.key_prefix.split(";")
        if self.count_key:
            count_keys = self.count_key.split(";")

        if self.is_get_total_count == "true":
            self.getTotalCount(querys, key_prefixs, count_keys)
        if self.is_tag_first_doc == "true":
            self.tagFirstDoc(querys, key_prefixs, count_keys)

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect Polymerization data: finished after ", spent_time)

    def getTotalCount(self, querys, key_prefixs, count_keys):
        for i in range(len(count_keys)):
            query = querys[i] if querys else None
            self.esClient.setToltalCount(self.from_d, query=query, count_key=count_keys[i],
                                         key_prefix=key_prefixs[i])

    def tagFirstDoc(self, querys, key_prefixs, count_keys):
        for i in range(len(querys)):
            query = querys[i] if querys else None
            self.esClient.setFirstItem(key_prefix=key_prefixs[i], query=query, key=count_keys[i],
                                       query_index_name=self.query_index_name)
