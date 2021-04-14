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
        self.esClient = ESClient(config)

    def run(self, from_time):
        startTime = time.time()
        querys = self.query.split(";")
        key_prefixs = self.key_prefix.split(";")
        count_keys = self.count_key.split(";")
        for i in range(len(querys)):
            self.esClient.setToltalCount(self.from_d, field=None, query=querys[i], count_key=count_keys[i], key_prefix=key_prefixs[i])
        # self.esClient.setToltalCount(from_d, "pv_count", field="user_login.keyword")

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect Polymerization data: finished after ", spent_time)
