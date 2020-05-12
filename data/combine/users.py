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


from datetime import timedelta, datetime

import threading
import json

from data import common
from data.common import ESClient

class Users(object):
    def __init__(self, config=None):
        self.config = config
        tables = config.get('user_from_index')
        if tables is None:
            print("please input 'user_from_index' value first")
            return
        self.tables = tables.split(',')
        self.url = config.get('es_url')
        self.index_name = config.get('index_name')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.indexMapping = {
            "maillist": "user_id",
            "baidutongji": "visitor_count",
            "nginx": "ip",
        }


    def run(self, from_time):
        # starTime = from_time
        from_time = None
        if from_time is None:
            from_time = self.esClient.getStartTime()

        self.setMailListUser(from_time)

        # self.esClient.safe_put_bulk(json)


    def setMailListUser(self, from_date):
        fromTime = datetime.strptime(from_date, "%Y%m%d")
        to = datetime.today().strftime("%Y%m%d")

        actions = ""
        while fromTime.strftime("%Y%m%d") < to:
            for t in self.tables:
                type = t.split(":")[0]
                index = t.split(":")[1]

                toTime = fromTime + timedelta(days=1)
                c = self.esClient.getCountByDate(
                    self.indexMapping[type], fromTime.strftime("%Y%m%dT00:00:00+08:00"),
                    toTime.strftime("%Y%m%dT23:59:59+08:00"))
                user = {
                    "all_user_count": c,
                    "created_at": vc["created_at"],
                    "updated_at": vc["updated_at"],
                    "is_user_" + type: 1
                }

                id = vc["created_at"] + type + index
                action = common.getSingleAction(self.index_name, id, user)
                actions += action
                fromTime = fromTime + timedelta(days=1)

        self.esClient.safe_put_bulk(actions)
