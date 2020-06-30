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
        print(self.tables)
        self.url = config.get('es_url')
        self.index_name = config.get('index_name')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.indexMapping = {
            "maillist": "user_id.keyword",
            "baidutongji": "ip_count",
            # "baidutongji": "lines_changed",
            "nginx": "ip.keyword",
        }


    def run(self, from_time):
        # self.collectTotal(from_time)
        # return
        if from_time is None:
            from_time = self.esClient.getStartTime()

        self.setMailListUser(from_time)

    def collectTotal(self, from_time):
        matchs = [{"name": "is_gitee_pull_request", "value": "1"}]
        from_date = datetime.strptime(from_time, "%Y%m%d")
        to_date = datetime.today()
        data = self.esClient.getCountByDateRange(matchs, from_date, to_date)
        print(data)
        actions = ""
        for d in data:
            print("date = %s, count = %s" % (
                d.get("to_as_string"), d.get("doc_count")))
            created_at = d.get("to_as_string")
            body = {
                "all_count": d.get("doc_count"),
                "created_at": created_at,
                "metadata__updated_on": created_at,
                "is_pull_request_total": 1
            }

            id = created_at + "_is_pull_request_total"
            action = common.getSingleAction(self.index_name, id, body)
            actions += action
        self.esClient.safe_put_bulk(actions)


    def setMailListUser(self, from_date):
        starTime = datetime.strptime(from_date, "%Y%m%d")
        fromTime = datetime.strptime(from_date, "%Y%m%d")
        to = datetime.today().strftime("%Y%m%d")

        actions = ""
        while fromTime.strftime("%Y%m%d") < to:
            print(fromTime)
            for t in self.tables:
                type = t.split(":")[0]
                index = t.split(":")[1]

                if type != "baidutongji":
                    c = self.esClient.getUniqueCountByDate(
                        self.indexMapping[type],
                        fromTime.strftime("%Y-%m-%dT00:00:00+08:00"),
                        fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
                        index_name=index)
                else:
                    c = self.esClient.getCountByTermDate(
                        "source_type_title.keyword",
                        # "repo_name",
                        self.indexMapping[type],
                        # starTime.strftime("%Y-%m-%dT00:00:00+08:00"),
                        fromTime.strftime("%Y-%m-%dT00:00:00+08:00"),
                        fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
                        index_name=index)
                    # return
                if c is not None:
                    user = {
                        # "all_lines_changed": c,
                        "all_user_count": c,
                        "created_at": fromTime.strftime("%Y-%m-%dT00:00:00+08:00"),
                        # "metadata__updated_on": fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
                        "updated_at": fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
                        "is_" + type: 1
                    }
                    print(c)
                    id = fromTime.strftime("%Y-%m-%dT00:00:00+08:00") + type + index
                    action = common.getSingleAction(self.index_name, id, user)
                    actions += action
            fromTime = fromTime + timedelta(days=1)

        self.esClient.safe_put_bulk(actions)
