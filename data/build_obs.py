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
# Create: 2022-03
#
import json
import datetime


from data.common import ESClient


class OBS(object):
    def __init__(self, config=None):

        self.config = config
        self.index_name = config.get('index_name')
        self.target_index_name = config.get('target_index_name')
        self.esClient = ESClient(config)
        self.from_date = config.get('from_date')

    def run(self, datatime=None):
        if self.from_date is None:
            self.from_date = datetime.datetime.today().strftime('%Y-%m') + "-01"

        packagenames = self.esClient.getObsAllPackageName()
        for i in packagenames["aggregations"]["each_project"]["buckets"]:
            packagename = i["key"]
            print(packagename)
            if ':' in packagename:
                packagename = str(packagename).replace(":", "\\\\:")

            res = self.esClient.getObsSumAndCount(packagename, self.from_date)
            actions = ''
            for i in res["aggregations"]["each_month"]["buckets"]:
                time = i["key_as_string"]
                for j in i["each_project"]["buckets"]:
                    project_name = j["key"]
                    for l in j["each_hostarch"]["buckets"]:
                        cpu_name = l["key"]
                        count = l["doc_count"]
                        avg_time = l["avg_duration"]["value"]
                        sum = count * avg_time
                        indexData = {"index": {"_index": self.target_index_name,
                                               "_id": project_name + '_' + cpu_name + '_' + packagename + '_' + time}}

                        subResData = {"project": project_name, "cpu": cpu_name, "package_name": packagename,
                                      "count": count,
                                      "sum_time": sum, "average": avg_time, "time": time}

                        actions += json.dumps(indexData) + '\n'
                        actions += json.dumps(subResData) + '\n'
            self.esClient.safe_put_bulk(actions)
