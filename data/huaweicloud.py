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

# import datetime
from openstack import connection
from openstack import utils

from os import path
from data import common
from data.common import ESClient


class HuaweiCloud(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.bandwidth_ids = config.get('bandwidth_ids')
        self.huaweicloud_password = config.get('huaweicloud_password')
        self.huaweicloud_username = config.get('huaweicloud_username')
        self.huaweicloud_projectids = config.get('huaweicloud_projectids').split(',')
        self.huaweicloud_userdomainids = config.get('huaweicloud_userdomainids').split(',')
        self.huaweicloud_resourceids = config.get('huaweicloud_resourceids').split(',')
        self.huaweicloud_endpoints = config.get('huaweicloud_endpoints').split(',')


        self.esClient = ESClient(config)


    def list_metric_aggregations(self, connection, id, from_date, to_date, dim_name="bandwidth_id"):
        now = datetime.now()
        # five_min_ago = now - timedelta(days=1)
        # to_date = from_date + timedelta(days=1)
        query = {
            "namespace": "SYS.VPC",
            "metric_name": "up_stream",
            "from": utils.get_epoch_time(from_date),
            "to": utils.get_epoch_time(to_date),
            "period": 3600,
            "filter": "sum",
            "dimensions": [{
                "name": dim_name,
                "value": id
            }]
        }
        return connection.cloud_eye.metric_aggregations(**query)
        # for aggregation in connection.cloud_eye.metric_aggregations(**query):
        #     print(aggregation)
        #     print(aggregation.__dict__)
        #     print(aggregation.timestamp)


    def initHuaweiCloudClient(self, username, password, projectId, userDomainId, url):
        conn = connection.Connection(auth_url=url,
                                     user_domain_id=userDomainId,
                                     project_id=projectId,
                                     username=username,
                                     password=password)
        return conn

    def getMetrics(self, from_date, username, password, projectId, userDomainId, url, id, dim_name="bandwidth_id"):
        print(".............connection start")

        conn = self.initHuaweiCloudClient(
                username, password, projectId, userDomainId, url)
        print(".............connection ok")
        total = 0
        print("id =", id)
        actions = ""

        now_date = datetime.now()
        tmp_from_date = datetime.strptime(from_date, "%Y%m%d")
        i = 0
        while tmp_from_date < now_date:
            to_date = tmp_from_date + timedelta(days=9)
            if to_date > now_date:
                to_date = now_date

            print("Get metric from %s to %s" % (tmp_from_date, to_date))
            aggregations = self.list_metric_aggregations(conn, id, tmp_from_date, to_date, dim_name)

            for aggregation in aggregations:
                created_at = datetime.fromtimestamp(
                    aggregation.timestamp/1000).strftime("%Y-%m-%dT%H:%M:%S+08:00")
                agg_gb = aggregation.sum / 1024 / 1024 / 1024
                total += agg_gb
                body = {
                    "sum_value": aggregation.sum,
                    "sum_value_gb": agg_gb,
                    "timestamp": aggregation.timestamp,
                    "namespace": "SYS.VPC",
                    "metric_name": "up_stream",
                    "created_at": created_at,
                    "dim_name": dim_name,
                    "id": id,
                    "total": total,
                    "total_num": total/6
                }
                index_id = created_at + dim_name + "_" + id
                action = common.getSingleAction(self.index_name, index_id, body)
                actions += action

            self.esClient.safe_put_bulk(actions)
            tmp_from_date = tmp_from_date + timedelta(days=9)

        print("total=", total)
        print("total_num=", total/6)


    def run(self, from_date=None):
        startTime = time.time()
        print("Collect download data from huaweicloud: staring")

        if from_date is None:
            from_date = self.esClient.getLastFormatTime()
            print("[huaweicloud] Get last format from_data is", from_date)

        for i in range(len(self.huaweicloud_projectids)):
            self.getMetrics(from_date, self.huaweicloud_username,
                            self.huaweicloud_password,
                            self.huaweicloud_projectids[i],
                            self.huaweicloud_userdomainids[i],
                            self.huaweicloud_endpoints[i],
                            self.huaweicloud_resourceids[i],
                            dim_name="publicip_id")

        from_d = "20190914"
        self.esClient.setToltalCount(from_d, "sum_value")

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect download data from huaweicloud:"
              " finished after (%s)" % (spent_time))
