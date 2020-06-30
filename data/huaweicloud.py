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

                # if i < 18000:
                #     gb = 2
                #     body2 = {
                #         "sum_value": 1024 * 1024 * 1024 * gb,
                #         "sum_value_gb": gb,
                #         "timestamp": aggregation.timestamp,
                #         "namespace": "SYS.I",
                #         "metric_name": "i_stream",
                #         "created_at": created_at,
                #         "dim_name": "temp",
                #         "id": "publicip_id_" + str(i),
                #         "total": i * gb,
                #         "total_num": i * gb / 6
                #     }
                #     index_id2 = created_at + "publicip_id_" + str(i)
                #     action2 = common.getSingleAction(self.index_name, index_id2,
                #                                      body2)
                #     actions += action2
                #     print("i=%d, created_at=%s" % (i, created_at))
                #
                # i += 1
                # print(action)
            self.esClient.safe_put_bulk(actions)
            tmp_from_date = tmp_from_date + timedelta(days=9)

        print("total=", total)
        print("total_num=", total/6)


    def run(self, from_date=None):
        if from_date is None:
            from_date = self.esClient.getLastFormatTime()
            print("[huaweicloud] Get last format from_data is", from_date)

        # hongkong 119.8.119.83 现在没有了
        # self.getMetrics(from_date, self.huaweicloud_username, self.huaweicloud_password,
        #                 "061254b4928026c02ffec017d91b5734",
        #                 "060600ffbe00251e0f6fc0176531c800",
        #                 "https://iam.ap-southeast-1.myhuaweicloud.com/v3",
        #                 "e71a8702-5eb3-4d6a-822c-67bd51f998f6")

        # cn-north-4 121.36.97.194的bandwidth 20200611中午换了bandwidth
        # self.getMetrics(from_date, self.huaweicloud_username, self.huaweicloud_password,
        #                 "0612bce5650026c12fbcc01710fb0df4",
        #                 "060600ffbe00251e0f6fc0176531c800",
        #                 "https://iam.cn-north-4.myhuaweicloud.com/v3",
        #                 "eed8adb9-7d0f-4059-9f35-4c34127740b3")

        # cn-north-4 121.36.97.194
        self.getMetrics(from_date, self.huaweicloud_username, self.huaweicloud_password,
                        "0612bce5650026c12fbcc01710fb0df4",
                        "060600ffbe00251e0f6fc0176531c800",
                        "https://iam.cn-north-4.myhuaweicloud.com/v3",
                        "299fb435-6e65-4a2d-8848-fa672687d521",
                        dim_name="publicip_id")

        # hongkong 159.138.11.195
        self.getMetrics(from_date, self.huaweicloud_username, self.huaweicloud_password,
                        "061254b4928026c02ffec017d91b5734",
                        "060600ffbe00251e0f6fc0176531c800",
                        "https://iam.ap-southeast-1.myhuaweicloud.com/v3",
                        "77acf4ff-7885-4a5b-817b-2c1bd5b677a2",
                        dim_name="publicip_id")

