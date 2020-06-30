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
from collect.baidutongji import BaiDuTongjiClient


SOURCE_METRIC = "pv_count,pv_ratio,visit_count,visitor_count,new_visitor_count,new_visitor_ratio,ip_count,bounce_ratio,avg_visit_time,avg_visit_pages,trans_count,trans_ratio"
ENTRANCE_PAGE_METRIC = "visit_count,visitor_count,new_visitor_count,new_visitor_ratio,ip_count,bounce_ratio,avg_visit_time,avg_visit_pages,trans_count,trans_ratio,out_pv_count"
VISIT_PAGE_METRIC = "pv_count,visitor_count,ip_count,visit1_count,outward_count,exit_count,average_stay_time,exit_ratio"
VISIT_REGION_METRIC = "pv_count,pv_ratio,visit_count,visitor_count,new_visitor_count,new_visitor_ratio,ip_count,bounce_ratio,avg_visit_pages,average_stay_time"

REGION_METRIC = "pv_count,pv_ratio,visit_count,visitor_count,new_visitor_count,new_visitor_ratio,ip_count,bounce_ratio,avg_visit_time,avg_visit_pages,trans_count,trans_ratio"
TREND_METRIC = "pv_count,pv_ratio,visit_count,visitor_count,new_visitor_count,new_visitor_ratio,ip_count,bounce_ratio,avg_visit_time,avg_visit_pages,trans_count,trans_ratio,avg_trans_cost,income,profit,roi"
RESEARCH_WORD_METRIC = "pv_count,pv_ratio,visit_count,visitor_count,new_visitor_count,new_visitor_ratio,ip_count,bounce_ratio,avg_visit_time,avg_visit_pages,trans_count,trans_ratio"



class BaiduTongji(object):
    def __init__(self, config=None):
        self.config = config
        self.is_baidutongji_enterprise = config.get('is_baidutongji_enterprise')
        self.index_name = config.get('index_name')
        self.site_id = config.get('site_id')
        self.esClient = ESClient(config)


    def getIndexName(self):
        return self.index_name


    def getBaiduAction(self, starTime, endTime, data, index_name, is_day_data=True):
        if not data:
            print("The data(%s) not exist" % data)
            return ''

        if self.is_baidutongji_enterprise == 'true':
            data = data.get('body')

            if not data:
                print("The data body(%s) not exist" % data)
                return
            data = data.get('data')[0]

        if 'result' not in data:
            print("The data (%s) result not exist" % data)
            return ''
        if data['result']['total'] < 1:
            print("The data(%s) result total not exist" % data)
            return ''
        source = data['result']['items'][0]
        value = data['result']['items'][1]

        i = 0
        actions = ''
        while i < len(source):
            result = {}
            title = ''
            if is_day_data:
                title = source[i][0]['name']
                result['created_at'] = self.getTime(starTime)
            else:
                title = source[i][0]
                result['created_at'] = self.getTime(starTime, title.split(" - ")[1])
            v = [title]
            v.extend(value[i])

            if 'source' in source[i][0]:
                result['source'] = source[i][0]['source']

            f = 0
            while f < len(data['result']['fields']):
                if v[f] == '--':
                    tmpv = 0
                else:
                    tmpv = v[f]
                result[data['result']['fields'][f]] = tmpv
                f += 1

            indexData = {"index": {"_index": index_name, "_id": title + starTime + self.site_id}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(result) + '\n'
            i += 1

        return actions


    def getDateByTime(self, start_date, metric, method, index_name):

        fromTime = datetime.strptime(start_date, "%Y%m%d")
        to = datetime.today().strftime("%Y%m%d")
        baiduClient = BaiDuTongjiClient(self.config)

        actions = []
        while fromTime.strftime("%Y%m%d") < to:
            collect_time = fromTime.strftime("%Y%m%d")

            print("collect data of ", collect_time, index_name, method)

            data = baiduClient.getCommon(collect_time, collect_time, metric, method)
            if not data:
                continue
            print(data)
            is_day_data = True
            if method == "trend/time/a":
                is_day_data = False
            actions = self.getBaiduAction(collect_time, collect_time, data, index_name, is_day_data)
            self.esClient.safe_put_bulk(actions)

            fromTime = fromTime + timedelta(days=1)

        return actions

    def getTime(self, time, endTime=None):
        if endTime is None:
            endTime = "23:59"
        return datetime.strptime(time, '%Y%m%d').strftime(
            '%Y-%m-%d') + "T" + endTime + ":59+08:00"

    # def setToltalCount(self, from_date, count_key):
    #     starTime = datetime.strptime(from_date, "%Y%m%d")
    #     fromTime = datetime.strptime(from_date, "%Y%m%d")
    #     to = datetime.today().strftime("%Y%m%d")
    #
    #     actions = ""
    #     while fromTime.strftime("%Y%m%d") < to:
    #         print(fromTime)
    #
    #         c = self.esClient.getCountByTermDate(
    #             "source_type_title.keyword",
    #             count_key,
    #             starTime.strftime("%Y-%m-%dT00:00:00+08:00"),
    #             fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
    #             index_name=self.index_name)
    #         # return
    #         if c is not None:
    #             user = {
    #                 "all_" + count_key: c,
    #                 "updated_at": fromTime.strftime("%Y-%m-%dT00:00:00+08:00"),
    #                 "created_at": fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
    #                 # "metadata__updated_on": fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
    #                 "is_all" + count_key: 1
    #             }
    #             id = fromTime.strftime("%Y-%m-%dT00:00:00+08:00") + "all_" + count_key
    #             action = common.getSingleAction(self.index_name, id, user)
    #             actions += action
    #         fromTime = fromTime + timedelta(days=1)
    #
    #     self.esClient.safe_put_bulk(actions)


    def run(self, from_time):
        print("baidutongji collect site id is:", self.site_id)

        if from_time is None:
            from_time = self.esClient.getStartTime()

        metricMap = {
            # 所有来源
            "source/all/a": SOURCE_METRIC,
            # 网站搜索引擎来源
            "source/engine/a": SOURCE_METRIC,
            # 网站外部链接来源
            "source/link/a": SOURCE_METRIC,
            # 搜索词
            "source/searchword/a": RESEARCH_WORD_METRIC,
            # 地域分布
            "visit/district/a": REGION_METRIC,
            #  地域分布按照国家
            "visit/world/a": REGION_METRIC,
            # 趋势分布
            "trend/time/a": TREND_METRIC,
            # 受访页面
            "visit/toppage/a": VISIT_PAGE_METRIC,
            # 入口页面
            "visit/landingpage/a": ENTRANCE_PAGE_METRIC,
            # 受访域名
            "visit/topdomain/a": VISIT_REGION_METRIC,
        }

        print("start to run from ", from_time)
        threads = []
        for m in metricMap:
            t = threading.Thread(target=self.getDateByTime, args=(from_time, metricMap[m], m, self.index_name))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        from_d = "20190914"

        self.esClient.setToltalCount(from_d, "ip_count", field="source_type_title.keyword")
        self.esClient.setToltalCount(from_d, "pv_count", field="source_type_title.keyword")
        print("All baidutongji thread finished")
