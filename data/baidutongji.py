#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 The community Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from datetime import timedelta, datetime

import threading
import json

from data import common
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
        self.url = config.get('es_url')
        self.index_name = config.get('index_name')
        self.authorization = config.get('authorization')
        self.default_headers = {
            'Authorization': self.authorization,
            'Content-Type': 'application/json'
        }


    def getIndexName(self):
        return self.index_name


    def getBaiduAction(self, starTime, endTime, data, index_name, is_day_data=True):
        data = data.get('body').get('data')[0]
        if not data:
            print("The data(%s) not exist" % data)
            return ''
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

            indexData = {"index": {"_index": index_name, "_id": title + starTime}}
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
            print(data)
            is_day_data = True
            if method == "trend/time/a":
                is_day_data = False
            actions = self.getBaiduAction(collect_time, collect_time, data, index_name, is_day_data)
            common.safe_put_bulk(actions, self.default_headers, self.url)

            fromTime = fromTime + timedelta(days=1)

        return actions

    def getTime(self, time, endTime=None):
        if endTime is None:
            endTime = "23:59"
        return datetime.strptime(time, '%Y%m%d').strftime(
            '%Y-%m-%d') + "T" + endTime + ":59+08:00"


    def run(self, from_time):
        starTime = from_time
        if starTime is None:
            starTime = common.getStartTime(self.index_name)

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

        print("start to run from ", starTime)
        threads = []
        for m in metricMap:
            t = threading.Thread(target=self.getDateByTime, args=(starTime, metricMap[m], m, self.index_name))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        print("All baidutongji thread finished")
