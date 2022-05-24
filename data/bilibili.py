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

from os import path
from data import common
from data.common import ESClient


class BILIBILI(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.download_log_dir = config.get('bilibili_log_dir')
        self.filename = config.get('bilibili_filename')
        self.topic_filename = config.get('bilibili_topic_filename')
        self.esClient = ESClient(config)


    def changeToDate(self, str_date):
        str_date = str_date + "T00:00:00+08:00"
        return str_date


    def getSingleDoc(self, single_text):
        if not single_text:
            return None, None

        data = single_text.split( )
        created_date = data[0]
        try:
            self.changeToDate(created_date)
        except:
            return None, None
        live_time_temp = data[1]
        live_time_unit = data[2]
        if live_time_unit == '小时':
            live_time = float(live_time_temp) * 60
        else:
            live_time = float(live_time_temp)

        live_count = data[3]
        live_revenue = data[4]
        peak_popularity = data[5]
        barrage = data[6]
        new_followers = data[7]
        view_time_per_capita = data[8]
        view_time_per_capita_unit = data[9]
        try:
            author_name = data[11]
            topic_title = data[12]
        except:
            author_name = "-"
            topic_title = "-"
        if view_time_per_capita_unit == '小时':
            view_time_per_capita = view_time_per_capita * 60


        created_at = self.changeToDate(created_date)
        single_data = {
            "live_time": live_time,
            "created_at": created_at,
            "live_count": float(live_count),
            "live_revenue": float(live_revenue),
            "peak_popularity": float(peak_popularity),
            "barrage": float(barrage),
            "new_followers": float(new_followers),
            "view_time_per_capita": float(view_time_per_capita),
            "author_name": author_name,
            "topic_title": topic_title,
        }

        id = created_at
        return single_data, id


    def getSingleTopicDoc(self, single_text):
        if not single_text:
            return None, None

        data = single_text.split(';')
        created_at = data[0].split( )[0] + "T" + data[0].split( )[1] + "+08:00"
        end_date = data[1].split( )[0] + "T" + data[1].split( )[1] + "+08:00"

        live_time_temp = data[2].split( )[0]
        if data[2].split( )[1] == '小时':
            live_time = float(live_time_temp) * 60
        else:
            live_time = float(live_time_temp)

        live_count = 1
        live_revenue = data[5]
        lr = live_revenue.split( )
        if len(lr) == 2:
            if lr[1] == "万":
                print("...wan")
                live_revenue = float(lr[0]) * 10000

        peak_popularity = data[3]
        barrage = data[4]
        view_time_per_capita = data[6].split( )[0]
        view_time_per_capita_unit = data[6].split( )[1]
        author_name = data[7]
        topic_title = data[8]
        if view_time_per_capita_unit == '小时':
            view_time_per_capita = float(view_time_per_capita) * 60

        single_data = {
            "live_time": live_time,
            "created_at": created_at,
            "end_date": end_date,
            "live_count": float(live_count),
            "live_revenue": float(live_revenue),
            "peak_popularity": float(peak_popularity),
            "barrage": float(barrage),
            "view_time_per_capita": float(view_time_per_capita),
            "author_name": author_name,
            "topic_title": topic_title,
            "is_topic": 1
        }

        id = created_at
        return single_data, id


    def setSingleFileData(self, filename):
        i = 0
        actions = ""
        f = open(filename, 'r')
        for line in f.readlines():
            if line is None or not line:
                continue
            if i == 0:
                i += 1
                continue
            doc, id = self.getSingleDoc(line)
            if doc is None:
                continue
            action = common.getSingleAction(self.index_name, id, doc)
            actions += action

        print(actions)
        self.esClient.safe_put_bulk(actions)


    def getPreFixDate(self, date):
        d = datetime.strptime(date, "%Y%m%d")
        format_date = d.strftime("%Y-%m-%d")
        prefix = self.object_prefix + "/" + format_date
        return prefix


    def getNextDate(self, date):
        date = datetime.strptime(date, "%Y%m%d") + timedelta(
            days=1)
        next_date = date.strftime("%Y%m%d")
        return next_date


    def setSingleTopicData(self, filename):
        i = 0
        actions = ""
        f = open(filename, 'r')
        for line in f.readlines():
            if line is None or not line:
                continue
            if i == 0:
                i += 1
                continue
            doc, id = self.getSingleTopicDoc(line)
            if doc is None:
                continue
            action = common.getSingleAction(self.index_name, id, doc)
            actions += action

        print(actions)
        self.esClient.safe_put_bulk(actions)


    def run(self, from_date=None):
        startTime = datetime.now()

        self.setSingleFileData(self.filename)
        self.setSingleTopicData(self.topic_filename)

        endTime = datetime.now()
        print("Collect bilibili download data finished, spend %s seconds" % (
              endTime - startTime).seconds)

