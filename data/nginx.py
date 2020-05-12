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


from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import time
from datetime import timedelta, datetime

from os import path
from data import common
from data.common import ESClient


class Nginx(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.download_log_dir = config.get('download_log_dir')
        self.esClient = ESClient(config)


    def writeIosDownDataByFile(self, filename):
        f = open(filename, 'r')

        actions = ""
        for line in open(filename):
            line = f.readline()
            sLine = line.split( )
            ip = sLine[0]


            location_raw = line.split('<')[1].split('>')[0]
            location_raw = location_raw.split(',')
            country = location_raw[1].lstrip( )
            hostname = location_raw[2].lstrip( )
            lat = location_raw[3].lstrip( )
            lon = location_raw[4].lstrip( )
            t = line.split('[')[1].split(']')[0].split( )[0]

            path = line.split( )[-1]
            link_raw = line.split('"')[3].split('"')[0]
            #07/Apr/2020:14:40:43
            t = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.strptime(t, "%d/%b/%Y:%H:%M:%S"))
            print(t)
            if country == "Hong Kong":
                country = "China"
                hostname = "Hong Kong"
            if hostname == "ShenZhen" and country != "China":
                hostname = country
            if path.endswith('/')  == True:
                continue
            body = {
                  "country": country,
                  "hostname": hostname,
                  "ip": ip,
                  "created_at": t,
                  "updated_at": t,
                  "link": link_raw,
                  "path": path,
                  "location": {
                    "lat": lat,
                    "lon": lon
                  }
            }

            id = t + ip + path
            action = common.getSingleAction(self.index_name, id, body)
            actions += action

        self.esClient.safe_put_bulk(actions)
        f.close()


    def run(self, from_date=None):
        nowDate = datetime.today()

        while datetime.strptime(from_date, "%Y%m%d") < nowDate:
            filename = self.download_log_dir + "/access.log." + from_date
            if path.exists(filename) == True:
                self.writeIosDownDataByFile(filename)
            else:
                print("The file(%s) not exist" % filename)

            fromTime = datetime.strptime(from_date, "%Y%m%d") + timedelta(days=1)
            from_date = fromTime.strftime("%Y%m%d")
            print(from_date)

