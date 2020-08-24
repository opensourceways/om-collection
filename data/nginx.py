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

import ujson as json
from os import path
from data import common
from data.common import ESClient


class Nginx(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.vhost = config.get('vhost')
        self.download_log_dir = config.get('download_log_dir')
        self.esClient = ESClient(config)
        self.esClient.initLocationGeoIPIndex()


    def writeIosDownDataByFile(self, filename):
        f = open(filename, 'r')

        actions = ""
        for line in f.readlines():
            # line = f.readline()
            sLine = line.split( )
            ip = sLine[0]
            if self.checkIsIP(ip) == False:
                ip = '-'


            location_raw = line.split('<')[1].split('>')[0]
            location_raw = location_raw.split(',')
            country_iso_code = location_raw[0].lstrip( )
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
                "country_iso_code": country_iso_code,
                "city": hostname,
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
            if ".iso" in path:
                body["is_iso_download"] = 1
            elif ".rpm" in path:
                body["is_rpm_download"] = 1


            id = t + ip + path
            action = common.getSingleAction(self.index_name, id, body)

            actions += action
        self.esClient.safe_put_bulk(actions)
        f.close()


    def checkIsIP(self, ip):
        a = ip.split('.')
        if len(a) != 4:
            return False
        for x in a:
            if not x.isdigit():
                return False
            i = int(x)
            if i < 0 or i > 255:
                return False
        return True


    def writeIosDownESDataByFile(self, filename):
        f = open(filename, 'r')

        actions = ""
        count = 0
        lineNum = 0
        for line in f.readlines():
            # line = f.readline()
            print("Get lineNum=", lineNum)
            lineNum += 1
            line_data = json.loads(line)
            log_data = line_data.get('_source').get('log')
            try:
                json_data = json.loads(log_data)
            except:
                print(log_data)
                continue
            if 'time' not in json_data:
                continue

            if self.vhost != json_data['vhost']:
                continue
            '''
            {
            "time": "2020-05-15T04:16:35+00:00",
            "proxy_remote_addr": "100.125.67.192",
            "remote_addr": "127.0.0.1",
            "x-forward-for": "127.0.0.1",
            "request_id": "aa63c76702939972a6a726c17f0c53b8",
            "remote_user": "-",
            "bytes_sent": 1831505,
            "request_time": 2.609,
            "status": 500,
            "vhost": "mailweb.openeuler.org",
            "request_proto": "HTTP/1.1",
            "path": "/accounts/openid/login/",
            "request_query": "next=%2Fhyperkitty%2Flist%2Fmarketing%40openeuler.org%2F2019%2F2%2F&openid=http%3A%2F%2Fme.yahoo.com&process=login",
            "request_length": 409,
            "duration": 2.609,
            "method": "GET",
            "http_referrer": "-",
            "http_user_agent": "Mozilla/5.0 (compatible; SemrushBot/6~bl; +http://www.semrush.com/bot.html)",
            "geoip2_city_country_name": "-",
            "geoip2_city": "-",
            "geoip2_latitude": "-",
            "geoip2_longitude": "-"
            }
            '''
            t = json_data['time']
            ip = json_data.get('proxy_remote_addr')
            if ip is None or ip == "-":
                ip = json_data.get('remote_addr')
            path = json_data['path']

            if path == "-":
                continue
            if path.endswith("/"):
                continue

            body = {
                  "country": json_data['geoip2_city_country_name'],
                  "city": json_data['geoip2_city'],
                  "hostname": json_data['vhost'],
                  "ip": ip,
                  "location_ip": json_data['remote_addr'],
                  "created_at": t,
                  "updated_at": t,
                  "link": json_data['request_query'],
                  "path": path,
                  "location": {
                    "lat": json_data['geoip2_latitude'],
                    "lon": json_data['geoip2_longitude']
                  }
            }

            if ".iso" in path:
                body["is_iso_download"] = 1
            elif ".rpm" in path:
                body["is_rpm_download"] = 1

            print("filename=%s, ip=%s, time=%s" % (filename, ip, t))
            id = t + ip + path
            action = common.getSingleAction(self.index_name, id, body)
            actions += action
            count += 1
            try:
                if count > 100000:
                    print("Start to Write data to es")
                    self.esClient.safe_put_bulk(actions)
                    actions = ""
                    count = 0
                    print("Write data to es success")
            except:
                print("Write data to es failed, count=%d, request id=%s"% (count, json_data.get("request_id")))
                count = 0
                continue
        self.esClient.safe_put_bulk(actions)
        f.close()


    def writeDataToDB(self, from_date):
        filename = self.download_log_dir + "/access.log." + from_date
        if path.exists(filename) == True:
            self.writeIosDownDataByFile(filename)
        else:
            print("The file(%s) not exist" % filename)


    def writeESDataToDB(self, from_date):
        file_date = datetime.strptime(from_date, "%Y%m%d").strftime("%Y.%m.%d")
        filename = self.download_log_dir + "/logstash-" + file_date + "-hk-app.json"
        if path.exists(filename) == True:
            self.writeIosDownESDataByFile(filename)
        else:
            print("The file(%s) not exist" % filename)


    def run(self, from_date=None):
        startTime = time.time()

        nowDate = datetime.today()
        while datetime.strptime(from_date, "%Y%m%d") < nowDate:
            # self.writeDataToDB(from_date)
            self.writeESDataToDB(from_date)

            fromTime = datetime.strptime(from_date, "%Y%m%d") + timedelta(days=1)
            from_date = fromTime.strftime("%Y%m%d")
            print(from_date)

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect download data from obs:"
              " finished after (%s)" % (spent_time))
