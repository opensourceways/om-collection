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
import re
import time
from datetime import timedelta, datetime

from os import path
from data import common
from data.common import ESClient
from obs import ObsClient



class OBS(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.access_key = config.get('access_key')
        self.access_secret = config.get('access_secret')
        self.cloud_endpoint = config.get('cloud_endpoint')
        self.download_log_dir = config.get('download_log_dir')
        self.esClient = ESClient(config)
        self.obsClient = ObsClient(
            access_key_id=self.access_key,
            secret_access_key=self.access_secret,
            server=self.cloud_endpoint
        )
        self.bucket_name = config.get('bucket_name')
        self.object_prefix = config.get('object_prefix')
        self.esClient.initLocationGeoIPIndex()
        self.ips = config.get('ips')


    def getObjectFiles(self, prefix):
        # 设置每页100个对象
        max_keys = 100
        index = 1
        marker = None
        objects = []
        while True:
            resp = self.obsClient.listObjects(
                        self.bucket_name, prefix=prefix, max_keys=max_keys, marker=marker)
            if resp.status < 300:
                print('requestId:', resp.requestId)
                for content in resp.body.contents:
                    print('object [' + str(index) + ']')
                    print('key:', content.key)
                    print('owner_id:', content.owner.owner_id)
                    print('owner_name:', content.owner.owner_name)
                    objects.append(content.key)
                    index += 1
                if not resp.body.is_truncated:
                    break
                marker = resp.body.next_marker
            else:
                print('errorCode:', resp.errorCode)
                print('errorMessage:', resp.errorMessage)
                break
        return objects


    def getFileContent(self, bucket_name, object_name):
        resp = self.obsClient.getObject(bucket_name, object_name,
                                   loadStreamInMemory=False)
        chunks = ""
        if resp.status < 300:
            print('requestId:', resp.requestId)
            # 读取对象内容
            while True:
                chunk = resp.body.response.read(65536)
                if not chunk:
                    break
                chunks += str(chunk, encoding="utf-8")
            resp.body.response.close()
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
        return chunks


    def changeToDate(self, str_date):
        # "12/May/2020:08:28:54 +0000"
        t = time.strftime("%Y-%m-%dT%H:%M:%S+00:00",
                          time.strptime(str_date, "%d/%b/%Y:%H:%M:%S +0000"))
        # d = datetime.strptime(str_date, "%Y%m%d")
        # format_date = d.strftime("%Y-%m-%d")
        return t


    def getSingleDoc(self, single_text):
        # e155282a24354ab7bdf7928f712b3797 ms-release [28/Mar/2020:09:36:26 +0000]
        #  116.66.184.188 e155282a24354ab7bdf7928f712b3797 00000171207FEBE8640E4D914FBD0554
        #  REST.GET.ACL 0.1.0-alpha/MindSpore/gpu/cuda-9.2/mindspore-0.1.0-cp37-cp37m-linux_x86_64.whl
        #  "GET /ms-release/0.1.0-alpha/MindSpore/gpu/cuda-9.2/mindspore-0.1.0-cp37-cp37m-linux_x86_64.whl?acl&versionId=null HTTP/1.1"
        #  200 - 504 - 8 8 "-" "HttpClient" - -
        if not single_text:
            return None, None
        BUKET_NAME_INDEX = 1
        DATE_INDEX = 2
        IP_INDEX = 4
        PATH_INDEX = 8
        OBJECT_TYPE = 7
        ID_INDEX = 6
        data = single_text.split( )
        buket_name = data[BUKET_NAME_INDEX]
        ip = data[IP_INDEX]
        path = data[PATH_INDEX]
        id = data[ID_INDEX]
        object_type = data[OBJECT_TYPE]
        type = data[9].replace('"', "")
        method_path = data[10]

        http_version = data[11].replace('"', "")
        response_code = data[12]
        tool = data[19].replace('"', "")

        try:
            from_system = single_text.split('(')[1].split(')')[0].split(";")[0]
        except:
            from_system = None

        created_date = single_text.split('[')[1].split(']')[0]
        print(self.changeToDate(created_date))
        url = single_text.split('"')[1].split('"')[0]

        # Donload_path_prefix = ['0.1.0-alpha', '0.2.0-alpha']
        path = path.replace("%2F", "/")
        if self.object_prefix in path:
            return None, None

        if path == "-":
            return None, None

        if path.endswith("/"):
            return None, None

        package_version = self.downloadPackageVersion(path)
        location = self.esClient.get_ip_location(ip)
        if self.ips and ip in self.ips:
            location = self.esClient.get_ds_ip_location(ip)
        print(".......location =", location)

        created_at = self.changeToDate(created_date)
        single_data = {
            "buket_name": buket_name,
            "created_at": created_at,
            "ip": ip,
            "path": path,
            "package_version": package_version,
            "url": url,
            "object_type": object_type,
            "method": type,
            "from_tool": tool,
            "method_path": method_path,
            "response_code": response_code,
            "http_version": http_version,
            "from_system": from_system,
            "location": location.get('location'),
            "country": location.get('country_iso_code'),
            "city": location.get('city_name'),
            "region_name": location.get('region_name'),
            "continent_name": location.get('continent_name'),
            "region_iso_code": location.get('region_iso_code'),
        }

        id += created_at
        return single_data, id


    def setOneDayData(self, prefix):
        objects = self.getObjectFiles(prefix=prefix)
        actions = ""
        for object in objects:
            data = self.getFileContent(self.bucket_name, object)
            all_data = data.split("\n")
            for d in all_data:
                if not d:
                    continue
                doc, id = self.getSingleDoc(d)
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


    def getThreadFuncs(self, from_date):
        nowDate = datetime.today()
        thread_func_args = {}
        prefixs = []
        while datetime.strptime(from_date, "%Y%m%d") < nowDate:
            prefix = self.getPreFixDate(from_date)
            prefixs.append(prefix)
            from_date = self.getNextDate(from_date)
        thread_func_args[self.setOneDayData] = prefixs
        return thread_func_args

    def downloadPackageVersion(self, path):
        res = ''
        if path is not None and re.match(r'^.*/.*', path):
            arr = re.split(r'/', path, maxsplit=1)
            res = arr[0]
        return res

    def run(self, from_date=None):
        startTime = time.time()
        print("Collect download data from obs(%s):"
              " staring" % self.bucket_name)

        if from_date is None:
            from_date = self.esClient.getLastFormatTime()
            print("Get last format from_data is", from_date)

        thread_func_args = self.getThreadFuncs(from_date)
        common.writeDataThread(thread_func_args)

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect download data from obs(%s):"
              " finished after (%s)" % (self.bucket_name, spent_time))