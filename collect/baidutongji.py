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

import time
import requests
from datetime import timedelta, datetime

import threading
import json

ENTERPRISE_URL = "https://api.baidu.com/json/tongji/v1/ReportService/getData"
URL = "https://openapi.baidu.com/rest/2.0/tongji/report/getData?"

class BaiDuTongjiClient():

    def __init__(self, config=None):
        self.username = config.get("username")
        self.password = config.get("password")
        self.token = config.get("token")
        self.site_id = config.get("site_id")
        self.is_enterprise = config.get("is_enterprise")

    # common
    def getCommon(self, starTime, endTime, metric, method):
        if self.is_enterprise == "true":
            data_json = {
                "header": {
                    "username": self.username,
                    "password": self.password,
                    "token": self.token,
                    "account_type": "1"
                },
                "body": {
                    "site_id": self.site_id,
                    "start_date": starTime,
                    "end_date": endTime,
                    "metrics": metric,
                    "method": method
                }
            }
            print(data_json)
            data = requests.post(url=ENTERPRISE_URL, json=data_json)
        else:
            params = {
                "access_token": self.token,
                "site_id": self.site_id,
                "start_date": starTime,
                "end_date": endTime,
                "metrics": metric,
                "method": method,
            }
            data = requests.get(url=URL, params=params)

        j = data.json()
        return j

