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
import requests
from datetime import timedelta, datetime

import threading
import json

ENTERPRISE_URL = "https://api.baidu.com/json/tongji/v1/ReportService/getData"
URL = "https://openapi.baidu.com/rest/2.0/tongji/report/getData?"
BAIDUTONGJI_REFRESH_TOKEN_URL = "http://openapi.baidu.com/oauth/2.0/token"

class BaiDuTongjiClient():

    def __init__(self, config=None):
        self.username = config.get("username")
        self.password = config.get("password")
        self.token = config.get("token")
        self.site_id = config.get("site_id")
        self.is_baidutongji_enterprise = config.get("is_baidutongji_enterprise")

    # common
    def getCommon(self, starTime, endTime, metric, method):
        if self.is_baidutongji_enterprise == "true":
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
            # print(data_json)
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

    def _refresh_access_token(self):
        """Send a refresh post access to the Gitee Server"""
        if self.token:
            url = GITEE_REFRESH_TOKEN_URL + "?grant_type=refresh_token&refresh_token=" + self.token
            logger.info("Refresh the access_token for Baidutongji API")
            self.session.post(url, data=None, headers=None, stream=False, auth=None)

