#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2024/5/13
import datetime
import json

from dateutil.relativedelta import relativedelta

from collect.api import request_url


class ExportTask(object):
    def __init__(self, config=None):
        self.config = config
        self.token_url = config.get('token_url')
        self.client_id = config.get('client_id')
        self.client_secret = config.get('client_secret')
        self.client_id = config.get('client_id')
        self.app_id = config.get('app_id')
        self.product_id = config.get('product_id')
        self.task_url = config.get('task_url')
        self.events = config.get('events')
        self.last_run_date = None

    def run(self, from_date):
        yesterday = datetime.datetime.today() - relativedelta(days=1)
        start_date = yesterday.strftime("%Y-%m-%d")
        if self.last_run_date == start_date:
            print("has been executed today")
            return
        self.last_run_date = start_date
        print(f'export task: {start_date}')
        self.get_task(start_date, start_date)

    def get_access_token(self):
        payload = f'grant_type=client_credentials&client_id={self.client_id}&client_secret={self.client_secret}'
        headers = {
            'Content-Type': 'Application/x-www-form-urlencoded'
        }
        response = request_url(self.token_url, headers=headers, payload=payload, method='POST')
        if response.status_code != 200:
            print('unauthenticated: ', response.text)
        token = response.json().get('access_token')
        return token

    def get_task(self, start_date, end_date):
        auth = 'Bearer ' + self.get_access_token()
        header = {
            "Authorization": auth,
            "x-App-id": self.app_id,
            "Content-Type": "application/json",
            "x-product-id": self.product_id
        }
        events = self.events.split(',')

        body = {
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            },
            "file_format": "csv",
            "filters": [{
                "name": "event_id",
                "values": events
            }]
        }
        payload = json.dumps(body)
        req = request_url(self.task_url, headers=header, payload=payload, method='POST')
        print(req.text, req.status_code)

