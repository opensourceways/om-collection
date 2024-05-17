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
# Create: 2024/5/14
import json
from datetime import datetime, timedelta

from collect.gitee_v8 import GiteeClient
from data.common import ESClient

PER_PAGE = 100


class EventLogV8(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.esClient.initLocationGeoIPIndex()
        self.org = config.get('org')
        self.index_name_token = config.get('index_name_token')
        self.service = config.get('service')
        self.collect_from = config.get('collect_from')
        self.collect_end = config.get('collect_end')

    def run(self, from_time):
        now = datetime.today()
        start_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        token = self.esClient.get_access_token_service(self.index_name_token, self.service)
        gitee_client = GiteeClient(self.org, token)
        if self.collect_from and self.collect_end:
            self.collect_event_by_day(gitee_client, self.collect_from)
        else:
            self.collect_event(gitee_client, start_date, start_date)

    def collect_event_by_day(self, gitee_client, from_date):
        from_time = datetime.strptime(from_date, "%Y-%m-%d")
        while from_time.strftime("%Y-%m-%d") <= self.collect_end:
            self.collect_event(gitee_client, from_time.strftime("%Y-%m-%d"), from_time.strftime("%Y-%m-%d"))
            from_time = from_time + timedelta(days=1)

    def collect_event(self, gitee_client, start_date, end_date):
        print("collect event log on ", start_date)
        token = self.esClient.get_access_token_service(self.index_name_token, self.service)
        gitee_client.access_token = token
        cur_page = 1
        res = gitee_client.get_event_log(page=cur_page, start_date=start_date, end_date=end_date)
        if res.status_code != 200:
            print('Gitee api get error:', res.text)
            return
        resp = res.json()
        total_page = gitee_client.get_total_page(resp["total_count"], PER_PAGE)
        print("Page: %i/%i" % (cur_page, total_page))
        event_data = resp.get('data')
        self.parse_event(event_data)
        while cur_page < total_page:
            cur_page += 1
            print("Page: %i/%i" % (cur_page, total_page))
            res = gitee_client.get_event_log(page=cur_page, start_date=start_date, end_date=end_date)
            if res.status_code != 200:
                print('Gitee api get error:', res.text)
                continue
            event_data = res.json().get('data')
            self.parse_event(event_data)

    def parse_event(self, events):
        actions = ''
        for event in events:
            action = {
                'event': event.get('stat_type_cn'),
                'ip': event.get('ip_filter'),
                'path': event.get('project').get('path_with_namespace'),
                'author_id': event.get('project').get('creator').get('id'),
                'user_login': event.get('project').get('creator').get('username'),
                'created_at': event.get('created_at'),
                'updated_at': event.get('updated_at')
            }
            if event.get('stat_type_cn') == 'DOWNLOAD ZIP' or event.get('stat_type_cn') == 'DOWNLOAD_ZIP':
                action.update(self.esClient.getLocationByIP(event.get('ip_filter')))
            doc_id = event.get('uuid') + event.get('stat_type_cn')
            index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)
