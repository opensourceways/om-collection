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

import requests
import json
from data.common import ESClient
from data import common
import datetime


class Meetings(object):

    def __init__(self, config=None):
        self.config = config
        self.org = config.get('org', '')
        self.target_es_url = config.get('target_es_url')
        self.target_authorization = config.get('target_authorization')
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.meetings_url = config.get('meetings_url')
        self.headers = {'Content-Type': 'application/json', 'Authorization': config.get('authorization')}
        self.use_headers = config.get('use_headers', 'true')
        self.query_token = config.get('query_token')

    def run(self, from_time):
        print("*** Meetings collection start ***")
        self.getGiteeId2Company()
        self.get_all_meetings()
        self.tagUserOrgChanged()

    def get_all_meetings(self):
        print('get all meetings start...')
        url = self.meetings_url + "allmeetings/?token=" + self.query_token
        if self.org == 'mindspore':
            url = self.meetings_url + "meetingslist/?token=" + self.query_token
        if self.use_headers == 'true':
            res = self.esClient.request_get(url=url, headers=self.headers)
        else:
            res = self.esClient.request_get(url=url)
        if res.status_code != 200:
            print('request fail, code=%d' % res.status_code)
            return
        print('meetings data len: %d' % len(res.json()))
        datap = ''
        for i in json.loads(res.content):
            meet_date = datetime.datetime.strptime(i.get("end"), "%H:%M") - datetime.datetime.strptime(i.get("start"), "%H:%M")
            i["duration_time"] = int(meet_date.seconds)
            participants = self.get_participants_by_meet(i.get("mid"))
            if len(participants) == 0:
                continue
            i["total_records"] = participants.get("total_records", 0)
            if self.org == 'mindspore':
                i["total_records"] = participants.get("total_count", 0)
            i["participants"] = participants.get("participants", [])
            company = 'independent'
            if i['sponsor'] in self.esClient.giteeid_company_dict:
                company = self.esClient.giteeid_company_dict[i['sponsor']]
            i["tag_user_company"] = company
            datar = common.getSingleAction(self.index_name, i['id'], i)
            datap += datar
        header = {
            "Content-Type": 'application/x-ndjson',
            'Authorization': self.target_authorization
        }
        self.esClient.safe_put_bulk(bulk_json=datap, header=header, url=self.target_es_url)
        print('get all meetings end...')

    def get_participants_by_meet(self, mid):
        res = self.esClient.request_get(url=self.meetings_url + "participants/" + mid + "/?token=" + self.query_token)
        if res.status_code != 200:
            if res.status_code == 404:
                print("The meeting participants not found: ", res.status_code)
                return {}
            else:
                print("Get participants failed: ", res.status_code)
                return {}

        participants = json.loads(res.content)
        return participants

    def getGiteeId2Company(self):
        dic = self.esClient.getOrgByGiteeID()
        self.esClient.giteeid_company_dict = dic[0]
        self.esClient.giteeid_company_change_dict = dic[1]

    def tagUserOrgChanged(self):
        if len(self.esClient.giteeid_company_change_dict) == 0:
            return

        for key, vMap in self.esClient.giteeid_company_change_dict.items():
            vMap.keys()
            times = sorted(vMap.keys())
            for i in range(1, len(times)):
                if i == 1:
                    startTime = '1990-01-01'
                else:
                    startTime = times[i - 1]
                if i == len(times):
                    endTime = '2222-01-01'
                else:
                    endTime = times[i]
                company = vMap.get(times[i - 1])

                query = '''{
                    "script": {
                        "source": "ctx._source['tag_user_company']='%s'"
                    },
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "create_time": {
                                            "gte": "%s",
                                            "lt": "%s"
                                        }
                                    }
                                },
                                {
                                    "term": {
                                        "sponsor.keyword": "%s"
                                    }
                                }
                            ]
                        }
                    }
                }''' % (company, startTime, endTime, key)
                self.esClient.updateByQuery(query=query.encode(encoding='UTF-8'))