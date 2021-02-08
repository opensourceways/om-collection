#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
from data.common import ESClient
from data import common
import datetime


class Meetings(object):

    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.meetings_url = config.get('meetings_url')
        self.headers = {'Content-Type': 'application/json', 'Authorization': config.get('authorization')}

    def run(self, from_time):
        self.get_all_meetings()

    def get_all_meetings(self):
        res = requests.get(url=self.meetings_url, headers=self.headers)
        datap = ''
        for i in json.loads(res.content):
            meet_date = datetime.datetime.strptime(i.get("end"), "%H:%M") - datetime.datetime.strptime(i.get("start"), "%H:%M")
            i["duration_time"] = int(meet_date.seconds)
            participants = self.get_participants_by_meet(i.get("mid"))
            i["total_records"] = participants.get("total_records", 0)
            i["participants"] = participants.get("participants", [])
            datar = common.getSingleAction(self.index_name, i['id'], i)
            datap += datar
        self.esClient.safe_put_bulk(datap)
        print('get all meetings end...')

    def get_participants_by_meet(self, mid):
        res = requests.get(url=self.meetings_url + "participants/" + mid + "/")
        if res.status_code != 200:
            if res.status_code == 404:
                print("The meeting participants not found: ", res.status_code)
                return {}
            else:
                print("Get participants failed: ", res.status_code)
                return {}

        participants = json.loads(res.content)
        return participants
