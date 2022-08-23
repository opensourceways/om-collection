#  Copyright (c) 2022.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import datetime
import json
import time

import requests
from dateutil.relativedelta import relativedelta
from data.common import ESClient


class DownloadCount(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.query_index_name = config.get('query_index_name')
        self.query = config.get('query')
        self.from_date = config.get('from_date')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.region = config.get('region')
        self.regions = self.region.split(',') if self.region else []

    def run(self, start=None):
        self.get_download_by_days()

    def get_query_data(self, index, query, from_time, end_time):
        query = query % (from_time, end_time)
        url = self.url + '/' + index + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        if res.status_code != 200:
            return []
        data = res.json()['aggregations']['group_filed']['buckets']
        return data

    def parse_data(self, data, created_at):
        actions = ''
        for r in data:
            country = r.get('key')
            is_oversea = 0 if country in self.regions else 1
            action = {
                "country": country,
                "download": r.get('doc_count'),
                "created_at": created_at,
                "is_oversea": is_oversea
            }
            indexData = {"index": {"_index": self.index_name, "_id": country + created_at}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
        return actions

    def parse_no_location_data(self, data, created_at):
        actions = ''
        for r in data:
            is_oversea = 0
            count_d = r.get('doc_count')
            action = {
                "download": count_d,
                "created_at": created_at,
                "is_oversea": is_oversea
            }
            indexData = {"index": {"_index": self.index_name, "_id": 'no_country' + created_at}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
        return actions

    def get_download_by_days(self):
        if self.from_date is None:
            from_date = datetime.date.today()
        else:
            from_date = datetime.datetime.strptime(self.from_date, "%Y%m%d")
        now_date = datetime.date.today().strftime("%Y%m%d")

        actions = ""
        count = 0
        while from_date.strftime("%Y%m%d") <= now_date:
            actions += self.get_download(from_date)
            from_date += relativedelta(days=1)
            count += 1
            if count > 10:
                self.esClient.safe_put_bulk(actions)
                actions = ""
                count = 0
        self.esClient.safe_put_bulk(actions)

    def get_download(self, date):
        end = date + relativedelta(days=1)
        from_time = time.mktime(date.timetuple()) * 1000
        end_time = time.mktime(end.timetuple()) * 1000
        print("Compute download: ", date)
        created_at = date.strftime("%Y-%m-%dT08:00:00+08:00")
        query = self.query.split(';') if self.query else None
        if query is None:
            return
        res = self.get_query_data(self.query_index_name, query[0], from_time, end_time)
        res_no_location = self.get_query_data(self.query_index_name, query[1], from_time, end_time)
        actions = self.parse_data(res, created_at) + self.parse_no_location_data(res_no_location, created_at)
        return actions
