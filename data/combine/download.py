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
        self.city_info_dict = None
        self.query_city = config.get('query_city')

        self.is_get_no_country_data = config.get('is_get_no_country_data')
        self.query_no_country = config.get('query_no_country')
        self.query_no_city = config.get('query_no_city')

    def run(self, start):
        if self.is_get_no_country_data == 'true':
            print('collect no country download data...')
            self.get_download_by_days(self.get_download_no_country)
            print('collect no country data over...')

        self.city_info_dict = self.get_city_info()
        print('city total count: ', len(self.city_info_dict))

        print('collect download data...')
        self.get_download_by_days(self.get_download_city)
        self.get_download_by_days(self.get_download_no_city)

    def get_query_data(self, index, query, from_time=None, end_time=None):
        if from_time is not None and end_time is not None:
            query = query % (from_time, end_time)
        url = self.url + '/' + index + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        if res.status_code != 200:
            return []
        data = res.json()['aggregations']['group_field']['buckets']
        return data

    def get_city_info(self):
        city_info_dict = {}
        res = self.get_query_data(self.query_index_name, self.query_city)
        for r in res:
            city = r.get('key')
            ip_buckets = r.get('ip').get('buckets')
            ip_address = None
            if ip_buckets and len(ip_buckets) > 0:
                ip_address = ip_buckets[0].get('key')
            location = self.esClient.getLocationByIP(ip_address)
            temp = {
                'ip': ip_address,
                'loc': location
            }
            city_info_dict.update({city: temp})
        return city_info_dict

    def get_download_by_days(self, func):
        if self.from_date is None:
            from_date = datetime.date.today()
        else:
            from_date = datetime.datetime.strptime(self.from_date, "%Y%m%d")
        now_date = datetime.date.today().strftime("%Y%m%d")

        actions = ""
        count = 0
        while from_date.strftime("%Y%m%d") <= now_date:
            actions += func(from_date)
            from_date += relativedelta(days=1)
            count += 1
            if count > 30:
                self.esClient.safe_put_bulk(actions)
                print("Compute download: ", from_date)
                actions = ""
                count = 0
        self.esClient.safe_put_bulk(actions)

    def get_download_no_country(self, date):
        res = self.get_download(date, self.query_no_country, self.query_index_name)
        created_at = date.strftime("%Y-%m-%dT08:00:00+08:00")
        actions = self.parse_no_location_data(res, created_at)
        return actions

    def get_download_no_city(self, date):
        res = self.get_download(date, self.query_no_city, self.query_index_name)
        created_at = date.strftime("%Y-%m-%dT08:00:00+08:00")
        actions = self.parse_no_city_data(res, created_at)
        return actions

    def get_download_city(self, date):
        res = self.get_download(date, self.query, self.query_index_name)
        created_at = date.strftime("%Y-%m-%dT08:00:00+08:00")
        actions = self.parse_city_data(res, created_at)
        return actions

    def get_download(self, date, query, index_name):
        end = date + relativedelta(days=1)
        from_time = time.mktime(date.timetuple()) * 1000
        end_time = time.mktime(end.timetuple()) * 1000
        if query is None:
            return
        res = self.get_query_data(index_name, query, from_time, end_time)
        return res

    def parse_no_city_data(self, data, created_at):
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

    def parse_city_data(self, data, created_at):
        actions = ''
        for r in data:
            city = r.get('key')
            city_info = self.city_info_dict.get(city)
            location = city_info.get('loc')
            country = location.get('country_iso_code')
            country_buckets = r.get('country').get('buckets')
            for country_data in country_buckets:
                country_origin = country_data.get('key')

            is_oversea = 1 if country not in self.regions else 0
            action = {
                "city": city,
                "download": r.get('doc_count'),
                "created_at": created_at,
                "ip": city_info.get('ip'),
                "location": location.get('location'),
                "region_name": location.get('region_name'),
                "country": country,
                "is_oversea": is_oversea
            }
            indexData = {"index": {"_index": self.index_name, "_id": city + created_at}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
        return actions
