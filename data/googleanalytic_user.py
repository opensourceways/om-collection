import json
import math
import time
from datetime import datetime, timedelta

import requests
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from collect.gitee import GiteeClient
from data.common import ESClient

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']


class GOOGLEANALYTICUSER(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.community_name = config.get('community_name')
        self.view_id = config.get("view_id")
        self.key_file_location = config.get('key_file_location')
        self.request_body_file_name = config.get('request_body_file_location')
        self.base_url = config.get('base_url')
        self.es_url = config.get('es_url')
        # self.gitee_token_v5 = config.get('gitee_token_v5')
        self.authorization = config.get('authorization')
        self.headers = {
            'Authorization': self.authorization,
            'Content-Type': 'application/json'
        }

    def initialize_analyticsreporting(self):
        """Initializes an Analytics Reporting API V4 service object.

        Returns:
          An authorized Analytics Reporting API V4 service object.
        """
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.key_file_location, SCOPES)

        # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        return analytics

    def get_report(self, body=None, pageToken=1, pageSize=1):
        """Queries the Analytics Reporting API V4.

        Args:
          analytics: An authorized Analytics Reporting API V4 service object.
        Returns:
          The Analytics Reporting API V4 response.
        """
        body['reportRequests'][0]['pageToken'] = str(pageToken)
        body['reportRequests'][0]['pageSize'] = str(pageSize)
        result = self.analytics.reports().batchGet(body=body).execute()
        # print(f'Success to fetch googleanalytic data from function: get_report.')
        return result

    def run(self, startTime):
        global totalCount
        yesterday = datetime.today() + timedelta(days=-1)

        # Build the GA Object
        self.analytics = self.initialize_analyticsreporting()

        if startTime:
            self.startDate = datetime.strptime(startTime, '%Y%m%d')
            self.startDate_str = self.startDate.strftime('%Y-%m-%d')

        if self.startDate > yesterday:
            print('Cannot collect data beyond yesterday')
            return

        print(f'collecting data from {self.startDate_str} to yesterday')

        # Prepare query file for request method
        with open(self.request_body_file_name, 'r', encoding="utf-8") as f:
            content = eval(f.read())
        content['reportRequests'][0]['viewId'] = self.view_id
        content['reportRequests'][0]['dateRanges'][0]['startDate'] = self.startDate_str

        # Get total data count firstly
        try:
            totalCount = self.get_report(body=content).get('reports')[0].get('data').get('rowCount')
            print(f'Get the totalCount: {totalCount} of your googleanalytic data successfully.')
        except Exception as ex:
            print(f'Failed to get the totalCount of your googleanalytic data. Program will be over.')
            print(repr(ex))
            return

        ## Accquire website data
        content_rows = self.fetch_items(body=content, totalCount=totalCount, pageSize=1000)

        # # Local test dialog
        # # write into local file for test
        # with open('result_demo_data.txt', mode='a', encoding='utf-8') as f:
        #     for element in content_rows:
        #         f.write(str(element) + '\n')
        #
        # content_rows = self.readContentFromLocal(file_name='result_demo_data.txt')

        # Parse data, then assemble them into regular format
        content_actions = self.parse_data(rows=content_rows)
        print(f'\nCollect data sucessfully, starting to store them...\n')

        ## Store data into ES
        self.esClient.safe_put_bulk(content_actions)

        # Generate all_ip_count and all_pv_count
        from_date = ''.join(self.startDate_str.split('-'))
        self.esClient.setToltalCount(from_date, "ip_count", field="source_type_title.keyword")
        self.esClient.setToltalCount(from_date, "pv_count", field="source_type_title.keyword")

        # Set incrementation of each day of ip_count and pv_count
        yesterday_str = datetime.strftime(yesterday, "%Y-%m-%d")
        self.setIncrementationOfMetric(from_date=self.startDate_str, end_date=yesterday_str,
                                       field='source_type_title.keyword')

        print(f'Run over!!!')

    def readContentFromLocal(self, file_name):
        content_list = []
        with open(file_name, encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines:
            content_list.append(eval(line.strip()))
        return content_list

    def getTime(self, time, endTime=None):
        time = datetime.strptime(time, '%Y%m%d').strftime('%Y-%m-%d')
        if endTime is None:
            endTime = "08:59"
        return time + "T" + endTime + ":59+08:00"

    def fetch_items(self, body, totalCount, pageSize):
        print(f'Starting to collect googleanalytic data...')

        # Calculate total page of data
        pageOdds = totalCount % pageSize
        total_page = math.floor(totalCount / pageSize)
        if pageOdds != 0:
            total_page += 1

        # Fetch data by per page
        row_list = []
        try:
            pageToken = 0
            for page in range(total_page):
                response = self.get_report(body=body, pageToken=pageToken, pageSize=pageSize)
                try:
                    pageToken = int(response.get('reports')[0].get('nextPageToken'))
                except:
                    pass
                rows = response['reports'][0]['data'].get('rows')
                row_list.extend(rows)
            print(f'Completely collected googlelanalytic data.')
        except Exception as ex:
            print(f'Occur some exception in fetching googleanalytic data.')
            print(repr(ex))
        return row_list

    def parse_data(self, rows):
        print(f'Starting to parse the accquired data...')
        actions = ''
        for row in rows:
            row_body = self.parse_each_row(row)
            id = hash(
                f"{row_body['country']}_{row_body['created_at']}_{row_body[self.google_baidu_map['pageTitle']]}_{row_body['source']}_{row_body['source_engine_title']}" \
                f"_{row_body[self.google_baidu_map['region']]}")
            action = self.getSingleAction(index_name=self.index_name, id=id, body=row_body)
            actions += action
        print(f'Completed parse data.')
        return actions

    def parse_each_row(self, row):
        row_body = {}
        dimensions = row.get('dimensions')
        values = row.get('metrics')[0].get('values')
        self.google_baidu_map = {'pageviews': 'pv_count', 'users': 'visit_count', 'avgTimeOnPage': 'avg_visit_time',
                                 'newUsers': 'new_visitor_count', 'region': 'visit_district_title',
                                 'pageTitle': 'visit_page_title'}

        row_body['resource'] = 'googleAnalytic'
        row_body['country'] = dimensions[0]
        row_body['created_at'] = self.getTime(dimensions[2])
        row_body[self.google_baidu_map['pageTitle']] = dimensions[6]
        row_body['source'] = dimensions[3]
        # row_body['medium'] = dimensions[4]

        if dimensions[4] in ['organic', 'cpc']:
            row_body['source_type_title'] = '搜索引擎'
            row_body['source_engine_title'] = dimensions[3]
        elif dimensions[4] == 'referral':
            row_body['source_type_title'] = '外部链接'
            row_body['source_engine_title'] = None
        else:
            row_body['source_type_title'] = '直接访问'
            row_body['source_engine_title'] = None

        # row_body['url'] = self.base_url + dimensions[5]
        row_body[self.google_baidu_map['pageviews']] = int(values[3])  # pv_count

        row_body[self.google_baidu_map['users']] = int(values[0])  # visit_count
        row_body['ip_count'] = int(values[0])  # ip_count is same as visit_count

        row_body[self.google_baidu_map['newUsers']] = int(values[1])  # new_visitor_count
        # row_body['sessions'] = int(values[2])
        row_body[self.google_baidu_map['avgTimeOnPage']] = round(float(values[4]), 2)  # avg_visit_time
        row_body[self.google_baidu_map['region']] = dimensions[1]  # visit_district_title
        return row_body

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""

        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData, ensure_ascii=False) + '\n'
        action += json.dumps(body, ensure_ascii=False) + '\n'
        return action

    def setIncrementationOfMetric(self, from_date=None, end_date=None, field=None):
        print(f'Starting to set the increment data into ES in fuction: setIncrementationOfMetric')
        global actions, data_buckets
        try:
            data = self.getAggregationResult(from_date=from_date, end_date=end_date, field=field)
            data_buckets = data.get('aggregations').get('histogram_by_created_at').get('buckets')
        except:
            print(f'Failed to fetch data in function getAggregationResult.')

        try:
            ## parse and assemble data
            actions = ''
            for bucket in data_buckets:
                query_day = bucket.get('key_as_string')
                timestamp = str(
                    time.mktime(time.strptime(f'{query_day}T08:59:59+08:00', '%Y-%m-%dT%H:%M:%S+08:00'))).replace(
                    '.', '')
                each_data_buckets = bucket.get('group_by_source_type').get('buckets')
                if not each_data_buckets:  ## it can cause has no data in a given period
                    continue

                each_data_buckets_body = {}
                for data_bucket in each_data_buckets:
                    source_type_title = data_bucket.get('key')
                    sum_of_ip_count = int(data_bucket.get('sum_of_ip_count').get('value'))
                    sum_of_pv_count = int(data_bucket.get('sum_of_pv_count').get('value'))
                    avg_time_of_each_visit_str = data_bucket.get('avg_time_of_each_visit').get('value')
                    avg_time_of_each_visit = float('%.2f' % avg_time_of_each_visit_str)

                    # Set values into each_data_buckets_body
                    each_data_buckets_body['created_at'] = f'{query_day}T08:59:59+08:00'
                    each_data_buckets_body['source_type_title'] = source_type_title
                    each_data_buckets_body['sum_of_ip_count'] = sum_of_ip_count
                    each_data_buckets_body['sum_of_pv_count'] = sum_of_pv_count
                    each_data_buckets_body['avg_time_of_each_visit'] = avg_time_of_each_visit
                    each_data_buckets_body['is_aggregation_data_each_day'] = 1

                    id = source_type_title + timestamp
                    action = self.getSingleAction(index_name=self.index_name, id=id, body=each_data_buckets_body,
                                                  act='index')
                    actions += action
        except:
            print(f'Failed to parse or assemble data_buckets in setIncrementationOfMetric')

        ## Write the aggs data to ES
        self.esClient.safe_put_bulk(actions)
        print(f'Finish function setIncrementationOfMetric.')

    def getAggregationResult(self, from_date, end_date, field):
        ## Prepare args for request
        global response
        url = self.es_url + '/' + self.index_name + '/_search'
        # query_string = '''{\"query\":{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"exit_count\"}},\"filter\":{\"range\":{\"created_at\":{\"gte\":\"%s\",\"lt\":\"%s\"}}}}},\"size\":0,\"aggs\":{\"histogram_by_created_at\":{\"date_histogram\":{\"field\":\"created_at\",\"interval\":\"day\",\"format\":\"yyyy-MM-dd\",\"min_doc_count\":1},\"aggs\":{\"group_by_source_type\":{\"terms\":{\"field\":\"%s"},\"aggs\":{\"sum_of_%s\":{\"sum\":{\"field\":\"%s\"}}}}}}}}''' % (
        #     from_date, end_date, field, metric, metric)
        query_string = '''
        {"query":{"bool":{"must_not":{"exists":{"field":"exit_count"}},"filter":{"range":{"created_at":{"gte":"%s","lt":"%s"}}}}},"size":0,"aggs":{"histogram_by_created_at":{"date_histogram":{"field":"created_at","interval":"day","format":"yyyy-MM-dd","min_doc_count":1},"aggs":{"group_by_source_type":{"terms":{"field":"source_type_title.keyword"},"aggs":{"sum_of_ip_count":{"sum":{"field":"ip_count"}},"sum_of_pv_count":{"sum":{"field":"pv_count"}},"avg_time_of_each_visit":{"avg":{"field":"avg_visit_time"}}}}}}}}
        ''' % (from_date, end_date)
        payload = query_string
        headers = self.headers

        ##Send request and get data
        try:
            response = requests.request("POST", url, headers=headers, data=payload, verify=False)
        except Exception as ex:
            print(f"Failed to fetch data in method: getAggregationResult.")
            pass

        if response.status_code != 200:
            print(f'Failed to fetch data in method: getAggregationResult.')
            return None
        data = json.dumps(response.text, ensure_ascii=False)
        data = data[1:-1].replace('\\', '')

        return json.loads(data)
