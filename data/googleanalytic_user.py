import json
import math
import traceback
import uuid
from datetime import datetime, timedelta

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from data.common import ESClient

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']


class GOOGLEANALYTICUSER(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.org = config.get('org')
        self.view_id = config.get("view_id")
        self.key_file_location = config.get('key_file_location')
        self.base_url = config.get('base_url')

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
        return self.analytics.reports().batchGet(
            body=body
        ).execute()

    def run(self, startTime):
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
        file_body = "body.json"
        with open(file_body, 'r', encoding="utf-8") as f:
            content = eval(f.read())
        content['reportRequests'][0]['viewId'] = self.view_id
        content['reportRequests'][0]['dateRanges'][0]['startDate'] = self.startDate_str

        # Get total data count firstly
        totalCount = self.get_report(body=content).get('reports')[0].get('data').get('rowCount')

        # Accquire website data
        content_rows = self.fetch_items(body=content, totalCount=totalCount, pageSize=1000)

        # Parse data, then assemble them into regular format
        content_actions = self.parse_data(rows=content_rows)
        print(f'\nCollect data sucessfully, starting to store them...\n')

        # Store data into ES
        self.esClient.safe_put_bulk(content_actions)

        print(f'Run over!!!')

    def getTime(self, time, endTime=None):
        time = datetime.strptime(time, '%Y%m%d').strftime('%Y-%m-%d')
        if endTime is None:
            endTime = "08:59"
        return time + "T" + endTime + ":59+08:00"

    def fetch_items(self, body, totalCount, pageSize):

        # Calculate total page of data
        pageOdds = totalCount % pageSize
        total_page = math.floor(totalCount / pageSize)
        if pageOdds != 0:
            total_page += 1

        # Fetch data by per page
        row_list = []
        pageToken = 0
        for page in range(total_page):
            response = self.get_report(body=body, pageToken=pageToken, pageSize=pageSize)
            try:
                pageToken = int(response.get('reports')[0].get('nextPageToken'))
            except:
                pass
            rows = response['reports'][0]['data'].get('rows')
            row_list.extend(rows)
        return row_list

    def parse_data(self, rows):
        actions = ''
        for row in rows:
            row_body = self.parse_each_row(row)
            id = hash(
                f"{row_body['country']}_{row_body['region']}_{row_body['created_at']}_{row_body['source']}_{row_body['medium']}" \
                f"_{row_body['search_engine']}_{row_body['url']}_{row_body['pageTitle']}")
            action = self.getSingleAction(index_name=self.index_name, id=id, body=row_body)
            actions += action
        return actions

    def parse_each_row(self, row):
        row_body = {}
        dimensions = row.get('dimensions')
        values = row.get('metrics')[0].get('values')
        google_baidu_map = {'pageviews': 'pv_count', 'users': 'visitor_count', 'avgTimeOnPage': 'avg_visit_time',
                            'newUsers': 'new_visitor_count'}

        row_body['country'] = dimensions[0]
        row_body['region'] = dimensions[1]
        row_body['created_at'] = self.getTime(dimensions[2])
        row_body['source'] = dimensions[3]
        row_body['medium'] = dimensions[4]

        if dimensions[4] == 'organic' or dimensions[4] == 'cpc':
            row_body['visit_source'] = '搜索引擎'
            row_body['search_engine'] = dimensions[3]
        elif dimensions[4] == 'referral':
            row_body['visit_source'] = '外部链接'
            row_body['search_engine'] = None
        else:
            row_body['visit_source'] = '直接访问'
            row_body['search_engine'] = None

        row_body['url'] = self.base_url + dimensions[5]
        row_body['pageTitle'] = dimensions[6]

        row_body[google_baidu_map['users']] = int(values[0])

        try:
            row_body[google_baidu_map['newUsers']] = int(values[1])
        except:
            return row_body
        row_body['sessions'] = int(values[2])
        row_body[google_baidu_map['pageviews']] = int(values[3])
        row_body[google_baidu_map['avgTimeOnPage']] = round(float(values[4]), 2)
        return row_body

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""

        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData) + '\n'
        action += json.dumps(body) + '\n'
        return action
