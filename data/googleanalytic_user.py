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
# Create: 2022-05
#

import copy
import hashlib
import json
import math
import sys
import threading
import time
from datetime import datetime, timedelta

import requests
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import data.common as common
from data.common import ESClient

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']


class NoneFuncArguException(Exception):
    def __init__(self):
        super().__init__(self)  # initialize super class
        self.function_name = sys._getframe(1).f_code.co_name  # get the calling func name
        self.code_location = sys._getframe(1).f_lineno  # get the calling func location

    def __str__(self):
        return str({'function_name': self.function_name, 'code_location': self.code_location})


class GoogleAnalyticUser(object):
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
        self.collect_endDate_str = config.get('collect_end_date_str')
        self.authorization = config.get('authorization')
        self.headers = {
            'Authorization': self.authorization,
            'Content-Type': 'application/json'
        }
        self.thread_name = threading.currentThread().getName()
        self.lock = threading.Lock()

    def initialize_analyticsreporting(self):
        """
        Initializes an Analytics Reporting API V4 service object.
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
        result = None
        try:
            body['reportRequests'][0]['pageToken'] = str(pageToken)
            body['reportRequests'][0]['pageSize'] = str(pageSize)
            result = self.analytics.reports().batchGet(body=body).execute()
        except Exception as exp:
            print(f'Occur some exception in function: self.get_report, Error as follows:\n{exp.__repr__()}')
            raise exp
        return result

    @common.show_spend_seconds_of_this_function
    def run(self, startTime):

        # <editor-fold desc="Created log dir">
        dest_dir = 'result_logs'
        is_result_log_exist = common.create_log_dir(dest_dir)
        if is_result_log_exist:
            print(f'log_dir exists.')
        else:
            print(f'create log_dir failure.')
        # </editor-fold>

        # <editor-fold desc="Initialize global variables">
        self.analytics = self.initialize_analyticsreporting()  # Build the GA Object
        self.google_baidu_map = {
            'date': 'date', 'region': 'visit_district_title', 'sourceMedium': 'sourceMedium',
            'keyword': 'simple_searchword_title', 'pagePath': 'visit_page_url', 'pageTitle': 'visit_page_title',
            'country': 'country', 'source': 'source', 'medium': 'medium', 'ip_count': 'ip_count',
            'searchKeyword': 'simple_searchword_title',

            'pageviews': 'pv_count', 'users': 'visitor_count', '1dayUsers': 'active_user_count',
            'newUsers': 'new_visitor_count', 'sessions': 'session_count', 'avgTimeOnPage': 'avg_visit_time',
            'pageviewsPerSession': 'avg_visit_pages', 'bounces': 'outward_count', 'bounceRate': 'bounce_rate',

            'total_pageviews': 'all_pv_count', 'total_users': 'all_visitor_count', 'total_ip_count': 'all_ip_count',
            'total_newUsers': 'all_new_visitor_count', 'total_sessions': 'all_session_count',
            'total_bounces': 'all_outward_count', 'avgBounceRate': 'avg_bounce_rate',
            'link_source': 'source_link_title'
        }
        # </editor-fold>

        # <editor-fold desc="Set collecting time window">

        self.set_collect_time_window(startTime)
        print(f'Collecting date: from {self.startDate_str} to {self.endDate_str}')
        # </editor-fold>

        self.request_body = self.get_request_body()

        # # <editor-fold desc="Collect date-1dayUsers data from fetch_auto function">
        date_dimension_list = ['date']
        date_metric_list = ['pageviews', '1dayUsers', 'newUsers', 'sessions', 'avgTimeOnPage', 'pageviewsPerSession',
                            'bounces', 'bounceRate']
        date_row_count, _ = self.process_auto_dimension_metric_data(date_dimension_list, date_metric_list, 'date')
        # # </editor-fold>

        # <editor-fold desc="fetch, parse, store increment data by fetch_increment function">
        increment_dimension_list = ['date']
        increment_field_metric_list = ['total_pageviews', 'total_users', 'total_newUsers', 'total_sessions',
                                       'total_ip_count', 'avgTimeOnPage', 'pageviewsPerSession', 'total_bounces',
                                       'bounceRate']
        increment_row_count = self.process_increment_data(increment_dimension_list, increment_field_metric_list,
                                                          data_type='increment')
        # </editor-fold>

        # # <editor-fold desc="collect whole fields data by fetch_auto function">
        detail_dimension_list = ['date', 'region', 'sourceMedium', 'keyword', 'pagePath', 'pageTitle']
        detail_metric_list = ['pageviews', 'users', 'newUsers', 'sessions', 'avgTimeOnPage', 'pageviewsPerSession',
                              'bounces', 'bounceRate']
        detail_row_count, detail_row_bodies = self.process_auto_dimension_metric_data(detail_dimension_list,
                                                                                      detail_metric_list,
                                                                                      data_type='detail')
        to_local = self.write_content_rows_to_local(content_rows=detail_row_bodies, top_count=len(detail_row_bodies))
        # # </editor-fold>

        total_row_count = date_row_count + increment_row_count + detail_row_count
        print(f'\n==============================================================')
        print(f'date data rows: {date_row_count};')
        print(f'increment data rows: {increment_row_count};')
        print(f'detail data: {detail_row_count};')
        print(f'total data: {total_row_count};')
        print(f'==============================================================\n')

        self.startDate_str = self.endDate_str

        print(f'Run over!!!')

    @common.show_spend_seconds_of_this_function
    def process_increment_data(self, increment_dimension_list, increment_field_metric_list, data_type):
        increment_total_rows = self.fetch_increment_total_rows(request_body=copy.deepcopy(self.request_body))
        increment_total_bodies = self.parse_increment_total_rows_of_each_date(increment_total_rows,
                                                                              increment_field_metric_list, data_type)
        stored_state = self.push_content_rows_into_es(increment_total_bodies, id_field_list=increment_dimension_list,
                                                      data_type=data_type)
        increment_row_count = len(increment_total_rows)
        print(f'Success process increment data.')
        return increment_row_count

    @common.show_spend_seconds_of_this_function
    def process_auto_dimension_metric_data(self, auto_dimension_list, auto_metric_list, data_type):
        _, data_rows = self.fetch_auto_defined_rows(dimension_name_list=auto_dimension_list,
                                                    metric_list=auto_metric_list,
                                                    request_body=copy.deepcopy(self.request_body))

        data_bodies = self.parse_auto_defined_rows(data_rows, auto_dimension_list, auto_metric_list,
                                                   data_type=data_type)
        self.push_content_rows_into_es(data_bodies, auto_dimension_list, data_type)
        print(f'Success process {data_type} data by auto function.')
        data_count = len(data_rows)
        return data_count, data_bodies

    def readContentFromLocal(self, file_name):
        content_list = []
        with open(file_name, encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines:
            content_list.append(eval(line.strip()))
        return content_list

    def getTime(self, time, endTime=None):
        '''
        if time format is %Y-%m-%d
        :param time:
        :param endTime:
        :return:
        '''

        try:
            time = datetime.strptime(time, '%Y%m%d').strftime('%Y-%m-%d')
        except Exception as exp:
            # print(exp.__repr__())
            pass

        if endTime is None:
            endTime = '08:59'
        return time + "T" + endTime + ":59+08:00"

    def fetch_items(self, request_body, totalCount, pageSize):
        '''
        fetch raw data from googleAnalytic by each page
        :param body:
        :param totalCount:
        :param pageSize:
        :return:
        '''
        first_page_report = self.get_report(body=request_body, pageToken=1, pageSize=1)
        total_pages = self.get_total_pages(pageSize, totalCount)
        print(f'Total {total_pages} pages data should be fetch.')
        print(f'Starting to collect googleanalytic data...')
        reside_pages = total_pages

        # Fetch data by per page
        row_list = []
        try:
            pageToken = 0
            for page in range(total_pages):
                response = self.get_report(body=request_body, pageToken=pageToken, pageSize=pageSize)
                rows = response['reports'][0]['data'].get('rows')
                row_list.extend(rows)
                reside_pages -= 1
                print(f'Residing {reside_pages} of total {total_pages} pages.')
                pageToken = response.get('reports')[0].get('nextPageToken')
                if not pageToken:
                    break

            print(f'Completely collected googlelanalytic data.')
        except Exception as ex:
            print(f'{self.thread_name} === Occur some exception in fetching googleanalytic data.'
                  f'Error is:\n {ex.__repr__()}')

        return row_list

    def get_total_pages(self, pageSize, totalCount):
        '''
        :param pageSize: records of each page
        :param totalCount: total count of given datetime window
        :return: total pages calculated from the pageSize and totalCount
        '''
        # Calculate total page of data

        pageOdds = totalCount % pageSize
        total_page = math.floor(totalCount / pageSize)
        if pageOdds != 0:
            total_page += 1
        return total_page

    def fetch_auto_defined_rows(self, dimension_name_list, metric_list, request_body):
        """
        :type request_body: object
        """
        specific_request_body = self.get_specific_requestBody(dimension_name_list, metric_list, request_body)

        try:
            first_page_reports = self.get_report(body=specific_request_body).get('reports')
        except Exception as exp:
            print(f'Failed to get the response reports of your googleanalytic data. Program will be over.')
            print(repr(exp))
            return None

        totalCount = first_page_reports[0].get('data').get('rowCount')
        print(f'Get the totalCount: {totalCount} of your googleanalytic data successfully.')
        content_totals = first_page_reports[0].get('data').get('totals')
        content_rows = self.fetch_items(request_body=request_body, totalCount=totalCount, pageSize=1000)

        return content_totals, content_rows

    @common.show_spend_seconds_of_this_function
    def push_content_rows_into_es(self, data_bodies, id_field_list, data_type):
        '''
        push the data_bodies which has been parsed successfully into es.
        :param data_bodies:
        :param is_contain_createdAt_field: contain created_at field to generate doc id not not
        :return:
        '''

        try:
            if not data_bodies:
                raise NoneFuncArguException
        except NoneFuncArguException as exp:
            print(f'{self.thread_name} ===  data_bodies is None.\n {exp.__repr__()} ')

        total_body_count = len(data_bodies)
        actions = ''
        for data_body in data_bodies:
            data_body_copy = copy.deepcopy(data_body)
            doc_id = self.get_es_doc_id(data_body=data_body_copy, id_field_list=id_field_list, data_type=data_type)
            action = common.getSingleAction(index_name=self.index_name, id=doc_id, body=data_body)
            actions += action

        print(f'{self.thread_name} === Starting to store {total_body_count} rows into ES ....')
        self.esClient.safe_put_bulk(actions)
        print(f'{self.thread_name} === Successfully Stored {total_body_count} rows into ES.')

        return True

    def get_es_doc_id(self, data_body, id_field_list, data_type=None):
        '''
        Generate the doc id for each doc in ES
        doc id comes form fields of data_body
        :param data_body:
        :return: doc id
        i.e.
        {'resource': 'googleAnalytic', 'created_at': '2021-11-21T08:59:59+08:00',
        'visit_district_title': 'Beijing', 'source_type_title': 'search_engine',
        'source_engine_title': 'baidu', 'simple_searchword_title': '(not set)', 'visit_page_url': '/zh/',
        'visit_page_title': 'openEuler', 'pv_count': 105, 'visitor_count': 83, 'ip_count': 83,
        'new_visitor_count': 51, 'session_count': 85, 'avg_visit_time': 272.89,
        'pv_count_per_session': 1.24, 'bounce_count': 48, 'bounce_rate': 56.47}
        '''

        doc_id_list = [str(data_body[self.google_baidu_map[field]]) for field in id_field_list]
        id_field_list.sort()
        doc_id_str = str(data_type) + '_'.join(doc_id_list)
        doc_id = hashlib.sha256('{}\n'.format(doc_id_str).encode('utf-8')).hexdigest()
        return doc_id

    def set_source_type_engine_title(self, dimension_name, dimensions, row_body):
        '''
        Set the fields: source_type_title and source_engine_title for row_body
        :param dimensions:
        :param row_body:
        :return:
        '''
        dimension_item = dimension_name.split('/')
        medium = dimension_item[0].strip()
        source = dimension_item[-1].strip()

        if source == 'organic':
            row_body['source_type_title'] = 'search_engine'
            row_body['source_engine_title'] = medium
            row_body[self.google_baidu_map['keyword']] = dimensions[3]
        elif source == '(none)' and medium == '(direct)':
            row_body['source_type_title'] = 'direct_visit'
        elif source == 'referral':
            row_body['source_type_title'] = 'out_link'
            row_body[self.google_baidu_map['link_source']] = medium
        else:
            row_body['source_type_title'] = 'unexpected'

    def get_request_body(self):
        '''
        Prepare query file for request method
        :return: prepared request_body (dict obj)
        '''
        with open(self.request_body_file_name, 'r', encoding="utf-8") as f:
            content = eval(f.read())

        content['reportRequests'][0]['viewId'] = self.view_id
        content['reportRequests'][0]['dateRanges'][0]['startDate'] = self.startDate_str
        content['reportRequests'][0]['dateRanges'][0]['endDate'] = self.endDate_str
        return content

    def write_content_rows_to_local(self, content_rows, dir_path='result_logs/result_demo_data.txt', top_count=10):
        '''
        :param content_rows: given response data from get_reports
        :param top_count: top records want show in local file
        :return: None
        '''
        content_rows_count = len(content_rows)
        with open(file=dir_path, mode='w', encoding='utf-8') as f:
            f.write(f'Get {content_rows_count} records in total, top {top_count} as follows:\n')
            f.mode = 'a'

            output_rows = content_rows[:top_count]
            for row in output_rows:
                f.write(str(row) + '\n')

        return None

    def set_collect_time_window(self, startTime):
        if not startTime:
            if self.startDate_str:
                startTime = self.startDate_str
            else:
                print(f'StartTime of collecting is not provide, please have a check.')
                raise ValueError

        try:
            startDate = datetime.strptime(startTime, '%Y%m%d')
        except ValueError as ve:
            startDate = datetime.strptime(startTime, '%Y-%m-%d')
        except Exception as exp:
            raise exp

        endDate = datetime.today() + timedelta(days=-1)  # set endDate to yesterday from now
        if self.collect_endDate_str:
            collect_endDate = datetime.strptime(self.collect_endDate_str, '%Y%m%d')
            if collect_endDate > startDate and collect_endDate < endDate:
                endDate = collect_endDate

        if startDate > endDate:
            raise ValueError('startDate beyond endDate')

        self.startDate_str = startDate.strftime('%Y-%m-%d')
        self.endDate_str = endDate.strftime('%Y-%m-%d')

    def get_standarded_numeric_value_from_string(self, num_str):
        '''
        Convert string to numeric value
        :param num_str:
        :return: converted numeric value
        '''

        value = 0

        try:
            if num_str.count('.') == 0:
                value = int(num_str)
            else:
                num_float = float(num_str)
                num_formatted_str = '%.2f' % num_float
                value = float(num_formatted_str)
        except Exception as exp:
            raise exp

        return value

    def get_id_field_list(self, data_bodies, metric_list):
        data_bodies_list = list(data_bodies[0].keys())
        metric_field_list = [self.google_baidu_map[field] for field in metric_list]
        metric_field_list.append('ip_count')
        field_list = list(set(data_bodies_list).difference(set(metric_field_list)))
        field_list.sort()
        return field_list

    @common.show_spend_seconds_of_this_function
    def fetch_increment_total_rows(self, request_body):
        start_date = datetime.strptime(self.startDate_str, '%Y-%m-%d')
        end_date = datetime.strptime(self.endDate_str, '%Y-%m-%d')
        increment_date_total_row_list = []

        days = 0
        while True:
            total_data_dict = {}
            cursor_date = start_date + timedelta(days=days)

            if cursor_date > end_date:
                break

            cursor_date_str = cursor_date.strftime('%Y-%m-%d')
            request_body['reportRequests'][0]['dateRanges'][0]['endDate'] = cursor_date_str

            first_page_reports = None
            try:
                first_page_reports = self.get_report(body=request_body).get('reports')
            except Exception as exp:
                print(f'Failed to get the response reports of your googleanalytic data. Program will be over.')
                print(repr(exp))

            total_data_dict['dimensions'] = [cursor_date.strftime('%Y%m%d')]
            total_data_dict['metrics'] = first_page_reports[0].get('data').get('totals')

            increment_date_total_row_list.append(total_data_dict)

            days += 1

        return increment_date_total_row_list

    @common.show_spend_seconds_of_this_function
    def parse_increment_total_rows_of_each_date(self, rows, increment_field_metric_list, data_type):
        content_bodies = []
        try:
            if not rows:
                raise NoneFuncArguException
        except NoneFuncArguException as exp:
            print(f'{self.thread_name} ===  rows is None.\n {exp.__repr__()} ')

        for row in rows:
            content_body = self.parse_each_increment_total_row(row, increment_field_metric_list, data_type)
            content_bodies.append(content_body)
        return content_bodies

    def parse_each_increment_total_row(self, row, increment_field_metric_list, data_type):
        '''
        {'values': ['7160', '5682', '978', '1784', '64.0561755952381', '4.013452914798206', '1001', '56.109865470852014'], 'date': '2021-11-21'}
        :param row:
        :return:
        '''

        row_body = {}
        dimensions = row.get('dimensions')
        metric = row.get('metrics')[0].get('values')
        metric_values = list(map(self.get_standarded_numeric_value_from_string, metric))
        date_str = dimensions[0]
        row_body['analytic_platform'] = 'GoogleAnalytic'
        row_body['created_at'] = self.getTime(date_str)
        row_body['date'] = date_str
        row_body['data_type'] = data_type

        row_body[self.google_baidu_map['total_pageviews']] = metric_values[0]  # pv_count
        row_body[self.google_baidu_map['total_users']] = metric_values[1]  # visit_count
        row_body[self.google_baidu_map['total_ip_count']] = metric_values[2]  # visit_count
        row_body[self.google_baidu_map['total_newUsers']] = metric_values[2]  # new_visitor_count
        row_body[self.google_baidu_map['total_sessions']] = metric_values[3]
        row_body[self.google_baidu_map['avgTimeOnPage']] = metric_values[4]  # avg_visit_time
        row_body[self.google_baidu_map['pageviewsPerSession']] = metric_values[5]
        row_body[self.google_baidu_map['total_bounces']] = metric_values[6]
        row_body[self.google_baidu_map['bounceRate']] = metric_values[7]

        return row_body

    def get_specific_requestBody(self, dimension_name_list, metric_expression_list, request_body):
        metrics = [{"expression": f"ga:{metric_expression}"} for metric_expression in metric_expression_list]
        dimensions = [{"name": f"ga:{dimension_name}"} for dimension_name in dimension_name_list]
        request_body["reportRequests"][0]["dimensions"] = dimensions
        request_body["reportRequests"][0]["metrics"] = metrics
        return request_body

    @common.show_spend_seconds_of_this_function
    def parse_auto_defined_rows(self, rows, specific_dimension_list, specific_metric_list, data_type):
        '''
        Parse the specific dimension data from GA
        :param rows:
        :return:
        '''

        content_bodies = []
        try:
            if not rows:
                raise NoneFuncArguException
        except NoneFuncArguException as exp:
            print(f'{self.thread_name} ===  rows is None.\n {exp.__repr__()} ')

        for row in rows:
            content_body = self.parse_each_row_of_auto_defined(row, specific_dimension_list, specific_metric_list,
                                                               data_type)
            content_bodies.append(content_body)

        return content_bodies

    def parse_each_row_of_auto_defined(self, row, specific_dimension_list, specific_metric_list, data_type=None):
        '''
        {'dimensions': ['20210705'], 'metrics': [{'values': ['584', '406', '406', '464', '294.65833333333336', '1.2586206896551724', '379', '81.68103448275862']}]}
        :param row:
        :return:
        '''

        row_body = {}
        dimensions = row.get('dimensions')
        metrics = row.get('metrics')[0].get('values')
        metric_values = list(map(self.get_standarded_numeric_value_from_string, metrics))

        row_body['analytic_platform'] = 'GoogleAnalytic'
        self.set_row_body_dimensions(row_body, dimensions, specific_dimension_list, data_type)
        self.set_row_body_metric_values(row_body, metric_values, specific_metric_list, data_type)

        return row_body

    def set_row_body_dimensions(self, row_body, dimensions, specific_dimension_list, data_type):
        row_body['data_type'] = data_type
        for indice in range(len(specific_dimension_list)):
            row_body[self.google_baidu_map[specific_dimension_list[indice]]] = dimensions[indice]

        if row_body.get('date'):
            row_body['created_at'] = self.getTime(row_body.get('date'))
        if 'sourceMedium' in specific_dimension_list:
            self.set_source_type_engine_title(dimension_name=row_body['sourceMedium'], dimensions=dimensions,
                                              row_body=row_body)
        return row_body

    def set_row_body_metric_values(self, row_body, metrics, specific_metric_list, data_type):
        for indice in range(len(specific_metric_list)):
            row_body[self.google_baidu_map[specific_metric_list[indice]]] = metrics[indice]

        if data_type == 'detail':
            row_body['ip_count'] = row_body[self.google_baidu_map['newUsers']]
        if data_type == 'date':
            row_body['ip_count'] = row_body[self.google_baidu_map['1dayUsers']]
        return row_body
