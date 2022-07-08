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
# Create: 2022-03
#
import datetime
import unittest

import sys
import os
import warnings
from unittest import mock

from dateutil.tz import tzoffset

os_path = os.getcwd()
sys.path.append("../..")

from data import common

# Mock reueqst response
class MockResponse(object):
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code
    
    def json(self):
        return self.json_data


class TestCommon(unittest.TestCase):
    """Common unit test"""

    def setUP(self):
        pass

    def tearDown(self):
        pass

    def test_getSingleAction(self):
        """test splice a doc with id, index name and body"""
        index_name = "test_index_name"
        id = "pull_request_342344"
        body = '{"a":1}'
        action = common.getSingleAction(index_name, id, body)

        self.assertEqual(action,
                         '{"index": {"_index": "test_index_name", "_id": "pull_request_342344"}}\n"{\\"a\\":1}"\n')

    def test_getIPbyLocation(self):
        esclient = common.ESClient(config={})
        res = esclient.getIPbyLocation(addr="成都市")
        self.assertEqual(res, {'lon': 104.0633717, 'lat': 30.6598628})

    def test_create_log_dir(self):
        warnings.simplefilter('ignore', ResourceWarning)
        test_dir = "C:\\Users"
        res = common.create_log_dir(test_dir)
        self.assertEqual(res, True)

    def test_get_date(self):
        time_str = '2022-01-01T00:00:00+08:00'
        res = common.get_date(time_str)
        self.assertEqual(res, '2022-01-01T00:00:00')

    def test_get_time_to_first_attention(self):
        common.get_time_to_first_attention = mock.Mock(return_value='2022-01-01T00:00:00+08:00')
        res = common.get_time_to_first_attention(None)
        self.assertEqual(res, '2022-01-01T00:00:00+08:00')

    def test_get_time_diff_days(self):
        start_time = '2022-01-01T00:00:00+08:00'
        end_time = '2022-02-01T12:00:00+08:00'
        res = common.get_time_diff_days(start_time, end_time)
        self.assertEqual(res, 31.5)

    def test_str_to_datetime(self):
        ts = '2022-01-01T00:00:00+08:00'
        res = common.str_to_datetime(ts)
        self.assertEqual(res, datetime.datetime(2022, 1, 1, 0, 0, tzinfo=tzoffset(None, 28800)))
        self.assertIsInstance(res, datetime.datetime)

    def test_getGenerator(self):
        response = '''{"data":[
                    {"index": {"_index": "test_index_name", "_id": "pull_request_342344"}},
                    {"index": {"_index": "test_index_name", "_id": "pull_request_342344"}},
                    {"index": {"_index": "test_index_name", "_id": "pull_request_342344"}}
                    ]}'''
        res = common.getGenerator(response)
        self.assertEqual(len(res['data']), 3)

    def test_convert_to_localTime(self):
        input_time = datetime.datetime.strptime('2022-01-01', '%Y-%m-%d')
        res = common.convert_to_localTime(input_time)
        self.assertIn('+', str(res))

    def test_getRepoSigs(self):
        """test get all repo sigs"""
        esClient = common.ESClient({
            "sig_index": "test_sig_index_name",
            "index_name": "test_index_name",
            "es_url": "test_es_url"})

        mock_get = MockResponse({'hits': {'hits': [{'_source': {'sig_name': 'Infra', 'repos': ['website']}}]}}, 200)
        esClient.request_get = mock.Mock(return_value=mock_get)
        sigs = esClient.getRepoSigs()
        self.assertEqual(sigs, {'website': ['Infra']})


if __name__ == '__main__':
    unittest.main()
