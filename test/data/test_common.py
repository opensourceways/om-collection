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

import unittest

import sys
import os

os_path = os.getcwd()
sys.path.append("../..")


from data import common


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


if __name__ == '__main__':
    unittest.main()

