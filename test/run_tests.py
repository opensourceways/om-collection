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


import os
import sys
import unittest


if __name__ == '__main__':

    test_suite = unittest.TestLoader().discover(os.path.join(os.path.dirname(os.path.abspath(__file__)), "."),
                                                pattern='test_*.py')
    result = unittest.TextTestRunner(buffer=True).run(test_suite)
    sys.exit(not result.wasSuccessful())
