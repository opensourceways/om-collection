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

import unittest

from collection.toolkit.uris import urijoin


class TestURIJoin(unittest.TestCase):
    """Unit tests for urijoin."""

    def test_join(self):
        """Test basic joins."""

        base_url = 'http://example.com/'
        base_url_alt = 'http://example.com'
        path0 = 'owner'
        path1 = 'repository'
        path2 = '/owner/repository'
        path3 = 'issues/8'

        url = urijoin(base_url, path0, path1)
        self.assertEqual(url, 'http://example.com/owner/repository')

        url = urijoin(base_url, path2)
        self.assertEqual(url, 'http://example.com/owner/repository')

        url = urijoin(base_url, path0, path1, path3)
        self.assertEqual(url, 'http://example.com/owner/repository/issues/8')

        url = urijoin(base_url_alt, path0, path1)
        self.assertEqual(url, 'http://example.com/owner/repository')

    def test_remove_trailing_backslash(self):
        """Test if trailing backslash is removed from URLs."""

        base_url = 'http://example.com/'
        path0 = 'repository/'

        url = urijoin(base_url)
        self.assertEqual(url, 'http://example.com')

        url = urijoin(base_url, path0)
        self.assertEqual(url, 'http://example.com/repository')

    def test_remove_double_slash(self):
        """Test if double backslash are removed from URIs."""

        base_url = 'http://example.com/'
        path0 = '/repository/'

        url = urijoin(base_url, path0)
        self.assertEqual(url, 'http://example.com/repository')

        base_uri = 'file:///tmp/'
        path0 = '/repository//'

        url = urijoin(base_uri, path0)
        self.assertEqual(url, 'file:///tmp/repository')


if __name__ == "__main__":
    unittest.main(warnings='ignore')
