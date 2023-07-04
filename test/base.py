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

import os
import shutil
import tempfile
import unittest

from collection.archive import Archive


class TestCaseBackendArchive(unittest.TestCase):
    """Unit tests for Backend using the archive"""

    def setUp(self):
        self.test_path = tempfile.mkdtemp(prefix='collection_')
        archive_path = os.path.join(self.test_path, 'myarchive')
        self.archive = Archive.create(archive_path)

    def tearDown(self):
        shutil.rmtree(self.test_path)

    def _test_fetch_from_archive(self, **kwargs):
        """Test whether the method fetch_from_archive works properly"""

        items = [items for items in self.backend_write_archive.fetch(**kwargs)]
        items_archived = [item for item in self.backend_read_archive.fetch_from_archive()]

        self.assertEqual(len(items), len(items_archived))

        for i in range(len(items)):
            item = items[i]
            archived_item = items_archived[i]

            del item['timestamp']
            del archived_item['timestamp']

            self.assertEqual(item, archived_item)
