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


import unittest

import collection.errors as errors


# Mock classes to test BaseError class
class MockErrorNoArgs(errors.BaseError):
    message = "Mock error without args"


class MockErrorArgs(errors.BaseError):
    message = "Mock error with args. Error: %(code)s %(msg)s"


class TestBaseError(unittest.TestCase):

    def test_subblass_with_no_args(self):
        """Check subclasses that do not require arguments.

        Arguments passed to the constructor should be ignored.
        """
        e = MockErrorNoArgs(code=1, msg='Fatal error')

        self.assertEqual("Mock error without args", str(e))

    def test_subclass_args(self):
        """Check subclasses that require arguments"""

        e = MockErrorArgs(code=1, msg='Fatal error')

        self.assertEqual("Mock error with args. Error: 1 Fatal error",
                         str(e))

    def test_subclass_invalid_args(self):
        """Check when required arguments are not given.

        When this happens, it raises a KeyError exception.
        """
        kwargs = {'code': 1, 'error': 'Fatal error'}
        self.assertRaises(KeyError, MockErrorArgs, **kwargs)


class TestArchiveError(unittest.TestCase):

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.ArchiveError(cause='archive item not found')
        self.assertEqual('archive item not found', str(e))


class TestArchiveManagerError(unittest.TestCase):

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.ArchiveManagerError(cause='archive not found')
        self.assertEqual('archive not found', str(e))


class TestBackendError(unittest.TestCase):

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.BackendError(cause='mock error on backend')
        self.assertEqual('mock error on backend', str(e))


class TestRepositoryError(unittest.TestCase):

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.RepositoryError(cause='error cloning repository')
        self.assertEqual('error cloning repository', str(e))


class TestRateLimitError(unittest.TestCase):

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.RateLimitError(cause="client rate exhausted",
                                  seconds_to_reset=10)
        self.assertEqual("client rate exhausted; 10 seconds to rate reset",
                         str(e))

    def test_seconds_to_reset_property(self):
        """Test property"""

        e = errors.RateLimitError(cause="client rate exhausted",
                                  seconds_to_reset=10)
        self.assertEqual(e.seconds_to_reset, 10)


class TestParseError(unittest.TestCase):

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.ParseError(cause='error on line 10')
        self.assertEqual('error on line 10', str(e))


class TestBackendCommandArgumentParserError(unittest.TestCase):

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.BackendCommandArgumentParserError(cause='mock error on backend command argument parser')
        self.assertEqual('mock error on backend command argument parser', str(e))


if __name__ == "__main__":
    unittest.main()
