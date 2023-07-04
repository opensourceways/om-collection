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


import datetime
import os
import shutil
import unittest
import unittest.mock

import dateutil
import httpretty

from collection.toolkit.uris import urijoin

from collection.backend import BackendCommandArgumentParser
from collection.backends.core.dockerhub import (DockerHub,
                                              DockerHubClient,
                                              DockerHubCommand)
from base import TestCaseBackendArchive

DOCKERHUB_URL = "https://hub.docker.com/"
DOCKERHUB_API_URL = DOCKERHUB_URL + 'v2'
DOCKERHUB_RESPOSITORY_URL = DOCKERHUB_API_URL + '/repositories/collection/collection'


def read_file(filename, mode='r'):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename), mode) as f:
        content = f.read()
    return content


def setup_http_server():
    """Setup a mock HTTP server"""

    body = read_file('data/dockerhub/dockerhub_repository_1.json', 'rb')

    httpretty.register_uri(httpretty.GET,
                           DOCKERHUB_RESPOSITORY_URL,
                           body=body, status=200)


class TestDockerHubBackend(unittest.TestCase):
    """DockerHub backend tests"""

    def test_initialization(self):
        """Test whether attributes are initializated"""

        dockerhub = DockerHub('collection', 'collection', tag='test')

        expected_origin = urijoin(DOCKERHUB_URL, 'collection', 'collection')

        self.assertEqual(dockerhub.owner, 'collection')
        self.assertEqual(dockerhub.repository, 'collection')
        self.assertEqual(dockerhub.origin, expected_origin)
        self.assertEqual(dockerhub.tag, 'test')
        self.assertIsNone(dockerhub.client)
        self.assertTrue(dockerhub.ssl_verify)

        # When tag is empty or None it will be set to
        # the value in
        dockerhub = DockerHub('collection', 'collection', ssl_verify=False)
        self.assertEqual(dockerhub.origin, expected_origin)
        self.assertEqual(dockerhub.tag, expected_origin)
        self.assertFalse(dockerhub.ssl_verify)

        dockerhub = DockerHub('collection', 'collection', tag='')
        self.assertEqual(dockerhub.origin, expected_origin)
        self.assertEqual(dockerhub.tag, expected_origin)

    def test_shortcut_official_owner(self):
        """Test if the shortcut owner is replaced when it is given on init"""

        # Value '_' should be replaced by 'library'
        dockerhub = DockerHub('_', 'redis', tag='test')

        expected_origin = urijoin(DOCKERHUB_URL, 'library', 'redis')

        self.assertEqual(dockerhub.owner, 'library')
        self.assertEqual(dockerhub.repository, 'redis')
        self.assertEqual(dockerhub.origin, expected_origin)

    def test_has_archiving(self):
        """Test if it returns True when has_archiving is called"""

        self.assertEqual(DockerHub.has_archiving(), True)

    def test_has_resuming(self):
        """Test if it returns True when has_resuming is called"""

        self.assertEqual(DockerHub.has_resuming(), True)

    @httpretty.activate
    @unittest.mock.patch('collection.backends.core.dockerhub.datetime_utcnow')
    def test_fetch(self, mock_utcnow):
        """Test whether it fetches data from a repository"""

        mock_utcnow.return_value = datetime.datetime(2017, 1, 1,
                                                     tzinfo=dateutil.tz.tzutc())
        setup_http_server()

        dockerhub = DockerHub('collection', 'collection')
        items = [item for item in dockerhub.fetch()]

        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item['data']['fetched_on'], 1483228800.0)
        self.assertEqual(item['uuid'], '0fa16dc4edab9130a14914a8d797f634d13b4ff4')
        self.assertEqual(item['origin'], 'https://hub.docker.com/collection/collection')
        self.assertEqual(item['updated_on'], 1483228800.0)
        self.assertEqual(item['category'], 'dockerhub-data')
        self.assertEqual(item['tag'], 'https://hub.docker.com/collection/collection')

        # Check requests
        self.assertEqual(len(httpretty.httpretty.latest_requests), 1)

    @httpretty.activate
    @unittest.mock.patch('collection.backends.core.dockerhub.datetime_utcnow')
    def test_search_fields(self, mock_utcnow):
        """Test whether the search_fields is properly set"""

        mock_utcnow.return_value = datetime.datetime(2017, 1, 1,
                                                     tzinfo=dateutil.tz.tzutc())
        setup_http_server()

        dockerhub = DockerHub('collection', 'collection')
        items = [item for item in dockerhub.fetch()]

        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(dockerhub.metadata_id(item['data']), item['search_fields']['item_id'])
        self.assertEqual(item['data']['name'], 'collection')
        self.assertEqual(item['data']['name'], item['search_fields']['name'])
        self.assertEqual(item['data']['namespace'], 'collection')
        self.assertEqual(item['data']['namespace'], item['search_fields']['namespace'])

    def test_parse_json(self):
        """Test if it parses a JSON stream"""

        raw_json = read_file('data/dockerhub/dockerhub_repository_1.json')

        item = DockerHub.parse_json(raw_json)
        self.assertEqual(item['user'], 'collection')
        self.assertEqual(item['name'], 'collection')


class TestDockerHubBackendArchive(TestCaseBackendArchive):
    """DockerHub backend tests using an archive"""

    def setUp(self):
        super().setUp()
        self.backend_write_archive = DockerHub('collection', 'collection', archive=self.archive)
        self.backend_read_archive = DockerHub('collection', 'collection', archive=self.archive)

    def tearDown(self):
        shutil.rmtree(self.test_path)

    @httpretty.activate
    @unittest.mock.patch('collection.backends.core.dockerhub.datetime_utcnow')
    def test_fetch_from_archive(self, mock_utcnow):
        """Test whether it fetches data from a repository"""

        mock_utcnow.return_value = datetime.datetime(2017, 1, 1,
                                                     tzinfo=dateutil.tz.tzutc())
        setup_http_server()
        self._test_fetch_from_archive()


class TestDockerHubClient(unittest.TestCase):
    """DockerHub API client tests.

    These tests do not check the body of the response, only if the call
    was well formed and if a response was obtained. Due to this, take
    into account that the body returned on each request might not
    match with the parameters from the request.
    """
    @httpretty.activate
    def test_repository(self):
        """Test repository API call"""

        # Set up a mock HTTP server
        setup_http_server()

        # Call API
        client = DockerHubClient()
        response = client.repository('collection', 'collection')

        req = httpretty.last_request()

        self.assertEqual(req.method, 'GET')
        self.assertRegex(req.path, '/v2/repositories/collection/collection')
        self.assertDictEqual(req.querystring, {})


class TestDockerHubCommand(unittest.TestCase):
    """Tests for DockerHubCommand class"""

    def test_backend_class(self):
        """Test if the backend class is DockerHub"""

        self.assertIs(DockerHubCommand.BACKEND, DockerHub)

    def test_setup_cmd_parser(self):
        """Test if it parser object is correctly initialized"""

        parser = DockerHubCommand.setup_cmd_parser()
        self.assertIsInstance(parser, BackendCommandArgumentParser)
        self.assertEqual(parser._backend, DockerHub)

        args = ['collection', 'collection', '--no-archive']

        parsed_args = parser.parse(*args)
        self.assertTrue(parsed_args.no_archive)
        self.assertTrue(parsed_args.ssl_verify)
        self.assertEqual(parsed_args.owner, 'collection')
        self.assertEqual(parsed_args.repository, 'collection')

        args = ['collection', 'collection', '--no-ssl-verify']

        parsed_args = parser.parse(*args)
        self.assertFalse(parsed_args.ssl_verify)
        self.assertEqual(parsed_args.owner, 'collection')
        self.assertEqual(parsed_args.repository, 'collection')


if __name__ == "__main__":
    unittest.main(warnings='ignore')
