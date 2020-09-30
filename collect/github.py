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

import datetime
import logging
import os
import json
import requests

import dateutil.parser
import dateutil.relativedelta
import dateutil.tz

MAX_CATEGORY_ITEMS_PER_PAGE = 100

BASE_URL = 'https://api.github.com'
GITHUB_URL = "https://github.com/"
logger = logging.getLogger(__name__)


class GithubClient(object):
    """HyperKitty backend.
    This class allows the fetch the email messages stored on a HyperKitty
    archiver. Initialize this class passing the URL where the mailing list
    archiver is and the directory path where the mbox files will be fetched
    and stored. The origin of the data will be set to the value of `url`.
    :param url: URL to the HyperKitty mailing list archiver
    :param dirpath: directory path where the mboxes are stored
    :param tag: label used to mark the data
    :param archive: archive to store/retrieve items
    :param ssl_verify: enable/disable SSL verification
    """
    def __init__(self, org, repository, token,
                 base_url=None, max_items=MAX_CATEGORY_ITEMS_PER_PAGE,):
        self.org = org
        self.repository = repository
        self.headers = {
                    'Content-Type': 'application/json'
                }
        self.headers["Authorization"] = token
        self.base_url = BASE_URL
        self.session = requests.Session()
        self.ssl_verify = True
        if self._set_extra_headers():
            self.session.headers.update(self._set_extra_headers())
        # refresh the access token
        # self._refresh_access_token()

    def getAllrepo(self):
        full_names = []
        r = requests.get('https://api.github.com/users/' + self.org + '/repos' + '?pape=1&per_page=10000',
                         headers=self.headers)
        data = r.json()
        return data


    def getClone(self, repository=None):
        if repository:
            repo = repository
        else:
            repo = self.repository
        c = requests.get('https://api.github.com/repos/' + self.org + '/' + repo + '/traffic/clones', headers=self.headers)
        cloneObj = c.json()
        return cloneObj


    def repo(self):
        """Get repository data"""
        path = self.urijoin(self.base_url, 'repos', self.org, self.repository)

        r = self.fetch(path)
        repo = r.json()

        return repo

    def fetch(self, url, payload=None, headers=None, method="GET", stream=False, auth=None):
        """Fetch the data from a given URL.
        :param url: link to thecommits resource
        :param payload: payload of the request
        :param headers: headers of the request
        :param method: type of request call (GET or POST)
        :param stream: defer downloading the response body until the response content is available
        :param auth: auth of the request
        :returns a response object
        """
        if method == 'GET':
            response = requests.get(url, headers=self.headers,
                             verify=self.ssl_verify,
                             stream=stream, auth=auth)
            # response = self.session.get(url, params=payload, headers=headers, stream=stream,
            #                             verify=self.ssl_verify, auth=auth)
        else:
            response = requests.post(url, headers=self.headers,
                                        verify=self.ssl_verify,
                                        stream=stream, auth=auth)

        return response

    def _set_extra_headers(self):
        """Set extra headers for session"""
        headers = {}
        # set the header for request
        headers.update({'Content-Type': 'application/json;charset=UTF-8'})
        return headers

    # def _refresh_access_token(self):
    #     """Send a refresh post access to the Gitee Server"""
    #     if self.access_token:
    #         url = GITHUB_URL + "?grant_type=refresh_token&refresh_token=" + self.access_token
    #         logger.info("Refresh the access_token for Gitee API")
    #         self.session.post(url, data=None, headers=None, stream=False, auth=None)

    def urijoin(self, *args):
        """Joins given arguments into a URI.
        """
        return '/'.join(map(lambda x: str(x).strip('/'), args))

