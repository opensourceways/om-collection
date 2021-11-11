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
import time

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
                 base_url=None, max_items=MAX_CATEGORY_ITEMS_PER_PAGE, ):
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

    def getAllRepoDetail(self):
        repo_detail_list = []

        current_page = 1
        while True:
            url = f'https://api.github.com/orgs/{self.org}/repos?page={current_page}&per_page=100'
            res = requests.get(url, headers=self.headers)
            data = res.json()
            if not data:  ## if data is a empty list, break loop
                break
            repo_detail_list.extend(data)
            current_page += 1

        return repo_detail_list

    def getUserByName(self, name):
        headers = self.headers
        headers['Accept'] = 'application/vnd.github.v3.star+json'
        r = requests.get('https://api.github.com/users/%s' % name,
                         headers=self.headers)
        data = r.json()
        return data

    def getUserByID(self, id):
        headers = self.headers
        headers['Accept'] = 'application/vnd.github.v3.star+json'
        r = requests.get('https://api.github.com/user/%s' % id,
                         headers=self.headers)
        data = r.json()
        return data

    def getAllOwnerRepo(self, owner):
        full_names = []
        r = requests.get('https://api.github.com/users/' + owner + '/repos' + '?pape=1&per_page=10000',
                         headers=self.headers)
        data = r.json()
        return data

    def getClone(self, repository=None):
        if repository:
            repo = repository
        else:
            repo = self.repository
        c = requests.get('https://api.github.com/repos/' + self.org + '/' + repo + '/traffic/clones',
                         headers=self.headers)
        cloneObj = c.json()
        return cloneObj

    def repo(self):
        """Get repository data"""
        path = self.urijoin(self.base_url, 'repos', self.org, self.repository)

        r = self.fetch(path)
        if r.status_code != 200:
            print(f'Failed get data from {self.repository}.')
            return
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

    def getStarDetails(self, owner):
        # return star details
        api_type_suffix = "stargazers"
        path = self.urijoin(self.base_url, 'repos', owner, self.repository, api_type_suffix)

        headers = self.headers
        headers['Accept'] = 'application/vnd.github.v3.star+json'
        result = self.getSpecificDetailsWithPath(path=path, headers=headers)
        return result

    def getIssueDetails(self, owner):
        # return issue details
        api_type_suffix = "issues"
        path = self.urijoin(self.base_url, 'repos', owner, self.repository, api_type_suffix)
        headers = self.headers
        headers['Accept'] = 'application/vnd.github.v3.star+json'

        # Accquire data through pagination
        page = 1
        per_page = 100

        repos = []
        while True:
            url = path + f"?page={page}&per_page={per_page}&state=all"
            r = self.session.get(url=url, headers=headers)

            print("Remining API calling times: ", r.headers.get('X-RateLimit-Remaining'))

            # Cause API has rate limit in a specific time, so sleep the thread before it exceed.
            if int(r.headers.get('X-RateLimit-Used')) >= int(r.headers.get('X-RateLimit-Limit')) - 1:
                print("Thread sleeping, cause exceed the rate limit of github...")
                time.sleep(3600)

            if r.text == "[]":
                break

            repo = r.json()
            repos.extend(repo)
            page += 1
        return repos

    def getSpecificDetailsWithPath(self, path, headers):

        # Accquire data through pagination
        page = 1
        per_page = 100

        repos = []
        while True:
            url = path + f"?page={page}&per_page={per_page}"
            r = self.session.get(url=url, headers=headers)

            print("Remining API calling times: ", r.headers.get('X-RateLimit-Remaining'))

            # Cause API has rate limit in a specific time, so sleep the thread before it exceed.
            if int(r.headers.get('X-RateLimit-Used')) >= int(r.headers.get('X-RateLimit-Limit')) - 1:
                print("Thread sleeping, cause exceed the rate limit of github...")
                time.sleep(3600)

            if r.text == "[]":
                break

            repo = r.json()
            repos.extend(repo)
            page += 1

        return repos

    def get_repos(self, org):
        url = self.urijoin(BASE_URL, 'orgs', org, 'repos')
        params = {
            'type': 'all',
            'sort': 'created',
            'direction': 'asc',
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        datas = []
        self.get_data(url=url, params=params, current_page=1, datas=datas)
        return datas

    def get_swf(self, owner, repo, item):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, item)
        params = {
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        datas = []
        self.get_data(url=url, params=params, current_page=1, datas=datas)
        return datas

    def get_pr(self, owner, repo):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, 'pulls')
        params = {
            'state': 'all',
            'sort': 'updated',
            'direction': 'desc',
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        datas = []
        self.get_data(url=url, params=params, current_page=1, datas=datas)
        return datas

    def get_issue(self, owner, repo):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, 'issues')
        params = {
            'state': 'all',
            'sort': 'updated',
            'direction': 'desc',
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        datas = []
        self.get_data(url=url, params=params, current_page=1, datas=datas)
        return datas

    def get_pr_review(self, owner, repo, pr_num):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, 'pulls', pr_num, 'reviews')
        params = {
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        datas = []
        self.get_data(url=url, params=params, current_page=1, datas=datas)
        return datas

    def get_pr_comment(self, owner, repo, pr_num):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, 'pulls', pr_num, 'comments')
        params = {
            'sort': 'updated',
            'direction': 'desc',
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        datas = []
        self.get_data(url=url, params=params, current_page=1, datas=datas)
        return datas

    def get_issue_comment(self, owner, repo, issue_num):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, 'issues', issue_num, 'comments')
        params = {
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        datas = []
        self.get_data(url=url, params=params, current_page=1, datas=datas)
        return datas

    def get_data(self, url, params, current_page, datas):
        print('****** Data page: %i ******' % current_page)
        req = self.http_req(url=url, params=params)

        if req.status_code != 200:
            print('Get data error, API: %s' % url)

        if int(req.headers.get('X-RateLimit-Used')) >= int(req.headers.get('X-RateLimit-Limit')) - 1:
            print("Thread sleeping, cause exceed the rate limit of github...")
            time.sleep(3600)
            current_page -= 1

        datas.extend(req.json())

        if 'next' in req.links:
            url_next = req.links['next']['url']
            current_page += 1
            self.get_data(url_next, params=params, current_page=current_page, datas=datas)

    def http_req(self, url, params, method='GET', headers=None, stream=False, auth=None):
        if headers is None:
            headers = self.headers
        headers['Accept'] = 'application/vnd.github.v3.star+json'

        if method == 'GET':
            response = self.session.get(url, params=params, headers=headers, stream=stream,
                                        verify=True, auth=auth, timeout=60)
        else:
            response = self.session.post(url, data=params, headers=headers, stream=stream,
                                         verify=True, auth=auth)
        return response
