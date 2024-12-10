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
                 base_url=None, max_items=MAX_CATEGORY_ITEMS_PER_PAGE, tokens=None):
        if tokens is None:
            tokens = []
        self.org = org
        self.repository = repository
        self.headers = {'Content-Type': 'application/json', "Authorization": token}
        self.base_url = BASE_URL
        self.session = requests.Session()
        self.ssl_verify = True
        if self._set_extra_headers():
            self.session.headers.update(self._set_extra_headers())
        self.tokens = tokens
        self.used_tokens = []
        self.from_page = 1
        self.end_page = None

    def getAllrepo(self):
        full_names = []
        r = requests.get('https://api.github.com/users/' + self.org + '/repos' + '?pape=1&per_page=10000',
                         headers=self.headers, timeout=60)
        data = r.json()
        return data

    def getAllRepoDetail(self):
        repo_detail_list = []

        current_page = 1
        while True:
            url = f'https://api.github.com/orgs/{self.org}/repos?page={current_page}&per_page=100'
            res = requests.get(url, headers=self.headers, timeout=60)
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
                         headers=self.headers, timeout=60)
        data = r.json()
        return data

    def getUserByID(self, id):
        headers = self.headers
        headers['Accept'] = 'application/vnd.github.v3.star+json'
        r = requests.get('https://api.github.com/user/%s' % id,
                         headers=self.headers, timeout=60)
        data = r.json()
        return data

    def getAllOwnerRepo(self, owner):
        full_names = []
        r = requests.get('https://api.github.com/users/' + owner + '/repos' + '?pape=1&per_page=10000',
                         headers=self.headers, timeout=60)
        data = r.json()
        return data

    def getClone(self, repository=None):
        if repository:
            repo = repository
        else:
            repo = self.repository
        c = requests.get('https://api.github.com/repos/' + self.org + '/' + repo + '/traffic/clones',
                         headers=self.headers, timeout=60)
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
                                    stream=stream, auth=auth,
                                    timeout=60)
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
            if r.status_code == 404:
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

    def get_repo(self, org, repo):
        url = self.urijoin(BASE_URL, 'repos', org, repo)
        datas = []
        self.get_data(url=url, params={}, current_page=1, datas=datas)
        return datas

    def get_swf(self, owner, repo, item):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, item)
        params = {
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        datas = []
        self.get_data(url=url, params=params, current_page=1, datas=datas)
        return datas

    def get_repo_info(self, owner, repo):
        url = self.urijoin(BASE_URL, 'repos', owner, repo)
        params = {
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        datas = []
        self.get_data(url=url, params=params, current_page=1, datas=datas)
        return datas

    def get_pr_pre(self, owner, repo, func):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, 'pulls')
        params = {
            'state': 'all',
            # 'sort': 'updated',
            'sort': 'created',
            'direction': 'desc',
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        self.get_data_pre(url=url, params=params, current_page=self.from_page, func=func, repo=repo)

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

    def get_pull_by_number(self, owner, repo, number):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, 'pulls', number)
        datas = []
        self.get_data(url=url, params={}, current_page=1, datas=datas)
        return datas

    def get_issue_pre(self, owner, repo, func):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, 'issues')
        params = {
            'state': 'all',
            'sort': 'updated',
            'direction': 'desc',
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE
        }
        self.get_data_pre(url=url, params=params, current_page=1, func=func, repo=repo)

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

    def git_search_repo(self, repo):
        url = self.urijoin(BASE_URL, 'search', 'repositories')
        params = {
            'q': repo,
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE,
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

    def get_commit_count(self, owner, repo):
        url = self.urijoin(BASE_URL, 'repos', owner, repo, 'commits')
        params = {
            'per_page': 10
        }
        return self.contribute_count(url=url, params=params)

    def get_contribute_count(self, owner, repo, item):
        url = self.urijoin(self.base_url, 'repos', owner, repo, item)
        payload = {
            'state': "all",
            'per_page': MAX_CATEGORY_ITEMS_PER_PAGE,
            'direction': "desc",
            'sort': "created"
        }
        return self.contribute_count(url, payload)

    def contribute_count(self, url, params):
        per_page = params['per_page']
        req = self.http_req(url=url, params=params)
        if req.status_code != 200:
            print('Get data error, API: %s' % url)
            print(req.text)

        if 'last' in req.links:
            url_last = req.links['last']['url']
            req_last = self.http_req(url=url_last, params=params)
            if req_last.status_code != 200:
                print('Get data error, API: %s' % url_last)
            last_page_len = len(req_last.json())
            last_page = int(url_last.split('page=')[-1])
            total_count = (last_page - 1) * per_page + last_page_len
        else:
            total_count = len(req.json())

        return total_count

    def get_data(self, url, params, current_page, datas):
        print('****** Data page: %i ******' % current_page)
        if self.end_page and current_page > self.end_page:
            return
        params['page'] = current_page
        req = self.http_req(url=url, params=params)

        if req.status_code != 200:
            print('Get data error, API: %s, req: %s' % (req.url, req.text))
            if req.headers.get('X-RateLimit-Used') is not None and req.headers.get(
                    'X-RateLimit-Limit') is not None and int(req.headers.get('X-RateLimit-Used')) >= (
                    int(req.headers.get('X-RateLimit-Limit')) - 1):
                print("Thread sleeping, cause exceed the rate limit of github...")
                self.change_token()
                self.get_data(url, params=params, current_page=current_page, datas=datas)
            else:
                print('No limit, API: %s, req: %s' % (req.url, req.text))
        else:
            print('Get success, API: %s' % req.url)
            js = req.json()
            if type(js) == dict:
                datas.append(js)
            else:
                datas.extend(req.json())

            if 'next' in req.links:
                url_next = req.links['next']['url']
                current_page += 1
                self.get_data(url_next, params=params, current_page=current_page, datas=datas)

    def get_data_pre(self, url, params, current_page, func, repo):
        print('******get_data_pre: Data page: %i ******' % current_page)
        if self.end_page and current_page > self.end_page:
            return
        datas = []
        params['page'] = current_page
        req = self.http_req(url=url, params=params)
        if req.status_code != 200:
            print('Get data error, API: %s, req: %s' % (url, req.text))
            if req.headers.get('X-RateLimit-Used') is not None and req.headers.get(
                    'X-RateLimit-Limit') is not None and int(req.headers.get('X-RateLimit-Used')) >= (
                    int(req.headers.get('X-RateLimit-Limit')) - 1):
                print("Thread sleeping, cause exceed the rate limit of github...")
                self.change_token()
                self.get_data_pre(url, params=params, current_page=current_page, func=func, repo=repo)
            else:
                print('No limit, API: %s, req: %s' % (req.url, req.text))
        else:
            print('Get success, API: %s' % req.url)
            js = req.json()
            if type(js) == dict:
                datas.append(js)
            else:
                datas.extend(req.json())
            print('start parse data')
            func(datas, self, repo)

            if 'next' in req.links:
                url_next = req.links['next']['url']
                current_page += 1
                self.get_data_pre(url_next, params=params, current_page=current_page, func=func, repo=repo)
            else:
                print('*** No next in links, current_page: %d' % current_page)
                return

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

    def change_token(self):
        print('Change token')
        diff = list(set(self.tokens).difference(set(self.used_tokens)))
        if len(diff) == 0:
            token = self.used_tokens[0]
            self.used_tokens = [token]
        else:
            token = diff[0]
        self.used_tokens.append(token)
        self.headers["Authorization"] = token
