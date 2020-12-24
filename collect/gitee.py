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


import os
import signal
import yaml

import json
import requests
import logging

import re
import types
import _thread

import dateutil.parser
import dateutil.rrule
import dateutil.tz
import threading
import time
import datetime
from configparser import ConfigParser

logger = logging.getLogger(__name__)

GITEE_URL = "https://gitee.com/"
GITEE_API_URL = "https://gitee.com/api/v5"
GITEE_REFRESH_TOKEN_URL = "https://gitee.com/oauth/token"

MAX_CATEGORY_ITEMS_PER_PAGE = 100
PER_PAGE = 100

# Default sleep time and retries to deal with connection/server problems
DEFAULT_SLEEP_TIME = 1
MAX_RETRIES = 5
globa_threadinfo = threading.local()
config = ConfigParser()
config.read('../config.ini')
retry_time = config.getint('general', 'retry_time')
retry_sleep_time = config.getint('general', 'retry_sleep_time')


def globalExceptionHandler(func):
    def warp(*args,**kwargs):
        try:
            # 第一次进来初始化重试次数变量
            if args[len(args) - 1] != "retry":
                globa_threadinfo.num = 0
                # 重试是否成功0 未成功，1 成功
                globa_threadinfo.retrystate = 0
            newarg = []
            # 执行func 参数去除retry标识
            for i in args:
                if i != 'retry':
                    newarg.append(i)
            response = func(*newarg)
        except requests.exceptions.RequestException as ex:
            while globa_threadinfo.num < retry_time and globa_threadinfo.retrystate == 0:
                try:
                    globa_threadinfo.num += 1
                    print(
                        "retry:" + threading.currentThread().getName() + str(func.__name__) + ":" + str(
                            globa_threadinfo.num) + "次")
                    print("error:" + str(ex))
                    print(args)
                    time.sleep(retry_sleep_time)
                    # 防止重复添加标识
                    if 'retry' not in args:
                        warp(*args,"retry",**kwargs)
                    else:
                        warp(*args,**kwargs)
                finally:
                    pass
        else:
            if isinstance(response, requests.models.Response):
                if response.status_code == 401 or response.status_code == 403:
                    print({"状态码": response.status_code})
            # 重试成功，修改状态
            globa_threadinfo.retrystate = 1
            globa_threadinfo.num = 0
            return response

    return warp


class GiteeClient():
    _users = {}  # users cache
    _users_orgs = {}  # users orgs cache

    def __init__(self, owner, repository, token,
                 base_url=None, max_items=MAX_CATEGORY_ITEMS_PER_PAGE, ):
        self.owner = owner
        self.repository = repository
        # Just take the first token
        if token:
            self.access_token = token
        else:
            self.access_token = None

        # Gitee doesn't have rate limit check yet
        self.last_rate_limit_checked = None
        self.max_items = max_items

        if base_url:
            base_url = self.urijoin(base_url, 'api', 'v5')
        else:
            base_url = GITEE_API_URL
        self.base_url = base_url
        self.ssl_verify = True
        self.session = requests.Session()

        if self._set_extra_headers():
            self.session.headers.update(self._set_extra_headers())
        # refresh the access token
        self._refresh_access_token()

    def issue_comments(self, issue_number, from_date=None):
        """Get the issue comments """

        payload = {
            'per_page': PER_PAGE
        }
        if from_date:
            payload['since'] = from_date

        path = self.urijoin("issues", issue_number, "comments")
        return self.fetch_items(path, payload)

    def issues(self, from_date=None):
        payload = {
            'state': 'all',
            'per_page': self.max_items,
            'direction': 'asc',
            'sort': 'created'
        }

        if from_date:
            payload['since'] = from_date.isoformat()

        path = self.urijoin("issues")
        return self.fetch_items(path, payload)

    def pulls(self, state='all'):
        payload = {
            'state': state,
            'per_page': self.max_items,
            'direction': 'asc',
            'sort': 'created'
        }

        path = self.urijoin("pulls")
        return self.fetch_items(path, payload)

    def pull_files(self, pr_number):
        """Get pull request action logs"""

        pull_files = self.urijoin("pulls", str(pr_number), "files")
        return self.fetch_items(pull_files, {})

    def events(self, page=1):
        """Fetch the pull requests from the repository.
        The method retrieves, from a Gitee repository, the pull requests
        updated since the given date.
        """
        payload = {
            'per_page': self.max_items,
            'page': page,
        }

        path = self.urijoin("events")
        return self.fetch_items(path, payload)

    def repo(self):
        """Get repository data"""

        path = self.urijoin(self.base_url, 'repos', self.owner, self.repository)

        r = self.fetch(path)
        repo = r.text

        return repo

    def collaborators(self):
        """Get collaborators data"""

        commit_url = self.urijoin('collaborators')

        return self.fetch_items(commit_url, {})

    def enterprise_members(self):
        """Get enterprise members data"""

        url = self.urijoin('enterprises', self.owner, 'members')
        payload = {
            'role': 'all',
            'per_page': PER_PAGE,
        }

        return self.fetch_items(url, payload)

    def org(self):
        """Get repository data"""
        commit_url = self.urijoin('orgs', self.owner, 'repos')

        payload = {
            'type': 'all',
            'per_page': PER_PAGE,
        }

        return self.fetch_items(commit_url, payload)

    def enterprises(self):
        """Get repository data"""
        commit_url = self.urijoin('enterprises', self.owner, 'repos')

        payload = {
            'type': 'all',
            'direct': True,
            'per_page': PER_PAGE,
        }

        return self.fetch_items(commit_url, payload)

    def forks(self):
        """Get forks data"""
        commit_url = self.urijoin('forks')

        payload = {
            'per_page': PER_PAGE,
        }

        return self.fetch_items(commit_url, payload)

    def stars(self):
        """Get stars data"""
        commit_url = self.urijoin('stargazers')

        payload = {
            'per_page': PER_PAGE,
        }

        return self.fetch_items(commit_url, payload)

    def watchs(self):
        """Get watchs data"""
        commit_url = self.urijoin('subscribers')

        payload = {
            'per_page': PER_PAGE,
        }

        return self.fetch_items(commit_url, payload)

    def pull_action_logs(self, pr_number):
        """Get pull request action logs"""

        pull_action_logs_path = self.urijoin("pulls", str(pr_number), "operate_logs")
        return self.fetch_items(pull_action_logs_path, {})
    def pull_code_diff(self,pr_number):
        """get pull code diff number"""
        pull_code_diff_path = self.urijoin("pulls", str(pr_number), "files")
        return self.fetch_items(pull_code_diff_path, {})


    def pull_commits(self, pr_number):
        """Get pull request commits"""

        payload = {
            'per_page': PER_PAGE,
        }

        commit_url = self.urijoin("pulls", str(pr_number), "commits")
        return self.fetch_items(commit_url, payload)

    def pull_review_comments(self, pr_number):
        """Get pull request review comments"""

        payload = {
            'per_page': PER_PAGE,
            'direction': 'asc',
            # doesn't suppor sort parameter
            # 'sort': 'updated'
        }

        comments_url = self.urijoin("pulls", str(pr_number), "comments")
        return self.fetch_items(comments_url, payload)

    def user(self, login):
        """Get the user information and update the user cache"""
        user = None

        if login in self._users:
            return self._users[login]

        url_user = self.urijoin(self.base_url, 'users', login)

        logger.debug("Getting info for %s" % url_user)

        r = self.fetch(url_user)
        user = r.text
        self._users[login] = user

        return user

    def user_orgs(self, login):
        """Get the user public organizations"""
        if login in self._users_orgs:
            return self._users_orgs[login]

        url = self.urijoin(self.base_url, 'users', login, 'orgs')
        try:
            r = self.fetch(url)
            orgs = r.text
        except requests.exceptions.HTTPError as error:
            # 404 not found is wrongly received sometimes
            if error.response.status_code == 404:
                logger.error("Can't get gitee login orgs: %s", error)
                orgs = '[]'
            else:
                raise error

        self._users_orgs[login] = orgs

        return orgs

    def org_followers(self, org):
        payload = {
            'per_page': PER_PAGE,
        }

        url = self.urijoin("orgs", org, "followers")
        return self.fetch_items(url, payload)

    @globalExceptionHandler
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
        # Add the access_token to the payload
        if self.access_token:
            if not payload:
                payload = {}
            payload["access_token"] = self.access_token

            # response = super().fetch(url, payload, headers, method, stream, auth)
            if method == 'GET':
                response = self.session.get(url, params=payload, headers=headers, stream=stream,
                                            verify=self.ssl_verify, auth=auth)
            else:
                response = self.session.post(url, data=payload, headers=headers, stream=stream,
                                             verify=self.ssl_verify, auth=auth)

            return response

    def fetch_items(self, path, payload):
        """Return the items from gitee API using links pagination"""
        page = 0  # current page
        total_page = None  # total page number

        if self.repository:
            url_next = self.urijoin(self.base_url, 'repos', self.owner, self.repository, path)
        else:
            url_next = self.urijoin(self.base_url, path)

        response = self.fetch(url_next, payload=payload)

        if response.status_code != 200:
            print("Gitee api get error: ", response.text)

        items = response.text

        page += 1
        total_page = response.headers.get('total_page')

        if total_page:
            total_page = int(total_page[0])
            print("Page: %i/%i" % (page, total_page))

        while items:
            yield items
            items = None
            if 'next' in response.links:
                url_next = response.links['next']['url']
                response = self.fetch(url_next, payload=payload)
                page += 1
                items = response.text
                print("Page: %i/%i" % (page, total_page))

    def _set_extra_headers(self):
        """Set extra headers for session"""
        headers = {}
        # set the header for request
        headers.update({'Content-Type': 'application/json;charset=UTF-8'})
        return headers

    def _refresh_access_token(self):
        """Send a refresh post access to the Gitee Server"""
        if self.access_token:
            url = GITEE_REFRESH_TOKEN_URL + "?grant_type=refresh_token&refresh_token=" + self.access_token
            logger.info("Refresh the access_token for Gitee API")
            self.session.post(url, data=None, headers=None, stream=False, auth=None)

    def urijoin(self, *args):
        """Joins given arguments into a URI.
        """
        return '/'.join(map(lambda x: str(x).strip('/'), args))
