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
# Create: 2021-05
#
import csv
import json
import logging
import os
import threading
import time
from configparser import ConfigParser

import requests

logger = logging.getLogger(__name__)

GITEE_API_URL = "https://api.gitee.com/enterprises"
GITEE_TOKEN_URL = "https://gitee.com/oauth"

MAX_CATEGORY_ITEMS_PER_PAGE = 100
PER_PAGE = 100

MEMBERS_DATA_FILE = f"enterprise_members_data.csv"

MAXIMUM_TIME = 31536000  # One year Unit second

# Default sleep time and retries to deal with connection/server problems
DEFAULT_SLEEP_TIME = 1
MAX_RETRIES = 5
globa_threadinfo = threading.local()
config = ConfigParser()
try:
    config.read('config.ini', encoding='UTF-8')
    retry_time = config.getint('general', 'retry_time', )
    retry_sleep_time = config.getint('general', 'retry_sleep_time')
except BaseException as ex:
    retry_sleep_time = 10
    retry_time = 10


def globalExceptionHandler(func):
    def warp(*args, **kwargs):
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
            response = func(*newarg, **kwargs)
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
                        warp(*args, "retry", **kwargs)
                    else:
                        warp(*args, **kwargs)
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

    def __init__(self, org, token, max_items=MAX_CATEGORY_ITEMS_PER_PAGE):
        self.org = org
        if token:
            self.access_token = token
        else:
            self.access_token = None
        self.last_rate_limit_checked = None
        self.max_items = max_items
        self.ssl_verify = True
        self.session = requests.Session()

        self.enterpriseId = self.getEnterpriseId()
        self.base_url = self.urijoin(GITEE_API_URL, self.enterpriseId)

    def getEnterpriseId(self):
        enterprise_id = 0
        url = "https://api.gitee.com/enterprises/list?direction=desc&page=1"
        try:
            result = self.fetch(url)
        except Exception as e:
            print("fetch failure")
            raise e

        if result.status_code != 200:
            return enterprise_id

        # parse result, get enterprise_id
        datas = json.loads(result.text)['data']
        for data in datas:
            if data['name'].lower() == self.org.lower():
                enterprise_id = data['id']
                break

        return enterprise_id

    def get_repos(self, access_token=None, current_page=1, start_date=None, end_date=None, per_page=None):
        """Return the items from gitee API using links pagination"""
        print("Starting getting repos....")
        path = "projects"
        if not access_token:
            access_token = self.access_token

        total_page_flag = False
        data = []
        while True:
            url = self.assemble_url(path=path, access_token=access_token, current_page=current_page,
                                    start_date=start_date, end_date=end_date, per_page=per_page)
            response = self.fetch(url=url)

            if response.status_code != 200:
                print("Gitee api get error: ", response.text)

            items = json.loads(response.text)  # transform string to dictionary

            records = items['data']
            project_name_dict = []
            for record in records:
                namespace_path = record['namespace']['path']
                project_path = record['path']
                project_name = namespace_path + "/" + project_path
                project_name_dict.append(project_name)

            data.extend(project_name_dict)

            if not total_page_flag:
                total_page = self.get_total_page(items["total_count"], PER_PAGE)
                total_page_flag = True

            complete_ratio = format((current_page / total_page) * 100, '.2f')
            print(f"Repos has been collected {complete_ratio}% ")
            current_page += 1

            if current_page > total_page:
                break

        # assemble data then return
        items['total_page'] = total_page
        items['data'] = data
        return items

    def urijoin(self, *args):
        """Joins given arguments into a URI.
        """
        return '/'.join(map(lambda x: str(x).strip('/'), args))

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

            if method == 'GET':
                response = self.session.get(url, params=payload, headers=headers, stream=stream,
                                            verify=self.ssl_verify, auth=auth, timeout=60)
            else:
                response = self.session.post(url, data=payload, headers=headers, stream=stream,
                                             verify=self.ssl_verify, auth=auth)
            return response

    def fetch_items(self, path, access_token=None, project_name=None, current_page=1, start_date=None, end_date=None,
                    per_page=None):
        """Return the items from gitee API using links pagination"""

        if not access_token:
            access_token = self.access_token

        total_page_flag = False
        data = []
        while True:
            url = self.assemble_url(path=path, access_token=access_token, project_name=project_name,
                                    start_date=start_date, end_date=end_date, per_page=per_page,
                                    current_page=current_page)
            response = self.fetch(url=url)

            if response.status_code != 200:
                print("Gitee api get error: ", response.text)

            items = json.loads(response.text)  # transform string to dictionary
            data.extend(items['data'])

            if not total_page_flag:
                total_page = self.get_total_page(items["total_count"], PER_PAGE)
                total_page_flag = True

            current_page += 1

            if current_page > total_page:
                break

        # assemble data then return
        items['total_page'] = total_page
        items['data'] = data
        return items

    def assemble_url(self, path, access_token=None, project_name=None, current_page=1, start_date=None, end_date=None,
                     per_page=None):
        args_str = f"?page={current_page}"
        args_str += f"&access_token={access_token}"
        if project_name:
            args_str += f"&project_name={project_name}"

        if start_date:
            args_str += f"&start_date={start_date}"

        if end_date:
            args_str += f"&end_date={end_date}"

        if per_page:
            args_str += f"&per_page={per_page}"
        else:
            args_str += f"&per_page={PER_PAGE}"
        url = self.urijoin(self.base_url, path)
        url += args_str
        return url

    def get_total_page(self, total_count, per_page):
        total_page = 0
        if total_count % per_page == 0:
            total_page = int(total_count / per_page)
        else:
            total_page = int(total_count / per_page) + 1
        return total_page

    def get_enterprise_members(self):
        """
        Get enterprise member list
        :return: True
        """
        flag = True
        request_url = f"https://api.gitee.com/enterprises/{self.enterpriseId}/members"
        response = self.fetch(url=request_url)

        if response.status_code != 200:
            print("Gitee api get error: ", response.text)
            flag = False
        else:
            with open(os.path.join(os.getcwd(), MEMBERS_DATA_FILE), "w+", encoding='utf-8', newline="") as csv_file_obj:
                csv_writer = csv.writer(csv_file_obj)

                csv_writer.writerow(["id", "username", "name", "remark",
                                     "phone", "email", "role_id", "role_name", "update_time"])

                response_body = response.json()
                ret_data = response_body.get('data')

                for row in ret_data:
                    user_id = row.get("id")
                    username = row.get("username")
                    name = row.get("name")
                    remark = row.get("remark")
                    phone = row.get("phone")
                    email = row.get("email")
                    role_id = row.get("enterprise_role").get("id")
                    role_name = row.get("enterprise_role").get("name")

                    updated_at = row.get("user").get("updated_at")
                    update_time = updated_at.split("+")[0].replace("T", " ")
                    time_stamp = int(time.mktime(time.strptime(update_time, "%Y-%m-%d %H:%M:%S")))
                    current_time_stamp = int(time.time()) - time_stamp

                    # This place can be changed according to actual needs
                    if current_time_stamp > MAXIMUM_TIME:  # The user has not been updated for more than one year
                        csv_writer.writerow([user_id, username, name, remark, phone,
                                             email, role_id, role_name, update_time])
        return flag

    def refresh_token(self, refresh_token):
        """Send a refresh post access to the Gitee Server"""
        if refresh_token:
            url = self.urijoin(GITEE_TOKEN_URL, 'token')
            _header = {'Content-Type': 'application/json;charset=UTF-8'}
            params = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token
            }
            print("Refresh the access_token for Gitee API")
            res = self.session.post(url, json=params, headers=_header, stream=False, auth=None)
            return res
   
