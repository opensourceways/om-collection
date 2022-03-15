#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2020-05
#

import sys
import threading
import time
from configparser import ConfigParser

import requests

from data import common

# Default sleep time and retries to deal with connection/server problems
DEFAULT_SLEEP_TIME = 1
MAX_RETRIES = 5
globa_threadinfo = threading.local()
config = ConfigParser()
try:
    config.read('config.ini', encoding='UTF-8')
    retry_time = config.getint('general', 'retry_time', )
    retry_sleep_time = config.getint('general', 'retry_sleep_time')
except BaseException as  ex:
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
                        response = warp(*args, "retry", **kwargs)
                    else:
                        response = warp(*args, **kwargs)
                    return response
                finally:
                    pass
        except Exception as e:
            print("globalExceptionHandler Exception: fetch error :" + str(
                e) + "retry:" + threading.currentThread().getName() + str(func.__name__) + ":" + str(
                globa_threadinfo.num) + " Count")
            raise e
        else:
            ## For debug while occurs bug
            # print("globalExceptionHandler else: check response instance." + "retry:" +
            #       threading.currentThread().getName() + str(func.__name__) + ":" + str(
            #         globa_threadinfo.num) + "次")
            if isinstance(response, requests.models.Response):
                if response.status_code == 401 or response.status_code == 403:
                    print({"状态码": response.status_code})
                else:
                    print("globalExceptionHandler else: response.status_code is not 401 and 403.")
            else:
                pass
                ## For debug while occurs bug
                # print("globalExceptionHandler else: response is not requests.models.Response.")
            # 重试成功，修改状态
            globa_threadinfo.retrystate = 1
            globa_threadinfo.num = 0
            ## For debug while occurs bug
            # print("globalExceptionHandler else: retry success globa_threadinfo.retrystate set to 1.")
            return response

    return warp


class GitlabClient(object):
    """
    Provide Gitlab fetch related data interface
    """

    def __init__(self):
        self.headers = {'Content-Type': 'application/json'}
        self.session = requests.Session()
        self.ssl_verify = True

    @common.show_spend_seconds_of_this_function
    @globalExceptionHandler
    def get_whole_project_commit(self, project_url, project_id, per_page=1000):

        start_page = 1
        commit_list = []
        while True:
            param_str = f'?page={start_page}&per_page={per_page}'
            api_url = f'{project_url}/api/v4/projects/{project_id}/repository/commits'
            url = api_url + param_str
            response = self.session.get(url=url, headers=self.headers)

            # return empty value justification
            if response.status_code != 200:
                print(f'Project_id: {project_id} get data failed. ')
                return commit_list

            text = response.text.replace('null', 'None')
            if text == '[]':
                break

            res_list = None
            try:
                res_list = eval(text)
            except Exception as ex:
                print(f'Project_id: {project_id}, page:{start_page} is wrong while eval() function.\n'
                      f'root cause: {ex.__repr__()}')
            commit_list.extend(res_list)
            start_page += 1

        return commit_list

    @common.show_spend_seconds_of_this_function
    @globalExceptionHandler
    def get_whole_project_commit_count(self, project_url, project_id, per_page=1000):
        start_page = 1
        commit_total_count = 0
        total_page = 1

        api_url = f'{project_url}/api/v4/projects/{project_id}/repository/commits'
        while True:
            param_str = f'?page={start_page}&per_page={per_page}'
            url = api_url + param_str
            response = self.session.get(url=url, headers=self.headers)

            # return empty value justification
            if response.status_code != 200:
                print(f'Function name: {sys._getframe().f_code.co_name}. '
                      f'Project_id: {project_id} get data failed. ')
                return commit_total_count

            if response.text == '[]':
                if start_page > 1:
                    total_page = start_page - 1

                is_last_page = True
                break
            print(f'Function name: {sys._getframe().f_code.co_name}. '
                  f'Project_id:{project_id},finish page: {start_page}')
            start_page += 1

        if is_last_page:
            last_page_param_str = f'?page={total_page}&per_page={per_page}'
            last_page_url = api_url + last_page_param_str
            response = self.session.get(url=last_page_url, headers=self.headers)
            text = response.text.replace('null', 'None')

            last_page_list = None
            try:
                last_page_list = eval(text)
            except Exception as ex:
                print(f'Function name: {sys._getframe().f_code.co_name}. '
                      f'Project_id: {project_id}, page:{start_page} is wrong while eval() function.\n'
                      f'root cause: {ex.__repr__()}')
            last_page_commit_count = len(last_page_list)

            commit_total_count = (total_page - 1) * per_page + last_page_commit_count

        return commit_total_count

    def get_all_project_basicInfo(self, boot_url):
        api_path = '/api/v4/projects'

        start_page = 1
        per_page = 100

        whole_project_list = []
        while True:
            page_param_str = f'?page={start_page}&per_page={per_page}'
            api_url = boot_url + api_path + page_param_str
            response = self.session.get(url=api_url, headers=self.headers)

            if response.status_code != 200:
                print(f'Failed to get all projects basic info while http request!')
                return None

            text = response.text
            if text == '[]':
                break
            else:
                unnull_text = text.replace('null', '"null"')
            page_project_list = eval(unnull_text)
            whole_project_list.extend(page_project_list)

            start_page += 1

        return whole_project_list
