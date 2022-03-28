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
#
import os
import platform
import re
import sys

import requests
import yaml

from data import common
from data.gitee import Gitee

HEADERS = {'Content-Type': 'application/json', 'charset': 'UTF-8'}
PAYLOAD = {}
API_BASE_URL = 'https://gitee.com/api/v5/repos'


class GiteeScore(object):
    def __init__(self, config=None):
        self.config = config
        self.access_token = config.get('access_token')
        self.index_name = config.get('index_name')
        self.repo_path = config.get('repo_path')
        self.owner = config.get('owner')
        self.repository = config.get('repository')
        self.issue_state = config.get('issue_state')
        self.score_admin_file_path = config.get('score_admin_file_path')
        self.esClient = common.ESClient(config)
        self.platform_name = platform.system().lower()
        self.gitee_api = Gitee(config)

    @common.show_spend_seconds_of_this_function
    def run(self, from_date):

        self.score_admins = self.get_score_admins()
        self.process_gitee_score()

        print(f'Function name: {sys._getframe().f_code.co_name} has run over.')

    def get_score_admins(self):
        score_admins = None
        if self.platform_name == 'linux':  ## Get data.yaml and company.yaml from Gitee in linux.
            cmd = 'wget -N %s' % self.score_admin_file_path
            local_file_name = self.score_admin_file_path.split('/')[-1]
            p = os.popen(cmd.replace('=', ''))
            p.read()
            yaml_dict = yaml.safe_load(open(local_file_name, encoding='UTF-8'))
            score_admins_str = yaml_dict.get('score_admins')
            score_admins = [username.strip() for username in score_admins_str.split()]
        elif self.platform_name == 'windows':  ###Test in windows without wget command
            yaml_response = requests.get(self.score_admin_file_path)
            if yaml_response.status_code != 200:
                print('Cannot fetch online yaml file.')
                return
            score_admins = [username.strip() for username in yaml_response.text.split('\n')[1:]]
        return score_admins

    @common.show_spend_seconds_of_this_function
    def process_gitee_score(self):
        issue_brief_list = self.get_repo_issue_numbers(per_page=100)
        issue_total_count = len(issue_brief_list)

        actions = ''
        for issue_brief in issue_brief_list:
            issue_number = issue_brief[0]
            issue_author = issue_brief[1]
            issue_created_at = issue_brief[2]

            comment_index = issue_brief_list.index(issue_brief)
            issue_comment_list = self.get_comments_by_issue_number(issue_number)
            author_username, score = self.parse_comment_list(issue_comment_list)
            print(f'Succeed to parse issue: {issue_number}, \t{comment_index + 1}/{issue_total_count} '
                  f'of issues')
            if not score:
                continue

            content_body = {}
            content_body['issue_number'] = issue_number
            content_body['created_at'] = issue_created_at
            content_body['user_login'] = issue_author
            content_body['scoring_admin'] = author_username
            content_body['score'] = score
            action = common.getSingleAction(self.index_name, issue_number, content_body)
            actions += action
        self.esClient.safe_put_bulk(actions)

    @common.show_spend_seconds_of_this_function
    def get_repo_issue_numbers(self, per_page):
        res_data_list = []
        start_page = 1

        while True:
            api_url = f'{API_BASE_URL}/{self.repo_path}/issues?state={self.issue_state}&sort=created&direction=desc&' \
                      f'page={start_page}&per_page={per_page}&access_token={self.access_token}'

            res = requests.get(api_url, headers=HEADERS, data=PAYLOAD)
            if res.status_code != 200:
                print(f'Failed to fetch data at page: {start_page}')
                break

            # Remove the special string which cannot be converted to python data structure
            res_text = res.text.replace('null', 'None').replace('true', 'True').replace('false', 'False')
            page_data_list = eval(res_text)

            if not page_data_list:  # last page data is []
                break

            res_data_list.extend(page_data_list)
            start_page += 1

        issue_brief_list = [(issue.get('number'), issue.get('user').get('login'), issue.get('created_at'))
                            for issue in res_data_list]
        print(f'Succeed to collect issue numbers: {len(res_data_list)}')

        return issue_brief_list

    def get_comments_by_issue_number(self, issue_number):
        comment_list = []
        start_page = 1
        while True:
            api_url = f'{API_BASE_URL}/openeuler/docs/issues/{issue_number}/comments?access_token={self.access_token}' \
                      f'&page={start_page}&per_page=100&order=asc'
            res = requests.get(api_url, headers=HEADERS, data=PAYLOAD)

            if res.status_code != 200:
                print(f'Failed to fetch comments from issue:{issue_number}')
                break

            res_text = res.text.replace('null', 'None').replace('true', 'True').replace('false', 'False')
            page_data_list = eval(res_text)

            if not page_data_list:  # last page data is []
                # print(f'Get {start_page - 1} page in all.')
                break

            comment_list.extend(page_data_list)
            start_page += 1

        return comment_list

    def parse_comment_list(self, comment_list):
        global author_username, issue_id, comment_index
        username = None
        score = None

        for comment in comment_list:
            comment_index = comment_list.index(comment)
            issue_id = comment.get('id')
            author_username = comment.get('user').get('login')
            author_id = comment.get('user').get('id')
            comment_content = comment.get('body')

            if author_username in self.score_admins and comment_content.strip().startswith("得分"):
                score = self.get_score_from_comment_content(comment_content)
                username = author_username
                break

        return username, score

    def get_score_from_comment_content(self, comment_content):

        comment_line = comment_content.split('\r\n')[0]
        score = None

        # split by either Chinese or English colon.
        try:
            score_str = re.split('\uff1a|:', comment_line)[1].strip()
        except Exception as exp:
            print(f'Cannot parse comment_content: {comment_content}')
            return score

        score = float(score_str)
        return score
