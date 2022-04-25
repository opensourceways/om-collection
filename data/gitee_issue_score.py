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
import json
import re
import sys

from collect.gitee import GiteeClient

from data import common

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
        self.robot_user_login = config.get('robot_user_login')
        self.esClient = common.ESClient(config)
        self.giteeClient = GiteeClient(owner=self.owner, repository=self.repository, token=self.access_token)
        self.bug_store_index_name = config.get('bug_store_index_name')
        self.bug_query_sentence = config.get('bug_query_sentence')
        self.bug_fragemnt_es_url = config.get('bug_fragemnt_es_url')
        self.bug_fragemnt_authorization = config.get('bug_fragemnt_authorization')


    @common.show_spend_seconds_of_this_function
    def run(self, from_date):

        # <editor-fold desc="Created log dir">
        dest_dir = 'result_logs'
        is_result_log_exist = common.create_log_dir(dest_dir)
        if is_result_log_exist:
            print(f'log_dir exists.')
        else:
            print(f'create log_dir failure.')
        # </editor-fold>

        self.failed_parse_issue_body = []
        self.failed_parse_issue_body=list(set(self.failed_parse_issue_body))
        bug_questionnaire_list = self.esClient.scrollSearchGiteeScore(index_name=self.bug_store_index_name, 
                                                                      search=self.bug_query_sentence,
                                                                      es_url=self.bug_fragemnt_es_url,
                                                                      es_authorization=self.bug_fragemnt_authorization)
        self.bugFragment_email_map = self.assemble_email_map(bug_questionnaire_list)


        self.robot_user_list = [robot_user.strip() for robot_user in self.robot_user_login.split(',')]
        self.score_admins = self.get_score_admins()
        if not self.score_admins:
            return
        self.process_gitee_score()

        with open(file=f'{dest_dir}/issue_failed_fetch_email.txt',mode='w',encoding='utf-8') as f:
            f.write('\n'.join(self.failed_parse_issue_body))

        print(f'Function name: {sys._getframe().f_code.co_name} has run over.')

    def get_score_admins(self):
        yaml_response = self.esClient.request_get(self.score_admin_file_path)
        if yaml_response.status_code != 200:
            print('Cannot fetch online yaml file.')
            return None
        score_admins = [username.strip() for username in yaml_response.text.split('\n')[1:]]
        return score_admins

    @common.show_spend_seconds_of_this_function
    def process_gitee_score(self):
        issue_brief_list = self.get_repo_issue_content_list()
        issue_total_count = len(issue_brief_list)

        actions = ''
        for issue_brief in issue_brief_list:
            issue_number = issue_brief[0]
            user_login = issue_brief[1]
            issue_created_at = issue_brief[2]
            version_num = issue_brief[3]
            folder_name = issue_brief[4]
            email = issue_brief[5]

            comment_index = issue_brief_list.index(issue_brief)
            issue_comment_list = self.get_comments_by_issue_number(issue_number)
            author_username, score = self.parse_comment_list(issue_comment_list)
            print(f'Succeed to parse issue: {issue_number}, \t{comment_index + 1}/{issue_total_count} of issues')

            content_body = {}
            if score:    # scored issue could get the two fields
                content_body['scoring_admin'] = author_username
                content_body['score'] = score
            
            content_body['issue_number'] = issue_number
            content_body['created_at'] = issue_created_at
            content_body['user_login'] = user_login
            content_body['version_num'] = version_num
            content_body['folder_name'] = folder_name
            content_body['email'] = email
            action = common.getSingleAction(self.index_name, issue_number, content_body)
            actions += action
        self.esClient.safe_put_bulk(actions)

    @common.show_spend_seconds_of_this_function
    def get_repo_issue_content_list(self):
        issue_generator = self.giteeClient.issues()
        total_issue_bodies = []

        for page_issue in issue_generator:
            page_issue_list = json.loads(page_issue)
            page_issue_bodies = []
            for single_issue_body in page_issue_list:

                # Only process the issue which issue title contains "文档捉虫"
                if not self.is_docDebug_issue(single_issue_body.get('title')):continue
                issue_number = single_issue_body.get('number')
                user_login = single_issue_body.get('user').get('login')
                if user_login in self.robot_user_login: continue  # Remove this issues which created by robot user
                created_at = single_issue_body.get('created_at')
                version_num, folder_name, bug_fragment = self.parse_single_issue_body(single_issue_body)

                email=None
                if bug_fragment:
                   email = self.bugFragment_email_map.get(bug_fragment)

                if not email:
                    self.failed_parse_issue_body.append(issue_number)
                page_issue_bodies.append((issue_number, user_login, created_at, version_num, folder_name, email))
            total_issue_bodies.extend(page_issue_bodies)

        print(f'Succeed to collect issue numbers: {len(total_issue_bodies)}')
        return total_issue_bodies

    def get_comments_by_issue_number(self, issue_number):
        issue_comment_list = []
        issue_comment_generator = self.giteeClient.issue_comments(issue_number)

        issue_comment_text = [issue_comment for issue_comment in issue_comment_generator][0]
        issue_comment_text = issue_comment_text.replace('\r', '').replace('\n', '\t').replace('null', 'None'). \
            replace('true', 'True').replace('false', 'False')

        try:
            issue_comment_list = eval(issue_comment_text)
        except Exception as exp:
            print(
                f'Failed to fetch comments from issue:{issue_number}. \t Error is {exp.__repr__()}')

        return issue_comment_list

    def parse_comment_list(self, comment_list):
        global author_username, issue_id, comment_index
        username = None
        score = None

        for comment in comment_list:
            comment_index = comment_list.index(comment)
            issue_id = comment.get('id')
            author_username = comment.get('user').get('login')
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

    def is_docDebug_issue(self, title):
        result = False
        if '有奖捉虫' in title:
            result = True
        return result

    def parse_single_issue_body(self, single_issue_body):
        doc_link = None
        version_num = None
        folder_name = None
        bug_fragment = None
        issue_number = single_issue_body.get('number')
        try:
            content_split_list = single_issue_body.get("body").replace('\r','').split('【')
            doc_link = content_split_list[1].split('http')[1].split('\n')[0].strip()
            version_num = doc_link.split('/')[5]
            folder_name = doc_link.split('/')[7]

            bug_fragment_rawStr = re.split('\n+', content_split_list[2])[1]
            bug_fragment = bug_fragment_rawStr.replace('>', '').strip()
        except Exception as exp:
            print(f'Issue number: {issue_number} failed to parse single_issue body in function: {sys._getframe().f_code.co_name}\n Error is: {exp.__repr__}')

        return version_num, folder_name, bug_fragment

    def assemble_email_map(self, bug_questionnaire_list):
        bugFragment_email_map = {}
        for bug_questionnaire in bug_questionnaire_list:
            email = bug_questionnaire.get('_source').get('email')
            bugDocFragment = bug_questionnaire.get( '_source').get('bugDocFragment')
            bugFragment_email_map[bugDocFragment] = email
        return bugFragment_email_map
