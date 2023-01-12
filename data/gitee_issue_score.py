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
        self.esClient = common.ESClient(config)
        self.access_token = config.get('access_token')
        self.index_name = config.get('index_name')
        self.repo_path = config.get('repo_path')
        self.owner = config.get('owner')
        self.repository = config.get('repository')
        self.issue_state = config.get('issue_state')
        self.score_admin_file_path = config.get('score_admin_file_path')
        self.robot_user_login = config.get('robot_user_login')

        self.bug_store_index_name = config.get('bug_store_index_name')
        self.bug_query_sentence = config.get('bug_query_sentence')

        self.failed_parse_issue_body = []
        self.score_admins = self.get_score_admins()
        self.bugFragment_email_map = {}
        self.robot_user_list = [robot_user.strip() for robot_user in self.robot_user_login.split(',')]
        self.repository_list = [repo for repo in self.repository.split(',')]
        self.giteeClient = None

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

        self.failed_parse_issue_body = list(set(self.failed_parse_issue_body))
        self.esClient.scrollSearch(index_name=self.bug_store_index_name,
                                   search=self.bug_query_sentence, func=self.assemble_email_map)
        print('bugFragment_email_map size: ', len(self.bugFragment_email_map))
        for repo in self.repository_list:
            print("Process repo: ", repo)
            self.giteeClient = GiteeClient(owner=self.owner, repository=repo, token=self.access_token)
            self.process_issue_score()
            self.process_pull_score()

        with open(file=f'{dest_dir}/issue_failed_fetch_email.txt', mode='w', encoding='utf-8') as f:
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
    def process_issue_score(self):
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
            state = issue_brief[6]

            comment_index = issue_brief_list.index(issue_brief)

            print(f'start to get issue score, issue number is : {issue_number}')
            issue_comment_list = self.get_comments_by_issue_number(issue_number)
            author_username, score = self.parse_comment_list(issue_comment_list)
            print(
                f'Succeed to parse issue: {issue_number}, \t{comment_index + 1}/{issue_total_count} of issues')

            content_body = {}
            if score:  # scored issue could get the two fields
                content_body['scoring_admin'] = author_username
                content_body['score'] = score
            content_body['issue_number'] = issue_number
            content_body['created_at'] = issue_created_at
            content_body['user_login'] = user_login
            content_body['version_num'] = version_num
            content_body['folder_name'] = folder_name
            content_body['email'] = email
            content_body['data_type'] = 'issue'
            content_body['state'] = state
            content_body['is_gitee_issue'] = 1
            action = common.getSingleAction(self.index_name, issue_number, content_body)
            actions += action
        self.esClient.safe_put_bulk(actions)

    @common.show_spend_seconds_of_this_function
    def process_pull_score(self):
        pull_brief_list = self.get_repo_pull_content_list()
        pull_total_count = len(pull_brief_list)

        PullNumber_indice, userLogin_indice, createdAt_indice, pullScore_indice, scoreAdmin_indice = 0, 1, 2, 3, 4
        actions = ''
        for pull_brief in pull_brief_list:
            pull_number = pull_brief[PullNumber_indice]
            user_login = pull_brief[userLogin_indice]
            pull_created_at = pull_brief[createdAt_indice]
            # pull_score should not be null, guaranteed by get_repo_pull_content_list
            pull_score = pull_brief[pullScore_indice]
            score_admin = pull_brief[scoreAdmin_indice]
            state = pull_brief[5]

            comment_index = pull_brief_list.index(pull_brief)
            print(
                f'Succeed to parse pull_request: {pull_number}, \t{comment_index + 1}/{pull_total_count} of pulls')

            content_body = {
                'scoring_admin': score_admin,
                'score': pull_score,
                'pull_number': pull_number,
                'created_at': pull_created_at,
                'user_login': user_login,
                'data_type': 'pull_request',
                'state': state,
                'is_gitee_pull_request': 1}
            action = common.getSingleAction(self.index_name, pull_number, content_body)
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
                if not self.is_docDebug_issue(single_issue_body.get('title'),
                                              single_issue_body.get('issue_type')):
                    continue
                issue_number = single_issue_body.get('number')
                user_login = single_issue_body.get('user').get('login')
                if user_login in self.robot_user_login:
                    continue  # Remove this issues which created by robot user
                created_at = single_issue_body.get('created_at')
                state = single_issue_body.get('state')
                version_num, folder_name, bug_fragment = self.parse_single_issue_body(single_issue_body)

                email = None
                if bug_fragment:
                    email = self.bugFragment_email_map.get(bug_fragment)

                if not email:
                    self.failed_parse_issue_body.append(issue_number)
                page_issue_bodies.append((issue_number, user_login, created_at, version_num, folder_name, email, state))
            total_issue_bodies.extend(page_issue_bodies)

        print(f'Succeed to collect issue numbers: {len(total_issue_bodies)}')
        return total_issue_bodies

    @common.show_spend_seconds_of_this_function
    def get_repo_pull_content_list(self):
        pull_generator = self.giteeClient.pulls(state='all', once_update_num_of_pr=0)
        whole_pull_list = self.esClient.getGenerator(pull_generator)

        not_score_prs = 0
        total_pull_bodies = []
        for single_pull_body in whole_pull_list:
            # Only process the issue which issue title contains "文档捉虫"
            if not self.is_docDebug_issue(single_pull_body.get('title'),
                                          single_pull_body.get('issue_type')):
                continue
            user_login = single_pull_body.get('user').get('login')
            if user_login in self.robot_user_login:
                continue  # Remove this issues which created by robot user
            pull_number = single_pull_body.get('number')
            created_at = single_pull_body.get('created_at')
            state = single_pull_body.get('state')
            comments_generator = self.giteeClient.pull_review_comments(pull_number)
            score_admin, pull_score = self.parse_pull_comments(comments_generator)

            if not pull_score:
                not_score_prs += 1
                continue

            total_pull_bodies.append((pull_number, user_login, created_at, pull_score, score_admin, state))

        print(f'Succeed to collect pull numbers: {len(total_pull_bodies)}')
        print(f'Not score pull numbers: {not_score_prs}')
        return total_pull_bodies

    def parse_pull_comments(self, comments_generator):
        pull_score = None
        pull_comments_str = comments_generator.__next__()
        pull_comments = eval(pull_comments_str.replace('null', 'None'))
        score_admin = None

        if not pull_comments:
            return score_admin, pull_score

        for pull_comment in pull_comments:
            score_admin = pull_comment.get('user').get('login')
            if pull_comment.get('body') is None:
                continue
            pull_comment_body = pull_comment.get('body').strip()
            if score_admin not in self.score_admins or not pull_comment_body.startswith('得分'):
                continue
            comment_body_list = re.split('\n+', pull_comment_body)
            score_str = comment_body_list[0]

            try:   # split by either Chinese or English colon.
                score_str = re.split('\uff1a|:', score_str)[1].strip()
                pull_score = int(score_str)
            except Exception as exp:
                print(f'Cannot parse comment_content: {score_str}')
        return score_admin, pull_score

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
        username = None
        score = None
        if comment_list is not None:
            for comment in comment_list:
                author_username = comment.get('user').get('login')
                comment_content = comment.get('body')
                if comment_content is None:
                    continue
                if author_username in self.score_admins and comment_content.strip().startswith("得分"):
                    score = self.get_score_from_comment_content(comment_content)
                    username = author_username
                    break
        return username, score

    def get_score_from_comment_content(self, comment_content):
        comment_line = comment_content.split('\n')[0].split('\r')[0]
        # split by either Chinese or English colon.
        try:
            score_str = re.split('\uff1a|:', comment_line)[1].strip()
            score = float(score_str)
        except Exception as exp:
            print(f'Cannot parse comment_content: {comment_content}')
            return
        return score

    def is_docDebug_issue(self, title, issue_type):
        result = False
        # 如果标题中、或者Issue类型中带有“捉虫”字样，就表示
        # 该Issue为有奖捉虫活动的Issue
        if title and '捉虫' in title:
            result = True
        if issue_type and '捉虫' in issue_type:
            result = True
        return result

    def parse_single_issue_body(self, single_issue_body):
        version_num = None
        folder_name = None
        bug_fragment = None
        issue_number = single_issue_body.get('number')
        try:
            content_split_list = single_issue_body.get("body").replace('\r', '').split('【')
            doc_link = content_split_list[1].split('http')[1].split('\n')[0].strip()
            version_num = doc_link.split('/')[5]
            folder_name = doc_link.split('/')[7]

            bug_fragment_rawStr = re.split('\n+', content_split_list[2])[1]
            bug_fragment = bug_fragment_rawStr.replace('>', '').strip()
        except Exception as exp:
            print(f'Issue number: {issue_number} failed to parse single_issue body in function: {sys._getframe().f_code.co_name}\n Error is: {exp.__repr__}')

        return version_num, folder_name, bug_fragment

    def assemble_email_map(self, bug_questionnaire_list):
        for bug_questionnaire in bug_questionnaire_list:
            email = bug_questionnaire.get('_source').get('email')
            bugDocFragment = bug_questionnaire.get('_source').get('bugDocFragment')
            self.bugFragment_email_map.update({bugDocFragment: email})
        return self.bugFragment_email_map
