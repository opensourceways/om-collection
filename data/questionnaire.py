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
import json
import re
from datetime import datetime

import git
import xlrd

from data.common import ESClient

D0_QUERY = '*'
D1_QUERY = 'is_gitee_issue:1 OR is_gitee_issue_comment:1 OR (is_gitee_pull_request:1 AND pull_state.keyword:\\"merged\\") OR is_gitee_pull_request_comment:1 OR is_gitee_review_comment:1 OR is_gitee_comment:1'
D2_QUERY = 'is_gitee_pull_request:1 AND pull_state:\\"merged\\"'


class Questionnaire(object):
    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.gitee_index_name = config.get('gitee_index_name')
        self.gitee_token = config.get('gitee_token')
        self.email_gitee_authorization = config.get('email_gitee_authorization')
        self.email_gitee_es = config.get('email_gitee_es')
        self.email_gitee_index = config.get('email_gitee_index')
        self.phone_email_index = config.get('phone_email_index')
        self.cell_name_index_dict = {}

    def run(self, from_time):
        # 本地服务
        print("Questionnaire collect: start")
        self.opengauss_data()
        print(11)


    def opengauss_data(self):
        path = ''
        phone_email_dict = self.getPhoneEmailDict()
        email_gitee_dict = self.getEmailGiteeDict()
        user_D0 = self.getUser(query_str=D0_QUERY)
        user_D1 = self.getUser(query_str=D1_QUERY)
        user_D2 = self.getUser(query_str=D2_QUERY)
        self.getOpengaussDataFromExcel(path, phone_email_dict, email_gitee_dict, user_D0, user_D1, user_D2)

    def openeulr_data(self):
        path = ''
        email_gitee_dict = self.getEmailGiteeDict()
        user_D0 = self.getUser(query_str=D0_QUERY)
        user_D1 = self.getUser(query_str=D1_QUERY)
        user_D2 = self.getUser(query_str=D2_QUERY)
        self.getOpeneulerDataFromExcel(path, email_gitee_dict, user_D0, user_D1, user_D2)

    def getOpeneulerDataFromExcel(self, path, email_gitee_dict, user_D0, user_D1, user_D2):
        wb = xlrd.open_workbook(path)
        sh = wb.sheet_by_index(0)

        for i in range(sh.ncols):
            cell_name = sh.cell_value(0, i)
            self.cell_name_index_dict.update({cell_name: i})

        actions = ""
        for r in range(2, sh.nrows):
            gitee_id_value = self.getCellValue(r, 'gitee_id', sh)
            email_value = self.getCellValue(r, 'email', sh)
            gitee_id = gitee_id_value if gitee_id_value != '(跳过)' else ''
            email = email_value if email_value.__contains__('@') else ''
            if email in email_gitee_dict:
                gitee_id = email_gitee_dict[email]
            if gitee_id == '':
                continue

            is_D2_user = is_D1_user = is_D0_user = is_no_contribute_user = 0
            user_contribute_type = 'no_contribute'
            if gitee_id in user_D2:
                user_contribute_type = 'D2'
                is_D2_user = 1
                is_D1_user = 1
                is_D0_user = 1
            elif gitee_id in user_D1:
                user_contribute_type = 'D1'
                is_D1_user = 1
                is_D0_user = 1
            elif gitee_id in user_D0:
                user_contribute_type = 'D0'
                is_D0_user = 1
            else:
                is_no_contribute_user = 1

            created_at_value = self.getCellValue(r, 'created_at', sh),
            created_date = datetime.strptime(created_at_value[0], '%Y/%m/%d %H:%M:%S')
            created_at = created_date.strftime('%Y-%m-%dT%H:%M:%S+08:00')
            action = {
                'gitee_id': gitee_id,
                'email': email,
                'created_at': created_at,
                'experience_score': self.getScore(self.getCellValue(r, 'experience_score', sh)),
                'contribute_score': self.getScore(self.getCellValue(r, 'contribute_score', sh)),
                'news_score': self.getScore(self.getCellValue(r, 'news_score', sh)),
                'doc_score': self.getScore(self.getCellValue(r, 'doc_score', sh)),
                'view_meeting_time_score': self.getScore(self.getCellValue(r, 'view_meeting_time_score', sh)),
                'opensource_score': self.getScore(self.getCellValue(r, 'opensource_score', sh)),
                'maillist_score': self.getScore(self.getCellValue(r, 'maillist_score', sh)),
                'sig_score': self.getScore(self.getCellValue(r, 'sig_score', sh)),
                'learning_score': self.getScore(self.getCellValue(r, 'learning_score', sh)),
                'test_score': self.getScore(self.getCellValue(r, 'test_score', sh)),
                'download_score': self.getScore(self.getCellValue(r, 'download_score', sh)),
                'HCIA_score': self.getScore(self.getCellValue(r, 'HCIA_score', sh)),
                'activity_score': self.getScore(self.getCellValue(r, 'activity_score', sh)),
                'other_website_score': self.getScore(self.getCellValue(r, 'other_website_score', sh)),
                'schedule_meeting_score': self.getScore(self.getCellValue(r, 'schedule_meeting_score', sh)),
                'view_meeting_score': self.getScore(self.getCellValue(r, 'view_meeting_score', sh)),
                'view_etherpad_score': self.getScore(self.getCellValue(r, 'view_etherpad_score', sh)),
                'publish_activity': self.getScore(self.getCellValue(r, 'publish_activity', sh)),
                'query_activity': self.getScore(self.getCellValue(r, 'query_activity', sh)),
                'sign_up_activity': self.getScore(self.getCellValue(r, 'sign_up_activity', sh)),
                'applets_score': self.getScore(self.getCellValue(r, 'applets_score', sh)),
                'language_satisfaction_score': self.getScore(self.getCellValue(r, 'language_satisfaction_score', sh)),
                'website_language_score': self.getScore(self.getCellValue(r, 'website_language_score', sh)),
                'website_guide_score': self.getScore(self.getCellValue(r, 'website_guide_score', sh)),
                'website_link_score': self.getScore(self.getCellValue(r, 'website_link_score', sh)),
                'website_simple_operation_score': self.getScore(self.getCellValue(r, 'website_simple_operation_score', sh)),
                'website_satisfaction_score': self.getScore(self.getCellValue(r, 'website_satisfaction_score', sh)),
                'website_functioning_score': self.getScore(self.getCellValue(r, 'website_functioning_score', sh)),
                'website_design_score': self.getScore(self.getCellValue(r, 'website_design_score', sh)),
                'website_page_score': self.getScore(self.getCellValue(r, 'website_page_score', sh)),
                'community_satisfaction_score': self.getScore(self.getCellValue(r, 'community_satisfaction_score', sh)),
                'recommend_other_score': self.getScore(self.getCellValue(r, 'recommend_other_score', sh)),
                'total_score': self.getScore(self.getCellValue(r, 'total_score', sh)),
                'user_contribute_type': user_contribute_type,
                'is_D2_user': is_D2_user,
                'is_D1_user': is_D1_user,
                'is_D0_user': is_D0_user,
                'is_no_contribute_user': is_no_contribute_user,
            }
            index_data = {"index": {"_index": self.index_name, "_id": gitee_id + '_' + created_at}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

        self.esClient.safe_put_bulk(actions)

    def getOpengaussDataFromExcel(self, path, phone_email_dict, email_gitee_dict, user_D0, user_D1, user_D2):
        wb = xlrd.open_workbook(path)
        sh = wb.sheet_by_index(0)

        for i in range(sh.ncols):
            cell_name = sh.cell_value(0, i)
            self.cell_name_index_dict.update({cell_name: i})

        actions = ""
        for r in range(2, sh.nrows):
            phone_value = self.getCellValue(r, 'phone', sh)
            gitee_id_value = self.getCellValue(r, 'gitee_id', sh)
            email_value = self.getCellValue(r, 'email', sh)
            gitee_id = gitee_id_value if gitee_id_value != '(跳过)' else ''
            email = email_value if email_value.__contains__('@') else ''
            if email == '' and email in phone_value in phone_email_dict:
                email = phone_email_dict[phone_value]
            if email in email_gitee_dict:
                gitee_id = email_gitee_dict[email]
            if gitee_id == '':
                continue

            is_D2_user = is_D1_user = is_D0_user = is_no_contribute_user = 0
            user_contribute_type = 'no_contribute'
            if gitee_id in user_D2:
                user_contribute_type = 'D2'
                is_D2_user = 1
                is_D1_user = 1
                is_D0_user = 1
            elif gitee_id in user_D1:
                user_contribute_type = 'D1'
                is_D1_user = 1
                is_D0_user = 1
            elif gitee_id in user_D0:
                user_contribute_type = 'D0'
                is_D0_user = 1
            else:
                is_no_contribute_user = 1

            created_at_value = self.getCellValue(r, 'created_at', sh),
            created_date = datetime.strptime(created_at_value[0], '%Y/%m/%d %H:%M:%S')
            created_at = created_date.strftime('%Y-%m-%dT%H:%M:%S+08:00')
            action = {
                'gitee_id': gitee_id,
                'email': email,
                'created_at': created_at,
                'experience_score': self.getScore(self.getCellValue(r, 'experience_score', sh)),
                'download_score': self.getScore(self.getCellValue(r, 'download_score', sh)),
                'doc_score': self.getScore(self.getCellValue(r, 'doc_score', sh)),
                'maillist_score': self.getScore(self.getCellValue(r, 'maillist_score', sh)),
                'sig_score': self.getScore(self.getCellValue(r, 'sig_score', sh)),
                'contribute_score': self.getScore(self.getCellValue(r, 'contribute_score', sh)),
                'news_score': self.getScore(self.getCellValue(r, 'news_score', sh)),
                'learning_score': self.getScore(self.getCellValue(r, 'learning_score', sh)),
                'view_meeting_time_score': self.getScore(self.getCellValue(r, 'view_meeting_time_score', sh)),
                'schedule_meeting_score': self.getScore(self.getCellValue(r, 'schedule_meeting_score', sh)),
                'cla_score': self.getScore(self.getCellValue(r, 'cla_score', sh)),
                'certification_score': self.getScore(self.getCellValue(r, 'certification_score', sh)),
                'activity_score': self.getScore(self.getCellValue(r, 'activity_score', sh)),
                'other_website_score': self.getScore(self.getCellValue(r, 'other_website_score', sh)),
                'language_satisfaction_score': self.getScore(self.getCellValue(r, 'language_satisfaction_score', sh)),
                'website_language_score': self.getScore(self.getCellValue(r, 'website_language_score', sh)),
                'website_guide_score': self.getScore(self.getCellValue(r, 'website_guide_score', sh)),
                'website_link_score': self.getScore(self.getCellValue(r, 'website_link_score', sh)),
                'website_simple_operation_score': self.getScore(self.getCellValue(r, 'website_simple_operation_score', sh)),
                'website_satisfaction_score': self.getScore(self.getCellValue(r, 'website_satisfaction_score', sh)),
                'website_functioning_score': self.getScore(self.getCellValue(r, 'website_functioning_score', sh)),
                'website_design_score': self.getScore(self.getCellValue(r, 'website_design_score', sh)),
                'website_page_score': self.getScore(self.getCellValue(r, 'website_page_score', sh)),
                'community_satisfaction_score': self.getScore(self.getCellValue(r, 'community_satisfaction_score', sh)),
                'recommend_other_score': self.getScore(self.getCellValue(r, 'recommend_other_score', sh)),
                'user_contribute_type': user_contribute_type,
                'is_D2_user': is_D2_user,
                'is_D1_user': is_D1_user,
                'is_D0_user': is_D0_user,
                'is_no_contribute_user': is_no_contribute_user,
                'phone': phone_value,
            }
            index_data = {"index": {"_index": self.index_name, "_id": gitee_id + '_' + created_at}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

        self.esClient.safe_put_bulk(actions)

    def getUser(self, query_str):
        users = []
        search = '''{
                      "size": 0,
                      "query": {
                        "bool": {
                          "filter": [
                            {
                              "query_string": {
                                "analyze_wildcard": true,
                                "query": "%s"
                              }
                            }
                          ]
                        }
                      },
                      "aggs": {
                        "user_login": {
                          "terms": {
                            "field": "user_login.keyword",
                            "size": 20000,
                            "order": {
                              "_key": "desc"
                            },
                            "min_doc_count": 1
                          }
                        }
                      }
                    }''' % query_str
        datas = self.esClient.esSearch(index_name=self.gitee_index_name, search=search)
        buckets = datas['aggregations']['user_login']['buckets']
        for bucket in buckets:
            users.append(bucket['key'])
        return users

    def getEmailGiteeDict(self):
        search = '"must": [{"match_all": {}}]'
        header = {
            'Content-Type': 'application/json',
            'Authorization': self.email_gitee_authorization
        }
        hits = self.esClient.searchEmailGitee(url=self.email_gitee_es, headers=header,
                                              index_name=self.email_gitee_index, search=search)
        data = {}
        if hits is not None and len(hits) > 0:
            for hit in hits:
                source = hit['_source']
                data.update({source['email']: source['gitee_id']})
        return data

    def getPhoneEmailDict(self):
        search = '"must": [{"match_all": {}}]'
        header = {
            'Content-Type': 'application/json',
            'Authorization': self.email_gitee_authorization
        }
        hits = self.esClient.searchEmailGitee(url=self.email_gitee_es, headers=header,
                                              index_name=self.phone_email_index, search=search)
        data = {}
        if hits is not None and len(hits) > 0:
            for hit in hits:
                source = hit['_source']
                if 'phone' not in source or 'email' not in source:
                    continue
                data.update({source['phone']: source['email']})
        return data

    def getCellValue(self, row_index, cell_name, sheet):
        cell_value = sheet.cell_value(row_index, self.cell_name_index_dict.get(cell_name))
        return cell_value

    def getScore(self, cell_value):
        if re.match(r'\d+', cell_value):
            score = int(cell_value)
        elif cell_value == '非常满意' or cell_value == '极有可能':
            score = 10
        else:
            score = 0
        return score
