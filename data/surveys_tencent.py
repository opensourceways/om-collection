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
import time
from collect.surveys_tencent import SurveysTencentApi
from data.common import ESClient


class SurveysTencent(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name_survey = config.get('index_name_survey')
        self.index_name_answer = config.get('index_name_answer')
        self.user_id = config.get('user_id')
        self.app_id = config.get('app_id')
        self.secret = config.get('secret')
        self.per_page = config.get('per_page')
        self.esClient = ESClient(config)
        self.surveys_tencent_api = SurveysTencentApi(self.app_id, self.secret, self.user_id)
        self.token = self.surveys_tencent_api.get_token()

    def run(self, from_time):
        print("Collect tencent surveys data: start")
        self.collect_survey_details()
        print("Collect tencent surveys data: finished")

    def convert_time(self, it):
        time_arr = time.localtime(it)
        return time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time_arr)

    def collect_survey_details(self):
        # 获取问卷列表（注意翻页）
        current_survey_page = 1
        actions = ""
        actions_answer = ""
        surveys_req = self.surveys_tencent_api.get_surveys(access_token=self.token, user_id=self.user_id,
                                                           current_page=current_survey_page,
                                                           per_page=self.per_page).json()
        total = surveys_req['data']['total']
        print("The total number of surveys is %d ." % total)
        surveys_page = total / int(self.per_page) + 1
        print("The total number of pages of the surveys is %d .\n" % surveys_page)
        while True:
            print("start current_survey_page: %d ..." % current_survey_page)
            surveys = surveys_req['data']['list']
            for survey in surveys:
                # 根据survey_id获取问卷详情
                survey_id = survey['id']
                survey_title = survey['title']
                survey_req = self.surveys_tencent_api.get_survey_legacy(access_token=self.token,
                                                                        survey_id=survey_id).json()
                survey = survey_req['data']
                survey['createTime'] = self.convert_time(survey['createTime'])
                survey['updateTime'] = self.convert_time(survey['updateTime'])
                survey['startTime'] = self.convert_time(survey['startTime'])
                print("survey_id: %s, survey_title: %s" % (survey_id, survey_title))
                questions_list = []
                pages = survey['pages']
                for p in pages:
                    p_questions = p['questions']
                    for question in p_questions:
                        questions_list.append({'title': question['title']})
                action = {
                    "survey_id": survey_id,
                    "survey_title": survey_title,
                    "survey_legacy": questions_list,
                    "createTime": survey['createTime'],
                    "updateTime": survey['updateTime'],
                    "startTime": survey['startTime']
                }
                index_data_survey = {"index": {"_index": self.index_name_survey, "_id": survey_id}}
                actions += json.dumps(index_data_survey) + '\n'
                actions += json.dumps(action) + '\n'

                # 根据survey_id获取回答列表，注意分页
                next_answer_id = 0
                answers_req = self.surveys_tencent_api.get_answers(access_token=self.token, survey_id=survey_id,
                                                                   per_page=self.per_page,
                                                                   last_answer_id=next_answer_id).json()
                answers = answers_req['data']
                next_answer_id = answers['last_answer_id']
                total = answers['total']
                print("The total number of answers is %d ." % total)
                while True:
                    # 根据survey_id和answer_id获取回答详情
                    answer_list = answers['list']
                    for answer in answer_list:
                        answer_id = answer['answer_id']
                        answer_legacy = self.surveys_tencent_api.get_answer_legacy(access_token=self.token,
                                                                                   survey_id=survey_id,
                                                                                   answer_id=answer_id).json()
                        item_answer = answer_legacy['data']
                        answer_legacy_list = []
                        for its in item_answer['answer']:
                            for it in its['questions']:
                                answer_legacy_list.append(it)
                        started_ats = item_answer['started_at'].split(' ')
                        ended_ats = item_answer['ended_at'].split(' ')
                        started_at = started_ats[0] + "T" + started_ats[1] + "+08:00"
                        ended_at = ended_ats[0] + "T" + ended_ats[1] + "+08:00"

                        action_answer = {
                            "survey_id": item_answer['survey_id'],
                            "answer_id": item_answer['answer_id'],
                            "started_at": started_at,
                            "ended_at": ended_at,
                            "answer_legacy_list": answer_legacy_list
                        }
                        str_survey_id = str(survey_id)
                        str_answer_id = str(answer_id)
                        index_data_answer = {"index": {"_index": self.index_name_answer,
                                                       "_id": str_survey_id + str_answer_id}}
                        actions_answer += json.dumps(index_data_answer) + '\n'
                        actions_answer += json.dumps(action_answer) + '\n'

                    print("answer_id %d has been done ." % next_answer_id)
                    if next_answer_id == total:
                        break
                    answers_req = self.surveys_tencent_api.get_answers(access_token=self.token, survey_id=survey_id,
                                                                       per_page=self.per_page,
                                                                       last_answer_id=next_answer_id).json()
                    answers = answers_req['data']
                    next_answer_id = answers['last_answer_id']
                print("survey_id %s has been done . \n" % survey_id)

            print("current_survey_page %d has been done .\n" % current_survey_page)
            current_survey_page += 1
            if current_survey_page > surveys_page:
                break
            surveys_req = self.surveys_tencent_api.get_surveys(access_token=self.token, user_id=self.user_id,
                                                               current_page=current_survey_page,
                                                               per_page=self.per_page).json()
        self.esClient.safe_put_bulk(actions)
        self.esClient.safe_put_bulk(actions_answer)

        print("Collect tencent surveys data: end")
