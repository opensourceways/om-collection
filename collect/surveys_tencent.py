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

import requests

BASE_URL = 'https://open.wj.qq.com/api'


class SurveysTencentApi(object):
    def __init__(self, app_id, secret, user_id):
        self.app_id = app_id
        self.secret = secret
        self.session = requests.Session()
        self.headers = {'Content-Type': 'application/json'}
        self.user_id = user_id

    # 获取access_token
    def get_token(self):
        url = self.url_join(BASE_URL, 'oauth2', 'access_token')
        params = {
            'appid': self.app_id,
            'secret': self.secret,
            'grant_type': 'client_credential',
        }

        res = self.http_req(url=url, params=params).json()
        if res['code'] != 'OK':
            return None
        else:
            return res['data']['access_token']

    # 获取问卷列表
    def get_surveys(self, access_token, user_id, current_page=1, per_page=20):
        url = self.url_join(BASE_URL, 'surveys')
        params = {
            'appid': self.app_id,
            'access_token': access_token,
            'user_id': user_id,
            'page': current_page,
            'per_page': per_page,
        }
        return self.http_req(url=url, params=params)

    # 获取问卷详情
    def get_survey_legacy(self, access_token, survey_id):
        url = self.url_join(BASE_URL, 'surveys', survey_id, 'legacy')
        params = {
            'appid': self.app_id,
            'access_token': access_token,
        }
        return self.http_req(url=url, params=params)

    # 获取回答列表
    def get_answers(self, access_token, survey_id, per_page=20, last_answer_id=0):
        url = self.url_join(BASE_URL, 'surveys', survey_id, 'answers')
        params = {
            'appid': self.app_id,
            'access_token': access_token,
            'survey_id': survey_id,
            'per_page': per_page,
            'last_answer_id': last_answer_id,
        }
        return self.http_req(url=url, params=params)

    # 获取回答详情
    def get_answer_legacy(self, access_token, survey_id, answer_id):
        url = self.url_join(BASE_URL, 'surveys', survey_id, 'answers', answer_id)
        params = {
            'appid': self.app_id,
            'access_token': access_token,
            'survey_id': survey_id,
            'answer_id': answer_id,
        }
        return self.http_req(url=url, params=params)

    def url_join(self, *args):
        return '/'.join(map(lambda x: str(x).strip('/'), args))

    def http_req(self, url, params, method='GET', headers=None, stream=False, auth=None):
        if headers is None:
            headers = self.headers

        if method == 'GET':
            response = self.session.get(url, params=params, headers=headers, stream=stream,
                                        verify=True, auth=auth, timeout=60)
        else:
            response = self.session.post(url, data=params, headers=headers, stream=stream,
                                         verify=True, auth=auth)
        return response



