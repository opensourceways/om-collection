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
import random
import time
import types
from json import JSONDecodeError

from obs import ObsClient

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data.common import ESClient


class TryMe(object):
    def __init__(self, config=None):
        self.esClient = ESClient(config)
        self.email_gitee_es = config.get('email_gitee_es')
        self.email_gitee_authorization = config.get('email_gitee_authorization')
        self.email_gitee_index = config.get('email_gitee_index')
        self.index_name = config.get('index_name')
        self.ak = config.get("access_key_id")
        self.sk = config.get("secret_access_key")
        self.server = config.get("server")
        self.bucket_name = config.get("bucket_name")
        self.object_key = config.get("object_key")
        self.gitee_token = config.get("gitee_token")
        self.github_token = config.get("github_token")
        self.create_randon_time = config.get("create_randon_time", 'false')
        self.start_time = config.get("start_time")
        self.end_time = config.get("end_time")
        self.time_format = config.get("time_format")
        self.obs_client = ObsClient(access_key_id=self.ak, secret_access_key=self.sk, server=self.server)
        self.gitee_client = GiteeClient(owner=None, repository=None, token=self.gitee_token)
        self.giteehub_client = GithubClient(org=None, repository=None, token=self.github_token)
        self.exists_ids = []

    def run(self, startTime):
        self.getAllIds()
        email_gitee = self.getEmailGiteeDict()
        self.userLogin(email_gitee)

    def userLogin(self, email_gitee):
        resp = self.obs_client.getObject(bucketName=self.bucket_name, objectKey=self.object_key)
        if resp.status > 300:
            return
        chunk = resp.body.response.read(resp.body.contentLength)
        if not chunk:
            return
        json_node = json.loads(chunk)
        actions = ''

        for _, user_info in json_node['userInfoMap'].items():
            res = user_info
            id = user_info['id']
            if id in self.exists_ids:
                continue
            name = user_info['name']
            email = user_info['email']

            if str(id).startswith('gitee'):
                user = self.getGenerator(self.gitee_client.user(name))
                res['is_gitee_account'] = 1
            elif str(id).startswith('github'):
                user_id = str(id).replace('github-', '')
                user = self.giteehub_client.getUserByID(user_id)
                res['is_github_account'] = 1
            else:
                continue
            # 随机时间
            if self.create_randon_time == 'true':
                created_at = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime(
                    self.randomTime(self.start_time, self.end_time, random.random(), self.time_format)))
            else:
                created_at = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())
            res['created_at'] = created_at
            res['is_tryme'] = 1

            if 'login' in user:
                res['user_id'] = user['id']
                res['user_login'] = user['login']
                res['user_name'] = user['name']
                res['user_html_url'] = user['html_url']
                userExtra = self.esClient.getUserInfo(res['user_login'])
                res.update(userExtra)
            elif email is not None and email in email_gitee:
                res['user_login'] = email_gitee.get(email)
                userExtra = self.esClient.getUserInfo(res['user_login'])
                res.update(userExtra)
                print('get user_login by email success. name=%s' % name)
            else:
                print('Not Found login, name=%s' % name)
                continue  # TODO 目前先过滤掉未找到user_login的数据
            res.__delitem__('id')

            index_data = {"index": {"_index": self.index_name, "_id": id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(res) + '\n'
        self.esClient.safe_put_bulk(actions)

    def getGenerator(self, response):
        data = ''
        try:
            if isinstance(response, types.GeneratorType):
                res_data = next(response)
                data = json.loads(res_data.encode('utf-8'))
            else:
                data = json.loads(response)
        except JSONDecodeError:
            return data
        return data

    def getEmailGiteeDict(self):
        search = '"must": [{"match_all": {}}]'
        header = {
            'Content-Type': 'application/json',
            'Authorization': self.email_gitee_authorization
        }
        hits = self.esClient.searchEmailGitee(url=self.email_gitee_es, headers=header, index_name=self.email_gitee_index, search=search)
        data = {}
        if hits is not None and len(hits) > 0:
            for hit in hits:
                source = hit['_source']
                data.update({source['email']: source['gitee_id']})
        return data

    def allIdsFunc(self, hit):
        for data in hit:
            self.exists_ids.append(data['_id'])

    def getAllIds(self):
        search = '''{
                      "size": 10000,
                      "_source": {
                        "includes": [
                          "id"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "term": {
                                "is_tryme": 1
                              }
                            }
                          ]
                        }
                      }
                    }'''
        self.esClient.scrollSearch(self.index_name, search=search, func=self.allIdsFunc)

    def randomTime(self, start, end, prop, frmt):
        if start and end and frmt:
            stime = time.mktime(time.strptime(start, frmt))
            etime = time.mktime(time.strptime(end, frmt))
            ptime = stime + prop * (etime - stime)
            return int(ptime)
