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
from data import common
from data.common import ESClient
from collect.gitee import GiteeClient
import time


class GiteeEvent(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.esClient.initLocationGeoIPIndex()
        self.owners = config.get('owners')
        self.gitee_token = config.get("gitee_token")
        self.start_time = config.get("start_time")
        self.event_count = 0
        self.actions = ''

    def run(self, from_time):
        start_time = time.time()
        dic = self.esClient.getOrgByGiteeID()
        self.esClient.giteeid_company_dict = dic[0]
        self.esClient.giteeid_company_change_dict = dic[1]
        print("Collect gitee event data: start", start_time)
        self.getEventFromRepo(self.owners)
        self.esClient.tagUserOrgChanged()
        end_time = time.time()
        print("Collect gitee event data: finished", end_time)
        print("Cost time: ", int(end_time - start_time))

    def getEventFromSingleRepo(self, owner, repo):
        page = 1
        prev_id = 0
        print("start  owner(%s) repo(%s) page=%d" % (
            owner, repo, page))
        gitee_api = GiteeClient(owner, repo, self.gitee_token)

        while True:
            try:
                response = gitee_api.events(prev_id, page)
                events_data = common.getGenerator(response)
                print("owner(%s) repo(%s) events data num=%s, page=%d" % (owner, repo, len(events_data), page))
                json_type = {
                    "is_gitee_StarEvent": 0,
                    "is_gitee_PushEvent": 0,
                    "is_gitee_PullRequestCommentEvent": 0,
                    "is_gitee_PullRequestEvent": 0,
                    "is_gitee_ProjectCommentEvent": 0,
                    "is_gitee_MilestoneEvent": 0,
                    "is_gitee_MemberEvent": 0,
                    "is_gitee_IssueEvent": 0,
                    "is_gitee_IssueCommentEvent": 0,
                    "is_gitee_ForkEvent": 0,
                    "is_gitee_CreateEvent": 0,
                    "is_gitee_DeleteEvent": 0,
                    "is_gitee_CommitCommentEvent": 0,
                }

                if len(events_data) == 0:
                    print("owner(%s) repo(%s) get event break " % (owner, repo))
                    break
                for e in events_data:
                    created_at = None
                    is_type = None
                    e.update(json_type)
                    index_id = owner + "-" + repo + "_"
                    if e.get('id'):
                        index_id = index_id + str(e.get('id'))
                        prev_id = e.get('id')
                    if e.get('created_at'):
                        created_at = e.get('created_at')
                        day = created_at.split('T')
                        e_time = time.mktime(time.strptime(day[0], '%Y-%m-%d'))
                        s_time = time.mktime(time.strptime(self.start_time, '%Y-%m-%d'))
                        if e_time < s_time:
                            print("owner(%s) repo(%s) get event over " % (owner, repo))
                            return
                    if e.get('type'):
                        is_type = 'is_gitee_' + e.get('type')
                        e[is_type] = 1
                        index_id = index_id + e.get('type')
                    is_inner_user = self.esClient.getUserInfo(e.get('actor')['login'])
                    index_data_survey = {"index": {"_index": self.index_name, "_id": index_id}}
                    action = {
                        'user_login': e.get('actor')['login'],
                        'id': prev_id,
                        'created_at': created_at,
                        'type': is_type,
                        'actor': e.get('actor'),
                        'repo': e.get('repo')
                    }
                    action.update(is_inner_user)
                    action.update(json_type)
                    action[is_type] = 1
                    self.actions += json.dumps(index_data_survey) + '\n'
                    self.actions += json.dumps(action) + '\n'
                    self.event_count += 1
                    print("event_count = ", self.event_count)
                    if self.event_count % 5000 == 0:
                        self.esClient.safe_put_bulk(self.actions)
                        self.actions = ''
                        print("put %d events over!" % self.event_count)
                page += 1
            except ValueError as error:
                print("error=%s, page=%d", (error, page))
                page += 1
                continue
            except TypeError as error:
                print("error=%s, page=%d", (error, page))
                page += 1
                continue

        print("owner(%s) repo(%s) end page=%d" % (owner, repo, page))

    def getEventFromRepo(self, owner):
        repo_page = 1
        gitee_api = GiteeClient(owner=owner, repository=None, token=self.gitee_token)
        while True:
            try:
                print("owner(%s), repo_page=%d" % (owner, repo_page))
                response = gitee_api.get_repos(cur_page=repo_page)
                if response.status_code != 200:
                    print('HTTP error!')
                    continue
                repos_object = response.json()
                if len(repos_object) == 0:
                    print("event_count = ", self.event_count)
                    break
                for repo_object in repos_object:
                    repo_name = repo_object['path']
                    print("repo name = ", repo_name)
                    self.getEventFromSingleRepo(owner, repo_name)
                repo_page += 1
            except ValueError as error:
                print("error=%s, page=%d", (error, repo_page))
                repo_page += 1
                continue
            except TypeError as error:
                print("error=%s, page=%d", (error, repo_page))
                repo_page += 1
                continue
        self.esClient.safe_put_bulk(self.actions)
        print("put %d events over!" % self.event_count)
