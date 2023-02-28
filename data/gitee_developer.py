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
import datetime
import json
import time

from data.common import ESClient
from collect.gitee import GiteeClient


class GiteeDeveloper(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name_committer = config.get('index_name_committer')
        self.esClient = ESClient(config)
        self.owners = config.get('owner')
        self.access_token = config.get('token')
        self.repository = None
        self.base_url = config.get('base_url')
        self.since = config.get('since')
        self.until = config.get('until')
        self.flag = 0

    def run(self, from_time):
        if self.config.get('since') is None:
            self.since = str(datetime.date.today() - datetime.timedelta(days=1))
        print("Collect gitee committers data start from %s" % self.since)
        startTime = time.time()
        self.collect_developer_details()
        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect gitee committers data finished after %s" % spent_time)

    def get_repo_branches(self, owner, repo_path):
        gitee = GiteeClient(owner, repo_path, self.access_token, self.base_url)
        branches = gitee.getSingleReopBranch()
        branches_info = self.esClient.getGenerator(branches)
        branches_name = []
        for b in branches_info:
            branches_name.append(b['name'])
        return branches_name

    def get_developer_info(self, owner, page, repo_path, branch):
        actions = ""
        gitee_api = GiteeClient(owner, self.repository, self.access_token, self.base_url)
        commits = gitee_api.get_commits(repo_path, cur_page=page, since=self.since, until=self.until, sha=branch)
        if commits.status_code != 200:
            print('HTTP get commits error!')
            return actions
        commits_legacy = commits.json()
        if len(commits_legacy) == 0:
            print("commit_page: %i finish..." % page)
            self.flag = 1
            return actions
        for commit in commits_legacy:
            if commit['author'] is not None and 'id' in commit['author']:
                email = commit['commit']['author']['email']
                gitee_id = commit['author']['login']
                id = commit['author']['id']
                action = {
                    'email': email,
                    'gitee_id': gitee_id,
                    'id': id,
                    'created_at': commit['commit']['author']['date']
                }
                es_id = str(id) + '_' + email + '_' + gitee_id
                index_data_survey = {"index": {"_index": self.index_name_committer, "_id": es_id}}
                actions += json.dumps(index_data_survey) + '\n'
                actions += json.dumps(action) + '\n'

            if commit['committer'] is not None and 'id' in commit['committer']:
                email = commit['commit']['committer']['email']
                gitee_id = commit['committer']['login']
                id = commit['committer']['id']
                action = {
                    'email': commit['commit']['committer']['email'],
                    'gitee_id': commit['committer']['login'],
                    'id': commit['committer']['id'],
                    'created_at': commit['commit']['committer']['date']
                }
                es_id = str(id) + '_' + email + '_' + gitee_id
                index_data_survey = {"index": {"_index": self.index_name_committer, "_id": es_id}}
                actions += json.dumps(index_data_survey) + '\n'
                actions += json.dumps(action) + '\n'
        return actions

    def collect_developer_details(self):
        owners = self.owners.split(',')
        for owner in owners:
            print("...start owner: %s..." % owner)
            gitee_api = GiteeClient(owner, self.repository, self.access_token, self.base_url)
            repo_page = 1
            while True:
                response = gitee_api.get_repos(cur_page=repo_page)
                if response.status_code == 429:
                    print('Too Many Requests, sleep 10 seconds')
                    time.sleep(10)
                    continue
                if response.status_code != 200:
                    print('HTTP get repos error!')
                    break
                repos = response.json()
                if len(repos) == 0:
                    break
                repo_page += 1
                print("repo_page: %i" % repo_page)
                for repo in repos:
                    actions = ""
                    repo_path = repo['path']
                    print("start repo: %s" % repo_path)
                    branches_name = self.get_repo_branches(owner, repo_path)
                    if len(branches_name) == 0:
                        break
                    for branch in branches_name:
                        print("start branch: %s" % branch)
                        page = 0
                        self.flag = 0
                        while True:
                            page += 1
                            action = self.get_developer_info(owner, page, repo_path, branch)
                            actions += action
                            if self.flag == 1:
                                break
                    print("collect repo: %s over." % repo_path)
                    self.esClient.safe_put_bulk(actions)
