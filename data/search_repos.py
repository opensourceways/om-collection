#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2022 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2022
#
import json
import datetime

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data.common import ESClient


class SearchRepos(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.index_name_gitee = config.get('index_name_gitee')
        self.index_name_github = config.get('index_name_github')
        self.gitee_token = config.get('gitee_token')
        self.github_token = config.get('github_token')
        self.tokens = config.get('tokens')
        self.repos = config.get('repos').split(',')
        self.is_get_gitee = config.get('is_get_gitee')
        self.is_get_github = config.get('is_get_github')

    def run(self, from_time):
        print("Collect repos data: start")
        for repo in self.repos:
            if self.is_get_gitee == 'true':
                self.get_gitee_repos(repo)
            if self.is_get_github == 'true':
                self.get_github_repos(repo)
        print("Collect repos data: finished")

    def get_gitee_repos(self, name):
        client = GiteeClient(None, None, self.gitee_token)
        response = client.gitee_search_repo(name)
        datas = self.esClient.getGenerator(response)
        self.write_repos(name, datas, 'gitee', self.index_name_gitee)

    def get_github_repos(self, name):
        datas = []
        client = GithubClient(org=None, repository=None, token=self.github_token)
        repos = client.git_search_repo(name)
        total_count = 0
        for repo in repos:
            total_count = repo.get('total_count')
            datas.extend(repo.get('items'))
        self.write_repos(name, datas, 'github', self.index_name_github, total_count)

    def write_repos(self, search, datas, platform, index_name, total_count=None):
        actions = ''
        for data in datas:
            repo_detail = {
                "search": search,
                "created_at": data["created_at"],
                "updated_at": data["updated_at"],
                "owner_login": data['owner']['login'],
                "user_id": data['owner']['id'],
                "user_login": data['owner']['login'],
                "repository": data["full_name"],
                "public": data.get("public"),
                "private": data.get("private"),
                "{}_repo".format(platform): data["html_url"],
                "description": data["description"],
                "is_{}_repo".format(platform): 1
            }
            index_id = platform + '_' + search + '_' + data["full_name"]
            index_data = {"index": {"_index": index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_detail) + '\n'
        if platform == 'gitee':
            total_count = len(datas)

        now_date = datetime.date.today()
        created_at = now_date.strftime("%Y-%m-%dT08:00:00+08:00")
        index_id = platform + '_' + search + '_total' + now_date.strftime("%Y%m%d")
        action = {
            "total_count": total_count,
            "search": search,
            "is_total_count": 1,
            "is_{}_repo".format(platform): 1,
            "created_at": created_at
        }
        index_data = {"index": {"_index": index_name, "_id": index_id}}
        actions += json.dumps(index_data) + '\n'
        actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)
