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
import types
from json import JSONDecodeError

import yaml

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data.common import ESClient

GITEE = 'https://gitee.com/'
GITHUB = 'https://github.com/'


class GiteeGithubCombine(object):
    def __init__(self, config=None):
        self.config = config
        self.gitee_token = config.get('gitee_token')
        self.github_token = config.get('github_token')
        self.tokens = config.get('tokens').split(',')
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.owner_project_yaml = config.get('owner_project_yaml', 'owner_project.yaml')
        self.org_owner_project = {}

    def run(self, from_time):
        print('start gitee and github stars forks collection')
        self.get_owner_project()

    def get_owner_project(self):
        datas = yaml.safe_load_all(open(self.owner_project_yaml, encoding='UTF-8')).__next__()
        for owner_item in datas['owner_items']:
            owner = owner_item['owner']
            items = owner_item['items']
            for item in items:
                print('****** star owner: %s, project: %s' % (owner, item))
                temps = str(item).split('/')
                org = temps[1]
                try:
                    repo = temps[2]
                except IndexError:
                    repo = None
                if str(item).startswith('gitee'):
                    repos = self.gitee_repos(org=org, repo=repo)
                    self.write_repos(owner=owner, org=org, repos=repos, platform=GITEE)
                else:
                    repos = self.github_org_repos(org=org, repo=repo)
                    self.write_repos(owner=owner, org=org, repos=repos, platform=GITHUB)

    def gitee_repos(self, org, repo):
        if repo is None:
            client = GiteeClient(org, None, self.gitee_token)
            repos = self.get_generator(client.org())
        else:
            client = GiteeClient(org, repo, self.gitee_token)
            repos = [self.get_generator(client.repo())]
        return repos

    def github_org_repos(self, org, repo):
        if repo is None:
            client = GithubClient(org=org, repository=None, token=self.github_token, tokens=self.tokens)
            repos = client.get_repos(org=org)
        else:
            client = GithubClient(org=org, repository=repo, token=self.github_token, tokens=self.tokens)
            repos = client.get_repo(org=org, repo=repo)
        return repos

    def write_repos(self, owner, org, repos, platform):
        actions = ''
        for repo in repos:
            try:
                pulls_count = self.repo_commit_count(org, repo, platform, 'pulls')
                issues_count = self.repo_commit_count(org, repo, platform, 'issues')
                commits_count = self.repo_commit_count(org, repo, platform, 'commits')
                if platform == GITHUB:
                    issues_count = issues_count - pulls_count
                full_name = repo['full_name']
                repo_data = {
                    'org': org,
                    'owner': owner,
                    'repo_name': repo['name'],
                    'repo_full_name': full_name,
                    'created_at': repo['created_at'],
                    'stargazers_count': repo['stargazers_count'],
                    'forks_count': repo['forks_count'],
                    'watchers_count': repo['watchers_count'],
                    'pulls_count': pulls_count,
                    'issues_count': issues_count,
                    'commits_count': commits_count,
                    'platform': platform,
                }
                index_data = {"index": {"_index": self.index_name, "_id": platform + full_name}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(repo_data) + '\n'
            except KeyError:
                print("****** not found project, owner: %s *****" % owner)

        self.esClient.safe_put_bulk(actions)

    def repo_commit_count(self, org, repo, platform, item):
        if platform == GITHUB:
            client = GithubClient(org=org, repository=repo['name'], token=self.github_token, tokens=self.tokens)
            total_count = client.get_contribute_count(org, repo['name'], item)
        else:
            client = GiteeClient(org, repo['path'], self.gitee_token)
            total_count = client.get_contribute_count(org, repo['path'], item)
        return total_count

    def get_generator(self, response):
        data = []
        try:
            while 1:
                if isinstance(response, types.GeneratorType):
                    res_data = next(response)
                    if isinstance(res_data, str):
                        data += json.loads(res_data.encode('utf-8'))
                    else:
                        data += json.loads(res_data.decode('utf-8'))
                else:
                    data = json.loads(response)
                    # if isinstance(data, dict):
                    #     data = []
                    break
        except StopIteration:
            return data
        except JSONDecodeError:
            print("Gitee get JSONDecodeError, error: ", response)
        except Exception as ex:
            print('*** getGenerator fail ***', ex)
            return data

        return data
