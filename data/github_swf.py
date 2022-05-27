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

import os
import sys

import requests

import json
import time

import datetime
from data import common
from data.common import ESClient
from collect.github import GithubClient


class GitHubSWF(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.org = config.get('github_org')
        self.esClient = ESClient(config)
        self.headers = {}
        self.github_authorization = config.get('github_authorization')
        self.url = config.get('es_url')
        self.from_data = config.get("from_data")
        self.headers = {'Content-Type': 'application/json', 'Authorization': config.get('authorization')}
        if 'github_types' in config:
            self.github_types = config.get('github_types').split(',')
        self.github_size = config.get('github_size')
        self.github_field = config.get('github_field')
        self.github_index_name = config.get('github_index_name')
        self.github_index_name_total = config.get('github_index_name_total')
        if 'gitee_types' in config:
            self.gitee_types = config.get('gitee_types').split(',')
        self.gitee_field = config.get('gitee_field')
        self.gitee_size = config.get('gitee_size')
        self.gitee_index_name = config.get('gitee_index_name')
        self.gitee_index_name_total = config.get('gitee_index_name_total')

        self.is_fetch_star_details = config.get('is_fetch_star_details')
        self.star_index_name = config.get('star_index_name')
        self.orgs = config.get('orgs')
        self.interval_sleep_time = config.get("sleep_time")
        self.refresh_node_times = config.get("refresh_node_times")

    def run(self, from_date):
        self.failed_process_repos = []  # Store the failed fetch data repos
        startTime = time.time()
        print("Collect github star watch fork data: staring")

        if self.refresh_node_times:  # for run in specific point time
            self.checkSleep()

        now = datetime.datetime.now()
        now_str = datetime.datetime.strftime(now, '%Y-%m-%d  %H:%M:%S')
        print(f"The accurate time for starting to collecting data is: {now_str}")

        service_flag = 0  # set a service_switch_flag, to do different service.
        if self.is_fetch_star_details == 'True':
            org_list = self.orgs.split(",")
            for org in org_list:
                self.getSWF_Stargazers(org)
            service_flag = 1

        if service_flag == 0:
            repoNames = self.getRepoNames()
            actions = ""
            for repoName in repoNames:
                action = self.getSWF(repoName)
                if not action:  # if get None from self.getSWF(repo)
                    continue
                actions += action
            print(f'There are {len(self.failed_process_repos)} repos cannot be fetched. They are:\n')
            for repo in self.failed_process_repos:
                print(repo)

            self.esClient.safe_put_bulk(actions)

            if self.github_index_name:
                for type in self.github_types:
                    self.getTotal(type=type, index_name=self.github_index_name,
                                  total_index=self.github_index_name_total,
                                  field=self.github_field, size=self.github_size, mark='github')
            if self.gitee_index_name:
                for type in self.gitee_types:
                    self.getTotal(type=type, index_name=self.gitee_index_name, total_index=self.gitee_index_name_total,
                                  field=self.gitee_field, size=self.gitee_size,
                                  search=',"must": [{ "match": { "is_gitee_repo":1 }}]', mark='gitee')

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S", time.gmtime(endTime - startTime))
        print("Collect github star watch fork data: finished after ", spent_time)

    def getTotal(self, type, index_name, total_index, url=None, date='2019-06-01', field=None, size='10', search='',
                 mark=None):
        if not url:
            url = self.url + '/' + index_name + '/_search'
        if not date:
            date = self.from_data[:4] + '-' + self.from_data[4:6] + '-' + self.from_data[6:]
        datei = datetime.datetime.strptime(date, "%Y-%m-%d")
        dateii = datei
        totalmark = 'is_' + mark + '_' + type + '_total'
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            if dateii == datenow + datetime.timedelta(days=1):
                break
            dateiise = dateii
            dateii += datetime.timedelta(days=1)
            stime = datetime.datetime.strftime(dateiise, "%Y-%m-%d")
            data = '''{
          "size": 0,
          "query": {
            "bool": {
              "filter": [
                {
                  "range": {
                    "created_at": {
                        "gte":"%sT00:00:00.000+0800",
                        "lt":"%sT00:00:00.000+0800"
                    }
                  }
                },
                {
                  "query_string": {
                    "analyze_wildcard": true,
                    "query": "*"
                  }
                }
              ]%s
            }
          },
          "aggs": {
            "2": {
              "terms": {
                "field": "%s",
                "size": %s,
                "order": {
                  "_key": "desc"
                },
                "min_doc_count": 1
              },
              "aggs": {
                "3": {
                  "max": {
                    "field": "%s"
                  }
                }
              }
            }
          }
        }''' % (str(dateiise).split()[0], str(dateii).split()[0], search, field, size, type)
            res = json.loads(
                             self.esClient.request_get(url=url, headers=self.headers,
                             data=data.encode('utf-8')).content)
            num = sum([int(r['3']['value']) for r in res["aggregations"]["2"]["buckets"]])
            body = {'created_at': stime + 'T00:00:00.000+0800', totalmark: 1, 'total_num': num}
            ID = totalmark + stime
            data = common.getSingleAction(total_index, ID, body)
            if num > 0:
                print('%s:%s' % (stime, num))
                self.esClient.safe_put_bulk(data)

    def getRepoNames(self):
        print(f'Starting to fetch all repos...')
        gitclient = GithubClient(self.org, "", self.github_authorization)
        repos = gitclient.get_repos(self.org)
        repoNames = []
        for repo in repos:
            if repo.get('name'):
                repoNames.append(repo.get('name'))
        print(f'Completed to fetch all repos, {len(repoNames)} are collected in all.')
        return repoNames

    def ensure_str(self, s):
        try:
            if isinstance(s, str):
                s = s.encode('utf-8')
        except:
            pass
        return s

    def getSWF(self, repo):
        print(f'Processing repo: {repo}.')
        client = GithubClient(self.org, repo, self.github_authorization)
        r = client.repo()

        if not r:
            print(f'Processing Repo: {repo} failed.\n')
            self.failed_process_repos.append(repo)
            return

        r["swf_update_time"] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S+08:00')
        id = "swf_" + r["swf_update_time"] + "_" + r.get("full_name")
        action = common.getSingleAction(self.index_name, id, r)
        return action

    def getSWF_Stargazers(self, owner):

        repos = self.getOwnerRepoNames(owner)
        actions = ""
        for repo in repos:
            client = GithubClient(self.org, repo, self.github_authorization)

            repo_star_user_list = client.getStarDetails(owner=owner)
            repo_issue_list = client.getIssueDetails(owner=owner)

            for repo_star_user in repo_star_user_list:
                star = {}
                id = str(repo_star_user['user'].get('id')) + "_star_" + repo
                star["created_at"] = repo_star_user.pop("starred_at")
                star["user_login"] = repo_star_user['user']["login"]
                star["user_id"] = repo_star_user['user']['id']
                star["owner"] = owner
                star["repo"] = repo
                star["is_github_star"] = 1
                action = common.getSingleAction(self.star_index_name, id, star)
                actions += action

            for repo_issue in repo_issue_list:
                issue = {}
                id = str(repo_issue['id']) + "_issue_" + repo
                issue["created_at"] = repo_issue['created_at']
                issue["issue_title"] = repo_issue['title']
                issue["issue_id"] = repo_issue['id']
                issue['user_login'] = repo_issue['user']['login']
                issue["owner"] = owner
                issue["repo"] = repo
                issue["is_github_issue"] = 1
                action = common.getSingleAction(self.star_index_name, id, issue)
                actions += action

        self.esClient.safe_put_bulk(actions)

    def getOwnerRepoNames(self, owner):
        gitclient = GithubClient(self.org, "", self.github_authorization)
        repos = gitclient.getAllOwnerRepo(owner)
        repoNames = []
        for rep in repos:
            repoNames.append(self.ensure_str(rep['name']))
        return repoNames

    def checkSleep(self):
        refresh_node_time_list = self.refresh_node_times.split(";")

        # get now utc now time
        utc_now = datetime.datetime.now(datetime.timezone.utc)

        # transfrom utc now time to beijing now time
        timedelta = datetime.timedelta(hours=+8)
        tz = datetime.timezone(timedelta)
        beijing_now = utc_now.astimezone(tz=tz)

        interval_sleep_time = int(self.interval_sleep_time)

        for i in range(len(refresh_node_time_list)):
            checkTime_str = datetime.datetime.today().strftime("%Y%m%d") + "-" + refresh_node_time_list[i]
            tz_checkTime = datetime.timezone(datetime.timedelta(hours=+0))
            checkTime = datetime.datetime.strptime(checkTime_str, '%Y%m%d-%H:%M:%S')
            checkTime = checkTime.astimezone(tz_checkTime)

            delta_sec = checkTime.timestamp() - beijing_now.timestamp()

            if delta_sec > 0 and delta_sec < interval_sleep_time:
                print(
                    f"Remaining {delta_sec} seconds from the {str(i + 1)} node time. I must sleep for that time point")
                time.sleep(delta_sec)
