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

    def run(self, from_date):
        startTime = time.time()
        print("Collect github star watch fork data: staring")
        repoNames = self.getRepoNames()
        actions = ""
        for repo in repoNames:
            action = self.getSWF(repo)
            # print(action)
            actions += action
        self.esClient.safe_put_bulk(actions)

        if self.github_index_name:
            for type in self.github_types:
                self.getTotal(type=type, index_name=self.github_index_name, total_index=self.github_index_name_total,
                              field=self.github_field,size=self.github_size, mark='github')
        if self.gitee_index_name:
            for type in self.gitee_types:
                self.getTotal(type=type, index_name=self.gitee_index_name, total_index=self.gitee_index_name_total,
                              field=self.gitee_field,size=self.gitee_size, search=',"must": [{ "match": { "is_gitee_repo":1 }}]', mark='gitee')

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S", time.gmtime(endTime - startTime))
        print("Collect github star watch fork data: finished after ", spent_time)

    def getTotal(self, type, index_name, total_index, url=None, date='2019-06-01', field=None, size='10', search='', mark=None):
        if not url:
            url = self.url + '/' + index_name + '/_search'
        if not date:
            date = self.from_data[:4] + '-' + self.from_data[4:6] + '-' + self.from_data[6:]
        datei = datetime.datetime.strptime(date, "%Y-%m-%d")
        dateii = datei
        totalmark = 'is_' + mark + '_' + type + '_total'
        x = []
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
            res = json.loads(requests.get(url=url, headers=self.headers, verify=False, data=data.encode('utf-8')).content)
            num = sum([int(r['3']['value']) for r in res["aggregations"]["2"]["buckets"]])
            body = {'created_at': stime+'T00:00:00.000+0800', totalmark: 1, 'total_num': num}
            ID = totalmark + stime
            data = common.getSingleAction(total_index, ID, body)
            if num > 0:
                print('%s:%s' % (stime, num))
                self.esClient.safe_put_bulk(data)
                x.append(num)

    def getRepoNames(self):
        gitclient = GithubClient(self.org, "", self.github_authorization)
        repos = gitclient.getAllrepo()
        repoNames = []
        for rep in repos:
            repoNames.append(self.ensure_str(rep['name']))
        return repoNames

    def ensure_str(self, s):
        try:
            if isinstance(s, unicode):
                s = s.encode('utf-8')
        except:
            pass
        return s


    def getSWF(self, repo):
        client = GithubClient(self.org, repo, self.github_authorization)
        r = client.repo()
        r["swf_update_time"] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S+08:00')
        # r["swf_update_time"] = "2020-06-19T22:21:25+08:00"
        id = "swf_" + r["swf_update_time"] + r.get("full_name")
        action = common.getSingleAction(self.index_name, id, r)
        return action
