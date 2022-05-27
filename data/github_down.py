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

import time

from data import common
from data.common import ESClient


class GitHubDown(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.org = config.get('github_org')
        self.repos = config.get('repos')
        self.esClient = ESClient(config)
        self.headers = {"Authorization": config.get('github_authorization')}
        self.repos_name = []

    def run(self, from_date):
        startTime = time.time()
        print("Collect github download clone view data: staring")
        if self.repos is None:
            self.repos_name = self.getFullNames(self.org, from_date)
        else:
            repos_name = str(self.repos).split(";")
            for name in repos_name:
                self.repos_name.append(self.org + '/' + name)
        print(self.repos_name)
        self.getClone(self.repos_name, self.org)
        self.getView(self.repos_name, self.org)

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect github download clone view data: finished after ",
              spent_time)

    def setFromFile(self, filename):
        f = open(filename, 'r')

        actions = ""
        for line in f.readlines():
            sLine = line.split(";")
            date = self.getFormat(sLine[0])
            full_name = self.getFormat(sLine[1])
            clone_count = self.getNumber(sLine[2])
            clone_uniques = self.getNumber(sLine[3])
            view_count = self.getNumber(sLine[4])
            view_uniques = self.getNumber(sLine[5])

            actions += self.setCloneDate(date, self.org,
                                         full_name, clone_count, clone_uniques)

            actions += self.setViewDate(date, self.org,
                                        full_name, view_count, view_uniques)
        self.esClient.safe_put_bulk(actions)

    def getFormat(self, str):
        if '"' in str:
            return str.split('"')[1].split('"')[0]
        return str

    def getNumber(self, sNum):
        n = sNum
        if '"' in n:
            n = n.split('"')[1].split('"')[0]
        if '\n' in n:
            n = n.split('\n')[0]
        num = int(n)
        return num

    def setCloneDate(self, date, org, full_name, clone_count, clone_uniques):
        body = {
            "clone_count": clone_count,
            "clone_uniques": clone_uniques,
            "path": "https://github.com/" + org + "/" + full_name,
            "project": org,
            "full_name": full_name,
            "created_at": date,
            "updated_at": date,
            "is_github_clone": 1
        }
        action = common.getSingleAction(
            self.index_name, "clone_" + date + body["path"], body)
        return action

    def setViewDate(self, date, org, full_name, view_count, view_uniques):
        body = {
            "view_count": view_count,
            "view_uniques": view_uniques,
            "path": "https://github.com/" + org + "/" + full_name,
            "project": org,
            "full_name": full_name,
            "created_at": date,
            "updated_at": date,
            "is_github_view": 1
        }
        action = common.getSingleAction(
            self.index_name, "view_" + date + body["path"], body)
        return action

    def getAllrepo(self, org):
        full_names = []
        r = self.esClient.request_get('https://api.github.com/users/' + org + '/repos' + '?pape=1&per_page=10000',
                                      headers=self.headers)
        data = r.json()

        for rep in data:
            full_names.append(self.ensure_str(rep['full_name']))
        return full_names

    def ensure_str(self, s):
        try:
            if isinstance(s, unicode):
                s = s.encode('utf-8')
        except:
            pass
        return s

    def getFullNames(self, org, from_date):
        full_names = self.getAllrepo(org)
        return full_names

    def getClone(self, full_names, org):
        actions = ""
        for full_name in full_names:
            c = self.esClient.request_get('https://api.github.com/repos/' + full_name + '/traffic/clones', headers=self.headers)

            cloneObj = c.json()

            if "clones" not in cloneObj:
                print("No data")
                continue

            clone = cloneObj['clones']
            for i in range(len(clone)):
                p = clone[i]

                date = p['timestamp']
                body = {
                    "clone_count": p['count'],
                    "clone_uniques": p['uniques'],
                    "path": "https://github.com/" + org + "/" + full_name,
                    "project": org,
                    "full_name": full_name,
                    "created_at": date,
                    "updated_at": date,
                    "is_github_clone": 1
                }
                action = common.getSingleAction(
                    self.index_name, "clone_" + date + body["path"], body)
                actions += action

            self.esClient.safe_put_bulk(actions)

    def getView(self, full_names, org):
        actions = ""
        for full_name in full_names:
            r = self.esClient.request_get('https://api.github.com/repos/' + full_name + '/traffic/views', headers=self.headers)

            viewObj = r.json()

            if "views" not in viewObj:
                print("No data")
                continue
            tmp = viewObj['views']
            for i in range(len(tmp)):
                p = tmp[i]
                date = p['timestamp']
                body = {
                    "view_count": p['count'],
                    "view_uniques": p['uniques'],
                    "path": "https://github.com/" + org + "/" + full_name,
                    "project": org,
                    "full_name": full_name,
                    "created_at": date,
                    "updated_at": date,
                    "is_github_view": 1
                }

                action = common.getSingleAction(
                    self.index_name, "view_" + date + body["path"], body)
                actions += action

        self.esClient.safe_put_bulk(actions)
