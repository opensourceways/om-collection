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
import signal
import yaml
import git
import types
import json

from json import JSONDecodeError
from datetime import datetime
from data import common
from data.common import ESClient
from collect.gitee import GiteeClient


class SIG(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.gitee_token = config.get('gitee_token')
        self.sig_link = config.get('sig_link')
        self.sig_file = config.get('sig_file')
        self.sig_repo = config.get('sig_repo')
        self.owner = config.get('owner')
        self.internal_users = config.get('internal_users')
        self.esClient = ESClient(config)
        self.esClient.initLocationGeoIPIndex()
        self.comitter_time = {}
        self.committer_time_by_self = {
            "Infrastructure": "2020-05-14T15:58:00+08:00",
            "doc": "2020-04-03T08:01:00+08:00",
            "sig-CICD": "2020-04-11T18:14:00+08:00",
            "Kernel": "2020-01-06T10:43:00+08:00",
            "sig-Community": "2020-03-28T14:43:00+08:00",
            "A-Tune": "2020-01-11T16:50:00+08:00",
            "kae": "2020-04-16T10:02:00+08:00",
            "Private": "2020-06-02T21:44:00+08:00",
            "oVirt": "2020-06-10T09:42:00+08:00",
            "sig-dpdk": "2020-02-29T11:01:00+08:00",
            "Marketing": "2020-05-13T18:28:00+08:00",
            "dev-utils": "2020-03-17T22:15:00+08:00",
            "sig-mate-desktop": "2020-03-21T10:43:00+08:00",
            "sig-bounds_checking_function": "2020-06-05T10:57:00+08:00",
            "sig-UKUI": "2020-04-01T10:45:00+08:00",
            "sig-RaspberryPi": "2020-03-27T16:24:00+08:00",
            "sig-Ha": "2020-04-29T15:24:00+08:00",
            "sig-ROS": "2020-04-14T15:42:00+08:00",
            "sig-ai-bigdata": "2020-05-09T14:15:00+08:00",
            "sig-EasyLife": "2020-06-01T09:11:00+08:00",
            "sig-security-facility": "2020-04-24T17:50:00+08:00",
            "sig-Compatibility-Infra": "2020-04-17T13:19:00+08:00",
            "sig-DDE": "2020-06-24T12:01:00+08:00",
            "sig-cms": "2020-04-22T22:08:00+08:00"
        }
        self.sigs = []
        self.openeulerUsers = []
        self.mindspore_sigs = {
            "leonwanghui":"2020-02-17T14:26:37+08:00",
            "guozhende":"2020-03-13T10:21:45+08:00",
            "weiluning":"2020-03-13T16:37:53+08:00",
            "leiyuning":"2020-03-13T16:41:01+08:00",
            "dingcheng":"2020-03-13T16:43:57+08:00",
            "zheng-huanhuan":"2020-03-13T20:26:08+08:00",
            "zhunaipan":"2020-03-18T15:38:48+08:00",
            "lujiale":"2020-03-22T09:51:12+08:00",
            "gaocongli":"2020-03-22T09:51:30+08:00",
            "liucunwei":"2020-03-22T09:51:40+08:00",
            "zhunaipan":"2020-03-22T09:51:48+08:00",
            "zhengweimin":"2020-03-25T17:04:21+08:00",
            "dingcheng":"2020-03-25T17:13:33+08:00",
            "zhangqiang":"2020-03-27T10:16:54+08:00",
            "zhenghuanhuan":"2020-04-02T10:08:58+08:00",
            "zhangzhenghai":"2020-04-02T11:38:34+08:00",
            "zhengweimin":"2020-04-07T09:30:16+08:00",
            "leiyuning":"2020-05-13T16:28:42+08:00",
            "wangsiyuan":"2020-05-20T17:26:38+08:00",
            "leonwanghui": "2020-03-13T16:35:53+08:00",
            "TommyLike": "2020-03-13T16:36:18+08:00",
            "zhengweimin": "2020-03-14T12:04:30+08:00",
            "zhunaipan": "2020-03-21T15:56:27+08:00",
            "liucunwei": "2020-03-21T15:56:44+08:00",
            "gaocongli": "2020-03-21T16:02:09+08:00",
            "lujiale": "2020-03-21T16:36:31+08:00",
            "dingcheng": "2020-03-22T09:51:21+08:00",
            "mindspore-ci-bot": "2019-12-03T18:32:29+08:00",
            "weiyang": "2020-03-04T17:55:14+08:00",
            "helloway": "2020-02-17T14:26:37+08:00",
            "absolutely_unexpected": "2020-03-17T14:26:37+08:00",

        }

    def run(self, from_date):

        # self.getCreatedTime()
        # self.getCommitter("mindspore", "mindspore")
        # self.getCommitter("mindspore", "mindinsight")
        # self.getCommitter("mindspore", "docs")
        # self.getCommitter("mindspore", "graphengine")
        # self.getCommitter("mindspore", "mindarmour")
        # self.getCommitter("mindspore", "ms-operator")
        # self.getCommitter("mindspore", "course")
        # self.getCommitter("mindspore", "community")
        # self.getCommitter("mindspore", "book")

        self.getOpeneulerCommitter()
        self.getOpenlokkengCommitter()

        # self.collectTotalByType(from_date)

    def getOpeneulerCommitter(self):
        self.openeulerUsers = self.getItselfUsers(self.internal_users)
        self.getCommiterAddedTime()
        self.prepare_list()

    def getOpenlokkengCommitter(self):
        repos = self.get_repos(self.sig)
        for repo in repos:
            repo_name = repo['full_name'].split('/')[1]
            if repo_name == 'openLooKeng-installation':
                continue
            url = repo['html_url']
            dir = self.clone_dir + '\\' + repo_name
            self.exceGitPull(dir, url)
            filename = dir + '\\OWNERS'
            self.getCommitterFromOwner(filename, self.sig, repo_name)

        # self.exceGitPull(self.clone_dir, self.clone_url)

        # self.getCommitterFromOwner()

    def exceGitPull(self, dir, url):
        if not os.path.exists(dir):
            git.Repo.clone_from(url=url, to_path=dir)
        else:
            g = git.Git(dir)
            g.execute('git reset --hard')
            g.execute('git pull origin master')

    def get_repos(self, org):
        client = GiteeClient(org, None, self.gitee_token)
        print(self.is_gitee_enterprise)
        if self.is_gitee_enterprise == "true":
            org_data = self.getGenerator(client.enterprises())
        else:
            org_data = self.getGenerator(client.org())

        repos = []
        for repo in org_data:
            repos.append(repo)

        return repos

    def getCommitterFromOwner(self, filename, sig, repo):
        data = yaml.load_all(open(filename))
        for d in data:
            for key in d:
                for owner in d[key]:
                    doc = {
                            "sig_name": sig,
                            "repo_name": repo,
                            "user_gitee_name": owner,
                            "created_at": '2020-05-10T10:10:00+08:00',
                            key: 1
                            }
                    id = owner + "_" + sig + "_" + repo
                    action = common.getSingleAction(self.index_name, id, doc)
                    self.esClient.safe_put_bulk(action)

    def getCreatedTime(self, filename="_成员管理日志-2020-07-16_17_39_24.csv"):
        f = open(filename, 'r')

        i = 0

        for line in f.readlines():
            if line is None or not line:
                continue
            if i == 0:
                i += 1
                continue

            sLine = line.split(',')
            created_at = sLine[2]
            name = sLine[4]
            tt = created_at.split("'")[1]
            created_at = tt.split( )[0] + "T" + tt.split( )[1] + "+08:00"
            self.mindspore_sigs[name] = created_at

    def getCommitter(self, owner, org):
        client = GiteeClient(owner, org, self.gitee_token)

        data = self.getGenerator(client.collaborators())
        actions = ""
        for d in data:
            user_login = d.get("login")
            user_name = d.get("name")
            is_admin = d.get("permissions").get("admin", 0)
            created_at = "2020-03-04T17:55:10+08:00"
            # if is_admin:
            if user_name in self.mindspore_sigs:
                created_at = self.mindspore_sigs.get(user_name)
            elif user_login in self.mindspore_sigs:
                created_at = self.mindspore_sigs.get(user_login)
            else:
                print("not exist:", user_name, user_login)
            # else:
            #     print("not exist:", user_name)
            doc = {
                "sig_name": org,
                "repo_name": org,
                "user_login": user_login,
                "user_name": user_name,
                "is_admin": is_admin,
                "is_committer": 1,
                "created_at": created_at
            }
            id = user_login + "_" + org + "_" + user_name + created_at
            # id = user_login + "_" + org + "_" + user_name
            action = common.getSingleAction(self.index_name, id,
                                            doc)
            actions += action
        self.esClient.safe_put_bulk(actions)


    def gitClone(self):
        print("............start to git clone ")
        git.Git("/root/zj/george/obs/george/").clone(self.sig_link)


    def getCommiterAddedTime(self):
        client = GiteeClient(self.owner, self.sig_repo, self.gitee_token)
        data = self.getGenerator(client.pulls(state='merged'))

        for d in data:
            print("getCommiterAddedTime pull request number=%d" % d["number"])
            pull_files = self.getGenerator(client.pull_files(d["number"]))
            for p in pull_files:
                if self.checkFileNameContaintCommiter(p["filename"]) == False:
                    continue
                sig_name = p["filename"].split("/")[1]

                lines = p["patch"]["diff"].split("\n")
                for line in lines:
                    if "+- " not in line:
                        continue
                    commiter_name = line.split("+- ")[1]
                    # print("sig_name=%s, commiter_name=%s, created_at=%s"% (sig_name, commiter_name, d['merged_at']))
                    self.comitter_time[sig_name + "_" + commiter_name] = d['merged_at']


    def checkFileNameContaintCommiter(self, file_name):
        fs = file_name.split("/")
        if fs[0] != "sigs":
            return False

        if len(fs) != 3:
            return False

        if fs[2] == "OWNERS":
            return True
        return False

    def getGenerator(self, response):
        data = []
        try:
            while 1:
                if isinstance(response, types.GeneratorType):
                    data += json.loads(next(response).encode('utf-8'))
                else:
                    data = json.loads(response)
                    break
        except StopIteration:
            # print(response)
            print("...end")
        except JSONDecodeError:
            print("Gitee get JSONDecodeError, error: ", response)

        return data

    def prepare_list(self):
        print("........ prepare_list start")

        f = open(self.sig_file)
        y = yaml.load_all(f)

        # t = datetime.now().strftime('%Y-%m-%d')
        sig_name_all = []
        for data in y:
            actions = ""
            for sig in data['sigs']:
                sig_name_all.append(sig['name'])
                print(sig['name'])
                filename = self.sig_repo + "/sigs/" + sig['name'] + "/OWNERS"
                ownerFile = open(filename)
                ownerFileData = yaml.load_all(ownerFile)
                maintainers = []
                committers = []
                for d in ownerFileData:
                    for maintainer in d['maintainers']:
                        maintainers.append(maintainer)
                    if d['committers']:
                        for committer in d['committers']:
                            committers.append(committer)
                ownerFile.close()

                for repo in sig['repositories']:
                    for m in maintainers:
                        doc = self.getSingleV(m, sig, repo, "is_maintainer")
                        id = m + "_" + sig['name'] + "_" + repo
                        action = common.getSingleAction(self.index_name, id,
                                                        doc)
                        actions += action
                    for m in committers:
                        doc = self.getSingleV(m, sig, repo, "is_committer")
                        id = m + "_" + sig['name'] + "_" + repo
                        action = common.getSingleAction(self.index_name, id,
                                                        doc)
                        actions += action
            self.esClient.safe_put_bulk(actions)
        print(self.sigs)
        f.close()
        for dir in os.walk(self.clone_dir + '\\community\\sig').__next__()[1]:
            if dir not in sig_name_all:
                onwerfile = self.clone_dir + '\\community\\sig\\' + dir + '\\' + 'ONWERS'
                self.getCommitterFromOwner(onwerfile, self.sig, dir)
        print("........ prepare_list end")

    def getSingleV(self, m, sig, repo, key):
        tmp_time = self.comitter_time.get(sig['name'] + "_" + m)
        if tmp_time is None:
            # print("get commiter (%s) created time is %s" % (
            #         sig['name'] + "_" + m,
            #         self.comitter_time.get(sig['name'] + "_" + m)))
            if sig['name'] not in self.sigs:
                self.sigs.append(sig['name'])
            tmp_time = self.committer_time_by_self.get(sig['name'],
                                                       "2020-06-24T12:01:00+08:00")
        doc = {
            "sig_name": sig['name'],
            "repo_name": repo,
            "user_gitee_name": m,
            "created_at": tmp_time,
            key: 1
        }
        if m in self.openeulerUsers:
            doc["tag_user_company"] = "huawei"
            doc["is_project_internal_user"] = 1
        else:
            doc["tag_user_company"] = "n/a"
            doc["is_project_internal_user"] = 0
        return doc


    def collectTotalByType(self, from_time):
        matchs = [{"name": "is_committer", "value": 1}]
        from_date = datetime.strptime(from_time, "%Y%m%d")
        to_date = datetime.today()
        data = self.esClient.getCountByDateRange(matchs, from_date, to_date)
        # print(data)
        actions = ""
        for d in data:
            print("date = %s, count = %s" % (
                d.get("to_as_string"), d.get("doc_count")))
            created_at = d.get("to_as_string")
            body = {
                "all_count": d.get("doc_count"),
                "created_at": created_at,
                "updated_at": created_at,
                "committer_total": 1
            }

            id = created_at + type + "_total"
            action = common.getSingleAction(self.index_name, id, body)
            actions += action
        self.esClient.safe_put_bulk(actions)

    def getItselfUsers(self, filename="users"):
        f = open(filename, 'r')

        users = []
        for line in f.readlines():
            if line != "\n":
                users.append(line.split('\n')[0])
        print(users)
        print(len(users))
        return users