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

import _thread

import glob
import time
import requests
import json


EVENT_ADD_REPO = "新增了仓库"
EVENT_FORK_REPO = "fork了仓库"
EVENT_DELETE_REPO = "删除了仓库"
EVENT_OPEN_REPO = "设为内部公开仓库"
EVENT_PRIVATE_REPO = "设为私有仓库"
EVENT_CODE_SSH_PULL = "SSH PULL"
EVENT_CODE_HTTP_PULL = "HTTP PULL"
EVENT_CODE_SSH_PUSH = "SSH PUSH"
EVENT_CODE_DOWNLOAD_ZIP = "DOWNLOAD ZIP"
OWNERS = ['mindspore']


from os import path
from data import common
from data.common import ESClient
from collect.gitee import GiteeClient
from configparser import ConfigParser


class GiteeEvent(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.url = config.get('es_url')
        self.is_from_log_files = config.get('is_from_log_files')
        self.is_gitee_enterprise = config.get('is_gitee_enterprise')
        self.headers = {'Content-Type': 'application/json'}
        self.org = config.get('org')
        self.filters = config.get('filters')
        self.esClient = ESClient(config)
        self.esClient.initLocationGeoIPIndex()
        self.gitee_token = config.get('gitee_token')
        self.OWNERS = config.get('OWNERS')
        if self.is_from_log_files == 'true':
            self.index_name_log = json.loads(config.get('index_name_log'))
            self.index_name_all = json.loads(config.get('index_name_all'))
            self.gitee_event_log_dir = config.get('gitee_event_log_dir', "").split(",")
        self.esClient.getEnterpriseUser()
        self.esClient.internalUsers = self.esClient.getItselfUsers(self.esClient.internal_users)

    def writeGiteeDownDataByFile(self, filename, indexName=None):
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        actions = ""
        i = 0
        bfbi = 0
        tlines = len(lines)
        for line in lines:
            # line = f.readline()
            if line is None or not line:
                continue
            if i == 0:
                i += 1
                continue

            try:
                sLine = line.split(',')
                author_id = sLine[0]
                author_name = sLine[1]
                time = sLine[2][1:]
                event = sLine[3]
                repo_full_name = sLine[4].split('(')[0]
                ip = sLine[5].split()
                # if ip == "127.0.0.1":
                #     continue

                time = time.split( )[0] + "T" + time.split( )[1] + "+08:00"
                is_forked_repo = 0
                if repo_full_name.split('/')[0] not in OWNERS:
                    is_forked_repo = 1
                location = self.esClient.getLocationByIP(ip)

                body = {
                    "author_id": author_id,
                    "country": location.get('country_iso_code'),
                    "city": location.get('city_name'),
                    "region_name": location.get('region_name'),
                    "continent_name": location.get('continent_name'),
                    "region_iso_code": location.get('region_iso_code'),
                    "author_name": author_name,
                    "ip": ip,
                    "created_at": time,
                    "updated_at": time,
                    "event": event,
                    "path": repo_full_name,
                    "is_forked_repo": is_forked_repo,
                    "location": location.get('location'),
                }

                id = author_id + ip + event
                indexName = self.index_name_log[filename.split('/')[-1].split('_')[0]]
                action = common.getSingleAction(indexName, id, body)
                actions += action
                i += 1
                if i % 1000 == 0:
                    self.esClient.safe_put_bulk(actions)
                    actions = ''

                bfbii = bfbi
                bfbi = "%.1f" % (i * 100 / tlines)
                if bfbi != bfbii:
                    print("%s%% :  %s / %s" % (bfbi, i, tlines))
            except:
                continue

        print('100%')
        self.esClient.safe_put_bulk(actions)
        f.close()


    def get_repos(self, org):
        client = GiteeClient(org, None, self.gitee_token)
        print(self.is_gitee_enterprise)
        if self.is_gitee_enterprise == "true":
            client = GiteeClient(org, None, self.gitee_token)
            org_data = common.getGenerator(client.enterprises())
        else:
            org_data = common.getGenerator(client.org())

        if self.filters is None:
            for org in org_data:
                print(org['path'])
            return org_data

        repos = []
        for org in org_data:
            path = org['path']
            if self.checkIsCollectRepo(path, org['public']) == True:
                print(org['path'])
                repos.append(org)

        return repos

    def getThreadFuncs(self, from_date, csv_path):
        thread_func_args = {}
        files = []
        for file in glob.glob(csv_path):
            # f = file.split(path)[1]
            print(file)
            files.append(file)
            # _thread.start_new_thread(writeGiteeDownDataByFile, (file, ))
        thread_func_args[self.writeGiteeDownDataByFile] = files
        return thread_func_args


    def getRepoThreadFuncs(self, from_date):
        thread_func_args = {}
        values = []
        repos = self.get_repos(self.org)
        for repo in repos:
            values.append((self.org, repo['path']))

        thread_func_args[self.getEventFromRepo] = values
        return thread_func_args


    def getEventFromRepo(self, owner, repo):
        page = 1
        print("start  owner(%s) repo(%s) page=%d" % (
        owner, repo, page))
        client = GiteeClient(owner, repo, self.gitee_token)
        actions = ''
        while 1:
            try:
                response = client.events(page)

                events_data = common.getGenerator(response)
                print("owner(%s) repo(%s) envents data num=%s, page=%d" % (owner, repo, len(events_data), page))
                # print(events_data)
                json_type ={
                    "is_gitee_StarEvent":0,
                    "is_gitee_PushEvent":0,
                    "is_gitee_PullRequestCommentEvent":0,
                    "is_gitee_ProjectCommentEvent":0,
                    "is_gitee_MilestonEevent":0,
                    "is_gitee_MemberEvent":0,
                    "is_gitee_IssueEvent":0,
                    "is_gitee_IssueCommentEvent":0,
                    "is_gitee_ForkEvent":0,
                    "is_gitee_CreateEvent":0,
                    "is_gitee_DeleteEvent":0,
                    "is_gitee_CommitCommentEvent":0,                 
                }

                if len(events_data) == 0:
                    print("owner(%s) repo(%s) get event break " % (owner, repo))
                    break
                for e in events_data:
                    e.update(json_type)
                    id = owner + "-" + repo + "_"
                    if e.get('id'):
                        id = id + str(e.get('id'))
                    if e.get('type'):
                        is_type='is_gitee_'+e.get('type')
                        e[is_type]=1
                        id = id + e.get('type')
                    is_inner_user = self.esClient.getUserInfo(e.get('actor')['login'])
                    e.update(is_inner_user)
                    action = common.getSingleAction(self.index_name, id, e)
                    actions += action
                page += 1
            except ValueError as e:
                print("error=%s, page=%d", (e, page))
                page += 1
                continue
            except TypeError as e:
                print("error=%s, page=%d", (e, page))
                page += 1
                continue

        self.esClient.safe_put_bulk(actions)
        print("owner(%s) repo(%s) end page=%d" % (owner, repo, page))


    def run(self, from_date):
        if self.is_from_log_files == 'true':
            for path in self.gitee_event_log_dir:
                thread_func_args = self.getThreadFuncs(from_date, path + "*.csv")
                common.writeDataThread(thread_func_args)
        else:
            thread_func_args = self.getRepoThreadFuncs(from_date)
            common.writeDataThread(thread_func_args)
        # writeGiteeDownDataByFile("仓库管理日志-2020-04-28_14_04_39.csv")