# -*- coding: utf-8 -*-
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


class GiteeEvent(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.is_gitee_enterprise = config.get('is_gitee_enterprise')
        if config.get('orgs'):
            self.orgs = config.get('orgs').split(",")
        self.filters = config.get('filters')
        self.esClient = ESClient(config)
        self.esClient.initLocationGeoIPIndex()
        self.gitee_token = config.get('gitee_token')


    def writeGiteeDownDataByFile(self, filename):
        f = open(filename, 'r')

        actions = ""
        i = 0
        for line in f.readlines():
            # line = f.readline()
            if line is None or not line:
                continue
            if i == 0:
                i += 1
                continue

            sLine = line.split(',')
            author_id = sLine[0]
            author_name = sLine[1]
            time = sLine[2][1:]
            event = sLine[3]
            repo_full_name = sLine[4].split('(')[0]
            ip = sLine[5][:-1]
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
            action = common.getSingleAction(self.index_name, id, body)
            actions += action
            i += 1

        print(actions)
        self.esClient.safe_put_bulk(actions)
        f.close()


    def get_repos(self, org):
        client = GiteeClient(org, None, self.gitee_token)
        print(self.is_gitee_enterprise)
        if self.is_gitee_enterprise == "true":
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

    def getThreadFuncs(self, from_date):
        thread_func_args = {}
        files = []
        for file in glob.glob("*.csv"):
            print(file)
            files.append(file)
            # _thread.start_new_thread(writeGiteeDownDataByFile, (file, ))
        thread_func_args[self.writeGiteeDownDataByFile] = files
        return thread_func_args


    def getRepoThreadFuncs(self, from_date):
        thread_func_args = {}
        values = []
        for org in self.orgs:
            repos = self.get_repos(org)
            for repo in repos:
                values.append((org, repo['path']))

        thread_func_args[self.getEventFromRepo] = values
        return thread_func_args


    def getEventFromRepo(self, owner, repo):
        page = 1
        print("start  owner(%s) repo(%s) page=%d" % (
        owner, repo, page))
        client = GiteeClient(owner, repo, self.gitee_token)
        while 1:
            try:
                actions = ""
                response = client.events(page)

                events_data = common.getGenerator(response)
                print("owner(%s) repo(%s) envents data num=%s, page=%d" % (owner, repo, len(events_data), page))
                if len(events_data) == 0:
                    print("owner(%s) repo(%s) get event break " % (owner, repo))
                    break
                for e in events_data:
                    id = owner + "_" + repo + "_"

                    if e.get('id'):
                        id = id + str(e.get('id'))
                    if e.get('type'):
                        id = id + e.get('type')
                    action = common.getSingleAction(self.index_name, id,
                                                    e)
                    actions += action

                page += 1
                self.esClient.safe_put_bulk(actions)
            except ValueError as e:
                print("error=%s, page=%d", (e, page))
                page += 1
                continue
            except TypeError as e:
                print("error=%s, page=%d", (e, page))
                page += 1
                continue

        print("owner(%s) repo(%s) end page=%d" % (owner, repo, page))


    def run(self, from_date):
        # thread_func_args = self.getRepoThreadFuncs(from_date)
        # common.writeDataThread(thread_func_args)
        thread_func_args = self.getThreadFuncs(from_date)
        common.writeDataThread(thread_func_args)
        # writeGiteeDownDataByFile("仓库管理日志-2020-04-28_14_04_39.csv")
