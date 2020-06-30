
#!/usr/bin/env python

import os
import sys

import requests

import json

from datetime import datetime
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

    def run(self, from_date):
        repoNames = self.getRepoNames()
        actions = ""
        for repo in repoNames:
            action = self.getSWF(repo)
            # print(action)
            actions += action
        self.esClient.safe_put_bulk(actions)

    def getRepoNames(self):
        gitclient = GithubClient(self.org, "", self.github_authorization)
        repos = gitclient.getAllrepo()
        repoNames = []
        for rep in repos:
            repoNames.append(self.ensure_str(rep['name']))
        print("....get repoNames=", repoNames)
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
        r["swf_update_time"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+08:00')
        # r["swf_update_time"] = "2020-06-19T22:21:25+08:00"
        id = "swf_" + r["swf_update_time"] + r.get("full_name")
        action = common.getSingleAction(self.index_name, id, r)
        return action
