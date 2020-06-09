
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
        self.esClient = ESClient(config)
        self.esClient.initLocationGeoIPIndex()
        self.comitter_time = {}

    def run(self, from_date):
        self.getCommiterAddedTime()
        self.prepare_list()

    def gitClone(self):
        print("............start to git clone ")
        git.Git("/root/zj/george/obs/george/").clone(self.sig_link)


    def getCommiterAddedTime(self):
        client = GiteeClient("openeuler", "community", self.gitee_token)
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
        if fs[0] != "sig":
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
        actions = ""
        f = open('community/sig/sigs.yaml')
        y = yaml.load_all(f)

        t = datetime.now().strftime('%Y-%m-%d')
        for data in y:
            for sig in data['sigs']:
                print(sig['name'])
                filename = "community/sig/" + sig['name'] + "/OWNERS"
                ownerFile = open(filename)
                ownerFileData = yaml.load_all(ownerFile)
                maintainers = []
                for d in ownerFileData:
                    for maintainer in d['maintainers']:
                        maintainers.append(maintainer)
                ownerFile.close()

                for repo in sig['repositories']:
                    for m in maintainers:
                        print("get commiter (%s) created time is %s" % (
                                sig['name'] + "_" + m,
                                self.comitter_time.get(sig['name'] + "_" + m)))
                        doc = {
                             "sig_name": sig['name'],
                             "repo_name": repo,
                             "user_gitee_name": m,
                             "created_at": self.comitter_time.get(sig['name'] + "_" + m),
                        }
                        id = m + "_" + sig['name'] + "_" + repo
                        action = common.getSingleAction(self.index_name, id,
                                                        doc)
                        actions += action
        # self.esClient.safe_put_bulk(actions)
        f.close()
