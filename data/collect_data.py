#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from bs4 import BeautifulSoup
import re
import git
import datetime
import json
import requests
import time
import yaml
from configparser import ConfigParser
from data import common
from data.common import ESClient
from data.gitee import Gitee
import pypistats
import traceback
from collect.gitee import GiteeClient


class CollectData(object):

    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.index_name_pypi = config.get('index_name_pypi')
        # self.index_name_code_all = config.get('index_name_code_all').split(',')
        # self.sigs_code_all = config.get('sigs_code_all').split(',')
        self.index_name_committers = config.get('index_name_committers')
        self.index_name_maillist = config.get('index_name_maillist')
        self.index_name_vpcdownload = config.get('index_name_vpcdownload')
        self.index_name_code_all = config.get('index_name_code_all')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        # self.org = config.get('github_org')
        self.esClient = ESClient(config)
        self.gitee = Gitee(config)
        self.org = config.get("org")
        self.sigs_dir = config.get('sigs_dir')
        self.sigs_url = config.get('sigs_url')
        self.index_name_sigs = config.get('index_name_sigs')
        self.is_gitee_enterprise=config.get("self.is_gitee_enterprise")
        self.gitee_token = config.get('gitee_token')
        self.sigs_source = config.get('sigs_source')
        self.headers = {'Content-Type': 'application/json'}
        self.headers["Authorization"] = config.get('authorization')
        self.pypi_orgs = None
        if 'pypi_orgs' in config:
            self.pypi_orgs = config.get('pypi_orgs').split(',')

    def run(self, time=None):
        if self.index_name_maillist:
            self.get_maillist_user()
        if self.index_name_committers:
            self.get_committers()
        if self.index_name_vpcdownload:
            self.get_donwlaod()
        if self.index_name_code_all:
            self.get_sigs_code_all()
        if self.index_name_sigs:
            self.get_sigs()
            self.get_sig_pr_issue()
        if self.pypi_orgs:
            startTime = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=60), "%Y-%m-%d")
            for sig in self.pypi_orgs:
                self.get_pypi_overall(startTime, sig)
                self.get_pypi_python_major(startTime, sig)
                self.get_pypi_python_minor(startTime, sig)
                self.get_pypi_system(startTime, sig)

    def untar(self, fname, dirs='./'):
        cmd = 'tar -zxvf %s -C %s' %(fname, dirs)
        res = os.popen(cmd)
        return res.read()

    def git_clone(self, url, dir):
        cmd = 'cd %s;git clone %s' % (dir, url)
        res = os.popen(cmd)
        return res.read()

    def code_check(self, clocpath, gitstatspath, gitresultpath):
        cmd = 'cd /home/git_stats/gitstats ;python3 gitstats.py %s %s' % (gitstatspath, gitresultpath)
        os.popen(cmd)

        cmd = 'cloc %s' % clocpath
        p = os.popen(cmd)
        res = p.read()
        return res

    def collect_data(self, htmlfile='', cloc=''):
        if htmlfile:
            soup = BeautifulSoup(open(htmlfile), 'html.parser')
            trs = soup.find_all('table')[0].findChildren('tr')

            for tr in trs:
                if tr:
                    a = tr.text.strip().split('\n')
                    print(a)

        if cloc:
            res = re.findall(r'SUM:\s*(\d+)\s*(\d+)\s*(\d+)\s*(\d+)', cloc)[0]
            sum_filrs = res[0]

            sum_code = res[-1]

    def safe_put_bulk(self, bulk_json, header=None, url=None):
        """Bulk items to a target index `url`. In case of UnicodeEncodeError,
        the bulk is encoded with iso-8859-1.

        :param url: target index where to bulk the items
        :param bulk_json: str representation of the items to upload
        """
        if not bulk_json:
            return
        _header = {
            "Content-Type": 'application/x-ndjson',
            'Authorization': self.authorization
        }
        if header:
            _header = header

        _url = self.url
        if url:
            _url = url

        try:
            res = requests.post(_url + "/_bulk", data=bulk_json,
                                headers=_header, verify=False)
            res.raise_for_status()
        except UnicodeEncodeError:

            # Related to body.encode('iso-8859-1'). mbox data
            bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
            res = requests.put(url, data=bulk_json, headers=headers)
            res.raise_for_status()

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""
        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData) + '\n'
        action += json.dumps(body) + '\n'
        return action

    def collect_code(self, repolist=[], date='2019-07-01'):
        res_all = {}

        # path = 'D:\\collect-code-clone\\mindspore\\'
        path = '/home/collect-code-clone/'
        if not os.path.exists(path):
            os.makedirs(path)

        for repourl in repolist:
            reL = []
            repo = repourl.split('/')[-1]
            gitpath = path + repo
            gc = git.Git(path)
            g = git.Git(gitpath)
            if not os.path.exists(gitpath):
                cmdclone = 'git clone %s.git' % repourl
                gc.execute(cmdclone)
            else:
                cmdpull = 'git pull'
                g.execute(cmdpull)
            datei = datetime.datetime.strptime(date, "%Y-%m-%d")

            dateii = datei

            while True:
                datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                  "%Y-%m-%d")
                # if datei == datenow:
                #     break
                # dateii = datei + datetime.timedelta(days=1)
                # logcmd = "git log --after=\"%s\" --before=\"%s\" --pretty=tformat: --numstat " % (datei, dateii)

                if dateii == datenow:
                    break
                dateii += datetime.timedelta(days=1)
                logcmd = "git log --after=\"%s\" --before=\"%s\" --pretty=tformat: --numstat " % (datei, dateii)
                res = g.execute(logcmd)

                # datei = dateii

                if res:
                    q1 = res.split('\n')
                    addall = 0
                    removall = 0
                    for q in q1:
                        add = q.split()[0]
                        remo = q.split()[1]
                        if add.isdigit():
                            addall += int(add)
                        if remo.isdigit():
                            removall += int(remo)
                else:
                    addall = 0
                    removall = 0
                # result = {'date': date, "add": addall, 'remove': removall, 'changes': addall - removall}

                # create_at = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%dT%H:%M:%S") + "+08:00"
                result = {'created_at': datetime.datetime.strftime(dateii, "%Y-%m-%d"), "add": addall,
                          'remove': removall, 'total': addall - removall}
                reL.append(result)
            res_all[repo] = reL
        return res_all

    def get_repos(self, org):
        client = GiteeClient(org, None, self.gitee_token)
        print(self.is_gitee_enterprise)
        if self.is_gitee_enterprise == "true":
            client = GiteeClient(org, None, self.gitee_token)
            org_data = common.getGenerator(client.enterprises())
        else:
            org_data = common.getGenerator(client.org())

        for org in org_data:
            print(org['path'])
        return org_data

    def get_sigs_code_all(self):
        for index in range(len(self.index_name_code_all)):
            # index_name = 'haoxiangyu_collect_code_12138'
            index_name = self.index_name_code_all[index]
            sig = self.sigs_code_all[index]
            with open('projects.json', 'r') as f:
                res = json.load(f)
                repos = res[sig]['git']
            res = self.collect_code(repos)
            # with open('D:/openeuler_code.txt', 'w') as f:
            #     f.write(json.dumps(res))
            # for r in res:
            #     for body in res[r]:
            #         # body = {'date': '2020-06-19', 'add': 2099, 'remove': 2515, 'total': -416}
            #         ID = r + '_' + body['date']
            #         data = getSingleAction(index_name, ID, body)
            #         safe_put_bulk(data)
            #         print(data)

            all_code = []

            for r in res:
                # res[r]['created_at']
                index = len(res[r])
                resfist = res[r]
                break
            for i in range(index):
                call = {}
                call['created_at'] = resfist[i]['created_at']
                call['add'] = 0
                call['remove'] = 0
                call['total'] = 0
                for r in res:
                    call['add'] += res[r][i]['add']
                    call['remove'] += res[r][i]['remove']
                    call['total'] += res[r][i]['total']
                all_code.append(call)

            # resall = json.dumps(all_code)
            # with open('D:/openeuler_code_all.txt', 'w') as f:
            #     f.write(resall)

            for body in all_code:
                ID = sig + '_all_' + body['created_at']
                data = self.getSingleAction(index_name, ID, body)
                self.safe_put_bulk(data)
                print(data)

    def get_totals(self, url, index_name, date, mactch, totalmark, id, down=False, commit=False):
        datei = datetime.datetime.strptime(date, "%Y-%m-%d")
        dateii = datei
        numList = []
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            if dateii == datenow+datetime.timedelta(days=1):
                break
            dateiise = dateii
            dateii += datetime.timedelta(days=1)
            stime = datetime.datetime.strftime(dateiise, "%Y-%m-%d")
            # data = '''{"size":9999,"query": {"bool": {"must": [{ "match": { "created_at":"%s" }}%s]}}}''' % (stime, mactch)
            if commit:
                commitdata = ''',"aggs": {
    "age_count": {
      "cardinality": {
        "field": "user_gitee_name.keyword"}}}'''
            else:
                commitdata = ''
            data = '''{"size":10000,
  "query": {
    "bool": {
      "filter":{
        "range":{
          "created_at":{
            "gte":"%sT00:00:00.000+0800",
            "lt":"%sT00:00:00.000+0800"
          }
        }
      }%s
    }
  }%s
}''' % (str(dateiise).split()[0], str(dateii).split()[0], mactch, commitdata)

            res = requests.get(url=url, headers=self.headers, verify=False, data=data)
            r = res.content
            re = json.loads(r)
            ind = re['hits']['hits']
            if not down:
                num = len(ind)
            else:
                if ind:
                    num = 0
                    for i in ind:
                        if 'sum_value' in i['_source']:
                            num += i['_source']['sum_value']
                else:
                    num = 0
            if commit:
                num = re["aggregations"]["age_count"]["value"]
            numList.append(num)

            body = {'created_at': stime, totalmark: 1, 'tatol_num': sum(numList)}
            ID = id + stime
            data = self.getSingleAction(index_name, ID, body)
            self.safe_put_bulk(data)
            print(data)
            print(numList)

    def get_committers(self):
        url = self.url + '/' + self.index_name_committers + '/_search'
        index_name = self.index_name_committers
        date = '2019-12-01'
        macth = ',"must": [ { "match": { "is_committer": 1 }} ]'
        totalmark = 'is_committers_tatol_num'
        id = 'hao_committers_tatol_'
        self.get_totals(url, index_name, date, macth, totalmark, id, commit=True)

    def get_maillist_user(self):
        url = self.url + '/' + self.index_name_maillist + '/_search'
        index_name = self.index_name_maillist
        date = "2019-10-25"
        macth = ''
        totalmark = 'is_maillist_user_tatol_num'
        id = 'hao_maillist_user_tatol_'
        self.get_totals(url, index_name, date, macth, totalmark, id)

    def get_donwlaod(self):
        url = self.url + '/' + self.index_name_vpcdownload + '/_search'
        index_name = self.index_name_vpcdownload
        date = '2020-01-01'
        macth = ''
        totalmark = 'is_vpc_donwlaod_gb_tatol_num'
        id = 'hao_vpc_donwlaod_gb_tatol_'
        self.get_totals(url, index_name, date, macth, totalmark, id, down=True)

    def get_pypi_overall(self, start_date, package):
        datei = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            if datei == datenow + datetime.timedelta(days=1):
                break
            overall = pypistats.overall(package, start_date=datei.strftime("%Y-%m-%d"),
                                        end_date=datei.strftime("%Y-%m-%d"), format="rst")
            With_Mirrors = self.get_data_num_pypi(overall, "with_mirrors")
            Without_Mirrors = self.get_data_num_pypi(overall, "without_mirrors")
            Total = self.get_data_num_pypi(overall, "Total", True)
            dataw = {"With_Mirrors": With_Mirrors, "Without_Mirrors": Without_Mirrors, "Total": Total,
                     "package": package+"_overall_download", "created_at": datei.strftime("%Y-%m-%d")+"T23:00:00+08:00"}
            print(dataw)
            ID = package + "_pypi_overall_" + datei.strftime("%Y-%m-%d")
            data = self.getSingleAction(self.index_name_pypi, ID, dataw)
            self.safe_put_bulk(data)
            datei += datetime.timedelta(days=1)

    def get_pypi_python_major(self, start_date, package):
        datei = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            if datei == datenow + datetime.timedelta(days=1):
                break
            major = pypistats.python_major(package, start_date=datei.strftime("%Y-%m-%d"),
                                           end_date=datei.strftime("%Y-%m-%d"), format="rst")
            Python3 = self.get_data_num_pypi(major, "3")
            null = self.get_data_num_pypi(major, "null")
            Total = self.get_data_num_pypi(major, "Total", True)
            dataw = {"Python3": Python3, "Others(null)": null, "Total": Total,
                     "package": package+"_python_major_download", "created_at": datei.strftime("%Y-%m-%d")+"T23:00:00+08:00"}
            print(dataw)
            ID = package+"_pypi_python_major_" + datei.strftime("%Y-%m-%d")
            data = self.getSingleAction(self.index_name_pypi, ID, dataw)
            self.safe_put_bulk(data)
            datei += datetime.timedelta(days=1)

    def get_pypi_python_minor(self, start_date, package):
        datei = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            if datei == datenow + datetime.timedelta(days=1):
                break
            minor = pypistats.python_minor(package, start_date=datei.strftime("%Y-%m-%d"),
                                           end_date=datei.strftime("%Y-%m-%d"), format="rst")
            Python37 = self.get_data_num_pypi(minor, "3\.7")
            null = self.get_data_num_pypi(minor, "null")
            Total = self.get_data_num_pypi(minor, "Total", True)
            dataw = {"Python37": Python37, "Others(null)": null, "Total": Total,
                     "package": package+"_python_minor_download", "created_at": datei.strftime("%Y-%m-%d")+"T23:00:00+08:00"}
            print(dataw)
            ID = package+"_pypi_python_minor_" + datei.strftime("%Y-%m-%d")
            data = self.getSingleAction(self.index_name_pypi, ID, dataw)
            self.safe_put_bulk(data)
            datei += datetime.timedelta(days=1)

    def get_pypi_system(self, start_date, package):
        datei = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            if datei == datenow + datetime.timedelta(days=1):
                break
            system = pypistats.system(package, start_date=datei.strftime("%Y-%m-%d"),
                                      end_date=datei.strftime("%Y-%m-%d"), format="rst")
            Windows = self.get_data_num_pypi(system, "Windows")
            Linux = self.get_data_num_pypi(system, "Linux")
            null = self.get_data_num_pypi(system, "null")
            Total = self.get_data_num_pypi(system, "Total", True)
            dataw = {"Windows": Windows, "Others(null)": null, "Linux": Linux, "Total": Total,
                     "package": package+"_system_download", "created_at": datei.strftime("%Y-%m-%d")+"T23:00:00+08:00"}
            print(dataw)
            ID = package+"_pypi_system_" + datei.strftime("%Y-%m-%d")
            data = self.getSingleAction(self.index_name_pypi, ID, dataw)
            self.safe_put_bulk(data)
            datei += datetime.timedelta(days=1)

    def get_data_num_pypi(self, data, mark, bm=False):
        if "%" in data:
            if bm:
                content = re.search(mark + '\s+(\d+)', data)
                num = content.group(1) if content else 0
            else:
                content = re.search(mark + '\s+\d+\.\d+%\s+(\d+)', data)
                num = content.group(1) if content else 0
        else:
            content = re.search(mark + '\s+(\d+)', data)
            num = content.group(1) if content else 0
        return int(num)

    def get_sigs(self):

        path = self.sigs_dir
        url = self.sigs_url

        if not os.path.exists(path):
            os.makedirs(path)

        gitpath = path + 'community'
        if not os.path.exists(gitpath):
            cmdclone = 'cd %s;git clone %s' % (path, url)
            os.system(cmdclone)
        else:
            cmdpull = 'cd %s;git pull' % path
            os.system(cmdpull)

        # sigs
        self.gitee.getEnterpriseUser()
        self.gitee.internalUsers = self.gitee.getItselfUsers(self.gitee.internal_users)
        sig_dir = gitpath + '/sig'
        dirs = os.walk(sig_dir).__next__()[1]
        for dir in dirs:
            repo_path = sig_dir + '/' + dir
            cmdlog = 'cd %s;git log -p README.md' % repo_path
            log = os.popen(cmdlog, 'r').read()

            loglist = log.split('\n')
            n = 0
            rs = []
            for index in range(len(loglist)):
                if re.search(r'^commit .*', loglist[index]):
                    rs.append('\n'.join(loglist[n:index]))
                    n = index
            rs.append('\n'.join(loglist[n:]))
            times = None
            for r in rs:
                if re.search(r'--- .*null\n\+\+\+ .*/README.md', r):
                    date = re.search(r'Date: (.*)\n', r).group(1)
                    time_struct = time.strptime(date.strip()[:-6], '%a %b %d %H:%M:%S %Y')
                    times = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time_struct)
                    break

            cmdowner = 'cd %s;git log -p OWNERS' % repo_path
            owners = os.popen(cmdowner, 'r').read()
            ownerslist = owners.split('\n')
            n2 = 0
            rs2 = []
            for index in range(len(ownerslist)):
                if re.search(r'^commit .*', ownerslist[index]):
                    rs2.append('\n'.join(ownerslist[n2:index]))
                    n2 = index
            rs2.append('\n'.join(ownerslist[n2:]))

            onwer_file = repo_path + '/' + 'OWNERS'
            onwers = yaml.load_all(open(onwer_file)).__next__()['maintainers']
            sig_file = sig_dir + '/' + 'sigs.yaml'
            data = yaml.load_all(open(sig_file)).__next__()['sigs']
            datas = ''
            try:
                for onwer in onwers:
                    times_onwer = None
                    for r in rs2:
                        if re.search(r'\+\s*-\s*%s' % onwer, r):
                            date = re.search(r'Date:\s*(.*)\n', r).group(1)
                            time_struct = time.strptime(date.strip()[:-6], '%a %b %d %H:%M:%S %Y')
                            times_onwer = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time_struct)

                    repo_mark = True
                    for d in data:
                        if d['name'] == dir:
                            repos = d['repositories']
                            for repo in repos:
                                ID = self.org + '_' + dir + '_' + repo + '_' + onwer
                                dataw = {"sig_name": dir,
                                         "repo_name": repo,
                                         "committer": onwer,
                                         "created_at": times,
                                         "committer_time": times_onwer,
                                         "is_sig_repo_committer": 1}
                                userExtra = self.gitee.getUserInfo(onwer)
                                dataw.update(userExtra)
                                datar = self.getSingleAction(self.index_name_sigs, ID, dataw)
                                datas += datar
                                repo_mark = False

                    if repo_mark:
                        ID = self.org + '_' + dir + '_null_' + onwer
                        dataw = {"sig_name": dir,
                                 "repo_name": None,
                                 "committer": onwer,
                                 "created_at": times,
                                 "committer_time": times_onwer,
                                 "is_sig_repo_committer": 1}
                        userExtra = self.gitee.getUserInfo(onwer)
                        dataw.update(userExtra)
                        datar = self.getSingleAction(self.index_name_sigs, ID, dataw)
                        datas += datar

                self.safe_put_bulk(datas)
                print("this sig done: %s" % dir)
                time.sleep(1)
            except:
                print(traceback.format_exc())

    def get_sig_pr_issue(self):

        path = self.sigs_dir
        url = self.sigs_url

        if not os.path.exists(path):
            os.makedirs(path)

        gitpath = path + 'community'
        if not os.path.exists(gitpath):
            cmdclone = 'cd %s;git clone %s' % (path, url)
            os.system(cmdclone)
        else:
            cmdpull = 'cd %s;git pull' % path
            os.system(cmdpull)

        sigs_data = yaml.load_all(open(gitpath + '/sig/sigs.yaml')).__next__()

        # pr
        url = self.url + '/' + self.sigs_source + '/_search'
        datei = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        dateii = datei
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            if dateii == datenow + datetime.timedelta(days=1):
                break
            dateiise = dateii
            dateii += datetime.timedelta(days=1)
            data = '''{"size":10000,
              "query": {
                "bool": {
                  "filter":{
                    "range":{
                      "created_at":{
                        "gte":"%sT00:00:00.000+0800",
                        "lt":"%sT00:00:00.000+0800"
                      }
                    }
                  },"must": [{ "match": { "is_gitee_pull_request":1}}]
                }
              }
            }''' % (str(dateiise).split()[0], str(dateii).split()[0])
            res = requests.get(url=url, headers=self.headers, verify=False, data=data)
            r = res.content
            re = json.loads(r)
            ind = re['hits']['hits']
            for i in ind:
                repo = i['_source']['gitee_repo'].split('/')[-2] + '/' + i['_source']['gitee_repo'].split('/')[-1]
                for sig in sigs_data['sigs']:
                    if repo in sig['repositories']:
                        body = i['_source']
                        body['is_sig_pr'] = 1
                        body['sig_name'] = sig['name']
                        ID = sig['name'] + i['_id']
                        if "pull_state" in body:
                            if body['pull_state'] == "merged":
                                body['is_pull_merged'] = 1
                            if body['pull_state'] == "closed":
                                body['is_pull_closed'] = 1
                            if body['pull_state'] == "open":
                                body['is_pull_open'] = 1
                        data = self.getSingleAction(self.index_name_sigs, ID, body)
                        self.safe_put_bulk(data)
                        print("data:%s" % data)

        # issue
        url = self.url + '/' + self.sigs_source + '/_search'
        datei = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        dateii = datei
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            if dateii == datenow + datetime.timedelta(days=1):
                break
            dateiise = dateii
            dateii += datetime.timedelta(days=1)
            data = '''{"size":10000,
                      "query": {
                        "bool": {
                          "filter":{
                            "range":{
                              "created_at":{
                                "gte":"%sT00:00:00.000+0800",
                                "lt":"%sT00:00:00.000+0800"
                              }
                            }
                          },"must": [{ "match": { "is_gitee_issue":1}}]
                        }
                      }
                    }''' % (str(dateiise).split()[0], str(dateii).split()[0])
            res = requests.get(url=url, headers=self.headers, verify=False, data=data)
            r = res.content
            re = json.loads(r)
            ind = re['hits']['hits']
            for i in ind:
                repo = i['_source']['gitee_repo'].split('/')[-2] + '/' + i['_source']['gitee_repo'].split('/')[-1]
                for sig in sigs_data['sigs']:
                    if repo.strip() in sig['repositories']:
                        body = i['_source']
                        body['is_sig_issue'] = 1
                        body['sig_name'] = sig['name']
                        ID = sig['name'] + i['_id']
                        if "issue_state" in body:
                            if body['issue_state'] == "closed":
                                body['is_issue_closed'] = 1
                            if body['issue_state'] == "open":
                                body['is_issue_open'] = 1
                        data = self.getSingleAction(self.index_name_sigs, ID, body)
                        self.safe_put_bulk(data)
                        print("data:%s" % data)

