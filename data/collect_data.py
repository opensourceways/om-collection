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
from configparser import ConfigParser
from data.common import ESClient
from data.gitee import Gitee
import pypistats


class CollectData(object):

    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.index_name_pypi = config.get('index_name_pypi')
        # self.index_name_pypi_ocerall = config.get('index_name_pypi_ocerall')
        # self.index_name_pypi_major = config.get('index_name_pypi_major')
        # self.index_name_pypi_minor = config.get('index_name_pypi_minor')
        # self.index_name_pypi_system = config.get('index_name_pypi_system')
        self.index_name_code_all = config.get('index_name_code_all').split(',')
        self.sigs_code_all = config.get('sigs_code_all').split(',')
        self.index_name_committers = config.get('index_name_committers')
        self.index_name_mailist = config.get('index_name_mailist')
        self.index_name_vpcdownload = config.get('index_name_vpcdownload')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        # self.org = config.get('github_org')
        self.esClient = ESClient(config)
        # self.gitee = Gitee(config)
        self.headers = {'Content-Type': 'application/json'}
        self.headers["Authorization"] = config.get('authorization')

    def run(self, time=None):
        self.get_maillist_user()
        self.get_committers()
        self.get_donwlaod()

        # self.get_pypi_overall("2020-04-01", "mindspore")
        # self.get_pypi_python_major("2020-04-01", "mindspore")
        # self.get_pypi_python_minor("2020-04-01", "mindspore")
        # self.get_pypi_system("2020-04-01", "mindspore")
        # self.get_pypi_overall("2020-04-02", "mindspore-ascend")
        # self.get_pypi_python_major("2020-04-02", "mindspore-ascend")
        # self.get_pypi_python_minor("2020-04-02", "mindspore-ascend")
        # self.get_pypi_system("2020-04-02", "mindspore-ascend")
        # self.get_pypi_overall("2020-04-02", "mindspore-gpu")
        # self.get_pypi_python_major("2020-04-02", "mindspore-gpu")
        # self.get_pypi_python_minor("2020-04-02", "mindspore-gpu")
        # self.get_pypi_system("2020-04-02", "mindspore-gpu")

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
        url = self.url + '/' + self.index_name_mailist + '/_search'
        index_name = self.index_name_mailist
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



if __name__ == '__main__':
    alll = []
    def test01(a,b):
        down = False
        commit = True
        numList = []
        url = "https://119.8.111.61:9200/openeuler_sig_20200629/_search"
        headers = {'Content-Type': 'application/json',"Authorization": "Basic YWRtaW46b3BlbmV1bGVyQDEyMw=="}

        data = '''{"size":10000,
          "query": {
            "bool": {
              "filter":{
                "range":{
                  "created_at":{
                    "gte":"2020-01-08T%s.000+0800",
                    "lt":"2020-01-08T%s.000+0800"
                  }
                }
              },"must": [ { "match": { "is_committer": 1 }} ]
            }
          },"aggs": {
    "age_count": {
      "cardinality": {
        "field": "user_gitee_name.keyword"}}
        }''' %(a, b)

        res = requests.get(url=url, headers=headers, verify=False, data=data)
        r = res.content
        re = json.loads(r)
        ind = re['hits']['hits']
        print(len(ind))
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
            if num:
                userl = []
                for i in ind:
                    if 'user_gitee_name' in i['_source'] and i['_source']['user_gitee_name'] not in userl:
                        userl.append(i['_source']['user_gitee_name'])
                num = len(userl)
        numList.append(num)

        userl.sort()
        print(numList)
        print(userl)
        print('----------------------')
        global alll
        alll += userl

    # test01('11:50:00', '12:00:00')
    # test01('15:40:00', '15:50:00')
    # test01('15:50:00', '16:00:00')
    # test01('17:55:00', '18:05:00')
    # test01('18:30:00', '18:40:00')
    #
    # alll.sort()
    # print(alll)
    # print(len(alll))
    datei = datetime.datetime.strptime('2020-01-01', "%Y-%m-%d")
    dateii = datei
    while True:
        datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                             "%Y-%m-%d")
        if dateii == datenow + datetime.timedelta(days=1):
            break
        dateiise = dateii
        dateii += datetime.timedelta(days=1)
        stime = datetime.datetime.strftime(dateiise, "%Y-%m-%d")
        test01('00:00:00','23:59:59')