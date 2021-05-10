#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
from urllib.parse import quote
from wsgiref import headers

import yaml
from bs4 import BeautifulSoup
from urllib3.connectionpool import xrange

from data import common
from data.gitee import Gitee

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
import git
import datetime
import json
import requests
from data.common import ESClient
import xlwt


class GitCommit(object):

    def __init__(self, config=None):
        self.config = config
        self.default_headers = {
            'Content-Type': 'application/json'
        }
        self.index_name = config.get('index_name').split(',')
        self.sigs_code_all = config.get('sigs_code_all').split(',')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.headers = {'Content-Type': 'application/json', "Authorization": config.get('authorization')}
        self.huawei_users = config.get('huawei_users')
        self.authordomain_addr = config.get('authordomain_addr')
        self.projects_repo = config.get('projects_repo')
        self.repo_scope = config.get('repo_scope')
        self.username = config.get('username')
        self.password = config.get('password')

        self.data_yaml_url = config.get('data_yaml_url')
        self.data_yaml_path = config.get('data_yaml_path')
        self.company_yaml_url = config.get('company_yaml_url')
        self.company_yaml_path = config.get('company_yaml_path')

    def run(self, from_date=None):
        self.domain_companies = self.getInfoFromCompany()['domains_company_list']
        self.alias_companies = self.getInfoFromCompany()['aliases_company_list']
        self.users = self.getInfoFromCompany()["datas"]["users"]
        if self.repo_scope != None:
            self.reposcope = self.get_repo_scope(self.repo_scope)
        self.get_sigs_code_all(from_date, self.projects_repo)

    def untar(self, fname, dirs='./'):
        cmd = 'tar -zxvf %s -C %s' % (fname, dirs)
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

            # pdb.set_trace()
            res.raise_for_status()
        except UnicodeEncodeError:

            # Related to body.encode('iso-8859-1'). mbox data
            bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
            res = requests.put(url, data=bulk_json, headers=headers)
            res.raise_for_status()

    def getItselfUsers(self, filename="huaweiusers"):
        file_path = os.path.abspath('.') + '/config/' + filename
        f = open(file_path, 'r')
        users = []
        for line in f.readlines():
            if line != "\n":
                users.append(line.split('\n')[0])
        # print(users)
        # print(len(users))
        return users

    def getauthordomain_addr(self, filename="gauthordomain_addr"):
        file_path = os.path.abspath('.') + '/config/' + filename
        with open(file_path, 'r', encoding='utf-8') as f:
            dic = []
            for line in f.readlines():
                line = line.strip('\n')
                b = line.split(' ')
                dic.append(b)

        dic = dict(dic)
        # print(dic)
        return dic

    def get_repo_scope(self, filename="repo_scope"):
        file_path = os.path.abspath('.') + '/config/' + filename
        with open(file_path, 'r', encoding='utf-8') as f:
            dic = []
            for line in f.readlines():
                line = line.strip('\n')
                b = line.split(':')
                dic.append(b)

        dic = dict(dic)
        # print(dic)
        return dic

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""
        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData) + '\n'
        action += json.dumps(body) + '\n'
        return action

    def collect_code(self, from_date, repolist=[], project=None):

        path = '/home/collect-code-clone/' + project + '/'
        if not os.path.exists(path):
            os.makedirs(path)
        reL = []
        flag = 1
        for repourl in repolist:

            repo = repourl.split('/')[-1]

            usr = self.username  # transform normal string into origin string
            pwd = quote(self.password)

            clone_url = 'https://' + usr + ':' + pwd + '@gitee.com/' + project + '/' + repo
            gitpath = path + repo
            gc = git.Git(path)
            g = git.Git(gitpath)
            # print(gitpath)

            if flag == 1:
                conf = 'git config --global core.compression -1'
                conf2 = 'git config --global http.postBuffer 1048576000'
                gc.execute(conf, shell=True)
                gc.execute(conf2, shell=True)
                flag = 2
            if not os.path.exists(gitpath):
                cmdclone = 'git clone %s.git' % clone_url

                try:
                    gc.execute(cmdclone, shell=True)
                except  Exception as e:

                    print('There is the Exception, which has no permisstion to clone: ', e.__class__)
                    continue
            else:
                setbrunch = 'git branch --set-upstream-to=origin/master master'
                g.execute(setbrunch, shell=True)
                cmdpull = 'git pull'
                try:
                    g.execute(cmdpull, shell=True)
                except:
                    print('pull error')
            datei = datetime.datetime.strptime(datetime.datetime.strftime(from_date, "%Y-%m-%d"),
                                               "%Y-%m-%d")
            dateii = datei
            while True:  # pull, parse, assemble commit records
                datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                     "%Y-%m-%d")
                if dateii == datenow:
                    break
                dateii += datetime.timedelta(days=3)
                if dateii > datenow:
                    dateii = datenow

                logcmd_no_merge = "git log --after=\"%s\" --before=\"%s\" --shortstat --pretty=\"%%an,%%ae,%%ad,%%H\" --no-merges " % (
                    datei, dateii)

                logcmd_merge = "git log --after=\"%s\" --before=\"%s\" --shortstat --pretty=\"%%an,%%ae,%%ad,%%H\" --merges " % (
                    datei, dateii)
                no_merge_log = g.execute(logcmd_no_merge, shell=True)
                merge_log = g.execute(logcmd_merge, shell=True)

                no_merged_commit = self.parse_commit(no_merge_log, datei, repourl, False)
                merged_commit = self.parse_commit(merge_log, datei, repourl, True)

                re.extend(merged_commit)
                reL.extend(no_merged_commit)

                datei = dateii
        return reL


    def parse_commit(self, log, log_date, repourl, is_merged):
        results = []
        result = {}

        if not log:
            return results

        repo = repourl.split('/')[-1]

        log = log.replace("\n\n", "@@")
        commit_logs = log.split('\n')

        if is_merged:
            for commit_log in commit_logs:
                result = self.parse_commit_log(commit_log, log_date)
                result['project'] = repo
                result['repo'] = repourl
                result['is_merged'] = 1
                results.append(result)

        else:
            for commit_log in commit_logs:
                line1 = commit_log.split('@@')[0]

                result = self.parse_commit_log(line1, log_date)
                result['project'] = repo
                result['repo'] = repourl

                file_changed = 0
                lines_added = 0
                lines_removed = 0
                result['project'] = repo
                result['repo'] = repourl
                result['is_merged'] = 0

                if len(commit_log.split('@@')) == 1:
                    results.append(result)
                    continue
                line2 = commit_log.split('@@')[1]

                file_pos = line2.find("file")
                add_pos = line2.find("insertion")
                del_pos = line2.find("deletion")

                if add_pos != -1:
                    lines_added_str = line2[int(line2.find(",") + 1):int(add_pos)]
                    lines_added = int(lines_added_str.strip())
                if del_pos != -1:
                    lines_removed_str = line2[int(self.find_last(line2, ",") + 1):int(del_pos)]
                    lines_removed = int(lines_removed_str.strip())
                if file_pos != -1:
                    file_changed = int(line2[:file_pos].strip())

                result['file_changed'] = file_changed
                result['add'] = lines_added
                result['remove'] = lines_removed
                result['total'] = lines_added + lines_removed

                results.append(result)

        return results

    def parse_commit_log(self, commit_log, log_date):
        split_list = commit_log.split(",")

        author = split_list[0]
        email = split_list[1]
        date_str = log_date.strftime("%Y-%m-%d")
        time_str = split_list[2].split()[3]

        # there are two dilemma in commit log: has time zone and has no time zone, such as  'Tue Jan 7 18:09:19 2020'  and 'Tue Feb 2 17:00:42 2021 +0800'. Give two solutions for them.
        try:
            time_zone = split_list[2].split()[5]
        except IndexError:
            time_zone = None
        # search the company of author then write into dict
        company_name = self.find_company(author, email)

        # if ':' not in time_str:
        #     continue
        if time_zone:
            preciseness_time_str = date_str + "T" + time_str + time_zone
        else:
            preciseness_time_str = date_str + "T" + time_str
        commit_id = split_list[- 1]

        result = {'created_at': preciseness_time_str, "file_changed": 0, "add": 0,
                  'remove': 0, 'total': 0, 'author': author, 'company': company_name,
                  'email': email, 'commit_id': commit_id}

        return result

    def find_company(self, author, email):
        company_name = 'independent'

        for user in self.users:
            if email in user["emails"]:
                company_name = self.alias_companies.get(user["companies"][0]["company_name"])

        if self.domain_companies.get(email.split("@")[-1]):
            company_name = self.domain_companies.get(email.split("@")[-1])

        print(author, "'s company is ", company_name)
        return company_name

    def getInfoFromCompany(self):
        companyInfo = {}

        if self.data_yaml_url and self.company_yaml_url:
            cmd = 'wget -N %s' % self.data_yaml_url
            p = os.popen(cmd.replace('=', ''))
            p.read()


            # Test in windows without wget command
            # self.data_yaml_path = "data/data.yaml"
            # self.company_yaml_path = "data/company.yaml"

            datas = yaml.load_all(open(self.data_yaml_path, encoding='UTF-8')).__next__()

            cmd = 'wget -N %s' % self.company_yaml_url
            p = os.popen(cmd.replace('=', ''))
            p.read()
            companies = yaml.load_all(open(self.company_yaml_path, encoding='UTF-8')).__next__()
            p.close()

            domains_company_dict = {}
            aliases_company_dict = {}
            for company in companies['companies']:
                for domain in company['domains']:
                    domains_company_dict[domain] = company['company_name']

                for alias in company["aliases"]:
                    aliases_company_dict[alias] = company['company_name']

            companyInfo["domains_company_list"] = domains_company_dict
            companyInfo["aliases_company_list"] = aliases_company_dict
            companyInfo["datas"] = datas

        return companyInfo

    def find_addr_index(self, split_list):
        for sp_str in split_list:
            if '@' in sp_str:
                return split_list.index(sp_str)

    def get_author(self, split_list, addr_index):
        author = ''
        for item in range(addr_index):
            if item == addr_index - 1:
                author += split_list[item]
            else:
                author += split_list[item] + ' '
        return author

    def find_last(self, string, str):
        last_position = -1
        while True:
            position = string.find(str, last_position + 1)
            if position == -1:
                return last_position
            last_position = position

    def getFromDate(self, from_date, filters):
        if from_date is None:
            from_date = self.esClient.get_from_create_date(filters)
        else:
            from_date = common.str_to_datetime(from_date)
        return from_date

    def getProjectFilePath(self, filename):
        return os.path.abspath('.') + "\\" + filename + '.json'

    def get_sigs_code_all(self, from_date=None, filename="projects"):
        project_file_path = self.getProjectFilePath(filename)

        for index in range(len(self.index_name)):
            index_name = self.index_name[index]
            from_date = self.getFromDate(from_date, [
                {"name": "is_git_commit", "value": 1}])
            sig = self.sigs_code_all[index]
            # project_file_path = os.path.abspath('.') + '/config/' + filename + '.json'

            with open(project_file_path, 'r') as f:
                res = json.load(f)
                repos = res[sig]['git']
            res = self.collect_code(from_date, repos, sig)

            for body in res:
                ID = body['commit_id']
                data = self.getSingleAction(index_name, ID, body)
                self.safe_put_bulk(data)
        # if self.huawei_users != None:
        #     self.statisticCommit()

    def statisticCommit(self):
        from_d = "20200701"
        # 根据is_git_commit分组，然后统计每组的new_author_count数量
        self.esClient.setToltalCount(from_d, "new_author_count", field="is_git_commit")
        self.esClient.setToltalCount(from_d, "new_huawei_author_count", field="is_git_commit")
        # 根据author分组，然后统计每个author的add数量
        self.esClient.setToltalCountByAddCount(from_d, "add", field="author.keyword")
        self.esClient.setToltalCountByAddCount(from_d, "add", field="author.keyword", query_filter="is_huawei_author")
        # 按月统计编码占比
        self.esClient.setToltalCountByMonth(from_d, "add", field="author.keyword",
                                            query_filter="is_huawei_author_by_month")

    def write_data_to_excel(self, all_code):
        self.delete_old_file()
        wbk = xlwt.Workbook()
        sheet = wbk.add_sheet('Sheet1', cell_overwrite_ok=True)
        tb_head = [
            u'created date',
            u'add',
            u'remove',
            u'author',
            u'email',
            u'project',
            u'repo',
            u'commit_id'
        ]
        export_info = ["created_at", "add", "remove", "author", "email", "project", "repo", "commit_id"]
        sheet.col(0).width = 4500
        sheet.col(3).width = 5000
        sheet.col(4).width = 6600
        sheet.col(5).width = 3500
        sheet.col(6).width = 10000
        sheet.col(7).width = 10500
        for i, item in enumerate(tb_head):
            sheet.write(0, i, item, self.style(sheet))

        # 遍历result中的每个元素。
        for i in xrange(len(all_code)):
            column = 0
            for k in all_code[i]:
                if k in export_info:
                    sheet.write(i + 1, column, all_code[i][k])
                    column += 1
        datei = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M%S")
        wbk.save("gitee_commit_info" + datei + '.xls')

    def style(self, sheet):
        # 初始化表头样式
        style = xlwt.XFStyle()
        # 设置单元格内字体样式
        font = xlwt.Font()
        font.bold = True
        font.height = 220
        font.name = 'Times New Roman'
        style.font = font
        pattern = xlwt.Pattern()
        pattern.pattern = xlwt.Pattern.SOLID_PATTERN
        pattern.pattern_fore_colour = 22
        style.pattern = pattern
        return style

    def delete_old_file(self):
        dirPath = os.path.abspath('.')
        os.chdir(dirPath)
        filelist = os.listdir(dirPath)
        for filename in filelist:
            print(filename)
            if "gitee_commit_info" in filename:
                os.remove(dirPath + '/' + filename)

    def get_totals(self, url, index_name, date, mactch, totalmark, id, down=False, commit=False):
        datei = datetime.datetime.strptime(date, "%Y-%m-%d")
        dateii = datei
        numList = []
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            if dateii == datenow + datetime.timedelta(days=1):
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
