#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import yaml
from data import common
import git
import datetime
import json
from data.common import ESClient
import base64

os.environ["GIT_PYTHON_REFRESH"] = "quiet"


class GitCommit(object):

    def __init__(self, config=None):
        self.config = config
        self.default_headers = {
            'Content-Type': 'application/json'
        }
        self.from_date = config.get("from_data")
        self.index_name = config.get('index_name').split(',')
        self.sigs_code_all = config.get('sigs_code_all').split(',')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.headers = {'Content-Type': 'application/json', "Authorization": config.get('authorization')}
        self.projects_repo = config.get('projects_repo')

        self.username = config.get('username')
        self.password = config.get('password')

        self.data_yaml_url = config.get('data_yaml_url')
        self.data_yaml_path = config.get('data_yaml_path')
        self.company_yaml_url = config.get('company_yaml_url')
        self.company_yaml_path = config.get('company_yaml_path')
        self.is_fetch_all_branches = config.get("is_fetch_all_branches")

    def run(self, startTime):

        if isinstance(self.from_date, str):
            from_date = self.getFromDate(self.from_date, [{"name": "is_git_commit", "value": 1}])
        else:
            from_date = self.from_date

        print(f"*************Begin to collect commits.  From :{from_date}***********")
        self.domain_companies = self.getInfoFromCompany()['domains_company_list']
        self.alias_companies = self.getInfoFromCompany()['aliases_company_list']
        self.users = self.getInfoFromCompany()["datas"]["users"]
        self.get_sigs_code_all(from_date, self.projects_repo)
        print(f"*********************Finish Collection***************************")

    def git_clone(self, url, dir):
        cmd = 'cd %s;git clone %s' % (dir, url)
        res = os.popen(cmd)
        return res.read()

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""
        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData) + '\n'
        action += json.dumps(body) + '\n'
        return action

    def get_sigs_code_all(self, from_date=None, filename="projects"):
        project_file_path = self.getProjectFilePath(filename)

        for index in range(len(self.index_name)):
            index_name = self.index_name[index]
            sig = self.sigs_code_all[index]
            with open(project_file_path, 'r') as f:
                res = json.load(f)
                repos = res[sig]['git']

            self.collect_code(from_date, index_name, repos, sig)

            if index == len(self.index_name) - 1:
                now = datetime.datetime.now()
                self.from_date = datetime.datetime(year=now.year, month=now.month, day=now.day - 1)

        print(f"Collected {len(repos)} repositories for this project")

    def collect_code(self, from_date, index_name, repourl_list, project=None):

        path = '/home/collect-code-clone/' + project + '/'
        if not os.path.exists(path):
            os.makedirs(path)

        for repourl in repourl_list:
            repo = repourl.split("/")[-1]
            g = self.pull_Repo_To_Local(repourl, project, path)

            # Get commits for each branch
            repo_commit_list = self.fetch_commit_log_from_repo(from_date, g, repourl)

            # store a single repo data into ES
            action = ''
            for commit in repo_commit_list:
                ID = commit["commit_id"]
                commit_str = self.getSingleAction(index_name, ID, commit)
                action += commit_str
            print(f"Start to store {repo} data to ES...")
            self.esClient.safe_put_bulk(action)

            print(repo, f" has {len(repo_commit_list)} commits. All has been collected into ES.")

    def get_repo_branches(self, g):
        branch_names = []
        text = g.execute("git branch -a", shell=True)
        branch_list = text.split("\n")

        master_index = 0
        for i in range(len(branch_list)):
            if branch_list[i].find("->") != -1:
                master_index = i
                break
        branches = branch_list[master_index + 1:][::-1]

        for branch in branches:
            branch_name = branch.split("/")[-1]
            branch_names.append(branch_name)
        return branch_names

    def push_repo_data_into_es(self, index_name, repo_data_list, repo):
        action = ''
        for commit in repo_data_list:
            ID = commit["commit_id"]
            commit_log = self.getSingleAction(index_name, ID, commit)
            action += commit_log
        self.esClient.safe_put_bulk(action)
        print(repo, " data has stored into ES")

    def pull_Repo_To_Local(self, repourl, project, path):
        repo = repourl.split("/")[-1]
        website = repourl.split("/")[2]
        username = base64.b64decode(self.username).decode()
        passwd = base64.b64decode(self.password).decode()

        clone_url = 'https://' + website + '/' + project + '/' + repo
        if username and passwd:
            clone_url = 'https://' + username + ':' + passwd + '@' + website + '/' + project + '/' + repo
        gitpath = path + repo
        gc = git.Git(path)
        g = git.Git(gitpath)

        flag = 1
        if flag == 1:
            conf = 'git config --global core.compression -1'
            conf2 = 'git config --global http.postBuffer 1048576000'
            gc.execute(conf, shell=True)
            gc.execute(conf2, shell=True)
            flag == 2

        # clone the repos to local
        if not os.path.exists(gitpath):
            cmdclone = 'git clone %s.git' % clone_url
            try:
                gc.execute(cmdclone, shell=True)
            except  Exception as e:
                print(f'Occur error when clone repository: {repo}. Error Name is: ', e.__class__)
        else:
            cmdpull = "git pull"
            try:
                g.execute(cmdpull, shell=True)
            except:
                print(f'{repo} pull error')

        return g

    def fetch_commit_log_from_repo(self, from_date, g, repourl):
        repo_commit_list = []
        repo = repourl.split("/")[-1]

        if self.is_fetch_all_branches == "True":
            branch_names = self.get_repo_branches(g)  # ensure branch name, then get its commits.
        else:
            branch_names = [g.execute(f"git branch", shell=True)]

        for branch_name in branch_names:
            try:
                g.execute(f"git checkout -f {branch_name}", shell=True)
                branch_text = g.execute(f"git branch", shell=True)
                current_branch_name = self.getCurrentBranchName(branch_text)
            except Exception as e:
                print(repr(e))

            branch_commits = []

            datei = datetime.datetime.strptime(datetime.datetime.strftime(from_date, "%Y-%m-%d"), "%Y-%m-%d")
            dateii = datei

            while True:  # pull, parse, assemble commit records
                datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                     "%Y-%m-%d")
                if dateii == datenow:
                    break
                dateii += datetime.timedelta(days=3)
                if dateii > datenow:
                    dateii = datenow

                logcmd_no_merge = "git log --after=\"%s\" --before=\"%s\" --shortstat --pretty=\"%%an;%%ae;%%ad;%%H\" --no-merges " % (
                    datei, dateii)
                logcmd_merge = "git log --after=\"%s\" --before=\"%s\" --shortstat --pretty=\"%%an;%%ae;%%ad;%%H\" --merges " % (
                    datei, dateii)

                no_merge_log = ''
                try:
                    no_merge_log = g.execute(logcmd_no_merge, shell=True)
                except:
                    pass

                merge_log = ''
                try:
                    merge_log = g.execute(logcmd_merge, shell=True)
                except:
                    pass

                no_merged_commit = self.parse_commit(no_merge_log, datei, repourl, current_branch_name, False)
                merged_commit = self.parse_commit(merge_log, datei, repourl, current_branch_name, True)

                branch_commits.extend(merged_commit)
                branch_commits.extend(no_merged_commit)

                temp_dateii = datetime.datetime.strftime(dateii, "%Y-%m-%d")
                temp_datei = datetime.datetime.strftime(datei, "%Y-%m-%d")
                print(
                    f"Repository: {repo}\tBranch_name: {branch_name} \t from date: {temp_datei}  end date: {temp_dateii}  commits has been collected.")
                datei = dateii
            repo_commit_list.extend(branch_commits)

        return repo_commit_list

    def getCurrentBranchName(self, branch_text):
        current_branch_name = ""
        branch_list = branch_text.split("\n")
        for each in branch_list:
            if each.find("*") != -1:
                current_branch_name = each.split()[-1]
                break
        return current_branch_name

    def parse_commit(self, log, log_date, repourl, branch_name, is_merged):
        results = []
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

                result['branch_name'] = branch_name
                results.append(result)

        else:
            for commit_log in commit_logs:
                line1 = commit_log.split('@@')[0]

                result = self.parse_commit_log(line1, log_date)
                result['project'] = repo
                result['repo'] = repourl
                result['project'] = repo
                result['repo'] = repourl
                result['is_merged'] = 0
                result['branch_name'] = branch_name

                file_changed = 0
                lines_added = 0
                lines_removed = 0

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
        split_list = commit_log.split(";")

        email = split_list[1]
        # if "yao@apache.org" in email:
        #     print("stop")
        author = self.get_author(split_list)
        date_str = log_date.strftime("%Y-%m-%d")
        time_str = split_list[2].split()[3]

        # there are two dilemma in commit log: has time zone and has no time zone, such as  'Tue Jan 7 18:09:19 2020'  and 'Tue Feb 2 17:00:42 2021 +0800'. Give two solutions for them.
        try:
            time_zone = split_list[2].split()[5]
        except IndexError:
            time_zone = None

        # search the company of author then write into dict
        company_name = self.find_company(email)

        if time_zone:
            preciseness_time_str = date_str + "T" + time_str + time_zone
        else:
            preciseness_time_str = date_str + "T" + time_str
        commit_id = split_list[- 1]

        result = {'created_at': preciseness_time_str, "file_changed": 0, "add": 0,
                  'remove': 0, 'total': 0, 'author': author, 'company': company_name,
                  'email': email, 'commit_id': commit_id}

        return result

    def find_company(self, email):
        company_name = ''
        email_tag = email.split("@")[-1]

        for user in self.users:
            if email in user["emails"]:
                company_name = self.alias_companies.get(user["companies"][0]["company_name"])

        if not company_name and self.domain_companies.get(email_tag):
            company_name = self.domain_companies.get(email_tag)

        if not company_name:
            company_name = "independent"

        return company_name

    def getInfoFromCompany(self):
        companyInfo = {}
        if self.data_yaml_url and self.company_yaml_url:
            # cmd = 'wget -N %s' % self.data_yaml_url
            # p = os.popen(cmd.replace('=', ''))
            # p.read()
            # datas = yaml.load_all(open(self.data_yaml_path, encoding='UTF-8')).__next__()
            # cmd = 'wget -N %s' % self.company_yaml_url
            # p = os.popen(cmd.replace('=', ''))
            # p.read()
            # companies = yaml.load_all(open(self.company_yaml_path, encoding='UTF-8')).__next__()
            # p.close()

            ###Test in windows without wget command
            self.data_yaml_path = "data/data.yaml"
            self.company_yaml_path = "data/company.yaml"
            datas = yaml.load_all(open(self.data_yaml_path, encoding='UTF-8'), Loader=yaml.FullLoader).__next__()
            companies = yaml.load_all(open(self.company_yaml_path, encoding='UTF-8'), Loader=yaml.FullLoader).__next__()

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

    def get_author(self, split_list):
        author = split_list[0]
        email = split_list[1]
        for user in self.users:
            if email in user['emails']:
                author = user['user_name']
                break
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
        return os.path.abspath('.') + "/" + filename + '.json'
