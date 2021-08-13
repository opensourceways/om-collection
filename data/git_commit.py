#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import yaml
from collect.gitee import GiteeClient
from data import common
import git
import datetime
import json
from data.common import ESClient
import base64
from data.github_down import GitHubDown

os.environ["GIT_PYTHON_REFRESH"] = "quiet"


class GitCommit(object):

    def __init__(self, config=None):
        self.config = config
        self.default_headers = {
            'Content-Type': 'application/json'
        }
        self.from_date = config.get("from_data")
        self.index_name = config.get('index_name')
        if config.get('sigs_code_all'):
            self.sigs_code_all = config.get('sigs_code_all').split(',')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.headers = {'Content-Type': 'application/json', "Authorization": config.get('authorization')}
        self.repo_file_name = config.get('repo_json_file_name')
        self.orgs = config.get('orgs')
        self.repo_sources = config.get('repo_sources')
        self.token_v5 = config.get('gitee_token_v5')
        self.username = config.get('username')
        self.password = config.get('password')
        self.gitHubDown = GitHubDown(config)
        self.data_yaml_url = config.get('data_yaml_url')
        self.data_yaml_path = config.get('data_yaml_path')
        self.company_yaml_url = config.get('company_yaml_url')
        self.company_yaml_path = config.get('company_yaml_path')

        self.users_yaml_url = config.get('users_yaml_url')
        self.is_fetch_all_branches = config.get("is_fetch_all_branches")
        self.is_get_users_code_all = config.get("is_get_users_code_all")

    def run(self, startTime):

        if isinstance(self.from_date, str):
            from_date = self.getFromDate(self.from_date, [{"name": "is_git_commit", "value": 1}])
        else:
            from_date = self.from_date
        self.failed_clone_repos = []
        self.success_parsed_repo_count = 0

        print(f"*************Begin to collect commits.  From :{from_date}***********")
        self.domain_companies = {}
        self.alias_companies = {}
        self.users = {}
        if self.data_yaml_url and self.company_yaml_url:
            companyInfo = self.getInfoFromCompany()
            self.domain_companies = companyInfo['domains_company_list']
            self.alias_companies = companyInfo['aliases_company_list']
            self.users = companyInfo["datas"]["users"]
        else:
            print("Personal info and company info must be given")

        if self.is_get_users_code_all == "true":
            self.get_users_code_all(from_date)
        else:
            self.get_sigs_code_all(from_date)
        print(f"*********************Finish Collection***************************")

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""
        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData) + '\n'
        action += json.dumps(body) + '\n'
        return action

    def get_users_code_all(self, from_date=None):
        if self.users_yaml_url:
            cmd = 'wget -N %s' % self.users_yaml_url
            p = os.popen(cmd)
            p.read()
            datas = yaml.load_all(open("data.yml", encoding='UTF-8')).__next__()
            p.close()

        res = datas.get("users")
        for r in res:
            sig = r.get("name")
            repos = r.get('repos')
            if repos:
                self.is_fetch_all_branches = "False"
                self.collect_code(from_date, self.index_name, repos, sig)

            repos_all_branches = r.get('repos_all_branches')
            if repos_all_branches:
                self.is_fetch_all_branches = "True"
                self.collect_code(from_date, self.index_name, repos_all_branches, sig)

        now = datetime.datetime.now()
        self.from_date = datetime.datetime(year=now.year, month=now.month, day=now.day - 1)

    def get_sigs_code_all(self, from_date=None):
        repos = []
        repo_sources = [element.strip().lower() for element in self.repo_sources.split(',')]

        # append all reppos from various source
        if 'json_file' in repo_sources:
            file_repo_list = self.getRepoFromFile()
            repos.extend(file_repo_list)
        if 'gitee' in repo_sources:
            gitee_repo_list = self.getGiteeRepos()
            repos.extend(gitee_repo_list)
        if 'github' in repo_sources:
            github_repo_list = self.getGithubRepos()
            repos.extend(github_repo_list)

        print(f'There are {len(repos)} repos to be collected in total for this opensource community.\n')
        self.collect_code(from_date, self.index_name, repos)

        today = datetime.datetime.today()
        self.from_date = today + datetime.timedelta(days=-1)

        print(f"Collected {self.success_parsed_repo_count} repositories for this opensource community\n")
        if self.failed_clone_repos:
            print(f'Failed to collect commits repos list is:')
            for repo in self.failed_clone_repos:
                print(f'{repo}\n')

    def collect_code(self, from_date, index_name, repourl_list):
        path = '/home/collect-code-clone/local_repo/'
        if not os.path.exists(path):
            os.makedirs(path)

        for repourl in repourl_list:
            repo = repourl.split("/")[-1]
            g = self.pull_Repo_To_Local(repourl, path)

            # Get commits for each branch
            repo_commit_list = self.fetch_commit_log_from_repo(from_date, g, repourl)
            if repo_commit_list is None:
                print(f'\nEEEEError!!! {repo} get nothing. Because failed to git clone the repo to local.\n\n')
                return

                # store a single repo data into ES
            action = ''
            for commit in repo_commit_list:
                ID = commit["commit_id"]
                commit_str = self.getSingleAction(index_name, ID, commit)
                action += commit_str

            print(f"Start to store {repo} data to ES...")
            self.esClient.safe_put_bulk(action)

            print(repo, f" has {len(repo_commit_list)} commits. All has been collected into ES.")

            ## delete current repository
            os.system(f"rm -rf {path + '/' + repo}")  # Execute on Linux
            print(f'Repository: {repo} has been removed.\n')

    def get_repo_branches(self, g):
        branch_names = []
        if self.is_fetch_all_branches == "True":
            text = g.execute("git branch -r", shell=True)  # ensure branch name, then get its commits.
        else:
            branch_name = g.execute("git symbolic-ref --short -q HEAD", shell=True)
            branch_names.append(branch_name)
            return branch_names
        branch_list = text.split("\n")

        for i in range(len(branch_list)):
            if branch_list[i].find("origin") != -1:
                branch_names.append(branch_list[i].split('/')[-1])
        branch_names = list(set(branch_names))
        print('Get branch names successfully.')

        return branch_names

    def push_repo_data_into_es(self, index_name, repo_data_list, repo):
        action = ''
        for commit in repo_data_list:
            ID = commit["commit_id"]
            commit_log = self.getSingleAction(index_name, ID, commit)
            action += commit_log
        self.esClient.safe_put_bulk(action)
        print(repo, " data has stored into ES")

    def pull_Repo_To_Local(self, repourl, path=None):
        repo = repourl.split("/")[-1]
        website = repourl.split("/")[2]
        project = repourl.split("/")[-2]
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
                print(f"Starting to clone repository: {repo}  ....")
                gc.execute(cmdclone, shell=True)
                print(f"Repository: {repo} clone done.\n")
            except  Exception as e:
                print(f'Occur error when clone repository: {repo}. Error Name is: ', e.__class__)
                self.failed_clone_repos.append(repo)
                return None
        else:
            cmdpull = "git pull"
            try:
                g.execute(cmdpull, shell=True)
            except:
                print(f'{repo} pull error')

        return g

    def fetch_commit_log_from_repo(self, from_date, g, repourl):

        # Return None if has not fetch repo to local, namely g is None
        if g is None:
            return None

        repo_commit_list = []
        repo = repourl.split("/")[-1]

        branch_names = self.get_repo_branches(g)  # ensure branch name, then get its commits.

        for branch_name in branch_names:
            try:
                g.execute(f"git checkout -f {branch_name}", shell=True)
            except:
                print('Failed to switch branch \n')
                print(repr(e))

            try:
                current_branch_name = g.execute(f"git symbolic-ref --short -q HEAD", shell=True)
            except Exception as e:
                print('git execute failure in acquire current branch name\n')
                print(repr(e))

            branch_commits = []

            datei = datetime.datetime.strptime(datetime.datetime.strftime(from_date, "%Y-%m-%d"), "%Y-%m-%d")
            dateii = datei

            while True:  # pull, parse, assemble commit records
                datenow = datetime.datetime.strptime(
                    datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                    "%Y-%m-%d")
                dateii += datetime.timedelta(days=3)

                logcmd_base = "git log --after=\"%s\" --before=\"%s\" --shortstat --pretty=\"@@@***@@@%%an;;;%%ae;;;%%ad;;;%%H;;;%%s;;;%%b\"  " % (
                    datei, dateii)

                logcmd_merge = logcmd_base + " --merges"
                logcmd_no_merge = logcmd_base + "--no-merges"

                no_merge_log = ''
                try:
                    no_merge_log = g.execute(logcmd_no_merge, shell=True)
                except:
                    print("logcmd_no_merge execute failed")
                    pass

                merge_log = ''
                try:
                    merge_log = g.execute(logcmd_merge, shell=True)
                except:
                    print("logcmd_merge execute failed")
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

                if dateii > datenow:
                    break

            repo_commit_list.extend(branch_commits)
        self.success_parsed_repo_count += 1
        return repo_commit_list

    def parse_commit(self, log, log_date, repourl, branch_name, is_merged):
        results = []
        if not log:
            return results

        repo = repourl.split('/')[-1]

        commit_logs = log.split('@@@***@@@')
        commit_logs.remove('')

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
                result = self.parse_commit_log(commit_log, log_date)
                result['project'] = repo
                result['repo'] = repourl
                result['project'] = repo
                result['repo'] = repourl
                result['is_merged'] = 0
                result['branch_name'] = branch_name

                results.append(result)

        return results

    def parse_commit_log(self, commit_log, log_date):
        # parse commit log and assemble data

        if not commit_log:
            return None

        split_list = commit_log.split(";;;")

        email = split_list[1]
        author = self.get_author(split_list)
        date_str = log_date.strftime("%Y-%m-%d")
        time_str = split_list[2].split()[3]

        # there are two dilemma in commit log: has time zone and has no time zone, such as  'Tue Jan 7 18:09:19 2020'  and 'Tue Feb 2 17:00:42 2021 +0800'. Give two solutions for them.
        try:
            time_zone = split_list[2].split()[5]
            time_zone = time_zone[:-2] + ':' + time_zone[-2:]
        except IndexError:
            time_zone = None

        # search the company of author then write into dict
        company_name = self.find_company(email)

        if time_zone:
            preciseness_time_str = date_str + "T" + time_str + time_zone
        else:
            preciseness_time_str = date_str + "T" + time_str
        commit_id = split_list[3]

        try:
            title = split_list[4]  # In case have no title
        except:
            title = None

        commit_content = self.get_commit_content_and_modifyInfo(split_list[5])
        commit_main_content = commit_content.get("commit_main_content")
        commit_log_modifying_info = commit_content.get("modifyInfo")

        commit_log_modifying_result = self.parse_modifying_info(commit_log_modifying_info)

        result = {'created_at': preciseness_time_str,
                  'author': author, 'company': company_name,
                  'email': email, 'commit_id': commit_id,
                  "file_changed": commit_log_modifying_result.get('file_changed'),
                  "add": commit_log_modifying_result.get('lines_added'),
                  'remove': commit_log_modifying_result.get('lines_removed'),
                  'total': commit_log_modifying_result.get('total'),
                  "tilte": title,
                  "commit_main_content": commit_main_content
                  }

        return result

    def get_commit_content_and_modifyInfo(self, text):
        result = {}
        last_line_feed_site = text.rfind("\n\n")
        modifyInfo = text[last_line_feed_site + 2:]

        if not text or len(modifyInfo) < 5:
            print("Get no main commit content and modifyInfo")
            return result
        commit_main_content = text[:last_line_feed_site]
        result["commit_main_content"] = commit_main_content
        result["modifyInfo"] = modifyInfo
        return result

    def parse_modifying_info(self, info_line):
        modify_info = {}

        if not info_line:
            return modify_info

        file_changed = 0
        lines_added = 0
        lines_removed = 0

        info_line = info_line.strip()
        file_pos = info_line.find("file")
        add_pos = info_line.find("insertion")
        del_pos = info_line.find("deletion")

        if add_pos != -1:
            lines_added_str = info_line[int(info_line.find(",") + 1):int(add_pos)]
            lines_added = int(lines_added_str.strip())
        if del_pos != -1:
            lines_removed_str = info_line[int(self.find_last(info_line, ",") + 1):int(del_pos)]
            lines_removed = int(lines_removed_str.strip())
        if file_pos != -1:
            try:
                file_changed = int(info_line[:file_pos].strip())
            except:
                print("Cannot get file_changed number")
        modify_info["file_changed"] = file_changed
        modify_info["lines_added"] = lines_added
        modify_info["lines_removed"] = lines_removed
        modify_info['total'] = lines_added + lines_removed
        return modify_info

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
            cmd = 'wget -N %s' % self.data_yaml_url
            p = os.popen(cmd.replace('=', ''))
            p.read()
            datas = yaml.load_all(open(self.data_yaml_path, encoding='UTF-8')).__next__()
            cmd = 'wget -N %s' % self.company_yaml_url
            p = os.popen(cmd.replace('=', ''))
            p.read()
            companies = yaml.load_all(open(self.company_yaml_path, encoding='UTF-8')).__next__()
            p.close()

            # ###Test in windows without wget command
            # self.data_yaml_path = "data.yaml"
            # self.company_yaml_path = "company.yaml"
            # datas = yaml.load_all(open(self.data_yaml_path, encoding='UTF-8'), Loader=yaml.FullLoader).__next__()
            # companies = yaml.load_all(open(self.company_yaml_path, encoding='UTF-8'),
            #                           Loader=yaml.FullLoader).__next__()

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

    def getRepoFromFile(self):
        repo_list = []
        try:
            file_path = os.path.abspath('.') + "/" + self.repo_file_name
            with open(file_path, 'r') as f:
                res = json.load(f)
            repos = res.get(self.org).get('git')
            repo_list.extend(repos)
            print(f'Collected {len(repo_list)} from json file.')
        except:
            print("Failed to get repos from json file, then empty list\n")
            pass
        return repo_list

    def getGiteeRepos(self):
        repo_url_list = []
        gitee_base_url = "http://www.gitee.com/"
        try:
            for org in self.orgs.split(','):
                client = GiteeClient(owner=org, repository=None, token=self.token_v5)
                gitee_items = common.getGenerator(client.org())
                for item in gitee_items:
                    repo_url = gitee_base_url + item.get('full_name')
                    repo_url_list.append(repo_url)
            print(f'Collected {len(repo_url_list)} from Gitee.')
        except:
            print("Failed to get repos from Github, then return empty list.\n")
            pass
        return repo_url_list

    def getGithubRepos(self):
        repo_url_list = []
        github_base_url = "https://github.com/"
        try:
            for org in self.orgs.split(','):
                github_items = self.gitHubDown.getFullNames(org=org, from_date=None)
                for item in github_items:
                    repo_url = github_base_url + item
                    repo_url_list.append(repo_url)
            print(f'Collected {len(repo_url_list)} from Github.')
        except:
            print("Failed to get repos from Github, then return empty list\n")
            pass
        return repo_url_list
