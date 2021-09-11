#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import errno
import os
import platform
import shutil
import stat
from pathlib import Path

import requests
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
        self.platform_name = platform.system().lower()
        self.default_headers = {
            'Content-Type': 'application/json'
        }
        self.from_date = config.get("from_data")
        self.index_name = config.get('index_name')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.headers = {'Content-Type': 'application/json', "Authorization": config.get('authorization')}
        self.community_name = config.get('community_name')
        self.repo_source_dict = config.get('repo_source_dict')
        self.token_v5 = config.get('gitee_token_v5')
        self.gitHubDown = GitHubDown(config)
        self.username = config.get('username')
        self.password = config.get('password')
        self.user_yaml_url = config.get('user_yaml_url')
        self.company_yaml_url = config.get('company_yaml_url')
        self.is_fetch_all_branches = config.get("is_fetch_all_branches")
        self.companyInfo = self.getInfoFromCompany()

    def run(self, startTime):

        if isinstance(self.from_date, str):  # jude program run first time or not
            from_date = self.getFromDate(self.from_date, [{"name": "is_git_commit", "value": 1}])
        else:
            from_date = self.from_date

        self.failed_clone_repos = []
        self.success_parsed_repo_count = 0
        self.total_commit_count = 0
        print(f"*************Begin to collect commits.  From :{from_date}***********")
        self.get_sigs_code_all(from_date)  # collect repo commit entrance
        print(
            f'Collect {self.success_parsed_repo_count} repos sucessfully, {self.total_commit_count} commits for  {self.community_name} totally.\n')
        if self.failed_clone_repos:
            print(f'In all, {len(self.failed_clone_repos)} are failed to clone, they are as follows:\n')
            for repo in self.failed_clone_repos:
                print(repo)
        print(f"*********************Finish Collection*******************************")

    def get_sigs_code_all(self, from_date=None):

        # collect all reppos from various source
        repos = []
        repo_source_dict = eval(self.repo_source_dict)  ## Transform repo_dict to dict
        if repo_source_dict.get('json_file'):
            file_repo_list = self.getRepoFromFile(repo_source_dict.get('json_file'))
            repos.extend(file_repo_list)
        if repo_source_dict.get('yaml_url_list'):
            yaml_repo_list = self.getRepoFromYamlOnline(repo_source_dict.get('yaml_url_list'))
            repos.extend(yaml_repo_list)
        if repo_source_dict.get('gitee_org_list'):
            gitee_repo_list = self.getRepoFromGitee(repo_source_dict.get('gitee_org_list'))
            repos.extend(gitee_repo_list)
        if repo_source_dict.get('github_org_list'):
            gitee_repo_list = self.getRepoFromGithub(repo_source_dict.get('github_org_list'))
            repos.extend(gitee_repo_list)

        # remove duplicate repos in case.
        repos = list(set(repos))
        print(f'There are {len(repos)} repos  for  {self.community_name} community totally.\n')
        self.collect_code(from_date, self.index_name, repos)

    def collect_code(self, from_date, index_name, repourl_list):
        path = '/home/collect-code-clone/local_repo/'
        if not os.path.exists(path):
            os.makedirs(path)

        for repourl in repourl_list:
            repo_name = repourl.split("/")[-1]
            print(
                f'Progress: processing the {repo_name}, it is the {repourl_list.index(repourl) + 1} of {len(repourl_list)} repos.\n')
            repo = self.pull_Repo_To_Local(repourl, path)  # Clone repo to local

            repo_commit_list = self.fetch_commit_log_from_repo(from_date, repo, repourl)  # Get commits from each branch
            self.removeLocalRepo(path + repo_name)  # Remove local repo immediately
            if repo_commit_list is None:
                print(f'\nEEEEError!!! {repo_name} get nothing. Because failed to git clone the repo to local.\n\n')
                continue
            self.total_commit_count += len(repo_commit_list)

            # store a single repo data into ES
            action = ''
            for commit in repo_commit_list:
                ID = commit["commit_id"]
                commit_str = self.getSingleAction(index_name, ID, commit)
                action += commit_str

            print(f"Start to store {repo_name} data to ES...")
            self.esClient.safe_put_bulk(action)
            print(repo_name, f" has {len(repo_commit_list)} commits. All has been collected into ES.")

    def get_repo_branch_names(self, repo):
        repo_name = repo.working_dir.split(os.sep)[-1]
        branch_names = []
        if self.is_fetch_all_branches == "True":
            try:
                text = repo.git.execute('git branch -r')
            except:
                print(f'Failed to get {repo_name} branches.')
                pass
        else:
            try:
                branch_name = repo.git.execute("git symbolic-ref --short -q HEAD", shell=True)
            except:
                print(f'{repo_name} failed to get current branch name.')
                pass
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
        repo_name = repourl.split("/")[-1]
        website = repourl.split("/")[2]
        project = repourl.split("/")[-2]
        username = base64.b64decode(self.username).decode()
        passwd = base64.b64decode(self.password).decode()
        local_repo_path = os.path.join(path, repo_name)
        repo_dir = Path(local_repo_path)

        if not username or not passwd:
            print(f'Have not fetch username and passwd from config.ini.\n')
            return None
        clone_url = 'https://' + username + ':' + passwd + '@' + website + '/' + project + '/' + repo_name

        if repo_dir.exists():  # Delete it if exits before clone
            removeResult = self.removeLocalRepo(local_repo_path)
            if not removeResult:
                print(f'Failed to remove {repo_name} repo before clone it.')
        try:
            print(f'Start to clone {repo_name} repo...')
            local_repo = git.Repo().clone_from(clone_url, local_repo_path)
            print(f'Clone {repo_name} repo successfully.')
        except Exception as cloneEx:
            print(repr(cloneEx))
            print(f'Failed to clone {repo_name}')
            self.failed_clone_repos.append(repo_name)
            return None
        return local_repo

    def fetch_commit_log_from_repo(self, from_date, repo, repourl):
        # Return None if has not fetch repo to local, namely g is None
        if repo is None:
            return None
        repo_name = repo.working_dir.split(os.sep)[-1]
        repo_commit_list = []

        branch_names = self.get_repo_branch_names(repo)  # ensure branch name, then get its commits.

        for branch_name in branch_names:
            try:
                repo.git.execute(f"git checkout -f {branch_name}", shell=True)
            except:
                print(f'Failed to checkout to branch name: {branch_name}. Error is: {repr(e)}\n')
                continue
            try:
                current_branch_name = repo.git.execute(f"git symbolic-ref --short -q HEAD", shell=True)
            except Exception as e:
                print(f'Failed to acquire current branch name. Error is: {repr(e)}.\n')
                continue
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
                    no_merge_log = repo.git.execute(logcmd_no_merge, shell=True)
                except:
                    print("logcmd_no_merge execute failed")
                    pass

                merge_log = ''
                try:
                    merge_log = repo.git.execute(logcmd_merge, shell=True)
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
                    f"Repository: {repo_name}\tBranch_name: {branch_name} \t from date: {temp_datei}  end date: {temp_dateii}.\t {len(no_merged_commit) + len(merged_commit)} commits has been collected.")
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
        if not self.companyInfo:
            print(f'There are no proper company information.')
            return 'Empty companyInfo'

        company_name = ''
        email_tag = email.split("@")[-1]
        users = self.companyInfo.get('users').get('users')
        domain_companies = self.companyInfo.get('domains_company_list')
        alias_companies = self.companyInfo.get('aliases_company_list')

        for user in users:
            if email in user["emails"]:
                # Get unique company_name from alias_companies
                company_name = alias_companies.get(user["companies"][0]["company_name"])
                return company_name

        # if email not in self.users's email list, then look for company name by email domain
        if domain_companies.get(email_tag):
            company_name = domain_companies.get(email_tag)
            return company_name

        return company_name

    def getInfoFromCompany(self):
        companyInfo = {}

        if not self.user_yaml_url or not self.company_yaml_url:
            return companyInfo
        try:
            user_filename = self.getYamlFilenameFromUrl(self.user_yaml_url)
            company_filename = self.getYamlFilenameFromUrl(self.company_yaml_url)
            if self.platform_name == 'linux':  ## Get data.yaml and company.yaml from Gitee in linux.
                cmd = 'wget -N %s' % self.user_yaml_url
                p = os.popen(cmd.replace('=', ''))
                p.read()

                users = yaml.load_all(open(user_filename, encoding='UTF-8')).__next__()
                cmd = 'wget -N %s' % self.company_yaml_url
                p = os.popen(cmd.replace('=', ''))
                p.read()
                companies = yaml.load_all(open(company_filename, encoding='UTF-8')).__next__()
                p.close()
            elif self.platform_name == 'windows':  ###Test in windows without wget command
                user_yaml_response = requests.get(self.user_yaml_url)
                company_yaml_response = requests.get(self.company_yaml_url)
                if user_yaml_response.status_code != 200 or company_yaml_response.status_code != 200:
                    print(
                        'Cannot connect the address of data.yaml or company.yaml online. then return the empty companyInfo dict.')
                    return companyInfo

                users = yaml.load(user_yaml_response.text, Loader=yaml.FullLoader)
                companies = yaml.load(company_yaml_response.text, Loader=yaml.FullLoader)
        except:
            print('Failed to get data.yaml or company.yaml online. then return the empty companyInfo dict.')
            return companyInfo

        domains_company_dict = {}
        aliases_company_dict = {}
        for company in companies['companies']:
            for domain in company['domains']:
                domains_company_dict[domain] = company['company_name']

            for alias in company["aliases"]:
                aliases_company_dict[alias] = company['company_name']

        companyInfo["domains_company_list"] = domains_company_dict
        companyInfo["aliases_company_list"] = aliases_company_dict
        companyInfo["users"] = users

        return companyInfo

    def get_author(self, split_list):
        author = split_list[0]
        email = split_list[1]
        try:
            users = self.companyInfo.get('users').get('users')
        except:
            print(f'Has not got users from self.companyInfo.')
            return author

        for user in users:
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

    def getRepoFromFile(self, file_path_dict):
        repo_list = []
        try:
            file_name = file_path_dict.get('file_name')
            org_name_list = file_path_dict.get('org_name_list')
        except:
            print(f'Failed to read config content correctly in getRepoFromFile.\n')
            return repo_list

        try:
            with open(file_name, 'r') as f:
                file_content_dict = eval(f.read())
        except:
            print(f'Failed to get repos from json file, will return empty list\n')
            return repo_list

        for org in org_name_list:
            org_repos = file_content_dict.get(org).get('git')
            repo_list.extend(org_repos)
            print(f'Collected {len(org_repos)} repos from {org}.\n')

        print(f'Collected {len(repo_list)} repos from json file totally.\n')

        return repo_list

    def getRepoFromGitee(self, gitee_org_list):
        repo_url_list = []
        gitee_base_url = "https://gitee.com/"

        for org in gitee_org_list:
            try:
                client = GiteeClient(owner=org, repository=None, token=self.token_v5)
                gitee_items = common.getGenerator(client.org())
                print(f'Get {len(gitee_items)} repos from {org} of Gitee.\n')
            except:
                print(f'Failed to get repos from {org} of Gitee.\n')
                continue

            # parse gitee_items and load them into repo_url_list
            for item in gitee_items:
                repo_url = gitee_base_url + item.get('full_name')
                repo_url_list.append(repo_url)

        print(f'Collected {len(repo_url_list)} repos from Gitee totally.')
        return repo_url_list

    def getRepoFromGithub(self, github_org_list):
        repo_url_list = []
        github_base_url = "https://github.com/"
        for org in github_org_list:
            try:
                github_items = self.gitHubDown.getFullNames(org=org, from_date=None)
                print(f'Get {len(github_items)} repos from {org} of Github.\n')
            except:
                print(f'Failed to get repos from {org} of Github.\n')
            for item in github_items:
                repo_url = github_base_url + item
                repo_url_list.append(repo_url)
        print(f'Collected {len(repo_url_list)} repos from Github totally.\n')
        return repo_url_list

    def getRepoFromYamlOnline(self, yaml_url_list=None):
        repos = []

        if not yaml_url_list:
            print('There is no yaml_url_list')
            return repos

        for yaml_url in yaml_url_list:
            if not yaml_url:
                print(f'{yaml_url} is not available.\n')
                continue
            try:
                # Fetch repo info from yaml file online
                yaml_response = requests.get(yaml_url)
                if yaml_response.status_code != 200:
                    print('Failed to get repo from yaml_url,return empty repo list.\n')
                    return repos
                datas = yaml.safe_load(yaml_response.text)
            except:
                print('Failed get repo info from online yaml file.')
                return repos

            # Parse and load repo info
            users = datas.get("users")
            for user in users:
                try:
                    res = repos.extend(user.get('repos'))
                except TypeError:
                    continue
            print(f'Get {len(repos)} repos from {yaml_url}.\n')
        print(f'Get {len(repos)} repos from yaml totally.\n')

        return repos

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""
        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData) + '\n'
        action += json.dumps(body) + '\n'
        return action

    def getYamlFilenameFromUrl(self, url):
        segment_list = url.split('/')
        return segment_list[-1]

    def handle_remove_read_only(self, func, path, exc):
        excvalue = exc[1]
        if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == errno.EACCES:
            os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)  # 0777
            func(path)
        else:
            raise

    def removeLocalRepo(self, path):
        repo_name = path.split('/')[-1]
        # Delete local repo directory after collecting over
        try:
            if self.platform_name == 'linux':  ## delete current local repo on linux
                os.system(f"rm -rf {path}")  # Execute on Linux
                print(f'Repository: {repo_name} has been removed.\n')
            elif self.platform_name == 'windows':  ## delete current local repo directory on windows
                shutil.rmtree(path, onerror=self.handle_remove_read_only)
                print(f'Repository: {repo_name} has been removed.\n')
        except Exception as ex:
            print(ex.__repr__())
            print(f'Error!!!  Failed to remove local repo directory.\n')
            return False
        return True
