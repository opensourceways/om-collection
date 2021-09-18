#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64
import errno
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import git
import requests
import yaml

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data import common
from data.common import ESClient


# os.environ["GIT_PYTHON_REFRESH"] = "quiet"


class GitCommit(object):

    def __init__(self, config=None):
        self.config = config
        self.platform_name = platform.system().lower()
        self.default_headers = {
            'Content-Type': 'application/json'
        }
        self.index_name = config.get('index_name')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.headers = {'Content-Type': 'application/json', "Authorization": config.get('authorization')}
        self.community_name = config.get('community_name')
        self.repo_source_dict = config.get('repo_source_dict')
        self.token_v5 = config.get('gitee_token_v5')
        # self.gitHubDown = GitHubDown(config)

        self.username = config.get('username')
        self.password = config.get('password')
        self.user_yaml_url = config.get('user_yaml_url')
        self.company_yaml_url = config.get('company_yaml_url')
        self.is_fetch_all_branches = config.get("is_fetch_all_branches")
        self.companyInfo = self.get_info_from_company()

    def run(self, startTime):

        start_date = datetime.strptime(startTime, "%Y%m%d")
        try:
            self.from_date
        except AttributeError:
            self.from_date = start_date

        self.from_date_str = self.from_date.strftime("%Y-%m-%d")

        self.total_repo_count = 0
        self.failed_clone_repos = []
        self.success_parsed_repo_count = 0

        self.total_commit_count = 0
        print(f"*************Begin to collect commits.  From :{self.from_date_str}***********")
        self.main_process(self.from_date)  # collect repo commit entrance

        self.success_parsed_repo_count = self.total_repo_count - len(self.failed_clone_repos)
        print(
            f'Collect {self.success_parsed_repo_count} repos sucessfully, {self.total_commit_count} commits for  {self.community_name} totally.\n')
        if self.failed_clone_repos:
            print(f'In all, {len(self.failed_clone_repos)} are failed to clone, they are as follows:\n')
            for repo in self.failed_clone_repos:
                print(repo)
        print(f"*********************Finish Collection*******************************")

    def main_process(self, from_date):

        ## accquire repo_url_list first
        repo_url_list = self.get_repos_from_sources(self.repo_source_dict)

        ## print basic repo_url_list info into log
        self.total_repo_count = len(repo_url_list)
        print(f'There are {self.total_repo_count} repo_url_list  for  {self.community_name} community totally.\n')

        ## process the repo_url_list
        self.process_repos(from_date, self.index_name, repo_url_list)

    def get_repos_from_sources(self, repo_source_dict):
        # collect all reppos from various source
        try:
            repos = []
            repo_source_dict = eval(repo_source_dict)  ## Transform repo_dict to dict
            if repo_source_dict.get('json_file'):
                file_repo_list = self.get_repo_list_from_json_file(repo_source_dict.get('json_file'))
                repos.extend(file_repo_list)
            if repo_source_dict.get('yaml_url_list'):
                yaml_repo_list = self.get_repo_from_online_yaml_file(repo_source_dict.get('yaml_url_list'))
                repos.extend(yaml_repo_list)
            if repo_source_dict.get('gitee_org_list'):
                gitee_repo_list = self.get_repo_list_from_Gitee(repo_source_dict.get('gitee_org_list'))
                repos.extend(gitee_repo_list)
            if repo_source_dict.get('github_org_list'):
                gitee_repo_list = self.get_repo_list_from_GitHub(repo_source_dict.get('github_org_list'))
                repos.extend(gitee_repo_list)
            repos = list(set(repos))
            return repos
        except Exception as e:
            print(f'Occur error at function: {sys._getframe().f_code.co_name}')
            raise e

    def process_repos(self, from_date, index_name, repo_url_list):
        ## process the all repos by traversal

        repos_local_path = '/home/repo_clone/local_repo/'
        if not os.path.exists(repos_local_path):
            os.makedirs(repos_local_path)

        for repo_url in repo_url_list:
            repo_name = repo_url.split("/")[-1]
            print(
                f'Progress: processing the {repo_name}, it is the {repo_url_list.index(repo_url) + 1} of {len(repo_url_list)} repos.\n')
            repo = self.clone_repo_from_remote_to_local(repo_url, repos_local_path)  # Clone repo to local
            if not repo:
                continue

            repo_commit_list = self.get_commit_from_each_repo(from_date, repo)  # Get commits from each branch

            ### Delete local repo after get its commit to reserve enough space for other repo
            self.remove_local_repo(os.path.join(repos_local_path, repo_name))

            self.total_commit_count += len(repo_commit_list)  ## the total count from all repos

            # store a single repo data into ES
            action = ''
            for commit in repo_commit_list:
                ID = self.generate_record_id(commit)  ## generate id based all fields from commit
                commit_str = self.getSingleAction(index_name, ID, commit)
                action += commit_str

            print(f"Start to store {repo_name} data to ES...")
            self.esClient.safe_put_bulk(action)
            print(f'Completed process repo:{repo_name}\n\n\n')

    def get_repo_branch_names(self, repo):

        repo_name = repo.working_dir.split(os.sep)[-1]

        if self.is_fetch_all_branches == "True":  ## Accquire all remote branch names
            remote_branch_names = [branch_name.remote_head for branch_name in repo.remote().refs]
            remote_branch_names.remove('HEAD')
            branch_names = remote_branch_names
            print(f'Get {len(branch_names)} branch names of repo:  {repo_name} .')
        else:
            branch_names = [repo.head.reference.name]  ## Accquire current branch names
            print(f'Get the current branch of {repo_name}')

        return branch_names

    def push_repo_data_into_es(self, index_name, repo_data_list, repo):
        action = ''
        for commit in repo_data_list:
            ID = commit["commit_id"]
            commit_log = self.getSingleAction(index_name, ID, commit)
            action += commit_log
        self.esClient.safe_put_bulk(action)
        print(repo, " data has stored into ES")

    def clone_repo_from_remote_to_local(self, repo_url, path):
        repo_name = repo_url.split("/")[-1]
        website = repo_url.split("/")[2]
        project = repo_url.split("/")[-2]
        username = base64.b64decode(self.username).decode()
        passwd = base64.b64decode(self.password).decode()
        local_repo_path = os.path.join(path, repo_name)
        repo_dir = Path(local_repo_path)

        if not username or not passwd:
            print(f'Have not fetch username or passwd from config.ini.\n')
            return None
        clone_url = 'https://' + username + ':' + passwd + '@' + website + '/' + project + '/' + repo_name

        if repo_dir.exists():
            local_repo = git.Repo(local_repo_path)
            check_repo = self.is_dir_git_repo(local_repo_path)
            if check_repo:
                local_repo.remote().pull()
                print(f'Pulled the repo: {repo_name}, since it has existed')
            else:
                print(f'{local_repo_path} is not repo.')
        else:
            try:
                print(f'Start to clone {repo_name} repo...')
                local_repo = git.Repo().clone_from(clone_url, local_repo_path)
                print(f'Clone {repo_name} repo successfully.')
            except git.GitCommandError as gitError:
                print(gitError.stderr)

                # openEuler/kernel can be cloned successfully, but still caught a exception
                if self.is_dir_git_repo(local_repo_path):
                    local_repo = git.Repo(local_repo_path)

                    # Recover the repo which delete a batch file by exception
                    local_repo.git.execute(f'git restore --source=HEAD :/', shell=True)
                else:
                    f'Repo {repo_name} is a private repo, it is invisible to public. Failed to clone it'
                    self.failed_clone_repos.append(repo_name)
                    return None
            except Exception as cloneEx:
                print(
                    f'Occurs unexpected error besides GitCommmandError while clone repo: {repo_name}. \nThe Error is:{repr(cloneEx)}')
                self.failed_clone_repos.append(repo_name)
                return None
        return local_repo

    def get_commit_from_each_repo(self, from_date, repo):
        # Return None if has not fetch repo to local, namely g is None
        if repo is None:
            return None
        repo_name = repo.working_dir.split(os.sep)[-1]
        repo_commit_list = []
        repo_url = self.get_url_from_loca_repo(repo)

        branch_names = self.get_repo_branch_names(repo)  # ensure branch name, then get its commits.

        for branch_name in branch_names:
            try:  ## traverse all branches fetch data
                self.remove_git_index_lock_file(repo)  ## Should remove index.lock before checkout branch
                repo.git.execute(f"git checkout -f {branch_name}", shell=True)
            except git.GitCommandError as gitCheckoutError:
                if branch_name != repo.head.reference.name:
                    print(
                        f'Failed to checkout to branch name: {branch_name}. Error is: {repr(gitCheckoutError.status)}\n')
                    continue
                print(f'Although occur some error in checkout process, checkout to {branch_name} successfully at last.')

            start_fetch_date = from_date
            today = datetime.today()
            branch_commits = []
            while True:  # pull, parse, assemble commit records
                start_fetch_date_str = start_fetch_date.strftime('%Y-%m-%d')
                end_fetch_date = start_fetch_date + timedelta(days=3)
                end_fetch_date_str = end_fetch_date.strftime('%Y-%m-%d')

                commit_log_dict = self.get_period_commit_log(repo, end_fetch_date_str, start_fetch_date_str)
                branch_period_commits = self.process_commit_log_dict(commit_log_dict, repo)

                branch_commits.extend(branch_period_commits)

                print(f"Repository: {repo_name};\tBranch_name: {branch_name};\tfrom date: {start_fetch_date_str};"
                      f"\tend date: {end_fetch_date_str}.\t {len(branch_period_commits)} commits has been collected. "
                      f"\tRepo_url: {repo_url}")

                start_fetch_date = end_fetch_date

                if end_fetch_date > today:
                    break
            print(f"\nRepository: {repo_name};\tBranch_name: {branch_name};"
                  f"\t{len(branch_commits)} commits has been collected.")
            repo_commit_list.extend(branch_commits)

        print(f"\nRepository: {repo_name}; \t\t{len(repo_commit_list)} commits has been collected.")

        return repo_commit_list

    def get_period_commit_log(self, repo, start_date_str, end_date_str):
        log_dict = {}
        logcmd_base = f'git log --after={end_date_str} --before={start_date_str} --shortstat --pretty=format:"@@@***@@@%n%an;;;%n%ae;;;%n%cd;;;%n%H;;;%n%s;;;%n%b;;;%n%N"' \
                      f' --date=format:"%Y-%m-%dT%H:%M:%S%z"'
        logcmd_merge = logcmd_base + " --merges"
        logcmd_no_merge = logcmd_base + " --no-merges"

        no_merge_log = ''
        try:
            no_merge_log = repo.git.execute(logcmd_no_merge, shell=True)
        except Exception as ex:
            print(ex.__repr__())
            print("logcmd_no_merge execute failed")
        merge_log = ''
        try:
            merge_log = repo.git.execute(logcmd_merge, shell=True)
        except Exception as ex:
            print(ex.__repr__())
            print("logcmd_merge execute failed")

        log_dict['merge_log'] = merge_log
        log_dict['no_merge_log'] = no_merge_log

        return log_dict

    def process_commit_log_dict(self, commit_log_dict, repo):
        result = []

        merge_log = commit_log_dict.get('merge_log')
        no_merge_log = commit_log_dict.get('no_merge_log')

        if merge_log:
            merge_commit_list = merge_log.split('@@@***@@@')
            merge_commit_list.remove('')
            for merge_commit in merge_commit_list:
                commit_result = self.parse_each_commit_log(repo, merge_commit, 1)
                result.append(commit_result)

        if no_merge_log:
            no_merge_log_list = no_merge_log.split('@@@***@@@')
            no_merge_log_list.remove('')
            for no_merge_commit in no_merge_log_list:
                commit_result = self.parse_each_commit_log(repo, no_merge_commit, 0)
                result.append(commit_result)

        return result

    def parse_each_commit_log(self, repo, commit_log, is_merged):  # parse commit log and assemble data

        if not commit_log:
            return None
        commit_log = commit_log.strip()
        branch_name = repo.head.reference.name
        repo_name = repo.working_tree_dir.split(os.sep)[-1]
        split_list = commit_log.split(";;;")

        author = self.get_author(split_list).strip()
        email = split_list[1].strip()

        commit_datetime_str = split_list[2].strip()

        # search the company for the author then write into dict
        company_name = self.find_company(email)
        commit_id = split_list[3].strip()
        title = split_list[4].strip()
        commit_content = split_list[5].strip()
        commit_log_modifying_info = split_list[6].strip()
        commit_log_modifying_dict = self.parse_modifying_info(commit_log_modifying_info)

        ## find out contributor & reviewer
        commit_contributors = self.get_role_from_commit_main_content(commit_content,
                                                                     role_keyword='openEuler_contributor')
        commit_reviewers = self.get_role_from_commit_main_content(commit_content,
                                                                  role_keyword='openEuler_reviewer')
        repo_url = self.get_url_from_loca_repo(repo)
        result = {'created_at': commit_datetime_str,
                  'author': author, 'company': company_name,
                  'email': email, 'commit_id': commit_id,
                  'file_changed': commit_log_modifying_dict.get('file_changed'),
                  'add': commit_log_modifying_dict.get('lines_added'),
                  'remove': commit_log_modifying_dict.get('lines_removed'),
                  'total': commit_log_modifying_dict.get('total'),
                  "title": title, 'is_merged': is_merged,
                  'branch_name': branch_name, 'repo_name': repo_name,
                  'repo_url': repo_url
                  }
        if commit_contributors:
            result['commit_contributors'] = commit_contributors
        if commit_reviewers:
            result['commit_contributors'] = commit_reviewers

        return result

    def get_url_from_loca_repo(self, repo):
        remote_urls = repo.remote().urls.__next__()
        address = remote_urls.split('@')[-1]
        common_url = 'https://' + address
        return common_url

    def get_commit_content_and_modifyInfo(self, text):
        result = {}
        last_line_feed_site = text.rfind("\n\n")
        if last_line_feed_site == -1:
            return result
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

    def get_info_from_company(self):
        companyInfo = {}

        if not self.user_yaml_url or not self.company_yaml_url:
            return companyInfo
        try:
            user_filename = self.get_yaml_file_name_from_url(self.user_yaml_url)
            company_filename = self.get_yaml_file_name_from_url(self.company_yaml_url)
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

    def get_from_date(self, from_date, filters):
        if from_date is None:
            from_date = self.esClient.get_from_create_date(filters)
        else:
            from_date = common.str_to_datetime(from_date)
        return from_date

    def get_repo_list_from_json_file(self, file_path_dict):
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

    def get_repo_list_from_Gitee(self, gitee_org_list):
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

    def get_repo_list_from_GitHub(self, github_org_list):
        repo_url_list = []
        github_base_url = "https://github.com/"
        for org in github_org_list:
            githubClient = GithubClient(org=org, repository=None, token=None)
            try:
                # github_items = self.gitHubDown.getFullNames(org=org, from_date=None)
                repo_detail_list = githubClient.getAllRepoDetail()
                print(f'Get {len(repo_detail_list)} repos from {org} of Github.\n')
            except:
                print(f'Failed to get repos from {org} of Github.\n')
            for item in repo_detail_list:
                repo_url = github_base_url + item.get('full_name')
                repo_url_list.append(repo_url)
        print(f'Collected {len(repo_url_list)} repos from Github totally.\n')
        return repo_url_list

    def get_repo_from_online_yaml_file(self, yaml_url_list=None):
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

    def get_yaml_file_name_from_url(self, url):
        segment_list = url.split('/')
        return segment_list[-1]

    def handle_remove_read_only(self, func, path, exc):
        excvalue = exc[1]
        if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == errno.EACCES:
            os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)  # 0777
            func(path)
        else:
            raise

    def remove_local_repo(self, path):
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

    def get_role_from_commit_main_content(self, content, role_keyword):

        try:
            if not content:
                return None
            role_email_list = []
            content_lines = content.split('\n')

            for line in content_lines:
                line = line.strip()
                if role_keyword.lower() in line.lower():
                    contributor_email = re.compile(r'<.*@\w*.\w*>').findall(line)[0]
                    role_email_list.append(contributor_email[1:-1])

            return ';\n'.join(role_email_list)
        except Exception as e:
            print(f'Occur error at function: {sys._getframe().f_code.co_name}')
            raise e

    def remove_git_index_lock_file(self, repo):
        try:
            repo_dir = repo.common_dir

            index_lock_file_path = os.path.join(repo_dir, 'index.lock')
            index_file_path = os.path.join(repo_dir, 'index')

            if self.platform_name == 'linux':
                if os.path.exists(index_lock_file_path):
                    os.system(f'rm -f {index_lock_file_path}')
                elif os.path.exists(index_file_path):
                    os.system(f'rm -f {index_file_path}')

            if self.platform_name == 'windows':
                if os.path.exists(index_lock_file_path):
                    os.remove(index_lock_file_path)
                elif os.path.exists(index_file_path):
                    os.remove((index_file_path))
        except Exception as ex:
            print(f'Occur a error in function: remove_git_index_lock_file. The error is:\n {repr(ex)}')
            raise FileNotFoundError

    def generate_record_id(self, commit):
        try:
            id_str = ''
            for field in commit:
                id_str += str(commit.get(field))
            id = hash(id_str)
            return id
        except:
            print('Occur error in function: generate_record_id')
            return None

    def is_dir_git_repo(self, path):
        ## justify the path is whether a git repo or not

        if not os.path.exists:
            return False
        check_command = 'git rev-parse --is-inside-work-tree'
        process = subprocess.Popen(check_command, stdout=subprocess.PIPE, cwd=path, universal_newlines=True, shell=True)
        process_output = process.communicate()
        is_git_repo = process_output[0].strip()
        if is_git_repo == 'true':
            return True

        return False
