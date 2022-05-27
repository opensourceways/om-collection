#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2022-03
#
import base64
import errno
import hashlib
import os
import pdb
import platform
import shutil
import stat
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import git
import pytz
import requests
import yaml

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data import common
from data.common import ESClient

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
PUBLIC_LOCAL_REPO_PATH = '/home/repo_clone/local_repo'


def count_seconds_of_prepare_local_repo(func):
    def wrapper(*args, **kw):
        thread_name = threading.current_thread().getName()
        repo_url = args[1]
        start_preare_time_point = time.time()
        func_value = func(*args, **kw)
        end_prepare_time_point = time.time()
        spend_seconds = end_prepare_time_point - start_preare_time_point
        pretty_second = round(spend_seconds, 1)
        print(f'\n{thread_name} === Prepared {repo_url} to be a local repo '
              f'spend {pretty_second} seconds')
        return func_value

    return wrapper


class ClocCode(object):

    def __init__(self, config=None):
        self.thread_name = threading.currentThread().getName()
        self.config = config
        self.platform_name = platform.system().lower()
        self.default_headers = {'Content-Type': 'application/json'}
        self.index_name = config.get('index_name')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.lock = threading.Lock()
        self.headers = {'Content-Type': 'application/json', "Authorization": config.get('authorization')}
        self.community_name = config.get('community_name')
        self.repo_source_dict = config.get('repo_source_dict')
        self.token_v5 = config.get('gitee_token_v5')
        self.username = config.get('username')
        self.password = config.get('password')
        self.user_yaml_url = config.get('user_yaml_url')
        self.company_yaml_url = config.get('company_yaml_url')
        self.is_fetch_all_branches = config.get('is_fetch_all_branches', 'False')
        self.is_remove_local_repo_immediate = config.get('is_remove_local_repo_immediate', 'True')
        self.max_thread_num_str = config.get('max_thread_num_str', '1')
        self.public_local_repo_path = config.get('public_local_repo_path', PUBLIC_LOCAL_REPO_PATH)

        self.is_fetch_all_branches = False if not self.is_fetch_all_branches else \
            eval(self.is_fetch_all_branches)
        self.is_remove_local_repo_immediate = True if not self.is_remove_local_repo_immediate else \
            eval(self.is_remove_local_repo_immediate)
        self.max_thread_num = 1 if not self.max_thread_num_str else eval(self.max_thread_num_str)
        self.public_local_repo_path = PUBLIC_LOCAL_REPO_PATH if not self.public_local_repo_path else \
            self.public_local_repo_path

    def run(self, start_time):
        start_point_time = datetime.utcnow()

        ## Install cloc in linux run environment
        install_cloc_result = self.execute_shell_command('apt-get install cloc', 'y')
        if not install_cloc_result:
            return

        if start_time:
            self.from_date = datetime.strptime(start_time, "%Y%m%d")
        self.from_date_str = self.from_date.strftime("%Y-%m-%d")

        print(f"*************Begin to collect commits.  From :{self.from_date_str}***********")
        self.main_process()  # collect repo code information entrance
        self.from_date = datetime.today()  # Set start_time for next run time

        end_point_time = datetime.utcnow()
        cost_time_seconds = end_point_time - start_point_time

        print("Cost time of this service:", cost_time_seconds)

        print(f"*********************Finish Collection*******************************")

    def execute_shell_command(self, command_statement, stdin_statement=''):
        ## install cloc in linux
        if self.platform_name != 'linux':
            print(f'{self.thread_name} === Current OS is not linux.\n {command_statement} would not be executed.')
            return None

        # 执行shell语句并定义输出格式
        subp = subprocess.Popen(command_statement, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

        # <editor-fold desc="when install cloc, a standard input to ensure intalling is needed. so must provide this
        # input string. then close the input stream">
        subp.stdin.write(stdin_statement.encode())
        subp.stdin.close()
        # </editor-fold>q

        wait = subp.wait()
        if wait == 0:
            print(f'{self.thread_name} === Statement: {command_statement} has been executed successfully.')
            result = subp.stdout.readlines()
            return result
        else:
            stderr_info = subp.stdout.read()
            print(f'{self.thread_name} === Failed to execute statement:  {command_statement}.\n'
                  f'the error exception as follows:\n'
                  f'{stderr_info}')
            return False

    def main_process(self):

        ## Set beijingTime_now_str
        utc_now = datetime.utcnow()
        beijing_now = utc_now + timedelta(hours=8)
        self.beijingTime_now_str = beijing_now.strftime('%Y-%m-%dT%X')

        ## Collect functions and repos for multi-thread
        thread_func_args = self.get_thread_funcs()

        ## Set initial state before processing
        self.total_repos_count = len(set(self.repo_url_dict))
        self.success_process_repo_count = 0
        self.reside_process_repo_count = self.total_repos_count
        self.failed_clone_repos = []
        self.success_process_repo_commit_info_dict = {}

        print(f"From date: {self.from_date_str}, Collected {self.total_repos_count} unique "
              f"repositories after removed duplicate ones in  {self.community_name} community totally.\n")

        ## Run multi-thread of collecting functions and store result to es
        common.writeDataThread(thread_func_args, max_thread_num=self.max_thread_num)

        ## Do statistic of result info:
        self.failed_clone_repo_count = len(self.failed_clone_repos)

        print(f'\n\nGame Over!\tProgram result info as follows:')
        print(f'Start time of program:{self.beijingTime_now_str}')
        print(f'Collecting time: {self.beijingTime_now_str}')
        print(f'Community name: {self.community_name}')
        print(f'Total repos count from sources: {self.total_repos_count}')
        print(f'Total processed repos count: {self.success_process_repo_count}')

        if self.failed_clone_repos:
            print(f'\nFailed to clone {self.failed_clone_repo_count} repos are:')
            print(self.failed_clone_repos, '\n')

        tracked_repo_url_list = []
        empty_code_repo_url_list = []
        for repo_url in self.success_process_repo_commit_info_dict:
            tracked_repo_url_list.append(repo_url)
            if self.success_process_repo_commit_info_dict[repo_url] == 0:
                empty_code_repo_url_list.append(repo_url)
        tracked_repo_url_list.extend(self.failed_clone_repos)

        succeed_store_repo_list = list(set(self.repo_url_dict) -
                                       set(empty_code_repo_url_list + self.failed_clone_repos))

        if succeed_store_repo_list:
            print(f'\nTotal {len(succeed_store_repo_list)} repos code num greater than zero, '
                  f'then they are stored to ES.')

        missed_repo = list(set(self.repo_url_dict).difference(set(tracked_repo_url_list)))
        if missed_repo:
            print(f'\nMissed {len(missed_repo)} repos, as follows:')
            print(missed_repo)

        if empty_code_repo_url_list:
            empty_repo_count = len(empty_code_repo_url_list)
            print(f'\nThere are {empty_repo_count} repos has no code record in the specific period. '
                  f'They are:')
            if empty_repo_count > 10:
                print(empty_code_repo_url_list[:10])
                print('......')

        with open(file='success_process_repo_code_info_dict_text', mode='w') as f:
            f.write(str(self.success_process_repo_commit_info_dict))

        with open(file='succeed_store_repo_list', mode='w') as f:
            f.write(str(succeed_store_repo_list))

        print(f'function: {sys._getframe().f_code.co_name} is over.')

    def get_thread_funcs(self):

        '''
        prepare thread function args for multiple thread running
        :param from_date_str:
        :return: thread_func_args
        '''

        thread_func_args = {}
        values = []
        self.repo_url_dict = self.get_repos_from_sources(self.repo_source_dict)

        for repo_url in self.repo_url_dict:
            values.append((repo_url, self.repo_url_dict[repo_url]))

        thread_func_args[self.process_each_repo] = values

        return thread_func_args

    def get_repos_from_sources(self, repo_source_dict):
        # collect all repos from various source from repo_source_dict from config file
        try:
            all_branches_repo_url_dict = {}
            general_repo_url_dict = {}
            default_repo_url_list = []
            repo_source_dict = eval(repo_source_dict)  ## Transform repo_dict to dict
            if repo_source_dict.get('json_file'):
                file_repo_list = self.get_repo_list_from_json_file(repo_source_dict.get('json_file'))
                default_repo_url_list.extend(file_repo_list)
            if repo_source_dict.get('yaml_url_list'):
                yaml_repo_dict = self.get_repo_from_online_yaml_file(repo_source_dict.get('yaml_url_list'))
                default_repo_url_list.extend(set(yaml_repo_dict['default_repo_url_dict']))
                all_branches_repo_url_dict = yaml_repo_dict['all_branches_repo_url_dict']
            if repo_source_dict.get('gitee_org_list'):
                gitee_repo_list = self.get_repo_list_from_Gitee(repo_source_dict.get('gitee_org_list'))
                default_repo_url_list.extend(gitee_repo_list)
            if repo_source_dict.get('github_org_list'):
                gitee_repo_list = self.get_repo_list_from_GitHub(repo_source_dict.get('github_org_list'))
                default_repo_url_list.extend(gitee_repo_list)

            for repo_url in default_repo_url_list:
                general_repo_url_dict[repo_url] = self.is_fetch_all_branches
            for repo_url in all_branches_repo_url_dict:
                general_repo_url_dict[repo_url] = all_branches_repo_url_dict[repo_url]

            print(f'\n{self.thread_name} === Collected {len(set(general_repo_url_dict))} unique repos from '
                  f'various sources.')
            return general_repo_url_dict
        except Exception as e:
            print(f'\n{self.thread_name} === Occur error at function: {sys._getframe().f_code.co_name}')
            raise e

    def process_each_repo(self, repo_url, is_fetch_all_branches):
        self.thread_name = threading.current_thread().getName()

        repo_name = repo_url.split('/')[-1]
        with self.lock:
            self.reside_process_repo_count -= 1
        print(f'\n{self.thread_name} === Progress: processing the {repo_url}; Has {self.reside_process_repo_count} '
              f'repos left.')

        repo = self.prepare_local_repo(repo_url)  # Clone repo to local
        if not repo:
            print(f'{self.thread_name} === Failed to prepare the repo: {repo_url}')
            with self.lock:
                self.failed_clone_repos.append(repo_url)
                return
        self.track_thread_log(repo_name)

        repo_code_list = self.get_code_info_from_each_repo(repo, is_fetch_all_branches)  # Get commits from each branch

        # store a single repo data into ES
        self.push_repo_data_into_es(self.index_name, repo_code_list, repo)

        with self.lock:
            self.success_process_repo_commit_info_dict[repo_url] = len(repo_code_list)
            self.success_process_repo_count += 1

        ### Delete local repo after get its code to reserve enough space for other repo

        if self.is_remove_local_repo_immediate:
            with self.lock:
                self.remove_local_repo(repo.working_tree_dir)
            print(f'{self.thread_name} === Repo {repo_url} has been removed.')

        print(f'{self.thread_name} === Completed process repo:{repo_url};\tCollected {len(repo_code_list)} '
              f'code records in all.\n')

    def push_repo_data_into_es(self, index_name, repo_data_list, repo):
        repo_url = self.get_url_from_local_repo(repo)
        if not repo_data_list:
            print(f'{self.thread_name} === Repo: {repo_url} has no code record in this period.')
            return

        action = ''
        for commit_body in repo_data_list:
            id_text = commit_body['repo_url'] + '_' + commit_body['branch_name'] + '_' + commit_body['language']
            id = hashlib.sha256('{}\n'.format(id_text).encode('utf-8')).hexdigest()
            commit_log = common.getSingleAction(index_name, id, commit_body)
            action += commit_log

        if self.lock:
            print(f'{self.thread_name} === Starting to store {len(repo_data_list)} records of {repo_url} into ES ....')
            self.esClient.safe_put_bulk(action)
            print(f'{self.thread_name} === Successfully Stored {len(repo_data_list)} records of {repo_url} into ES.')

    @count_seconds_of_prepare_local_repo
    def prepare_local_repo(self, repo_url):
        '''
        Prepare a local repo object for parsing
        :param repo_url: the remote url of a repo
        demo: https://github.com/postgres/postgres
        :return: repo object
        '''
        repo_name = repo_url.split("/")[-1]
        website = repo_url.split("/")[2]
        project = repo_url.split("/")[-2]
        username = base64.b64decode(self.username).decode()
        passwd = base64.b64decode(self.password).decode()
        local_repo_path = self.public_local_repo_path + '/' + repo_name
        repo_dir = Path(local_repo_path)
        local_repo = None

        if not username or not passwd:
            print(f'{self.thread_name} === Have not fetch username or passwd from config.ini.\n')
            return None
        clone_url = 'https://' + username + ':' + passwd + '@' + website + '/' + project + '/' + repo_name

        if repo_dir.exists():
            try:
                local_repo = git.Repo(local_repo_path)
                check_repo = self.is_dir_git_repo(local_repo_path)
            except Exception as ex:
                print(f'{self.thread_name} === local_repo_path cannot be build as git.Repo in'
                      f' {sys._getframe(1).f_code.co_name}, Error as follows:\n{ex.__repr__()}')
                return local_repo
            if check_repo:
                try:
                    print(f'{self.thread_name} === Pulling repo {repo_url}, since it has existed...')
                    with self.lock:
                        local_repo.remote().pull()
                        print(f'{self.thread_name} === Pulled the repo: {repo_url} successfully.')
                except Exception as ex:
                    print(f'{self.thread_name} === Failed to pull existed repo:{repo_url}\n'
                          f' Exception : {ex.__repr__()}')
                    return None

            else:
                print(f'{self.thread_name} === {local_repo_path} is not repo.')
        else:
            try:
                print(f'\n{self.thread_name} === Start to clone {repo_url} repo...')
                with self.lock:
                    local_repo = git.Repo().clone_from(clone_url, local_repo_path)
                    print(f'{self.thread_name} === Clone {repo_url} repo successfully.')
            except git.GitCommandError as gitError:

                if os.path.exists(local_repo_path):
                    if self.is_dir_git_repo(local_repo_path):
                        local_repo = git.Repo(local_repo_path)
                        # <editor-fold desc=" Recover the repo which delete a batch file by exception. such as openEuler/kernel">
                        try:
                            local_repo.git.execute(f'git restore --source=HEAD :/', shell=True)
                        except Exception as ex:
                            print(f'{self.thread_name} === Repo: {repo_url} is cloned uncompleted.\n'
                                  f'Exception: {ex.__repr__()}')
                        # </editor-fold>
                        print(f'{self.thread_name} === Repo {repo_url} is cloned successfully, but becomes a bare ' \
                              'repo with deleted all files')
                        return local_repo

                if gitError.stderr.__contains__('fatal:'):
                    stderr_fatal = gitError.stderr.split('fatal:')[1].strip()

                    if stderr_fatal.startswith('Authentication failed for'):
                        print(f'{self.thread_name} === Failed to clone {repo_url} repo cause of authentication '
                              f'failure.')
                        return None

                    if gitError.stderr.__contains__('not found'):
                        print(f'{self.thread_name} === Failed to clone {repo_url} repo since cannot reach from'
                              f' this repo_url')
                        return None

                print(f'{self.thread_name} === Unpredicted GitCommandError, stderr as follows: {gitError.stderr}')
                return None

            except Exception as cloneEx:
                print(f'{self.thread_name} === Occurs unexpected error besides GitCommmandError '
                      f'while clone repo: {repo_url}. \nThe Error is:{repr(cloneEx)}')
                return None
        return local_repo

    def get_code_info_from_each_repo(self, repo, is_fetch_all_branches):
        if repo is None:
            return None

        repo_url = self.get_url_from_local_repo(repo)
        repo_name = repo.working_dir.split(os.sep)[-1]
        self.track_thread_log(repo_name)

        branch_names = self.get_repo_branch_names(repo, is_fetch_all_branches)

        each_repo_code_list = []
        for branch_name in branch_names:
            each_branch_code_list = self.get_code_info_from_branch(repo, branch_name)
            each_repo_code_list.extend(each_branch_code_list)

        print(f'\n{self.thread_name} === Repository: {repo_url}; \tall branches records has been collected.')

        return each_repo_code_list

    def get_url_from_local_repo(self, repo):
        remote_url = repo.remote().urls.__next__()
        if remote_url.__contains__('@'):
            address = remote_url.split('@')[-1]
            common_url = 'https://' + address
        else:
            common_url = remote_url
        return common_url

    def get_repo_list_from_json_file(self, file_path_dict):
        repo_list = []
        try:
            file_name = file_path_dict.get('file_name')
            org_name_list = file_path_dict.get('org_name_list')
        except:
            print(f'{self.thread_name} === Failed to read config content correctly in getRepoFromFile.\n')
            return repo_list

        try:
            with open(file_name, 'r') as f:
                file_content_dict = eval(f.read())
        except:
            print(f'{self.thread_name} === Failed to get repos from json file, will return empty list\n')
            return repo_list

        for org in org_name_list:
            org_repos = file_content_dict.get(org).get('git')
            repo_list.extend(org_repos)
            print(f'{self.thread_name} === Collected {len(org_repos)} repos from {org}.\n')

        print(f'{self.thread_name} === Collected {len(repo_list)} repos from json file totally.\n')

        return repo_list

    def get_repo_list_from_Gitee(self, gitee_org_list):
        repo_url_list = []
        gitee_base_url = "https://gitee.com/"

        for org in gitee_org_list:
            try:
                client = GiteeClient(owner=org, repository=None, token=self.token_v5)
                gitee_items = common.getGenerator(client.org())
                print(f'{self.thread_name} === Get {len(gitee_items)} repos from {org} of Gitee.\n')
            except:
                print(f'Failed to get repos from {org} of Gitee.\n')
                continue

            # parse gitee_items and load them into repo_url_list
            for item in gitee_items:
                repo_url = gitee_base_url + item.get('full_name')
                repo_url_list.append(repo_url)

        print(f'{self.thread_name} === Collected {len(repo_url_list)} repos from Gitee totally.')
        return repo_url_list

    def get_repo_list_from_GitHub(self, github_org_list):
        repo_url_list = []
        github_base_url = "https://github.com/"
        for org in github_org_list:
            githubClient = GithubClient(org=org, repository=None, token=None)
            try:
                # github_items = self.gitHubDown.getFullNames(org=org, from_date=None)
                repo_detail_list = githubClient.get_repos(org)
                print(f'Get {len(repo_detail_list)} repos from {org} of Github.\n')
            except:
                print(f'Failed to get repos from {org} of Github.\n')
            for item in repo_detail_list:
                repo_url = github_base_url + item.get('full_name')
                repo_url_list.append(repo_url)
        print(f'{self.thread_name} === Collected {len(repo_url_list)} repos from Github totally.\n')
        return repo_url_list

    def get_repo_from_online_yaml_file(self, yaml_url_list=None):
        repos = {}

        if not yaml_url_list:
            print('There is no yaml_url_list')
            return repos

        default_repo_url_list = []
        all_branches_repo_url_list = []
        for yaml_url in yaml_url_list:
            if not yaml_url:
                print(f'{self.thread_name} === {yaml_url} is not available.\n')
                continue
            try:
                # Fetch repo info from yaml file online
                yaml_response = self.esClient.request_get(yaml_url)
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
                user_repos = user.get('repos')
                user_all_branches_repos = user.get('repos_all_branches')
                if user_repos:
                    default_repo_url_list.extend(user_repos)
                if user_all_branches_repos:
                    all_branches_repo_url_list.extend(user_all_branches_repos)

        default_repo_url_dict = {}
        for repo_url in default_repo_url_list:
            default_repo_url_dict[repo_url] = self.is_fetch_all_branches

        all_branches_repo_url_dict = {}
        for repo_url in all_branches_repo_url_list:
            all_branches_repo_url_dict[repo_url] = True

        repos['default_repo_url_dict'] = default_repo_url_dict
        repos['all_branches_repo_url_dict'] = all_branches_repo_url_dict

        repo_total_count = len(set(repos['default_repo_url_dict']).union(set(repos['all_branches_repo_url_dict'])))
        print(f'{self.thread_name} === Get {repo_total_count} repos from yaml_url.\n')

        return repos

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
                with self.lock:
                    os.system(f"rm -rf {path}")  # Execute on Linux
                    print(f'{self.thread_name} === Repository: {repo_name} has been removed.\n')
            elif self.platform_name == 'windows':  ## delete current local repo directory on windows
                with self.lock:
                    shutil.rmtree(path, onerror=self.handle_remove_read_only)
                    print(f'{self.thread_name} === Repository: {repo_name} has been removed.\n')
        except Exception as ex:
            print(f'{self.thread_name} === Error!!!  Failed to remove local repo directory.\n'
                  f'Exception information: ex.__repr__()')
            return False
        return True

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
            print(
                f'{self.thread_name} === Occur a error in function: remove_git_index_lock_file. The error is:\n {repr(ex)}')
            raise FileNotFoundError

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

    def track_thread_log(self, repo_name):
        print(f'\n{self.thread_name} === ********Thread tracking info************')
        print(f'{self.thread_name} === function site: {sys._getframe(1).f_code.co_name}')
        print(f'{self.thread_name} === code line seq: {sys._getframe().f_lineno}')
        print(f'{self.thread_name} === repo_name: {repo_name}')
        print(f'{self.thread_name} === thread name: {threading.currentThread().getName()}')
        print(f'{self.thread_name} === active running thread: {threading.activeCount()}')
        print(f'{self.thread_name} === current thread is alive? {threading.currentThread().is_alive()}')
        print(f'{self.thread_name} === ********Thread tracking info************\n')

    def trans_datetime_to_beijingTime(self, datetime_str):
        beijing_datetime_str = ''
        try:
            raw_datetime = datetime.strptime(datetime_str, '%Y-%m-%dT%X%z')
            tz = pytz.timezone('Asia/Shanghai')
            beijing_datetime = raw_datetime.astimezone(tz)
            beijing_datetime_str = beijing_datetime.strftime('%Y-%m-%dT%X+08:00')
        except Exception as ex:
            print(f'{self.thread_name} === Occurs some exception in function:{sys._getframe(1).f_code.co_name}\n'
                  f'{ex.__repr__()}')
        return beijing_datetime_str

    def parse_each_record_line(self, record_line, repo_url, branch_name):
        '''
        :param recordLine: 'Python                          42           1861           5119          10399'
        :return:
        '''

        record_dict = {}
        each_record_list = record_line.split()
        record_dict['created_at'] = self.beijingTime_now_str
        record_dict['repo_url'] = repo_url
        record_dict['repo_org'] = repo_url.split('/')[-2]
        record_dict['branch_name'] = branch_name

        if each_record_list.__len__() == 5:
            record_dict['language'] = each_record_list[0].strip()
        elif each_record_list.__len__() > 5:
            record_dict['language'] = ' '.join(each_record_list[:-4])
        else:
            print(f'\n{self.thread_name} === Repository: {repo_url}; \trecord_line is incompleted, please check')
            return record_dict

        record_dict['files'] = self.write_int_type_into_record_dict(each_record_list[-4].strip(), repo_url)
        record_dict['blank'] = self.write_int_type_into_record_dict(each_record_list[-3].strip(), repo_url)
        record_dict['comment'] = self.write_int_type_into_record_dict(each_record_list[-2].strip(), repo_url)
        record_dict['code'] = self.write_int_type_into_record_dict(each_record_list[-1].strip(), repo_url)

        return record_dict

    def write_int_type_into_record_dict(self, field, repo_url):

        field_int = 0
        try:
            field_int = int(field)
        except:
            print(f'\n{self.thread_name} === Repository: {repo_url}; \tfield: {field} is not a number, '
                  f'please check.')
            return f'{field} is not a digital'
        return field_int

    def get_repo_branch_names(self, repo, is_fetch_all_branches):
        repo_url = self.get_url_from_local_repo(repo)
        if is_fetch_all_branches:  ## If True, accquire all remote branch names
            remote_branch_names = [branch_name.remote_head for branch_name in repo.remote().refs]
            remote_branch_names.remove('HEAD')
            branch_names = remote_branch_names
            print(f'{self.thread_name} === Get {len(branch_names)} branch names of repo:  {repo_url}.')
        else:
            branch_names = [repo.head.reference.name]  ## Accquire current branch names
            print(f'{self.thread_name} === Get the current branch of {repo_url}')

        return branch_names

    def get_code_info_from_branch(self, repo, branch_name):
        '''
        :param repo: a local repo object
        :param branch_name: a branch_name of the repo param
        :return:
        # outlines = [b'      51 text files.\n',
        #             b'classified 51 files\r      51 unique files.                              \n',
        #             b'       8 files ignored.\n', b'\n',
        #             b'github.com/AlDanial/cloc v 1.74  T=0.17 s (279.6 files/s, 106924.9 lines/s)\n',
        #             b'-------------------------------------------------------------------------------\n',
        #             b'Language                     files          blank        comment           code\n',
        #             b'-------------------------------------------------------------------------------\n',
        #             b'Python                          42           1861           5119          10399\n',
        #             b'Markdown                         2            131              0            335\n',
        #             b'Dockerfile                       1             12              3             22\n',
        #             b'INI                              1             19             61              7\n',
        #             b'JSON                             1              0              0              7\n',
        #             b'-------------------------------------------------------------------------------\n',
        #             b'SUM:                            47           2023           5183          10770\n',
        #             b'-------------------------------------------------------------------------------\n']
        ;;;;
        # Another situation: outlines has no SUM line
        # outlines = ['       5 text files.\n',
        #             'classified 5 files\r       5 unique files.                              \n',
        #             '       5 files ignored.\n', '\n',
        #             'github.com/AlDanial/cloc v 1.74  T=0.01 s (82.9 files/s, 3150.6 lines/s)\n',
        #             '-------------------------------------------------------------------------------\n',
        #             'Language                     files          blank        comment           code\n',
        #             '-------------------------------------------------------------------------------\n',
        #             'Markdown                         1             12              0             26\n',
        #             '-------------------------------------------------------------------------------\n']
        '''

        repo_url = self.get_url_from_local_repo(repo)
        repo_dir = repo.working_dir.replace('\\', '/')

        try:
            execute = repo.git.execute(f'git checkout -f {branch_name}', shell=True)
        except Exception as exp:
            print(f'{self.thread_name} === Repo: {repo_url} has failed to checkout to branch_name: {branch_name}.\n'
                  f'Reason is:{exp.__repr__()}')
        outlines = self.execute_shell_command(f'cloc {repo_dir}')

        if not outlines and isinstance(outlines, bool):
            return []

        recordLines = []
        append_flag = False
        for line in outlines:
            ### May have no 'Language', please consider this situation
            line = line.strip()
            if isinstance(line, bytes):
                recordLine = str(line, 'utf-8')
            else:
                recordLine = str(line)

            if recordLine.startswith('Language'):
                append_flag = True

            if append_flag:
                with self.lock:
                    recordLines.append(recordLine)

            if recordLine.startswith('SUM'):
                append_flag = False

        if not recordLines:
            print(f'\n{self.thread_name} === Repository: {repo_url}; \tBranch: {branch_name} \thave no code '
                  f'records  collected.')
            return []

        try:
            if append_flag:
                recordLines = recordLines[2:-1]
            else:
                recordLines = recordLines[2:-2]

        except Exception as exp:
            print(f'\n{self.thread_name} === Repository: {repo_url}; \tBranch: {branch_name} \tcannot get code '
                  f'info because of no language code line was recorded.')
            return []

        each_branch_code_list = []
        for recordLine in recordLines:
            record_dict = self.parse_each_record_line(recordLine, repo_url, branch_name)
            each_branch_code_list.append(record_dict)

        print(f'\n{self.thread_name} === Repository: {repo_url}; \tBranch: {branch_name} \tHas collected '
              f'{len(each_branch_code_list)} records.')
        return each_branch_code_list
