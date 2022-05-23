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
import json
import os
import platform
import re
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


class GitCommit(object):

    def __init__(self, config=None):
        self.config = config
        self.thread_name = threading.currentThread().getName()
        self.lock = threading.Lock()
        self.platform_name = platform.system().lower()
        self.esClient = ESClient(config)

        self.community_name = config.get('community_name')
        self.index_name = config.get('index_name')
        self.url = config.get('es_url')
        self.company_info_index_name = config.get('company_info_index_name')
        self.authorization = config.get('authorization')
        self.default_headers = {'Content-Type': 'application/json', 'Authorization': self.authorization}
        self.token_v5 = config.get('gitee_token_v5')
        self.username = config.get('username')
        self.password = config.get('password')

        self.repo_source_dict = config.get('repo_source_dict')
        self.public_local_repo_path = config.get('public_local_repo_path', PUBLIC_LOCAL_REPO_PATH)
        self.is_fetch_all_branches = config.get('is_fetch_all_branches', 'False')
        self.is_remove_local_repo_immediate = config.get('is_remove_local_repo_immediate', 'True')
        self.user_yaml_url = config.get('user_yaml_url')
        self.company_yaml_url = config.get('company_yaml_url')
        self.end_collect_date_str = config.get('end_collect_date_str')
        self.max_thread_num_str = config.get('max_thread_num_str', '1')
        self.period_day_num_str = config.get('period_day_num_str', '3')

        self.yaml_companyInfo = self.generate_yaml_companyInfo()
        self.is_fetch_all_branches = False if not self.is_fetch_all_branches else \
            eval(self.is_fetch_all_branches)
        self.is_remove_local_repo_immediate = True if not self.is_remove_local_repo_immediate else \
            eval(self.is_remove_local_repo_immediate)
        self.max_thread_num = 1 if not self.max_thread_num_str else eval(self.max_thread_num_str)
        self.public_local_repo_path = PUBLIC_LOCAL_REPO_PATH if not self.public_local_repo_path else \
            self.public_local_repo_path
        self.test_repo_url_dict_dir = config.get('test_repo_url_dict_dir')

    def run(self, start_time):

        if start_time:
            self.from_date = datetime.strptime(start_time, "%Y%m%d")
        self.from_date_str = self.from_date.strftime("%Y-%m-%d")

        print(f"*************Begin to collect commits.  From :{self.from_date_str}***********")
        self.main_process()  # collect repo commit entrance
        self.from_date = datetime.today()  # Set start_time for next run time

        print(f"*********************Finish Collection*******************************")

    @common.show_spend_seconds_of_this_function
    def main_process(self):

        # <editor-fold desc="Created log dir">
        dest_dir = 'result_logs'
        is_result_log_exist = self.create_log_dir(dest_dir)
        if is_result_log_exist:
            print(f'log_dir exists.')
        else:
            print(f'create log_dir failure.')

        # </editor-fold>

        # <editor-fold desc="Initialize the several time variable">
        beijingTime_now = common.get_beijingTime()
        beijingTime_now_str = beijingTime_now.strftime('%Y-%m-%d %X')
        tomorrow = beijingTime_now.today() + timedelta(days=1)

        final_end_fetch_date = tomorrow
        if self.end_collect_date_str:
            end_collect_date = datetime.strptime(self.end_collect_date_str, '%Y%m%d')
            if end_collect_date < tomorrow:
                final_end_fetch_date = end_collect_date

        self.final_end_fetch_date = common.convert_to_localTime(final_end_fetch_date)
        self.final_end_fetch_date_str = final_end_fetch_date.strftime('%Y-%m-%d')
        # </editor-fold>

        if not self.yaml_companyInfo:
            print(f'There are no company information provided from config.ini')

        # <editor-fold desc="Set several log file directory">
        acquired_total_repos_log = dest_dir + os.sep + 'acquired_total_repos_log.txt'
        tracked_repo_log_dir = dest_dir + os.sep + 'tracked_repos_log.txt'
        success_processed_repos_log_dir = dest_dir + os.sep + 'success_processed_repos_log.txt'
        success_stored_repos_log_dir = dest_dir + os.sep + 'success_stored_repos_log.txt'
        failed_process_repos_log_dir = dest_dir + os.sep + 'failed_process_repos_log.txt'
        empty_commit_repos_log_dir = dest_dir + os.sep + 'empty_commit_repos_log.txt'
        missed_repos_log_dir = dest_dir + os.sep + 'missed_repos_repos_log.txt'
        # </editor-fold>

        # <editor-fold desc="Collect functions and repos for multi-thread">
        thread_func_args = self.get_thread_funcs()
        # </editor-fold>

        ## Set initial state before processing
        self.total_repos_count = len(set(self.repo_url_dict))

        # <editor-fold desc="Store self.repo_url_dict into local file">
        with open(file=acquired_total_repos_log, mode='w', encoding='utf-8') as f:
            f.write(f'There are {self.total_repos_count} repos were collected from sources:\n')
            f.mode = 'a'
            for repo_url in self.repo_url_dict:
                f.write(repo_url + f': {self.repo_url_dict[repo_url]}\n')
        # </editor-fold>

        # <editor-fold desc="Initialize several result global variable">
        self.total_commit_count = 0
        self.reside_process_repo_count = self.total_repos_count

        self.failed_prepare_repo_dict = {}
        self.empty_commit_repo_url_list = []
        self.success_store_es_repo_commit_info_dict = {}

        # </editor-fold>

        print(f"From date: {self.from_date_str}, Collected {self.total_repos_count} unique "
              f"repositories after removed duplicate ones in  {self.community_name} community totally.\n")

        # <editor-fold desc="Run multi-thread of collecting functions and store result to es">
        common.writeDataThread(thread_func_args, max_thread_num=self.max_thread_num)
        # </editor-fold>

        # <editor-fold desc="Store result log info into local file">

        # <editor-fold desc="Store success_processed repos to local file">
        success_processed_repo_list = []
        success_processed_repo_list.extend(self.empty_commit_repo_url_list)
        success_processed_repo_list.extend(list(set(self.success_store_es_repo_commit_info_dict)))
        success_processed_repo_count = len(success_processed_repo_list)
        with open(file=success_processed_repos_log_dir, mode='w') as f:
            f.write(f'success processed {success_processed_repo_count} repos: \n')
            f.mode = 'a'
            f.write('\n'.join(success_processed_repo_list))
        # </editor-fold>

        # <editor-fold desc="Store tracked_repo_url_list to local file">
        tracked_repo_url_list = []
        tracked_repo_url_list.extend(success_processed_repo_list)
        tracked_repo_url_list.extend(self.failed_prepare_repo_dict)
        tracked_repo_count = len(tracked_repo_url_list)

        with open(file=tracked_repo_log_dir, mode='w', encoding='utf-8') as f:
            f.write(f'There are {tracked_repo_count} repos were tracked:\n')
            f.mode = 'a'
            f.write('\n'.join(tracked_repo_url_list))
        # </editor-fold>

        # <editor-fold desc="Store succeed_stored repos to local file">
        succeed_store_repos_count = len(self.success_store_es_repo_commit_info_dict)
        with open(file=success_stored_repos_log_dir, mode='w', encoding='utf-8') as f:
            f.write(f'Total {succeed_store_repos_count} repos: \n')
            f.mode = 'a'
            for repo_url in self.success_store_es_repo_commit_info_dict:
                record_line = f'{repo_url}: {self.success_store_es_repo_commit_info_dict[repo_url]}\n'
                f.write(record_line)

        # </editor-fold>

        # <editor-fold desc="Store failed_clone_repos to local file">
        self.failed_prepare_repo_count = len(self.failed_prepare_repo_dict)
        if self.failed_prepare_repo_dict:
            with open(file=failed_process_repos_log_dir, mode='w', encoding='utf-8') as f:
                f.write(f'There are {self.failed_prepare_repo_count} failed prepared repos:\n')
                f.mode = 'a'
                for repo_url in self.failed_prepare_repo_dict:
                    f.write(repo_url)
                    f.write(self.failed_prepare_repo_dict[repo_url] + '\n\n')
        # </editor-fold>

        # <editor-fold desc="Store empty_commit_repo_url_list to local file">
        empty_repo_count = len(self.empty_commit_repo_url_list)
        if self.empty_commit_repo_url_list:
            with open(file=empty_commit_repos_log_dir, mode='w', encoding='utf-8') as f:
                f.write(f'There are {empty_repo_count} empty repos:\n')
                f.mode = 'a'
                f.write('\n'.join(self.empty_commit_repo_url_list))
        # </editor-fold>

        # <editor-fold desc="Store missed_repo_list to local file">
        missed_repo_list = list(set(self.repo_url_dict).difference(set(tracked_repo_url_list)))
        missed_repo_count = len(missed_repo_list)
        if missed_repo_list:
            with open(file=missed_repos_log_dir, mode='w', encoding='utf-8') as f:
                f.write(f'Missed {missed_repo_count} repos: \n')
                f.mode = 'a'
                f.write('\n'.join(missed_repo_list))
        # </editor-fold>

        # </editor-fold  show all>

        # <editor-fold desc="Calculate the total collected commits">
        for repo_url in self.success_store_es_repo_commit_info_dict:
            self.total_commit_count += self.success_store_es_repo_commit_info_dict[repo_url]
        # </editor-fold>

        # <editor-fold desc="Show result infomation after running">
        print(f'\n\nGame Over!\tProgram result info as follows:')
        print(f'Start time of program:\t{beijingTime_now_str}')
        print(f'From date: {self.from_date_str};\tend date: {self.final_end_fetch_date_str}')
        print(f'Community name: {self.community_name}')
        print(f'Total repos count from sources: {self.total_repos_count}')
        print(f'Total repos which successfully tracked in whole: {tracked_repo_count}')
        print(f'Total processed repos count: {success_processed_repo_count}')
        print(f'Total repos which successfully stored into ES: {succeed_store_repos_count}')
        print(f'Total repos which have empty commit cannot store to ES: {empty_repo_count}')
        print(f'Failed to prepare (clone or pull) for  local repos: {self.failed_prepare_repo_count}')
        print(f'Total missing repos: {missed_repo_count}')
        print(f'Total commits from successfully processed repos: {self.total_commit_count}')
        print(f'Stored ES index name: {self.index_name}')
        print(f'\nPlease check the detail info from local run log files.\n')
        # </editor-fold>

    def get_thread_funcs(self):

        '''
        prepare thread function args for multiple thread running
        :param from_date_str:
        :return: thread_func_args
        '''

        thread_func_args = {}
        values = []
        if self.test_repo_url_dict_dir:
            self.repo_url_dict = self.get_repos_from_local_file(False)
        else:
            self.repo_url_dict = self.get_repos_from_sources(self.repo_source_dict)

        for repo_url in self.repo_url_dict:
            values.append((repo_url, self.repo_url_dict[repo_url]))

        thread_func_args[self.process_each_repo] = values

        return thread_func_args

    @common.show_spend_seconds_of_this_function
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
                  f'various souces.')
            return general_repo_url_dict
        except Exception as e:
            print(f'\n{self.thread_name} === Occur error at function: {sys._getframe().f_code.co_name}')
            raise e

    @common.show_spend_seconds_of_this_function
    def process_each_repo(self, repo_url, is_all_branches):
        self.thread_name = threading.current_thread().getName()
        repo_name = repo_url.split('/')[-1]
        with self.lock:
            self.reside_process_repo_count -= 1
        print(f'\n{self.thread_name} === Progress: processing the {repo_url}; Has {self.reside_process_repo_count} '
              f'repos left.')
        repo = None

        result = self.prepare_local_repo(repo_url)  # Clone repo to local

        if isinstance(result, git.repo.base.Repo):
            repo = result
        elif isinstance(result, str):
            print(f'{self.thread_name} === Failed to prepare the repo: {repo_url}')
            with self.lock:
                self.failed_prepare_repo_dict[repo_url] = result
                return
        self.track_thread_log(repo_name)

        repo_commit_list = self.get_commit_from_each_repo(repo, is_all_branches)  # Get commits from each branch

        # store a single repo data into ES
        self.push_repo_data_into_es(self.index_name, repo_commit_list, repo)

        # region Alter the global variable
        repo_commit_count = len(repo_commit_list)
        if repo_commit_count == 0:
            self.empty_commit_repo_url_list.append(repo_url)
        elif repo_commit_count > 0:
            self.success_store_es_repo_commit_info_dict[repo_url] = repo_commit_count
        # endregion

        ### Delete local repo after get its commit to reserve enough space for other repo
        if self.is_remove_local_repo_immediate:
            self.remove_local_repo(repo.working_tree_dir)
            print(f'{self.thread_name} === Repo {repo_name} has been removed.')

        print(f'{self.thread_name} === Completed process repo:{repo_name};\tCollected {len(repo_commit_list)} '
              f'commits in all.\n')

    def get_repo_branch_names(self, repo, is_all_branches):
        repo_name = repo.working_dir.split(os.sep)[-1]
        if is_all_branches:  ## Accquire all remote branch names
            remote_branch_names = [branch_name.remote_head for branch_name in repo.remote().refs]
            remote_branch_names.remove('HEAD')
            branch_names = remote_branch_names
            print(f'{self.thread_name} === Get {len(branch_names)} branch names of repo:  {repo_name}.')
        else:
            branch_names = [repo.head.reference.name]  ## Accquire current branch names
            print(f'{self.thread_name} === Get the current branch of {repo_name}')

        return branch_names

    def push_repo_data_into_es(self, index_name, repo_data_list, repo):
        repo_url = self.get_url_from_local_repo(repo)

        action = ''
        for commit_body in repo_data_list:
            branch_name = commit_body['branch_name']
            commit_id = commit_body['commit_id']
            id_text = repo_url + '_' + branch_name + '_' + commit_id
            id = hashlib.sha256('{}\n'.format(id_text).encode('utf-8')).hexdigest()

            commit_log = common.getSingleAction(index_name, id, commit_body)
            action += commit_log

        if self.lock:
            print(f'{self.thread_name} === Starting to store {len(repo_data_list)} commits of {repo_url} into ES ....')
            self.esClient.safe_put_bulk(action)
            print(f'{self.thread_name} === Successfully Stored {len(repo_data_list)} commits of {repo_url} into ES.')

    @common.show_spend_seconds_of_this_function
    def prepare_local_repo(self, repo_url):
        '''
        Prepare a local repo object for parsing
        :param repo_url: the remote url of a repo
        demo: https://github.com/postgres/postgres
        :return: repo object
        '''
        username = base64.b64decode(self.username).decode()
        passwd = base64.b64decode(self.password).decode()

        repo_name = repo_url.split("/")[-1]
        website = repo_url.split("/")[2]
        org = repo_url.split("/")[-2]
        local_repo_file_name = repo_url.split('://')[-1].replace('/', '-')
        local_repo_path = self.public_local_repo_path + '/' + local_repo_file_name
        repo_dir = Path(local_repo_path)
        local_repo = None

        if not username or not passwd:
            print(f'{self.thread_name} === Have not fetch username or passwd from config.ini.\n')
            return 'Lack of username or passwd'
        clone_url = 'https://' + username + ':' + passwd + '@' + website + '/' + org + '/' + repo_name

        if repo_dir.exists():
            try:
                local_repo = git.Repo(local_repo_path)
                check_repo = self.is_dir_git_repo(local_repo_path)
            except Exception as ex:
                print(f'{self.thread_name} === local_repo_path cannot be build as git.Repo in'
                      f' {sys._getframe(1).f_code.co_name}, Error as follows:\n{ex.__repr__()}')
                return ex.__repr__()
            if check_repo:
                try:
                    print(f'{self.thread_name} === Pulling repo {repo_url}, since it has existed...')
                    with self.lock:
                        local_repo.remote().pull()
                        print(f'{self.thread_name} === Pulled the repo: {repo_url} successfully.')
                except Exception as ex:
                    print(f'{self.thread_name} === Failed to pull existed repo:{repo_url}\n'
                          f' Exception : {ex.__repr__()}')
                    return ex.__repr__()
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
                            ## Restore all files in the repo
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
                        return 'Authentication failure'

                    if gitError.stderr.__contains__('not found'):
                        print(f'{self.thread_name} === Failed to clone {repo_url} repo since cannot reach from'
                              f' this repo_url')
                        return 'Cannot reach from this repo_url since it is private.'

                print(f'{self.thread_name} === Unpredicted GitCommandError, stderr as follows: {gitError.stderr}')
                return gitError.stderr

            except Exception as cloneEx:
                print(f'{self.thread_name} === Occurs unexpected error besides GitCommmandError '
                      f'while clone repo: {repo_url}. \nThe Error is:{repr(cloneEx)}')
                return cloneEx.__repr__()

        return local_repo

    @common.show_spend_seconds_of_this_function
    def get_commit_from_each_repo(self, repo, is_all_branches):
        if repo is None:
            return None
        repo_name = repo.working_dir.split(os.sep)[-1]
        repo_commit_list = []
        repo_url = self.get_url_from_local_repo(repo)
        branch_names = self.get_repo_branch_names(repo, is_all_branches)  # ensure branch name, then get its commits.

        self.track_thread_log(repo_name)

        for branch_name in branch_names:
            branch_commits = self.get_commits_from_each_branch(branch_name, repo)
            repo_commit_list.extend(branch_commits)

        print(f'\n{self.thread_name} === Repository: {repo_url}; \t{len(repo_commit_list)} commits has been'
              f' collected, from {self.from_date_str}\t to {self.final_end_fetch_date_str}')

        return repo_commit_list

    @common.show_spend_seconds_of_this_function
    def get_commits_from_each_branch(self, branch_name, repo):
        repo_url = self.get_url_from_local_repo(repo)

        # <editor-fold desc="Switch to current branch">
        try:
            self.remove_git_index_lock_file(repo)  ## Should remove index.lock before checkout branch
            repo.git.execute(f"git checkout -f {branch_name}", shell=True)
        except Exception as exp:
            if branch_name != repo.head.reference.name:
                print(f'{self.thread_name} === Failed to switch to branch name: {branch_name}. '
                      f'Error is: {repr(exp)}\n')
                return []
            else:
                print(f'{self.thread_name} === Although occur some error in checkout process, checkout to '
                      f'{branch_name} successfully at last.')
        # </editor-fold>

        # <editor-fold desc="Prepare start time and time window">

        from_date = datetime.strptime(self.from_date_str, '%Y-%m-%d')
        start_fetch_date = common.convert_to_localTime(from_date)

        if not self.period_day_num_str:
            period_day_num = 3
        else:
            period_day_num = eval(self.period_day_num_str)
        # </editor-fold>

        # <editor-fold desc="Fetch commits based on time window between start time to end time">
        branch_commits = []
        while True:  # pull, parse, assemble commit records
            start_fetch_date_str = start_fetch_date.strftime('%Y-%m-%d %X')
            end_fetch_date = start_fetch_date + timedelta(days=period_day_num)

            if end_fetch_date > self.final_end_fetch_date:
                end_fetch_date = self.final_end_fetch_date

            end_fetch_date_str = end_fetch_date.strftime('%Y-%m-%d %X')
            commit_log_dict = self.get_period_commit_log(repo, end_fetch_date_str, start_fetch_date_str)
            branch_period_commits = self.process_commit_log_dict(commit_log_dict, repo)

            print(f'{self.thread_name} === Repository: {repo_url};\tBranch_name: {branch_name};\tfrom date: '
                  f'{start_fetch_date_str};\tend date: {end_fetch_date_str}.\t {len(branch_period_commits)} ' \
                  'commits has been collected. ')

            branch_commits.extend(branch_period_commits)
            start_fetch_date = end_fetch_date

            if end_fetch_date == self.final_end_fetch_date:
                print(f'Reach deadline, collecting is over.')
                break
        print(f'\n{self.thread_name} === Repository: {repo_url};\tBranch_name: {branch_name};\t{len(branch_commits)} ' \
              'commits has been collected.')
        # </editor-fold>
        return branch_commits

    def get_period_commit_log(self, repo, start_date_str, end_date_str):
        log_dict = {}
        logcmd_base = f'git log --after="{end_date_str}" --before="{start_date_str}" --shortstat --pretty=format:"@@@***@@@%n%an;;;%n%ae;;;%n%cd;;;%n%H;;;%n%s;;;%n%b;;;%n%N"' \
                      f' --date=format:"%Y-%m-%dT%H:%M:%S%z"'
        logcmd_merge = logcmd_base + ' --merges'
        logcmd_no_merge = logcmd_base + ' --no-merges'

        no_merge_log = ''
        try:
            no_merge_log = repo.git.execute(logcmd_no_merge, shell=True)
        except Exception as ex:
            print(f'{self.thread_name} === logcmd_no_merge execute failed, caused by: {ex.__repr__()}')
        merge_log = ''
        try:
            merge_log = repo.git.execute(logcmd_merge, shell=True)
        except Exception as ex:
            print(f'{self.thread_name} === logcmd_merge execute failed, caused by: {ex.__repr__()}')

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

    def parse_each_commit_log(self, repo, commit_log, is_merged):

        '''
        :param repo: local repo object comes from GitPython
        :param commit_log:
            specific_user_name;;;
            specific_email@huawei.com;;;
            Thu Sep 3 19:52:22 2020 +0800;;;
            2c2bb377080f6e962982749340d43f1700c89;;;
            sig优化;;;
            sig优化
            ;;;
            1 file changed, 1 insertion(+), 1 deletion(-)
        :param is_merged: 0 or 1, marked the commit_log is merged or no merged commit
        :return: format output data, like follows:
            {
            'created_at': '2021-07-27T01:32:01+00:00',
            'author': 'opengauss-bot',
            'company': 'Huawei',
            'email': 'person_email@huawei.com',
            commit_id': '8a98a77d57b32b84422345fagdf82095615732d3636cc7',
            'file_changed': 15,
            'add': 12,
            'remove': 21,
            'total': 33,
            'tilte': '!4 bugfix Merge pull request !4 from chenxiaobin/master',
            'is_merged': 1,
            'commit_main_content': '',
            'branch_name': 'master',
            'project': 'openGauss-tools-ora2og',
            'repo_org': 'opengauss-mirror',
            'repo_url': 'https://github.com/opengauss-mirror/openGauss-tools-ora2og'
            }
        '''

        if not commit_log:
            return None
        commit_log = commit_log.strip()
        branch_name = repo.head.reference.name
        repo_name = repo.working_tree_dir.split(os.sep)[-1]
        split_list = commit_log.split(";;;")

        raw_author = split_list[0].strip()
        email = split_list[1].strip()
        unified_author = self.get_author(raw_author, email)
        commit_datetime_raw_str = split_list[2].strip()
        commit_beijing_datetime_str = self.trans_datetime_to_beijingTime(commit_datetime_raw_str)
        company_name = self.get_company_name(email)
        commit_id = split_list[3].strip()
        title = split_list[4].strip()
        commit_content = split_list[5].strip()

        commit_log_modifying_info = split_list[6].strip()
        commit_log_modifying_dict = self.parse_modifying_info(commit_log_modifying_info)

        ## find out contributor & reviewer
        commit_contributors = self.get_role_from_commit_content(commit_content, 'openEuler_contributor')
        commit_reviewers = self.get_role_from_commit_content(commit_content, 'openEuler_reviewer')
        repo_url = self.get_url_from_local_repo(repo)
        repo_org = repo_url.split('/')[-2]
        result = {'created_at': commit_beijing_datetime_str,
                  'raw_datetime': commit_datetime_raw_str,
                  'author': raw_author,
                  'unified_author': unified_author,
                  'company': company_name,
                  'email': email,
                  'commit_id': commit_id,
                  'file_changed': commit_log_modifying_dict.get('file_changed'),
                  'add': commit_log_modifying_dict.get('lines_added'),
                  'remove': commit_log_modifying_dict.get('lines_removed'),
                  'total': commit_log_modifying_dict.get('total'),
                  'tilte': title,
                  'is_merged': is_merged,
                  'title': title,
                  'commit_main_content': commit_content,
                  'branch_name': branch_name,
                  'project': repo_name,
                  'repo': repo_url,
                  'repo_name': repo_name,
                  'repo_url': repo_url,
                  'repo_org': repo_org
                  }
        if commit_contributors:
            result['commit_contributors'] = commit_contributors
        if commit_reviewers:
            result['commit_contributors'] = commit_reviewers

        return result

    def get_url_from_local_repo(self, repo):
        local_dir = repo.working_tree_dir
        repo_name = local_dir.split(os.sep)[-1]
        remote_url = ''
        try:
            remote_url = repo.remote().urls.__next__()
        except Exception as exp:
            f'\n{self.thread_name} === Cannot get repo_url: {sys._getfram().f_code.co_name}'
            print(f'Error: {exp.__repr__()}')
        if remote_url.__contains__('@'):
            address = remote_url.split('@')[-1]
            common_url = 'https://' + address
        else:
            common_url = remote_url
        return common_url

    def parse_modifying_info(self, info_line):
        '''
        :param info_line: info string
            2 file changed, 5 insertion(+), 7 deletion(-)
        :return:
            {'file_changed': 2, 'lines_added': 5, 'lines_removed': 7, 'total': 12}
        '''

        modify_info = {}

        if not info_line:
            return modify_info

        file_changed = 0
        lines_added = 0
        lines_removed = 0
        info_line = info_line.strip()
        if info_line.__contains__('file'):
            change_file_info_str = info_line.split('file')[0]
            file_changed = int(change_file_info_str.strip())
        if info_line.__contains__('insertion'):
            lines_added_info_str = info_line.split('insertion')[0].split(',')[-1]
            lines_added = int(lines_added_info_str.strip())
        if info_line.__contains__('deletion'):
            lines_removed_info_str = info_line.split('deletion')[0].split(',')[-1]
            lines_removed = int(lines_removed_info_str.strip())

        modify_info["file_changed"] = file_changed
        modify_info["lines_added"] = lines_added
        modify_info["lines_removed"] = lines_removed
        modify_info['total'] = lines_added + lines_removed
        return modify_info

    def get_company_name(self, email):
        company_name = None
        if self.company_info_index_name:
            company_name = self.get_company_name_from_es(index_name=self.company_info_index_name,
                                                         field_name='email.keyword', keyword=email)
        if not company_name and self.yaml_companyInfo:
            company_name = self.get_companyName_from_YamlCompanyInfo(email)

        return company_name

    def get_companyName_from_YamlCompanyInfo(self, email):

        if not self.yaml_companyInfo:
            ## There are no proper company information from give online yaml.
            return 'Empty CompanyInfo'

        company_name = 'No Company_name'
        email_tag = email.split("@")[-1]
        users = self.yaml_companyInfo.get('users').get('users')
        domain_companies = self.yaml_companyInfo.get('domains_company_list')
        alias_companies = self.yaml_companyInfo.get('aliases_company_list')

        for user in users:
            if email in user["emails"]:
                # Get unique company_name from alias_companies
                company_name = alias_companies.get(user["companies"][0]["company_name"])
                return company_name

        # if email not in self.users's email list, then look for company name by email domain
        if domain_companies.get(email_tag):
            company_name = domain_companies.get(email_tag)
            return company_name

        ## Find nothing after traverse the self.companyInfo
        return company_name

    def generate_yaml_companyInfo(self):
        companyInfo = {}

        if not self.user_yaml_url or not self.company_yaml_url:
            return companyInfo

        users = {}
        companies = {}
        try:
            user_filename = self.get_yaml_file_name_from_url(self.user_yaml_url)
            company_filename = self.get_yaml_file_name_from_url(self.company_yaml_url)
            if self.platform_name == 'linux':  ## Get data.yaml and company.yaml from Gitee in linux.
                cmd = 'wget -N %s' % self.user_yaml_url
                p = os.popen(cmd.replace('=', ''))
                p.read()

                users = yaml.safe_load(open(user_filename, encoding='UTF-8'))
                cmd = 'wget -N %s' % self.company_yaml_url
                p = os.popen(cmd.replace('=', ''))
                p.read()
                companies = yaml.safe_load(open(company_filename, encoding='UTF-8'))
                p.close()
            elif self.platform_name == 'windows':  ###Test in windows without wget command
                user_yaml_response = self.esClient.request_get(self.user_yaml_url)
                company_yaml_response = self.esClient.request_get(self.company_yaml_url)
                if user_yaml_response.status_code != 200 or company_yaml_response.status_code != 200:
                    print('Cannot connect the address of data.yaml or company.yaml online. then return'
                          ' the empty companyInfo dict.')
                    return companyInfo

                users = yaml.load(user_yaml_response.text, Loader=yaml.FullLoader)
                companies = yaml.load(company_yaml_response.text, Loader=yaml.FullLoader)
        except:
            print('Failed to get data.yaml or company.yaml online. then return the empty companyInfo '
                  'dict.')
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

    def get_company_name_from_es(self, index_name, field_name, keyword):
        company_name = None
        if not index_name:
            return company_name

        url = self.url + '/' + index_name + '/_doc' + '/_search'
        request_dict = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                            }
                        }
                    ]
                }
            }
        }

        request_dict['query']['bool']['must'][0]['match'][field_name] = keyword
        payload = json.dumps(request_dict)

        try:
            res = requests.post(url=url, headers=self.default_headers, verify=False, data=payload)
            res_content = json.loads(res.content)
            if res.status_code != 200:
                print(f'{self.thread_name} === {sys._getframe(1).f_code.co_name} Failed get '
                      f'right response data because of unproper request params; \nFailure reason:'
                      f'\t{res_content}')

            company_info = res_content['hits']['hits']
            if not company_info: return 'No Company_name'

            company_name = company_info[0].get('_source').get('organization')
        except Exception as exp:
            print(f'{self.thread_name} === Encounter exception when get company name from email : {keyword}; \n'
                  f'Exception as follows:\t{exp.__repr__()}')

        return company_name

    def get_author(self, raw_author, email):
        author = raw_author

        if not self.yaml_companyInfo:
            return author

        users = self.yaml_companyInfo.get('users').get('users')
        if not users:
            return author

        for user in users:
            if email in user['emails']:
                author = user['user_name']
                break
        return author

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

        ## all_branches_repo_url_dict is set fetch all branches commits of this repo
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
            print(ex.__repr__())
            print(f'{self.thread_name} === Error!!!  Failed to remove local repo directory.\n')
            return False
        return True

    def get_role_from_commit_content(self, content, role_keyword):

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
            print(f'{self.thread_name} === Occur error at function: {sys._getframe().f_code.co_name}')
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

    def create_log_dir(self, dest_dir):
        '''
        create a local dir object under root project dir
        :param dest_dir: the wanted dir name
        :return: local dir object
        '''
        windows_commandline = f'md {dest_dir}'
        linux_commandline = f'mkdir -p {dest_dir}'

        if os.path.exists(dest_dir):
            return True

        if self.platform_name == 'windows':
            exec_out_file = subprocess.Popen(windows_commandline, shell=True, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
        elif self.platform_name == 'linux':
            exec_out_file = subprocess.Popen(linux_commandline, shell=True, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)

        if os.path.exists(dest_dir):
            return True
        else:
            print(f'Failed to create log dir for some reasons')
            return False

    def get_repos_from_local_file(self, is_branch_all):
        '''
        get repo_url_dict from local file for test
        :param is_branch_all: where collect all branches from the specific repo
        :return: repo_url_dict
        '''
        repo_url_dict = {}
        with open(file=self.test_repo_url_dict_dir, mode='r', encoding='utf-8') as f:
            repos_lines = f.readlines()
        for line in repos_lines:
            repo_url_dict[line.strip()] = is_branch_all

        return repo_url_dict
