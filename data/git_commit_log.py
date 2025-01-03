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
import datetime
import hashlib
import json
import os
import re
import types
from collections import defaultdict
from json import JSONDecodeError

import git
import requests
import yaml
from git import GitCommandError

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data.common import ESClient

GITEE_BASE = "gitee.com"
GITHUB_BASE = "github.com"
HUGGINGFACE_BASE = "huggingface.co"
CODEARTS_BASE = "codehub.devcloud.cn-southwest-2.huaweicloud.com"  # codearts域名
DEFAULT_BRANCH_HEAD = "  origin/HEAD ->"


class GitCommitLog(object):
    def __init__(self, config=None):
        self.config = config
        self.org = config.get('org') # 组织名称
        self.index_name = config.get('index_name') # ES索引名称
        self.code_base_path = config.get('code_base_path') #代码拉取到本地的存储根目录
        self.platform_owner_token = config.get('platform_owner_token')
        self.start_date = config.get('start_date')
        self.end_date = config.get('end_date')
        self.before_days = config.get('before_days')
        self.user_commit_name = config.get('user_commit_name')
        self.gitee_repo_branch = config.get('gitee_repo_branch')
        self.github_repo_branch = config.get('github_repo_branch')
        self.codearts_repo_branch = config.get('codearts_repo_branch') # 对codearts仓库指定的分支配置
        self.username = config.get('username')
        self.password = config.get('password')
        self.write_bulk = int(config.get('write_bulk', 1000)) # 写入ES的批次大小，默认1000。累积到1000后，一次bulk写入
        self.esClient = ESClient(config) # 创建ESClient 实例，用来执行 bulk 操作和各种查询

        self.username = config.get('username')
        self.password = config.get('password')
        self.write_bulk = int(config.get('write_bulk', 1000)) # 写入ES的批次大小，默认1000。累积到1000后，一次bulk写入
        self.esClient = ESClient(config)
        self.email_orgs_dict = {}
        self.domain_orgs_dict = {}
        self.repo_sigs_dict = defaultdict(dict)
        self.white_box_yaml = config.get('white_box_yaml') # 白名单指定仓库的YAML路径
        self.codearts_yaml = config.get('codearts_yaml', 'true')
        self.upstream_yaml = config.get('upstream_yaml')
        self.user_file = config.get('user_file')
        self.company_yaml = config.get('company_yaml')
        self.model_repo_yaml = config.get('model_repo_yaml')
        self.all_repo_default_branch = config.get('all_repo_default_branch', 'false') # 是否只拉取所有仓库的默认分支
        self.all_repo_default_branch = config.get('all_repo_default_branch', 'false') # 是否只拉取
        self.is_gitee_enterprise = config.get('is_gitee_enterprise')
        self.huggingface_access_token = config.get('huggingface_access_token')
        self.github_access_token = config.get('github_access_token')
        self.gitee_access_token = config.get('gitee_access_token')
        self.codearts_access_token = config.get('codearts_access_token')  # 新增CodeARts访问Token
        self.tokens = config.get('tokens').split(',') if config.get('tokens') else None
        

        self.email_user_dict = {}

    def run(self, from_time):
        print("Git commit log collect: start")
        # 邮箱 -> 企业
        self.email_orgs_dict = self.esClient.getOrgByEmail()

        # 仓库对应的sig
        self.repo_sigs_dict = self.esClient.getRepoSigs()

        # 配置默认获取最近 <before_days> 天的数据
        if self.start_date is None and self.before_days:
            self.start_date = datetime.date.today() + datetime.timedelta(days=-int(self.before_days))

        if self.white_box_yaml:  # white box 指定仓库
            all_branch_repos, default_branch_repos = self.getReposFromYaml(yaml_file=self.white_box_yaml)
            self.getCommitWhiteBox(default_branch_repos, 'default')  # 只是获取默认分支commit
            self.getCommitWhiteBox(all_branch_repos)  # 获取全部分支commit
        elif self.upstream_yaml:  # upstream 指定仓库
            self.email_orgs_dict, self.domain_orgs_dict, self.email_user_dict = self.getUpstreamCompany(
                user_file=self.user_file, company_yaml=self.company_yaml)
            all_branch_repos, default_branch_repos = self.getReposFromYaml(yaml_file=self.upstream_yaml)
            self.getCommitWhiteBox(default_branch_repos, 'default')  # 只是获取默认分支commit
            self.getCommitWhiteBox(all_branch_repos)  # 获取全部分支commit
            self.update_company_changed()
        elif self.model_repo_yaml:  # 大模型指定仓库
            self.domain_orgs_dict, aliases_company_dict = self.get_domain_org(company_yaml=self.company_yaml)
            all_branch_repos, default_branch_repos = self.getReposFromYaml(yaml_file=self.model_repo_yaml)
            self.getCommitWhiteBox(default_branch_repos, 'default')
            self.getCommitWhiteBox(all_branch_repos)
        # elif self.codearts_yaml:
        #     default_branch_repos = ['http://codehub.devcloud.cn-southwest-2.huaweicloud.com/709968f4a69145deba5559c5faf4eca8/openlibing-gateway']
        #     all_branch_repos = ['http://codehub.devcloud.cn-southwest-2.huaweicloud.com/709968f4a69145deba5559c5faf4eca8/openlibing-gateway']
        #     self.getCommitWhiteBox(default_branch_repos, 'default')  # 只是获取默认分支commit
        #     self.getCommitWhiteBox(all_branch_repos)
        elif self.codearts_yaml:  # 针对 CodeArts 平台的指定仓库
            all_branch_repos, default_branch_repos = self.getReposFromYaml(yaml_file=self.codearts_yaml)
            self.getCommitWhiteBox(default_branch_repos, 'default')  # 只是获取默认分支 commit
            self.getCommitWhiteBox(all_branch_repos)  # 获取全部分支 commit
        else:
            # 上述配置都不存在，默认获取platform_owner_token对应的所有仓库commit
            self.getCommit()

    def getCommit(self):
        # 代码托管平台 gitee or github
        for items in self.platform_owner_token.split(';'):
            if not str(items).__contains__('->'):
                continue
            vs = items.split('->')
            platform = vs[0]
            owner = vs[1]
            token = None if vs[2] == '' else vs[2]

            # 指定了仓库则获取指定仓库数据，否则获取owner下的所有仓库
            repos = []
            # 使用CodeArtsClients获取codearts仓库列表
            if platform == 'codearts': 
                if self.codearts_repo_branch:
                    repos = self.codearts_repo_branch.split(';')
                else:
                    repos = self.codearts_repos(owner=owner, token=token)
            elif platform == 'gitee':
                if self.gitee_repo_branch:
                    repos = self.gitee_repo_branch.split(';')
                else:
                    repos = self.gitee_repos(owner=owner, token=token)
            elif platform == 'github':
                if self.github_repo_branch:
                    repos = self.github_repo_branch.split(';')
                else:
                    repos = self.github_repos(owner=owner, token=token)
            else:
                # 预留其它平台扩展
                continue

            for repo in repos:
                if not str(repo).__contains__('->'):
                    continue
                rb = repo.split('->')
                branch_name = rb[1]

                # 配置如果是self.all_repo_default_branch == 'true',只获取默认分支
                if self.all_repo_default_branch == 'true':
                    branch_name = 'default'
                try:
                    self.getLog(platform, owner, repo_name=rb[0], branch_name=branch_name)
                except Exception:
                    print('*** platform: %s, owner: %s, repo: %s, fail ***' % (platform, owner, repo))

    def getLog(self, platform, owner, repo_name, branch_name, path=None):
        """
        执行克隆操作
        """
        # 本地仓库目录
        owner_path = self.code_base_path + platform + os.sep + owner + os.sep
        if not os.path.exists(owner_path):
            os.makedirs(owner_path)
        code_path = owner_path + repo_name

        username = base64.b64decode(self.username).decode()
        print(platform)

        # 托管平台格式构造远程仓库URL
        if platform == 'gitee':
            remote_repo = 'https://%s/%s/%s' % (GITEE_BASE, owner, repo_name)
            clone_url = 'https://%s:%s@%s/%s/%s' % (username, self.gitee_access_token, GITEE_BASE, owner, repo_name)
        elif platform == 'github':
            remote_repo = 'https://%s/%s/%s' % (GITHUB_BASE, owner, repo_name)
            clone_url = 'https://%s:%s@%s/%s/%s' % (username, self.github_access_token, GITHUB_BASE, owner, repo_name)
        elif path and platform == 'huggingface':
            remote_repo = 'https://%s/%s' % (HUGGINGFACE_BASE, path)
            clone_url = 'https://%s:%s@%s/%s' % (
                username, self.huggingface_access_token, HUGGINGFACE_BASE, path)
        elif platform in CODEARTS_BASE:  # CodeArts的URL构造
            remote_repo = f"https://{CODEARTS_BASE}/{owner}/{repo_name}"
            clone_url = f"https://{username}:{self.codearts_access_token}@{CODEARTS_BASE}/{owner}/{repo_name}"
            print(clone_url)
        else:
            remote_repo = None
            clone_url = None

        # 本地仓库已存在执行git pull；否则执行git clone
        self.removeGitLockFile(code_path)
        if not os.path.exists(code_path):
            if clone_url is None:
                return
            cmd_clone = 'cd %s;git clone %s' % (owner_path, clone_url + '.git')
            os.system(cmd_clone)

        # 用GitPython加载本地仓库
        try:
            repo = git.Repo(code_path)
            repo.git.remote('prune', 'origin') # 清理远程无效分支
        except Exception:
            print('*** repo clone fail: %s' % remote_repo)
            return

        self.reset_remote_url(repo, clone_url)

        # 如果指定分支为default，则指定分支为默认分支
        default_branch = 'master'
        branchs = repo.git.branch('-r').split('\n')
        for b in branchs:
            if b.startswith(DEFAULT_BRANCH_HEAD):
                default_branch = b.replace(DEFAULT_BRANCH_HEAD, '').split('/', 1)[1]
                break
        if branch_name == 'default':
            branch_name = default_branch

        # 分支不为空，代表获取指定分支，否则遍历所有远程分支
        if branch_name != '':
            print('*** start %s repo: %s/%s; branch: %s ***' % (platform, owner, repo_name, branch_name))
            # checkout到指定分支
            if self.check_branch_faild(repo, branch_name):
                return
            self.get_pull_branch(repo, code_path, branch_name)

            # 拉取merges commits
            merge_commits = list(
                repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name,
                                  merges=True))
            self.parse_commits(merge_commits, platform, owner, branch_name, remote_repo, 1, default_branch, repo_name)
            no_merge_commits = list(
                repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name,
                                  no_merges=True))
            self.parse_commits(no_merge_commits, platform, owner, branch_name, remote_repo, 0, default_branch,
                               repo_name)
        else:
            # 遍历所有分支，获取数据
            for branch in branchs:
                if branch.startswith(DEFAULT_BRANCH_HEAD):
                    continue
                branch_name = branch.split('/', 1)[1]
                print('*** start %s repo: %s/%s; branch: %s ***' % (platform, owner, repo_name, branch_name))
                if self.check_branch_faild(repo, branch_name):
                    continue
                self.get_pull_branch(repo, code_path, branch_name)

                merge_commits = list(
                    repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name,
                                      merges=True))
                self.parse_commits(merge_commits, platform, owner, branch_name, remote_repo, 1, default_branch,
                                   repo_name)
                no_merge_commits = list(
                    repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name,
                                      no_merges=True))
                self.parse_commits(no_merge_commits, platform, owner, branch_name, remote_repo, 0, default_branch,
                                   repo_name)               

    # 统计仓库总行数
    def parse_repo_linecount(self, code_path, platform, owner, repo_name, branch, remote_repo):
        """
        统计当前仓库中所有文件行数，将结果写入到ES中
        """
        try:
            total_lines = self.get_repo_linecount(code_path)
            # 控制台输出
            print(f"[RepoLineCount] {platform}/{owner}/{repo_name} (branch={branch}) -> {total_lines} lines")

            # 统计结果->ES
            doc_id_str = f"{remote_repo}-{branch}-linecount"
            doc_id = hashlib.md5(doc_id_str.encode('utf-8')).hexdigest()
            doc = {
                "platform": platform,
                "owner": owner,
                "repo_name": repo_name,
                "branch": branch,
                "repo_url": remote_repo,
                "total_lines_of_code": total_lines,
                "collected_at": str(datetime.datetime.now().isoformat()) # 记录当前收集数据时间
            }

            # bulk操作前先准备index行
            index_data = {"index": {"_index": self.repo_line_index, "_id": doc_id}}
            actions = json.dumps(index_data) + '\n' + json.dumps(doc) + '\n'
            self.esClient.safe_put_bulk(actions)

        except Exception as e:
            print(f"parse_repo_linecount failed: {e}")

    def get_repo_linecount(self, repo_path):
        """
        遍历gitcode中所有的文件,累加统计数量
        """
        import subprocess
        total_lines = 0
        try:
            cmd = f'cd "{repo_path}" && git ls-files' # git ls-files获取文件列表
            output = subprocess.check_output(cmd, shell=True)
            file_list = output.decode().strip().split('\n')

            for rel_path in file_list:
                full_path = os.path.join(repo_path, rel_path)
                if not os.path.join(full_path):
                    continue

                # 逐行读取普通文本，添加额外判断排除二进制文件
                with open(full_path, 'r', errors='ignore') as f:
                    lines = sum(1 for _ in f)
                total_lines += lines
        except Exception as e:
            print(f"get_repo_linecount erroe: {e}")
        
        return total_lines

    # 数据解析
    def parse_commits(self, commits, platform, owner, branch, repo_url, is_merge, default_branch, repo_name):
        is_default_branch = 0
        if branch == default_branch:
            is_default_branch = 1
        print(' -> is merge: %d, commit count: %d' % (is_merge, len(commits)))
        actions = ''
        count = 0
        for commit in commits:
            file_code = commit.stats.total

            company = 'independent'
            email = commit.author.email
            email_domain = email.split('@')[-1]
            if self.email_orgs_dict and email in self.email_orgs_dict:
                companies = self.email_orgs_dict[email]
                commit_time = str(commit.committed_datetime).split(' ')[0]
                company = self.get_company_by_end_date(companies, commit_time)
            elif self.domain_orgs_dict and email_domain in self.domain_orgs_dict:
                company = self.domain_orgs_dict[email_domain]

            unified_user = commit.author.name
            if self.email_user_dict and email in self.email_user_dict:
                unified_user = self.email_user_dict[email]

            sigs = ['No-SIG']
            owner_repo = '%s/%s' % (owner, repo_name)
            if self.repo_sigs_dict and owner_repo in self.repo_sigs_dict:
                sigs = self.repo_sigs_dict[owner_repo]

            mess = commit.message
            action = {
                'commit_id': commit.hexsha,
                'created_at': str(commit.committed_datetime).replace(' ', 'T'),
                'author': commit.author.name,
                'email': commit.author.email,
                'tag_user_company': company,
                'sig_names': sigs,
                'title': commit.summary,
                'body': mess,
                'file_changed': file_code['files'],
                'add': file_code['insertions'],
                'remove': file_code['deletions'],
                'total': file_code['lines'],
                'branch': branch,
                'is_default_branch': is_default_branch,
                'repo_name': repo_name,
                'repo': repo_url,
                'owner': owner,
                'org': self.org,
                'platform': platform,
                'commit_url': repo_url + '/commit/' + commit.hexsha,
                'is_merge': is_merge,
                'unified_user': unified_user
            }

            # 协作者信息
            co_author, co_author_email = self.getCoAuthor(mess)
            if co_author:
                action['co_author'] = co_author
                action['co_author_email'] = co_author_email

            # openeuler kernel 下 contributor和reviewer
            commit_contributors, commit_contributor_emails, commit_reviewers, commit_reviewer_emails = self.getContributorAndReviewer(
                owner, repo_name, mess)
            if commit_contributors:
                action['commit_contributors'] = commit_contributors
                action['commit_contributor_emails'] = commit_contributor_emails
            if commit_reviewers:
                action['commit_reviewers'] = commit_reviewers
                action['commit_reviewer_emails'] = commit_reviewer_emails

            id_str = action['commit_url'] + '-' + branch
            index_id = hashlib.md5(id_str.encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

            # 因数据量太大，写入时太慢，减少批量写入条数，默认1000
            count += 1
            if count == self.write_bulk:
                print('*** write bulk count: %d' % count)
                self.esClient.safe_put_bulk(actions)
                count = 0
                actions = ''
        self.esClient.safe_put_bulk(actions)

    @staticmethod
    def get_company_by_end_date(companies, commit_time):
        if not isinstance(companies, list):
            return companies
        if len(companies) == 1:
            return companies[0]['company_name']

        for company_info in companies:
            if commit_time < company_info['end_date']:
                return company_info['company_name']
        return companies[0]['company_name']

    def update_company_changed(self):
        for email, companies in self.email_orgs_dict.items():
            start_date = '0000-01-01'
            for i in range(1, len(companies)):
                if start_date > self.start_date:
                    continue
                end_date = companies[i]['end_date']
                company = companies[i]['company_name']
                query = '''{
                    "script": {
                        "source": "ctx._source['tag_user_company']='%s'"
                    },
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "range": {
                                        "created_at": {
                                            "gt": "%s",
                                            "lte": "%s"
                                        }
                                    }
                                },
                                {
                                    "query_string": {
                                        "analyze_wildcard": true,
                                        "query": "email.keyword.keyword:%s AND !tag_user_company.keyword:%s"
                                    }
                                }
                            ]
                        }
                    }
                }''' % (company, start_date, end_date, email, company)
                start_date = end_date
                self.esClient.updateByQuery(query=query.encode('utf-8'))

    # 删除git lock
    def removeGitLockFile(self, code_path):
        lock_file = code_path + '/.git/index.lock'
        if os.path.exists(lock_file):
            os.remove(lock_file)

    # openeuler kernel需要识别标记的contributor和reviewer
    def getContributorAndReviewer(self, owner, repo, mess):
        commit_contributors = []
        commit_contributor_emails = []
        commit_reviewers = []
        commit_reviewer_emails = []
        if owner != 'openeuler' or repo != 'kernel':
            return commit_contributors, commit_contributor_emails, commit_reviewers, commit_reviewer_emails
        if mess.__contains__('Signed-off-by') or mess.__contains__('Reviewed-by'):
            items = re.split(r'\nSigned-off-by:|\nReviewed-by:', mess)
            items.pop(0)
            for item in items:
                ele = item.strip()
                if ele.__contains__('openEuler_contributor') or ele.__contains__('openeuler_contributor'):
                    commit_contributors.append(re.findall(r'.*<', ele)[0].replace('<', '').strip())
                    commit_contributor_emails.append(
                        re.findall(r'<.*@\w*.\w*>', ele)[0].replace('<', '').replace('>', ''))
                if ele.__contains__('openEuler_reviewer') or ele.__contains__('openeuler_reviewer'):
                    commit_reviewers.append(re.findall(r'.*<', ele)[0].replace('<', '').strip())
                    commit_reviewer_emails.append(re.findall(r'<.*@\w*.\w*>', ele)[0].replace('<', '').replace('>', ''))
        return commit_contributors, commit_contributor_emails, commit_reviewers, commit_reviewer_emails

    # 协作者信息,message中带’Co-authored-by:‘或者’Signed-off-by:‘的为协作者
    def getCoAuthor(self, mess):
        co_author = []
        co_author_email = []
        if mess.__contains__('Co-authored-by:'):
            try:
                items = re.split(r'\nCo-authored-by:', mess)
                items.pop(0)
                for item in items:
                    ele = item.strip()
                    co_author.append(re.findall(r'.*<', ele)[0].replace('<', '').strip())
                    co_author_email.append(re.findall(r'<.*>', ele)[0].replace('<', '').replace('>', ''))
            except Exception:
                print('*** Co-authored parse failed ')
        return co_author, co_author_email

    def gitee_repos(self, owner, token):
        client = GiteeClient(owner, None, token)
        if self.is_gitee_enterprise == "true":
            repos = self.getGenerator(client.enterprises())
        else:
            repos = self.getGenerator(client.org())
        repos_names = []
        for repo in repos:
            repos_names.append(repo['path'] + '->')
        return repos_names

    def github_repos(self, owner, token):
        client = GithubClient(org=owner, repository=None, token=token, tokens=self.tokens)
        repos = client.get_repos(org=owner)
        repos_names = []
        for repo in repos:
            repos_names.append(repo['name'] + '->')
        return repos_names
    
    # def codearts_repos(self,owner, token):
    #     """
    #     指定owne在gitcode中的仓库列表
    #     """
    #     client = CodeartsClient(owner, None, token)
    #     url = f"rest/orgs/{owner}/repos"
    #     repos = client.fetch_items('repos', {'per_page': 100})
    #     repos_names = []
    #     for repo_page in repos:
    #         for repo in repo_page:
    #             repos_names.append(repo['name'] + '->')  #CodeArts->API返回的字段
    #     return repos_names
    
    def getGenerator(self, response):
        data = []
        try:
            while 1:
                if isinstance(response, types.GeneratorType):
                    res_data = next(response)
                    if isinstance(res_data, str):
                        data += json.loads(res_data.encode('utf-8'))
                    else:
                        data += json.loads(res_data.decode('utf-8'))
                else:
                    data = json.loads(response)
                    break
        except StopIteration:
            return data
        except JSONDecodeError:
            print("CodeArts get JSONDecodeError, error: ", response)
        except Exception as ex:
            print('*** getGenerator fail ***', ex)
            return data

        return data

    # get repos from white box yaml
    def getReposFromYaml(self, yaml_file):
        all_branch_repos = []
        default_branch_repos = []
        try:
            content = self.get_yaml_file(yaml_file)
            for user in content['users']:
                if 'repos' in user and user['repos']:
                    default_branch_repos.extend(user['repos'])
                if 'repos_all_branches' in user and user['repos_all_branches']:
                    all_branch_repos.extend(user['repos_all_branches'])

            # 去重
            all_branch_repos_res = set(all_branch_repos)
            default_branch_repos_res = set(default_branch_repos)
            repos_in_all_branch = all_branch_repos_res.intersection(default_branch_repos_res)
            default_branch_repos_res = default_branch_repos_res - repos_in_all_branch
            return list(all_branch_repos_res), list(default_branch_repos_res)
        except Exception:
            return all_branch_repos, default_branch_repos

    # 获取white box指定仓库的commit
    def getCommitWhiteBox(self, repos, branch=''):
        for repo in repos:
            items = repo.split('/')
            platform = items[2].replace('.com', '').replace('.co', '')
            owner = items[-2]
            repo_name = items[-1]
            branch_name = branch
            path = '/'.join(items[3::])
            try:
                self.getLog(platform, owner, repo_name, branch_name, path)
            except Exception:
                print('*** platform: %s, owner: %s, repo: %s, fail ***' % (platform, owner, repo))

    # upstream 需要从yaml中获取组织
    def getUpstreamCompany(self, user_file, company_yaml):
        email_org_dict = {}
        domain_org_dict = {}
        email_user_dict = {}
        try:
            domain_org_dict, aliases_company_dict = self.get_domain_org(company_yaml)

            # 用户邮箱和组织
            yaml_list = self.get_yaml_list(user_file)
            user_datas = self.get_user_info_from_yaml(yaml_list)
            for user in user_datas:
                user_companies = []
                for company in user['companies']:
                    user_company = company['company_name']
                    if user_company in aliases_company_dict:
                        user_company = aliases_company_dict[user_company]
                    company['company_name'] = user_company
                    user_companies.append(company)

                user_companies.sort(key=lambda x: x['end_date'])
                for email in user['emails']:
                    email_org_dict.update({email: user_companies})
                    email_user_dict.update({email: user.get('user_name')})

            return email_org_dict, domain_org_dict, email_user_dict
        except Exception:
            return email_org_dict, domain_org_dict, email_user_dict

    def get_domain_org(self, company_yaml):
        domain_org_dict = {}
        aliases_company_dict = {}
        try:
            # 企业别名和企业名称
            company_datas = self.get_yaml_file(company_yaml)
            for company in company_datas['companies']:
                company_name = company['company_name']
                for alias in company['aliases']:
                    aliases_company_dict.update({alias: company_name})
                for domain in company['domains']:
                    domain_org_dict.update({domain: company_name})
            return domain_org_dict, aliases_company_dict
        except Exception:
            return domain_org_dict, aliases_company_dict

    def get_yaml_file(self, yaml_file):
        if CODEARTS_BASE in yaml_file: 
            token = self.codearts_access_token
        elif GITEE_BASE in yaml_file:
            token = self.gitee_access_token
        elif GITHUB_BASE in yaml_file:
            token = self.github_access_token
        else:
            token = self.github_access_token 

        headers = {'Authorization': f'token {token}'}
        yaml_response = requests.get(yaml_file, headers=headers, verify=False, timeout=60)
        if yaml_response.status_code != 200:
            print('Cannot fetch online yaml file.', yaml_response.text)
            return
        try:
            yaml_json = yaml.safe_load(yaml_response.text)
        except yaml.YAMLError as e:
            print(f'Error parsing YAML: {e}')
            return
        return yaml_json

    def get_yaml_list(self, yaml_file):
        yaml_list = []
        if CODEARTS_BASE in yaml_file:
            token = self.codearts_access_token
        elif GITEE_BASE in yaml_file:
            token = self.gitee_access_token
        elif GITHUB_BASE in yaml_file:
            token = self.github_access_token
        else:
            token = self.github_access_token 

        headers = {'Authorization': f'token {token}'}
        response = requests.get(yaml_file, headers=headers, verify=False, timeout=60)
        if response.status_code != 200:
            return yaml_list
        for file in response.json():
            if file.get('download_url', '').endswith('yaml'):
                yaml_list.append(file.get('download_url'))
        return yaml_list

    def get_user_info_from_yaml(self, yaml_list):
        users = []
        for yaml_url in yaml_list:
            yaml_json = self.get_yaml_file(yaml_url).get('users')
            users.extend(yaml_json)
        return users

    @staticmethod
    def get_pull_branch(repo, code_path, branch):
        try:
            origin = repo.remote(name='origin')
            origin.fetch()

            # git reset --hard origin/<branch>
            reset_branch = f'origin/{branch}'
            repo.git.reset('--hard', reset_branch)
            print(f"{code_path} git pull {branch} success!")
        except GitCommandError as e:
            print(f"{code_path} git pull {branch} failed!", e)

    def reset_remote_url(self, repo, new_remote_url):
        try:
            origin = repo.remotes.origin
            origin.pull()
        except GitCommandError as e:
            print('pull failed:', e)
            print('Try to set remote url')
            self.set_remote_url(repo, new_remote_url)
            
    def set_remote_url(self, repo, new_remote_url):
        try:
            origin = repo.remotes.origin
            origin.set_url(new_remote_url)
        except GitCommandError as e:
            print('set remote url failed:', e)

    # 切换分支，并且检查是否切换成功
    def check_branch_faild(self, repo, branch_name):
        try:
            repo.git.checkout('-f', branch_name)
        except Exception:
            print('*** branch checkout fail: %s' % branch_name)
            return True
        return False