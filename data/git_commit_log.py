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
import yaml

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data.common import ESClient

GITEE_BASE = "gitee.com"
GITHUB_BASE = "github.com"
DEFAULT_BRANCH_HEAD = "  origin/HEAD ->"


class GitCommitLog(object):
    def __init__(self, config=None):
        self.config = config
        self.org = config.get('org')
        self.index_name = config.get('index_name')
        self.code_base_path = config.get('code_base_path')
        self.platform_owner_token = config.get('platform_owner_token')
        self.start_date = config.get('start_date')
        self.end_date = config.get('end_date')
        self.before_days = config.get('before_days')
        self.user_commit_name = config.get('user_commit_name')
        self.gitee_repo_branch = config.get('gitee_repo_branch')
        self.github_repo_branch = config.get('github_repo_branch')
        self.username = config.get('username')
        self.password = config.get('password')
        self.write_bulk = int(config.get('write_bulk', 1000))
        self.esClient = ESClient(config)
        self.email_orgs_dict = {}
        self.domain_orgs_dict = {}
        self.repo_sigs_dict = defaultdict(dict)
        self.white_box_yaml = config.get('white_box_yaml')
        self.upstream_yaml = config.get('upstream_yaml')
        self.user_yaml = config.get('user_yaml')
        self.company_yaml = config.get('company_yaml')
        self.all_repo_default_branch = config.get('all_repo_default_branch', 'false')
        self.is_gitee_enterprise = config.get('is_gitee_enterprise')

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
            self.email_orgs_dict, self.domain_orgs_dict = self.getUpstreamCompany(user_yaml=self.user_yaml, company_yaml=self.company_yaml)
            all_branch_repos, default_branch_repos = self.getReposFromYaml(yaml_file=self.upstream_yaml)
            self.getCommitWhiteBox(default_branch_repos, 'default')  # 只是获取默认分支commit
            self.getCommitWhiteBox(all_branch_repos)  # 获取全部分支commit
        else:
            # 全部commits
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
            if platform == 'gitee':
                if self.gitee_repo_branch:
                    repos = self.gitee_repo_branch.split(';')
                else:
                    repos = self.gitee_repos(owner=owner, token=token)
            elif platform == 'github':
                if self.github_repo_branch:
                    repos = self.github_repo_branch.split(';')
                else:
                    repos = self.github_repos(owner=owner, token=token)

            for repo in repos:
                if not str(repo).__contains__('->'):
                    continue
                rb = repo.split('->')
                branch_name = rb[1]
                if self.all_repo_default_branch == 'true':
                    branch_name = 'default'
                try:
                    self.getLog(platform, owner, repo_name=rb[0], branch_name=branch_name)
                except Exception:
                    print('*** platform: %s, owner: %s, repo: %s, fail ***' % (platform, owner, repo))

    def getLog(self, platform, owner, repo_name, branch_name):
        # 本地仓库目录
        owner_path = self.code_base_path + platform + '/' + owner + '/'
        if not os.path.exists(owner_path):
            os.makedirs(owner_path)
        code_path = owner_path + repo_name

        username = base64.b64decode(self.username).decode()
        passwd = base64.b64decode(self.password).decode()
        if platform == 'gitee':
            remote_repo = 'https://%s/%s/%s' % (GITEE_BASE, owner, repo_name)
            clone_url = 'https://%s:%s@%s/%s/%s' % (username, passwd, GITEE_BASE, owner, repo_name)
        elif platform == 'github':
            remote_repo = 'https://%s/%s/%s' % (GITHUB_BASE, owner, repo_name)
            clone_url = 'https://%s:%s@%s/%s/%s' % (username, passwd, GITHUB_BASE, owner, repo_name)
        else:
            remote_repo = None
            clone_url = None

        # 本地仓库已存在执行git pull；否则执行git clone
        self.removeGitLockFile(code_path)
        if os.path.exists(code_path):
            cmd_pull = 'cd %s;git pull' % code_path
            os.system(cmd_pull)
        else:
            if clone_url is None:
                return
            cmd_clone = 'cd %s;git clone %s' % (owner_path, clone_url + '.git')
            os.system(cmd_clone)

        try:
            repo = git.Repo(code_path)
        except Exception:
            print('*** repo clone fail: %s' % remote_repo)
            return

        # 如果指定分支为default，则指定分支为默认分支
        default_branch = 'master'
        branchs = repo.git.branch('-r').split('\n')
        for b in branchs:
            if b.startswith(DEFAULT_BRANCH_HEAD):
                default_branch = b.replace(DEFAULT_BRANCH_HEAD, '').split('/', 1)[1]
                break
        if branch_name == 'default':
            branch_name = default_branch
        if branch_name != '':
            # checkout到指定分支获取数据
            print('*** start %s repo: %s/%s; branch: %s ***' % (platform, owner, repo_name, branch_name))
            repo.git.checkout(branch_name)
            cmd_pull = 'cd %s;git pull' % code_path
            os.system(cmd_pull)
            merge_commits = list(repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name, merges=True))
            self.parse_commits(merge_commits, platform, owner, branch_name, remote_repo, 1, default_branch, repo_name)
            no_merge_commits = list(repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name, no_merges=True))
            self.parse_commits(no_merge_commits, platform, owner, branch_name, remote_repo, 0, default_branch, repo_name)
        else:
            # 遍历所有分支，获取数据
            for branch in branchs:
                if branch.startswith(DEFAULT_BRANCH_HEAD):
                    continue
                branch_name = branch.split('/', 1)[1]
                print('*** start %s repo: %s/%s; branch: %s ***' % (platform, owner, repo_name, branch_name))
                repo.git.checkout(branch_name)
                cmd_pull = 'cd %s;git pull' % code_path
                os.system(cmd_pull)
                merge_commits = list(repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name, merges=True))
                self.parse_commits(merge_commits, platform, owner, branch_name, remote_repo, 1, default_branch, repo_name)
                no_merge_commits = list(repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name, no_merges=True))
                self.parse_commits(no_merge_commits, platform, owner, branch_name, remote_repo, 0, default_branch, repo_name)

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
                company = self.email_orgs_dict[email]
            elif self.domain_orgs_dict and email_domain in self.domain_orgs_dict:
                company = self.domain_orgs_dict[email_domain]

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
            }

            # 协作者
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
                    commit_contributor_emails.append(re.findall(r'<.*@\w*.\w*>', ele)[0].replace('<', '').replace('>', ''))
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
        client = GithubClient(org=owner, repository=None, token=token)
        repos = client.get_repos(org=owner)
        repos_names = []
        for repo in repos:
            repos_names.append(repo['name'] + '->')
        return repos_names

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
            print("Gitee get JSONDecodeError, error: ", response)
        except Exception as ex:
            print('*** getGenerator fail ***', ex)
            return data

        return data

    # get repos from white box yaml
    def getReposFromYaml(self, yaml_file):
        all_branch_repos = []
        default_branch_repos = []
        try:
            cmd = 'wget -N -P %s %s' % (self.code_base_path, yaml_file)
            os.system(cmd)

            # 解析yaml
            file_name = str(yaml_file).split('/')[-1]
            file = self.code_base_path + file_name
            datas = yaml.safe_load_all(open(file, encoding='UTF-8')).__next__()
            for data in datas['users']:
                if 'repos' in data and data['repos']:
                    default_branch_repos.extend(data['repos'])
                if 'repos_all_branches' in data and data['repos_all_branches']:
                    all_branch_repos.extend(data['repos_all_branches'])

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
            platform = items[2].replace('.com', '')
            owner = items[3]
            repo_name = items[4]
            branch_name = branch
            try:
                self.getLog(platform, owner, repo_name, branch_name)
            except Exception:
                print('*** platform: %s, owner: %s, repo: %s, fail ***' % (platform, owner, repo))

    # upstream 需要从yaml中获取组织
    def getUpstreamCompany(self, user_yaml, company_yaml):
        email_org_dict = {}
        domain_org_dict = {}
        try:
            cmd = 'wget -N -P %s %s' % (self.code_base_path, user_yaml)
            os.system(cmd)
            user_file_name = str(user_yaml).split('/')[-1]
            user_file = self.code_base_path + user_file_name

            cmd = 'wget -N -P %s %s' % (self.code_base_path, company_yaml)
            os.system(cmd)
            company_file_name = str(company_yaml).split('/')[-1]
            company_file = self.code_base_path + company_file_name

            # 企业别名和企业名称
            company_datas = yaml.safe_load_all(open(company_file, encoding='UTF-8')).__next__()
            aliases_company_dict = {}
            for company in company_datas['companies']:
                company_name = company['company_name']
                for alias in company['aliases']:
                    aliases_company_dict.update({alias: company_name})
                for domain in company['domains']:
                    domain_org_dict.update({domain: company_name})

            # 用户邮箱和组织
            user_datas = yaml.safe_load_all(open(user_file, encoding='UTF-8')).__next__()
            for user in user_datas['users']:
                user_company = user['companies'][0]['company_name']
                if user_company in aliases_company_dict:
                    user_company = aliases_company_dict[user_company]
                for email in user['emails']:
                    email_org_dict.update({email: user_company})

            return email_org_dict, domain_org_dict
        except Exception:
            return email_org_dict, domain_org_dict
