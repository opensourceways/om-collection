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
# Create: 2020-05
#
import base64
import datetime
import hashlib
import json
import os
import subprocess
import time
import types
from json import JSONDecodeError

import git
import yaml

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data.common import ESClient

GITEE_BASE = "gitee.com"
GITHUB_BASE = "github.com"
DEFAULT_BRANCH_HEAD = "  origin/HEAD ->"


class CodeStatistics(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.orgs = config.get('orgs')
        self.platform = config.get('platform')
        self.token = config.get('token')
        self.repo_index_name = config.get('repo_index_name')
        self.version_index_name = config.get('version_index_name')
        self.obs_meta_org = config.get('obs_meta_org')
        self.obs_meta_repo = config.get('obs_meta_repo')
        self.obs_meta_dir = config.get('obs_meta_dir')
        self.obs_versions = config.get('obs_versions')
        self.code_base_path = config.get('code_base_path')
        self.username = config.get('username')
        self.password = config.get('password')
        self.is_gitee_enterprise = config.get('is_gitee_enterprise')
        self.is_repo_statistic = config.get('is_repo_statistic')
        self.is_version_statistic = config.get('is_version_statistic')
        self.company_aliases_yaml_url = config.get('company_aliases_yaml_url')
        self.company_aliases_yaml_path = config.get('company_aliases_yaml_path')
        self.time_now = ''

    def run(self, from_time):
        print("code statistics: start")
        # 代码统计时间
        self.time_now = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())

        # 企业别名
        company_aliases_dict = self.getCompanyAliasesName()

        # 仓库 -> 维护仓库的企业（根据maintainer的gitee_id识别）
        repo_company_dict = self.esClient.getRepoOrganizations(field='tag_user_company',
                                                               company_aliases_dict=company_aliases_dict,
                                                               is_sig_info_yaml=False)

        # 仓库 -> 维护仓库的组织（根据sig-info.yaml记录识别）
        repo_org_dict = self.esClient.getRepoOrganizations(field='organization',
                                                           company_aliases_dict=company_aliases_dict,
                                                           is_sig_info_yaml=True)

        # 仓库对应的sig
        repo_sigs_dict = self.esClient.getRepoSigs()

        for org in self.orgs.split(';'):
            # 获取组织下所有的仓库
            repos = self.get_repos(owner=org)
            for repo in repos:
                org_repo = org + '/' + repo
                # 维护该仓库的sigs
                sig_names = ['No-SIG']
                if org_repo in repo_sigs_dict:
                    sig_names = repo_sigs_dict[org_repo]
                # 维护该仓库的companies
                tag_user_companies = ['independent']
                if org_repo in repo_org_dict:
                    tag_user_companies = repo_org_dict[org_repo]
                elif org_repo in repo_company_dict:
                    tag_user_companies = repo_company_dict[org_repo]

                repo_url = 'https://{}.com/{}/{}'.format(self.platform, org, repo)

                repo_info = {'repo_platform': self.platform,
                             'repo_owner': org,
                             'repo_name': repo,
                             'repo_url': repo_url,
                             'update_at': self.time_now,
                             'sig_names': sig_names,
                             'tag_user_company': tag_user_companies,
                             'is_last': 1}

                # 统计每个仓库的代码量
                if self.is_repo_statistic == 'true':
                    self.statistics_code_of_repo(owner=org, repo=repo, repo_info=repo_info)

                # 统计每个版本的代码量
                if self.is_version_statistic == 'true' and org == self.obs_meta_org:
                    # 仓库对应的版本
                    repo_versions = self.get_obs_meta()
                    self.statistics_code_of_version(owner=org, repo=repo, repo_info=repo_info,
                                                    repo_versions=repo_versions)

    def getCompanyAliasesName(self):
        cmd = 'wget -N -P %s %s' % (self.company_aliases_yaml_path, self.company_aliases_yaml_url)
        os.system(cmd)
        user_file_name = str(self.company_aliases_yaml_url).split('/')[-1]
        user_file = self.company_aliases_yaml_path + user_file_name

        datas = yaml.safe_load_all(open(user_file, encoding='UTF-8')).__next__()
        company_aliases_dict = {}
        for item in datas['companies']:
            company_cn = item['company_cn']
            if item.get('aliases') is None:
                continue
            for aliases in item.get('aliases'):
                company_aliases_dict[aliases] = company_cn
        return company_aliases_dict

    def statistics_code_of_version(self, owner, repo, repo_info, repo_versions):
        print('*** statistics_code_of_version start : %s/%s' % (owner, repo))
        if repo not in repo_versions:
            print('*** repo has no versions : %s/%s' % (owner, repo))
            return
        branches = repo_versions.get(repo)

        action = repo_info.copy()
        repo_path = self.git_clone_or_pull_repo(platform=self.platform, owner=owner, repo_name=repo)
        git_repo = None
        try:
            git_repo = git.Repo(repo_path)
        except Exception:
            print('*** repo clone or pull fail : %s/%s' % (owner, repo))

        for branch in branches:
            try:
                print('*** branch : %s' % branch)
                s_time = datetime.datetime.now()
                actions = ''
                # 切换到版本分支
                try:
                    git_repo.git.checkout(branch)
                except Exception:
                    print('*** %s/%s has no branch : %s' % (owner, repo, branch))
                    continue
                print('*** checkout branch : %s' % branch)
                # 解压压缩文件
                self.decompress(repo_path)
                # 统计代码量
                cmd_cloc = 'cloc %s --json' % repo_path
                res_cloc = os.popen(cmd_cloc)
                res_json = json.loads(res_cloc.read())
                sum_code = res_json.get('SUM')
                action['files'] = int(sum_code.get('nFiles'))
                action['blank'] = int(sum_code.get('blank'))
                action['comment'] = int(sum_code.get('comment'))
                action['code'] = int(sum_code.get('code'))
                action['obs_version'] = branch

                # 删除解压文件
                cmd_clean = 'cd %s;git clean -f -d -x' % repo_path
                os.system(cmd_clean)

                update_script = "repo_url.keyword: \\\"%s\\\" AND obs_version.keyword:\\\"%s\\\"" % (
                    action['repo_url'], branch)
                self.tagLastUpdateAt(index=self.version_index_name, query_str=update_script)

                id_str = action['repo_url'] + '-' + branch + self.time_now
                index_id = hashlib.md5(id_str.encode('utf-8')).hexdigest()
                index_data = {"index": {"_index": self.version_index_name, "_id": index_id}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'

                self.esClient.safe_put_bulk(actions)

                e_time = datetime.datetime.now()
                seconds = (e_time - s_time).seconds
                print('*** %s/%s %s : %d seconds' % (owner, repo, branch, seconds))
            except Exception:
                print('*** statistics_code_of_version fail : %s/%s' % (owner, repo))
                continue
        print('*** statistics_code_of_version finish : %s/%s' % (owner, repo))

    def statistics_code_of_repo(self, owner, repo, repo_info):
        print('*** statistics_code_of_repo start : %s/%s' % (owner, repo))
        action = repo_info.copy()
        repo_path = self.git_clone_or_pull_repo(platform=self.platform, owner=owner, repo_name=repo)
        try:
            s_time = datetime.datetime.now()
            git_repo = git.Repo(repo_path)
            # 识别主分支
            default_branch = 'master'
            branchs = git_repo.git.branch('-r').split('\n')
            for b in branchs:
                if b.startswith(DEFAULT_BRANCH_HEAD):
                    default_branch = b.replace(DEFAULT_BRANCH_HEAD, '').split('/', 1)[1]
                    break
            # 切换到主分支
            git_repo.git.checkout(default_branch)

            # 清理未追踪文件
            cmd_clean = 'cd %s;git clean -f -d -x' % repo_path
            os.system(cmd_clean)

            cmd_cloc = 'cloc %s --json' % repo_path
            res_cloc = os.popen(cmd_cloc)
            res_json = json.loads(res_cloc.read())
            actions = ''
            for k, v in res_json.items():
                if k == 'header' or k == 'SUM':
                    continue
                action['language'] = k
                action['files'] = int(v.get('nFiles'))
                action['blank'] = int(v.get('blank'))
                action['comment'] = int(v.get('comment'))
                action['code'] = int(v.get('code'))

                id_str = action['repo_url'] + '-' + k + self.time_now
                index_id = hashlib.md5(id_str.encode('utf-8')).hexdigest()
                index_data = {"index": {"_index": self.repo_index_name, "_id": index_id}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'

            update_script = "repo_url.keyword: \\\"%s\\\"" % action['repo_url']
            self.tagLastUpdateAt(index=self.repo_index_name, query_str=update_script)

            self.esClient.safe_put_bulk(actions)

            e_time = datetime.datetime.now()
            seconds = (e_time - s_time).seconds
            print('*** %s/%s : %d seconds' % (owner, repo, seconds))
            print('*** statistics_code_of_repo finish : %s/%s' % (owner, repo))
        except Exception:
            print('*** statistics_code_of_repo fail : %s/%s' % (owner, repo))
            return

    def get_obs_meta(self):
        obs_path = self.git_clone_or_pull_repo(platform=self.platform, owner=self.obs_meta_org,
                                               repo_name=self.obs_meta_repo)
        meta_dir = obs_path if self.obs_meta_dir is None else os.path.join(obs_path, self.obs_meta_dir)
        root, dirs, _ = os.walk(meta_dir).__next__()
        if self.obs_versions:
            obs_versions = self.obs_versions.split(";")
            inter_versions = list(set(obs_versions).intersection(set(dirs)))
        else:
            def check_version(s):
                return s.startswith("openEuler-")

            inter_versions = list(filter(check_version, dirs))

        repo_versions = {}
        for version in inter_versions:
            package_dirs = []
            package_epol_dir = []
            try:
                # 注意，windows下不支持目录中包含”:“等符号
                package_path = os.path.join(root, version, version.replace('-', ':'))
                package_epol_path = os.path.join(root, version, ('%s:Epol' % version.replace('-', ':')))
                if os.path.exists(package_path):
                    _, package_dirs, _ = os.walk(package_path).__next__()
                if os.path.exists(package_epol_path):
                    _, package_epol_dir, _ = os.walk(package_epol_path).__next__()
            except:
                continue
            package_union_dirs = list(set(package_dirs).union(set(package_epol_dir)))
            for dir in package_union_dirs:
                versions = repo_versions.get(dir)
                if versions is not None:
                    versions.append(version)
                    repo_versions.update({dir: versions})
                else:
                    repo_versions.update({dir: [version]})
        return repo_versions

    def get_repos(self, owner):
        if self.platform == 'gitee':
            repos = self.gitee_repos(owner=owner, token=self.token)
        elif self.platform == 'github':
            repos = self.github_repos(owner=owner, token=self.token)
        else:
            repos = []
        return repos

    def git_clone_or_pull_repo(self, platform, owner, repo_name):
        # 本地仓库目录
        owner_path = self.code_base_path + platform + '/' + owner + '/'
        if not os.path.exists(owner_path):
            os.makedirs(owner_path)
        code_path = owner_path + repo_name

        username = base64.b64decode(self.username).decode()
        passwd = base64.b64decode(self.password).decode()
        if platform == 'gitee':
            clone_url = 'https://%s:%s@%s/%s/%s' % (username, passwd, GITEE_BASE, owner, repo_name)
        elif platform == 'github':
            clone_url = 'https://%s:%s@%s/%s/%s' % (username, passwd, GITHUB_BASE, owner, repo_name)
        else:
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

        return code_path

    def gitee_repos(self, owner, token):
        client = GiteeClient(owner, None, token)
        if self.is_gitee_enterprise == "true":
            repos = self.getGenerator(client.enterprises())
        else:
            repos = self.getGenerator(client.org())
        repos_names = []
        for repo in repos:
            repos_names.append(repo['path'])
        return repos_names

    def github_repos(self, owner, token):
        client = GithubClient(org=owner, repository=None, token=token)
        repos = client.get_repos(org=owner)
        repos_names = []
        for repo in repos:
            repos_names.append(repo['name'])
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

    # 删除git lock
    def removeGitLockFile(self, code_path):
        lock_file = code_path + '/.git/index.lock'
        if os.path.exists(lock_file):
            os.remove(lock_file)

    # 解压
    def decompress(self, path):
        root, dirs, files = os.walk(path).__next__()

        def check_tar_file(s):
            return s.endswith("tar.gz") or s.endswith("tar.xz") or s.endswith("tar.bz2") \
                   or s.endswith("tar_2.gz") or s.endswith(".tgz") or s.endswith("tar.z") \
                   or s.endswith("tar.bz") or s.endswith(".tar")

        def check_zip_file(s):
            return s.endswith(".zip") or s.endswith(".xpi") or s.endswith(".jar")

        def check_gz_file(s):
            return s.endswith(".dat.gz") or s.endswith(".gz")

        tar_files = list(filter(check_tar_file, files))
        for file in tar_files:
            cmd_tar = 'cd %s;tar -xvf %s' % (path, file)
            os.system(cmd_tar)
            print('*** decompress tar file : %s/%s' % (path, file))

        zip_files = list(filter(check_zip_file, files))
        for file in zip_files:
            cmd_tar = 'cd %s;unzip %s' % (path, file)
            os.system(cmd_tar)
            print('*** decompress zip file : %s/%s' % (path, file))

        gz_files = list(filter(check_gz_file, files))
        for file in gz_files:
            cmd_tar = 'cd %s;gzip -d %s' % (path, file)
            os.system(cmd_tar)
            print('*** decompress gz file : %s/%s' % (path, file))

    # 标记数据是否是最近更新
    def tagLastUpdateAt(self, index, query_str):
        try:
            query = '''{
                          "script": {
                            "source": "ctx._source['is_last']=0"
                          },
                          "query": {
                            "query_string": {
                              "analyze_wildcard": true,
                              "query": "%s"
                            }
                          }
                        }''' % query_str
            self.esClient.updateByQuery(query, index=index, query_es=self.esClient.url,
                                        es_authorization=self.esClient.authorization)
        except Exception:
            pass
