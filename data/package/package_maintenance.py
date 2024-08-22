#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2024/8/14
import json
import logging
import os
import threading
import time

import requests
import yaml

from data.common import ESClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class PackageStatus(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.url = config.get('es_url')
        self.from_data = config.get("from_data")
        self.headers = {'Content-Type': 'application/json', 'Authorization': config.get('authorization')}

        self.gitee_all_index_name = config.get('gitee_all_index_name')
        self.cve_index_name = config.get('cve_index_name')
        self.sig_index_name = config.get("sig_index_name")
        self.repo_query = config.get("repo_query")
        self.active_query = config.get("active_query")
        self.query_format = config.get("query_format")

        self.pkg_version_url = config.get('pkg_version_url')
        self.gitee_repo_base = config.get("gitee_repo_base")
        self.repo_active = {}
        self.repo_versions = {}
        self.user_owner_repo = {}
        self.collaboration_update_repo = {}
        self.repo_type = {}
        self.company_num = int(config.get('company_num', '5'))
        self.user_num = int(config.get('company_num', '10'))

        self.platform = config.get('platform')
        self.obs_meta_org = config.get('obs_meta_org')
        self.obs_meta_repo = config.get('obs_meta_repo')
        self.obs_meta_dir = config.get('obs_meta_dir')
        self.versions = config.get('versions', 'openEuler-24.03-LTS')
        self.code_base_path = config.get('code_base_path')

        self.thread_pool_num = int(config.get('thread_pool_num', 8))
        self.thread_max_num = threading.Semaphore(self.thread_pool_num)
        self.actions = ''
        self._lock = threading.Lock()

    # 删除git lock
    @staticmethod
    def remove_git_lock_file(code_path):
        lock_file = code_path + '/.git/index.lock'
        if os.path.exists(lock_file):
            os.remove(lock_file)

    @staticmethod
    def pr_merged(hits):
        for hit in hits:
            if hit.get('key') == 'merged' and hit.get('doc_count') > 0:
                return {"is_positive": 1, "status": "有PR合入"}
        return {"is_positive": 0, "status": "有PR提交未合入"}

    @staticmethod
    def issue_state(hits):
        for hit in hits:
            if hit.get('key') == 'closed' and hit.get('doc_count') == 0:
                return {"is_positive": 0, "status": "没有Issue修复"}
            if hit.get('key') == 'open' and hit.get('doc_count') == 0:
                return {"is_positive": 1, "status": "全部Issue修复"}
        return {"is_positive": 0, "status": "有部分Issue修复"}

    @staticmethod
    def get_repo_maintenance(action):
        if action.get('cve').get('status') == '有CVE全部未修复' and action.get('issue').get('status') == '没有Issue修复':
            status = {'status': '没有人维护', 'is_no_maintenance': 1}
        elif action.get('cve').get('status') == '有CVE全部未修复':
            status = {'status': '缺人维护', 'is_lack_of_maintenance': 1}
        elif action.get('cve').get('status') == '有CVE部分修复':
            status = {'status': '缺人维护', 'is_lack_of_maintenance': 1}
        elif action.get('cve').get('status') == '有CVE且全部修复':
            status = {'status': '健康', 'is_health': 1}
        elif action.get('cve').get('status') == '没有CVE问题' \
                and action.get('package_update').get('status') == '没有PR提交' \
                and action.get('package_version').get('status') == '最新版本':
            status = {'status': '健康', 'is_health': 1}
        elif action.get('cve').get('status') == '没有CVE问题' \
                and action.get('package_update').get('status') == '没有PR提交' \
                and action.get('package_version').get('status') == '落后版本':
            status = {'status': '静止', 'is_inactive': 1}
        elif action.get('cve').get('status') == '没有CVE问题':
            status = {'status': '活跃', 'is_active': 1}
        else:
            status = {'status': '其他'}
        return status

    @staticmethod
    def get_version_repo_type(root, version: str):
        repo_types = {}
        package_dirs = []
        try:
            # 注意，windows下不支持目录中包含”:“等符号
            package_path = os.path.join(root, version)
            if os.path.exists(package_path):
                _, package_dirs, _ = os.walk(package_path).__next__()
        except OSError as e:
            print('package_path error: ', e)
            return repo_types

        for pkg_dir in package_dirs:
            if pkg_dir == 'delete':
                continue
            repo_path = os.path.join(root, version, pkg_dir, 'pckg-mgmt.yaml')
            repo_info = yaml.safe_load(open(repo_path)).get('packages')
            for repo in repo_info:
                repo_name = repo.get('name')
                repo_types.update({repo_name: pkg_dir})
        return repo_types

    def get_obs_meta(self):
        obs_path = self.git_clone_or_pull_repo(platform=self.platform, owner=self.obs_meta_org)
        meta_dir = obs_path if self.obs_meta_dir is None else os.path.join(obs_path, self.obs_meta_dir)
        root, _, _ = os.walk(meta_dir).__next__()
        return root

    def git_clone_or_pull_repo(self, platform, owner):
        # 本地仓库目录
        owner_path = os.path.join(self.code_base_path, platform, owner)
        if not os.path.exists(owner_path):
            os.makedirs(owner_path)
        repo_name = self.obs_meta_repo.split('/')[-1]
        code_path = os.path.join(owner_path, repo_name)

        if platform == 'gitee':
            clone_url = self.obs_meta_repo
        else:
            clone_url = None

        # 本地仓库已存在执行git pull；否则执行git clone
        self.remove_git_lock_file(code_path)
        if os.path.exists(code_path):
            cmd_pull = 'cd %s;git checkout .;git pull --rebase' % code_path
            os.system(cmd_pull)
        else:
            if not clone_url:
                return
            cmd_clone = 'cd "%s";git clone %s' % (owner_path, clone_url + '.git')
            os.system(cmd_clone)

        return code_path

    def write_data_thread(self):
        t1 = time.time()
        self.actions = ''
        threads = []
        repos = self.get_repo_list()
        self.get_active_dict()
        for repo, sig in repos.items():
            if repo not in self.repo_type:
                continue
            with self.thread_max_num:
                t = threading.Thread(
                    target=self.write_repo_data,
                    args=(repo, sig,))
                threads.append(t)
                t.start()
        for t in threads:
            t.join()
        t2 = time.time()
        print('cost time: ', t2 - t1)
        self.esClient.safe_put_bulk(self.actions)

    def run(self, from_date):
        # Get repos update by the collaboration platform
        self.collaboration_update_repo = self.get_collaboration_update_repo()
        # tag old data
        self.tag_last_update()

        self.repo_versions = self.get_all_repo_version()
        self.user_owner_repo = self.get_repo_query("user_login")

        for version in self.versions.split(','):
            root_path = self.get_obs_meta()
            self.repo_type = self.get_version_repo_type(root_path, version)
            self.write_data_thread()

    def single_data(self, action, doc_id):
        index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
        actions = json.dumps(index_data) + '\n'
        actions += json.dumps(action) + '\n'
        return actions

    def get_active(self, repo):
        repo_active = self.repo_active.get(repo)
        participant = {"is_positive": 0, "status": "贡献人数少", "num": 0}
        company = {"is_positive": 0, "status": "贡献组织少", "num": 0}
        if repo_active:
            if repo_active['users'] > self.user_num:
                participant = {"is_positive": 1, "status": "贡献人数多", "num": repo_active['users']}
            else:
                participant = {"is_positive": 0, "status": "贡献人数少", "num": repo_active['users']}
            if repo_active['companies'] > self.company_num:
                company = {"is_positive": 1, "status": "贡献组织多", "num": repo_active['companies']}
            else:
                company = {"is_positive": 0, "status": "贡献组织少", "num": repo_active['companies']}
        return participant, company

    def get_active_dict(self):
        url = self.url + '/' + self.gitee_all_index_name + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers,
                            verify=False, data=self.active_query.encode('utf-8'))
        buckets = res.json()['aggregations']['group_field']['buckets']
        for bucket in buckets:
            repo_name = bucket['key']
            repo = repo_name.split('/')[-1]
            companies = bucket['company']['value']
            users = bucket['user']['value']
            self.repo_active.update({
                repo: {
                    'users': users,
                    'companies': companies
                }
            })

    def get_repo_list(self):
        repo_list = {}
        repo_sig = self.get_repo_query("sig_name")
        for repo, sig_list in repo_sig.items():
            sig = 'No-SIG'
            for item in sig_list:
                sig = item
            repo_list.update({repo: sig})
        return repo_list

    def get_package_update(self, repo):
        repo = self.gitee_repo_base + repo
        query = f'''gitee_repo.keyword:\\"{repo}\\" AND is_gitee_pull_request:1'''
        search = self.query_format % (query, "pull_state.keyword")
        resp = self.esClient.esSearch(index_name=self.gitee_all_index_name, search=search)
        aggregations = resp.get('aggregations').get('group_field').get('buckets')
        if len(aggregations) == 0:
            package_update = {"is_positive": 0, "status": "没有PR提交"}
        else:
            package_update = self.pr_merged(aggregations)
        return package_update

    def get_issue_update(self, repo):
        repo = self.gitee_repo_base + repo
        query = f'''gitee_repo.keyword:\\"{repo}\\" AND is_gitee_issue:1'''
        search = self.query_format % (query, "issue_state.keyword")

        resp = self.esClient.esSearch(index_name=self.gitee_all_index_name, search=search)
        aggregations = resp.get('aggregations').get('group_field').get('buckets')
        issue_state = self.issue_state(aggregations)
        return issue_state

    def get_all_repo_version(self):
        page = 1
        repo_versions = {}
        while True:
            params = {"page": page, "items_per_page": 100}
            resp = requests.get(url=self.pkg_version_url, params=params, timeout=60)
            page += 1
            if resp.status_code != 200:
                print('get version error', resp.text)
                resp.raise_for_status()
            items = resp.json().get('items')
            if not items:
                break
            for item in items:
                name = item.get('name')
                if name not in repo_versions:
                    repo_versions[name] = {}

                if item.get('tag').endswith('_up'):
                    up_version = item.get('version')
                    repo_versions[name].update({"up_version": up_version})
                if item.get('tag').endswith('_openeuler'):
                    euler_version = item.get('version')
                    repo_versions[name].update({"version": euler_version})

        version_status = {}
        for repo, version in repo_versions.items():
            up_version = version.get('up_version')
            euler_version = version.get('version')
            if up_version == euler_version and up_version and euler_version:
                version = {"is_positive": 1, "status": "最新版本", "version": euler_version,
                           "up_version": up_version}
            else:
                version = {"is_positive": 0, "status": "落后版本", "version": euler_version,
                           "up_version": up_version}
            version_status[repo] = version
        return version_status

    def get_repo_version(self, repo):
        if repo in self.repo_versions:
            version = self.repo_versions.get(repo)
        else:
            version = {"is_positive": 0, "status": "落后版本", "version": None, "up_version": None}
        return version

    def get_openeuper_cve_state(self, repo_name):
        """
        Add CVE state to repo_dic
        :param repo_name: repo name
        :return: repo_dic
        """
        cve = {}
        search = (
                """
        {
            "track_total_hits": true,
            "size": 1000,
            "_source": [
                "repository",
                "CVE_level",
                "issue_state",
                "issue_customize_state",
                ""
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "repository": "%s"
                            }
                        },
                        {
                            "range": {
                                "created_at": {
                                    "gte": "now-1y/y",
                                    "lte": "now"
                                }
                            }
                        }
                    ]
                }
            }
        }"""
                % repo_name
        )
        scroll_duration = "1m"
        data_dic_list = []

        def func(data):
            for item in data:
                data_dic_list.append(item['_source'])

        self.esClient.scrollSearch(self.cve_index_name, search, scroll_duration, func)
        fixed_cve_count = 0
        cve_count = len(data_dic_list)
        for data_dic in data_dic_list:
            if (
                    data_dic["issue_state"] == "closed"
                    or data_dic["issue_state"] == "rejected"
            ):
                fixed_cve_count += 1
        if fixed_cve_count == cve_count > 0:
            cve["is_positive"] = 1
            cve["status"] = "有CVE且全部修复"
        elif fixed_cve_count == cve_count == 0:
            cve["is_positive"] = 1
            cve["status"] = "没有CVE问题"
        elif cve_count > fixed_cve_count > 0:
            cve["is_positive"] = 0
            cve["status"] = "有CVE部分修复"
        elif fixed_cve_count == 0 and cve_count > 0:
            cve["is_positive"] = 0
            cve["status"] = "有CVE全部未修复"
        return cve

    def write_repo_data(self, repo, sig):
        print(f'start collect repo: {repo}')
        created_at = time.strftime("%Y-%m-%d", time.localtime())
        action = self.get_repo_status(repo)
        action['created_at'] = created_at
        action['sig_names'] = sig
        action['repo'] = repo
        action['is_last'] = 1
        action['maintainers'] = self.user_owner_repo.get(repo)
        action['kind'] = self.repo_type.get(repo)

        doc_id = created_at + '_' + repo
        actions = self.single_data(action, doc_id)
        with self._lock:
            self.actions += actions
            total_num = self.actions.count("\n")
            if total_num > 1000:
                self.esClient.safe_put_bulk(self.actions)
                self.actions = ''
        print(f'collect {repo} over')

    def get_repo_status(self, repo):
        participant, company = self.get_active(repo)
        cve = self.get_openeuper_cve_state(repo)
        issue = self.get_issue_update(repo)
        package_update = self.get_package_update(repo)
        package_version = self.get_repo_version(repo)
        metric = {
            'participant': participant,
            'company': company,
            'cve': cve,
            'issue': issue,
            'package_update': package_update,
            'package_version': package_version
        }

        action = self.get_repo_maintenance(metric)
        action.update(metric)

        if repo in self.collaboration_update_repo:
            metric.update(self.collaboration_update_repo.get(repo))
            action['is_update'] = 1
        metric['status'] = self.get_repo_maintenance(metric).get('status')
        action['collaboration'] = metric

        return action

    def get_repo_query(self, field):
        repo_dic = {}
        query = self.repo_query % field
        url = self.url + '/' + self.sig_index_name + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        buckets = res.json()['aggregations']['repos']['buckets']
        for bucket in buckets:
            repo_name = bucket['key']
            repo = repo_name.split('/')[-1]
            item_buckets = bucket['fields']['buckets']
            items = []
            for item_bucket in item_buckets:
                items.append(item_bucket['key'])
            repo_dic.update({repo: items})
        return repo_dic

    def get_collaboration_update_repo(self):
        search = '''{
            "size": 1000,
            "_source": [
                "collaboration","repo"
            ],
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "is_last:1 AND is_update:1"
                            }
                        }
                    ]
                }
            }
        }'''
        collaboration_update_repo = {}
        res_list = []

        def func(data):
            for item in data:
                res_list.append(item['_source'])

        self.esClient.scrollSearch(self.index_name, search, '1m', func)

        for res in res_list:
            repo = res.get('repo')
            collaboration = res.get('collaboration')
            update_metric = {}
            for metric, status in collaboration.items():
                if isinstance(status, dict) and status.get('is_update') == 1:
                    update_metric.update({metric: status})
            collaboration_update_repo[repo] = update_metric
        return collaboration_update_repo

    # 标记数据是否是最近更新
    def tag_last_update(self):
        try:
            query = '''{
                "script": {
                    "source": "ctx._source['is_last']=0"
                },
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "range": {
                                    "created_at": {
                                        "lt": "%s"
                                    }
                                }
                            },
                            {
                                "query_string": {
                                    "analyze_wildcard": true,
                                    "query": "is_last:1"
                                }
                            }
                        ]
                    }
                }
            }''' % time.strftime("%Y-%m-%d", time.localtime())
            self.esClient.updateByQuery(query)
        except Exception as e:
            logger.info(e)
