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
import functools
import json
import logging
import os
import re
import threading
import time
from itertools import zip_longest
from packaging.version import Version, InvalidVersion

import requests
import yaml

from constant.pkg_maintenance_constant import CVE_STATUS, VERSION_STATUS, CONTRIBUTE_STATUS, UPDATE_STATUS, \
    ISSUE_STATUS, REPO_STATUS
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
        self.package_level_index_name = config.get("package_level_index_name")
        self.repo_query = config.get("repo_query")
        self.active_query = config.get("active_query")
        self.query_format = config.get("query_format")

        self.pkg_up_version_url = config.get('pkg_up_version_url')
        self.pkg_euler_version_url = config.get('pkg_euler_version_url')
        self.gitee_repo_base = config.get("gitee_repo_base")
        self.repo_active = {}
        self.repo_versions = {}
        self.user_owner_repo = {}
        self.collaboration_update_repo = {}
        self.repo_type = {}
        self.package_level = {}
        self.company_num = int(config.get('company_num', '5'))
        self.user_num = int(config.get('company_num', '10'))

        self.platform = config.get('platform')
        self.obs_meta_org = config.get('obs_meta_org')
        self.obs_meta_repo = config.get('obs_meta_repo')
        self.obs_meta_dir = config.get('obs_meta_dir')
        self.versions = config.get('versions', 'openEuler-24.09')
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
    def compare_version(version1, version2):
        version1 = PackageStatus.format_version(version1)
        version2 = PackageStatus.format_version(version2)
        v1_parts = list(map(int, filter(None, version1.split('.'))))
        v2_parts = list(map(int, filter(None, version2.split('.'))))

        for v1, v2 in zip_longest(v1_parts, v2_parts, fillvalue=0):
            if v1 < v2:
                return -1
            elif v1 > v2:
                return 1
        return 0

    @staticmethod
    def format_version(version: str):
        version = re.sub(r'[a-zA-Z]', '', version)
        version = re.sub(r'[-/_+~]', '.', version)
        return version

    @staticmethod
    def pr_merged(hits):
        for hit in hits:
            if hit.get('key') == 'merged' and hit.get('doc_count') > 0:
                return {"is_positive": 1, "status": UPDATE_STATUS.get('pr_merged')}
        return {"is_positive": 0, "status": UPDATE_STATUS.get('no_merged')}

    @staticmethod
    def issue_state(hits):
        for hit in hits:
            if hit.get('key') == 'closed' and hit.get('doc_count') == 0:
                return {"is_positive": 0, "status": ISSUE_STATUS.get('no_fixed')}
            if hit.get('key') == 'open' and hit.get('doc_count') == 0:
                return {"is_positive": 1, "status": ISSUE_STATUS.get('all_fixed')}
        return {"is_positive": 0, "status": ISSUE_STATUS.get('some_fixed')}

    @staticmethod
    def get_repo_maintenance(action):
        status = {'status': '其他'}
        if action.get('cve').get('status') == CVE_STATUS.get('no_fixed') \
                and action.get('issue').get('status') == ISSUE_STATUS.get('no_fixed'):
            status = {'status': REPO_STATUS.get('no_maintenance'), 'is_no_maintenance': 1}
        elif action.get('cve').get('status') == CVE_STATUS.get('no_fixed'):
            status = {'status': REPO_STATUS.get('lack_of_maintenance'), 'is_lack_of_maintenance': 1}
        elif action.get('cve').get('status') == CVE_STATUS.get('some_fixed'):
            status = {'status': REPO_STATUS.get('lack_of_maintenance'), 'is_lack_of_maintenance': 1}
        elif action.get('cve').get('status') == CVE_STATUS.get('all_fixed'):
            status = {'status': REPO_STATUS.get('health'), 'is_health': 1}
        elif action.get('cve').get('status') == CVE_STATUS.get('no_cve') \
                and action.get('package_update').get('status') == UPDATE_STATUS.get('no_pr') \
                and action.get('package_version').get('status') == VERSION_STATUS.get('normal'):
            status = {'status': REPO_STATUS.get('health'), 'is_health': 1}
        elif action.get('cve').get('status') == CVE_STATUS.get('no_cve') \
                and action.get('package_update').get('status') == UPDATE_STATUS.get('no_pr') \
                and action.get('package_version').get('status') == VERSION_STATUS.get('outdated'):
            status = {'status': REPO_STATUS.get('inactive'), 'is_inactive': 1}
        elif action.get('cve').get('status') == CVE_STATUS.get('no_cve'):
            status = {'status': REPO_STATUS.get('active'), 'is_active': 1}
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

    @staticmethod
    def get_version_api(version_url, up_stream=False):
        repo_versions = {}
        resp = requests.get(url=version_url, timeout=60)
        if resp.status_code != 200:
            print('get up version error', resp.text)
            resp.raise_for_status()
        items = resp.json().get('data').get('res')
        for repo, value in items.items():
            if not repo or not value:
                continue
            pkgs = {}
            for val in value:
                if not val.get('name') or not val.get('version'):
                    continue
                pkg_name = val.get('name')
                if not up_stream:
                    version = val.get('version').split('-')[0]
                else:
                    version = val.get('version').split(pkg_name + '-')[-1]
                pkgs[pkg_name] = version
            repo_versions[repo] = pkgs
        return repo_versions

    @staticmethod
    def check_repo_version(up_version, euler_version):
        if not up_version or not euler_version:
            return False
        if len(up_version) == 1:
            version = next(iter(up_version.values()))
            version = PackageStatus.format_version(version)
            return all(PackageStatus.compare_version(PackageStatus.format_version(ver), version) == 0
                       for ver in euler_version.values())

        return all(ver == up_version.get(pkg) for pkg, ver in euler_version.items())

    @staticmethod
    def convert_versions(items: dict):
        if not items:
            return None
        pkgs = []
        for key, value in items.items():
            pkg = {
                'pkg_name': key,
                'pkg_version': value
            }
            pkgs.append(pkg)
        return pkgs

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

    def write_data_thread(self, version):
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
                    args=(repo, sig, version,))
                threads.append(t)
                t.start()
        for t in threads:
            t.join()
        t2 = time.time()
        print('cost time: ', t2 - t1)
        self.esClient.safe_put_bulk(self.actions)

    def single_data(self, action, doc_id):
        index_data = {"index": {"_index": self.index_name, "_id": doc_id}}
        actions = json.dumps(index_data) + '\n'
        actions += json.dumps(action) + '\n'
        return actions

    def get_package_level(self):
        search = '''{
            "size": 1000,
            "_source": [
                "repo",
                "kind",
                "level",
                "responsible_team",
                "responsible"
            ]   
        }'''

        data_dic_list = []

        def func(data):
            for item in data:
                data_dic_list.append(item['_source'])

        self.esClient.scrollSearch(self.package_level_index_name, search, "1m", func)

        res = {}
        for data_item in data_dic_list:
            res[data_item['repo']] = data_item
        return res

    def get_repo_level(self, repo, kind):
        if kind == 'baseos' and self.package_level.get(repo).get('kind') == kind:
            level = self.package_level.get(repo).get('level')
        elif kind == 'baseos':
            level = 'L3'
        elif kind == 'epol':
            level = 'epol'
        else:
            level = 'L4'
        return level

    def get_active(self, repo):
        repo_active = self.repo_active.get(repo)
        participant = {"is_positive": 0, "status": CONTRIBUTE_STATUS.get('few_participants'), "num": 0}
        company = {"is_positive": 0, "status": CONTRIBUTE_STATUS.get('few_orgs'), "num": 0}
        if repo_active:
            if repo_active['users'] > self.user_num:
                participant = {"is_positive": 1, "status": CONTRIBUTE_STATUS.get('many_participants'),
                               "num": repo_active['users']}
            else:
                participant = {"is_positive": 0, "status": CONTRIBUTE_STATUS.get('few_participants'),
                               "num": repo_active['users']}
            if repo_active['companies'] > self.company_num:
                company = {"is_positive": 1, "status": CONTRIBUTE_STATUS.get('many_orgs'),
                           "num": repo_active['companies']}
            else:
                company = {"is_positive": 0, "status": CONTRIBUTE_STATUS.get('few_orgs'),
                           "num": repo_active['companies']}
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
            package_update = {"is_positive": 0, "status": UPDATE_STATUS.get('no_pr')}
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

    def get_repo_euler_version(self, version):
        version_url = self.pkg_euler_version_url % version
        repo_versions = self.get_version_api(version_url)
        return repo_versions

    def get_repo_up_version(self):
        repo_versions = self.get_version_api(self.pkg_up_version_url, up_stream=True)
        return repo_versions

    def check_up_version(self, up_version, euler_version):
        if not up_version:
            return True
        euler_version_list = list(euler_version.values())
        up_version_list = list(up_version.values())
        print(euler_version_list, up_version_list)
        euler_version_list.sort(key=functools.cmp_to_key(self.compare_version))
        up_version_list.sort(key=functools.cmp_to_key(self.compare_version))
        return self.compare_version(euler_version_list[-1], up_version_list[-1]) == 1

    def get_all_repo_version_status(self, up_versions, euler_versions):
        repo_version_status = {}
        for repo, euler_version in euler_versions.items():
            up_version = up_versions.get(repo)
            if self.check_up_version(up_version, euler_version):
                up_version = None
                version_status = VERSION_STATUS.get('outdated')
                positive = 0
            elif self.check_repo_version(up_version, euler_version):
                version_status = VERSION_STATUS.get('normal')
                positive = 1
            else:
                version_status = VERSION_STATUS.get('outdated')
                positive = 0
            version = {
                "is_positive": positive,
                "status": version_status,
                "version": self.convert_versions(euler_version),
                "up_version": self.convert_versions(up_version)
            }
            repo_version_status[repo] = version
        # 只有上游版本，没有下游版本
        for repo, up_version in up_versions.items():
            if repo not in repo_version_status:
                repo_version_status[repo] = {
                    "is_positive": 0,
                    "status": VERSION_STATUS.get('outdated'),
                    "version": None,
                    "up_version": self.convert_versions(up_version)
                }
        return repo_version_status

    def get_repo_version(self, repo):
        if repo in self.repo_versions:
            version = self.repo_versions.get(repo)
        else:
            version = {"is_positive": 0, "status": VERSION_STATUS.get('outdated'), "version": None, "up_version": None}
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
                            "match": {
                                "repository.keyword": "%s"
                            }
                        },
                        {
                            "range": {
                                "created_at": {
                                    "gte": "now-1y",
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
                    or data_dic["issue_customize_state"] == "已挂起"
            ):
                fixed_cve_count += 1
        if fixed_cve_count == cve_count > 0:
            cve["is_positive"] = 1
            cve["status"] = CVE_STATUS.get('all_fixed')
        elif fixed_cve_count == cve_count == 0:
            cve["is_positive"] = 1
            cve["status"] = CVE_STATUS.get('no_cve')
        elif cve_count > fixed_cve_count > 0:
            cve["is_positive"] = 0
            cve["status"] = CVE_STATUS.get('some_fixed')
        elif fixed_cve_count == 0 and cve_count > 0:
            cve["is_positive"] = 0
            cve["status"] = CVE_STATUS.get('no_fixed')
        return cve

    def write_repo_data(self, repo, sig, version):
        print(f'start collect repo: {repo}')
        created_at = time.strftime("%Y-%m-%d", time.localtime())
        action = self.get_repo_status(repo)
        action['created_at'] = created_at
        action['sig_names'] = sig
        action['repo'] = repo
        action['version'] = version
        action['is_last'] = 1
        action['maintainers'] = self.user_owner_repo.get(repo)
        action['kind'] = self.repo_type.get(repo)
        level = self.get_repo_level(repo, self.repo_type.get(repo))
        action['level'] = level

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

    def run(self, from_date):
        # Get repos update by the collaboration platform
        self.collaboration_update_repo = self.get_collaboration_update_repo()

        self.user_owner_repo = self.get_repo_query("user_login")
        self.package_level = self.get_package_level()
        for version in self.versions.split(','):
            root_path = self.get_obs_meta()
            self.repo_type = self.get_version_repo_type(root_path, version)
            repo_euler_version = self.get_repo_euler_version(version=version)
            repo_up_version = self.get_repo_up_version()
            self.repo_versions = self.get_all_repo_version_status(repo_up_version, repo_euler_version)
            self.write_data_thread(version)

        # tag old data
        self.tag_last_update()
