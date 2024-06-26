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
# Create: 2024/4/3
import csv
import datetime
import json
import logging
import os
import time

import requests
import yaml
from dateutil.relativedelta import relativedelta

from data.common import ESClient

GITEE_BASE = "gitee.com"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

STATUS_MAP = {
    "活跃": "is_active",
    "静止": "is_inactive",
    "健康": "is_health",
    "缺人维护": "is_lack_of_maintenance",
    "没有人维护": "is_no_maintenance"
}


class PackageOverview(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.url = config.get('es_url')
        self.from_data = config.get("from_data")
        self.headers = {'Content-Type': 'application/json', 'Authorization': config.get('authorization')}

        self.platform = config.get('platform')

        self.maintainer_index_name = config.get('maintainer_index_name')
        self.gitee_all_index_name = config.get('gitee_all_index_name')
        self.account_index_name = config.get('account_index_name')
        self.package_level_index_name = config.get('package_level_index_name')
        self.package_status_index_name = config.get('package_status_index_name')
        self.obs_meta_org = config.get('obs_meta_org')
        self.obs_meta_repo = config.get('obs_meta_repo')
        self.obs_meta_dir = config.get('obs_meta_dir')
        self.versions = config.get('versions', 'openEuler-24.03-LTS')
        self.code_base_path = config.get('code_base_path')
        self.username = config.get('username')
        self.access_token = config.get('access_token')

        self.active_query = config.get('active_query')
        self.maintainer_query = config.get('maintainer_query')
        self.account_query = config.get('account_query')
        self.d2_query = config.get('d2_query')
        self.package_level = {}

    def run(self, from_date):
        start = datetime.datetime.strptime(from_date, "%Y%m%d").strftime("%Y-%m-%d")
        end_date = datetime.datetime.strptime(from_date, "%Y%m%d") + relativedelta(years=1)
        end = end_date.strftime("%Y-%m-%d")
        self.tag_last_update()
        versions = self.versions.split(',')
        self.package_level = self.get_package_level()
        for version in versions:
            self.write_data(version=version, start=start, end=end)

    def write_data(self, version, start, end):
        actions = ''
        repo_types = self.get_obs_meta(version)
        repo_maintainers = self.get_maintainer()
        active_users = self.get_active(start, end)
        d2_users = self.get_d2(start, end)
        repo_sigs = self.get_repo_sig()
        user_company = self.get_user_company()
        maintainer_company, maintainer_email = self.get_maintainer_company()
        package_status = self.get_package_status()
        created_at = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
        for repo, types in repo_types.items():
            sig_names = repo_sigs.get(repo) if repo_sigs.get(repo) else []
            for kind in types:
                level = self.get_repo_level(repo, kind)
                action = {
                    'repo': repo,
                    'kind': kind,
                    'level': level,
                    'sig_names': sig_names,
                    'maintainer_list': repo_maintainers.get(repo),
                    'version': version,
                    'created_at': created_at,
                    'is_last': 1
                }
                action.update(package_status.get(repo))
                maintainers = repo_maintainers.get(repo)
                users = active_users.get(repo)
                if users and maintainers:
                    for maintainer in maintainers:
                        if maintainer not in users:
                            continue
                        if maintainer_company.get(maintainer):
                            company = maintainer_company.get(maintainer)
                        else:
                            company = user_company.get(maintainer, 'independent')
                        company = self.normalize_company(company)
                        email = maintainer_email.get(maintainer, '')
                        item = {'maintainer': maintainer, 'is_maintainer': 1, 'tag_user_company': company,
                                'email': email}
                        item.update(action)
                        doc_id = version + '_' + repo + '_' + kind + '_' + 'maintainer_' + maintainer + created_at
                        indexData = {"index": {"_index": self.index_name, "_id": doc_id}}
                        actions += json.dumps(indexData) + '\n'
                        actions += json.dumps(item) + '\n'
                users = d2_users.get(repo)
                if users:
                    for user in users:
                        item = {'d2': user, 'is_d2': 1, 'tag_user_company': user_company.get(user, 'independent')}
                        item.update(action)
                        doc_id = version + '_' + repo + '_' + kind + '_' + 'd2_' + user + created_at
                        indexData = {"index": {"_index": self.index_name, "_id": doc_id}}
                        actions += json.dumps(indexData) + '\n'
                        actions += json.dumps(item) + '\n'
                action.update({'is_repo': 1})
                status = package_status.get(repo)
                if status.get('status') in STATUS_MAP:
                    action.update({
                        STATUS_MAP[status.get('status')]: 1
                    })
                doc_id = version + '_' + repo + '_' + kind
                indexData = {"index": {"_index": self.index_name, "_id": doc_id}}
                actions += json.dumps(indexData) + '\n'
                actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)
        repos = repo_types.keys()
        self.update_status(repos)

    def normalize_company(self, company):
        if "kylinsoft" in company.lower() or "麒麟软件" in company:
            return "麒麟软件有限公司"
        elif "华为" in company or "huawei" in company.lower():
            return "huawei"
        elif "kylinsec" in company.lower():
            return "湖南麒麟信安科技股份有限公司"
        elif "hoperun" in company.lower():
            return "江苏润和软件股份有限公司"
        elif "xfusion" in company.lower():
            return "超聚变数字技术有限公司"
        elif "uniontech" in company.lower():
            return "统信软件技术有限公司"
        else:
            return company

    def get_package_status(self):
        search = '''{
            "size": 1000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "is_last": "1"
                            }
                        }
                    ]
                }
            },
            "_source": [
                "repo",
                "responsible_team",
                "responsible",
                "status"
            ]
        }'''
        repo_status = {}
        data_dic_list = []

        def func(data):
            for item in data:
                data_dic_list.append(item['_source'])

        self.esClient.scrollSearch(self.package_status_index_name, search, "1m", func)
        for data_item in data_dic_list:
            repo_status[data_item.get('repo')] = data_item

        return repo_status

    def get_repo_sig(self):
        search = '''{
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 AND is_sig_repo_committer:1"
                            }
                        }
                    ]
                }
            },
            "_source": [
                "repo_name",
                "sig_name"
            ]
        }'''
        repo_sig = {}
        data_dic_list = []

        def func(data):
            for item in data:
                data_dic_list.append(item['_source'])

        self.esClient.scrollSearch(self.maintainer_index_name, search, "1m", func)
        for data_item in data_dic_list:
            if data_item.get('repo_name'):
                repo = data_item.get('repo_name').split('/')[-1]
                repo_sig[repo] = [data_item.get('sig_name')]

        return repo_sig

    def get_active(self, start, end):
        query = self.active_query % (start, end)
        url = self.url + '/' + self.gitee_all_index_name + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        data = res.json()['aggregations']['2']['buckets']
        repo_user = {}
        for d in data:
            repo_name = d['key']
            repo = repo_name.split('/')[-1]
            users = d['3']['buckets']
            user_list = []
            for user in users:
                user_list.append(user['key'])
            repo_user.update({repo: user_list})
        return repo_user

    def get_maintainer(self):
        query = self.maintainer_query
        url = self.url + '/' + self.maintainer_index_name + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        data = res.json()['aggregations']['2']['buckets']
        repo_maintainers = {}
        for d in data:
            repo_name = d['key']
            repo = repo_name.split('/')[-1]
            users = d['3']['buckets']
            user_list = []
            for user in users:
                user_list.append(user['key'])
            repo_maintainers.update({repo: user_list})
        return repo_maintainers

    def get_maintainer_company(self):
        query = '''{
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 AND is_sig_original:1"
                            }
                        }
                    ]
                }
            }

        }'''
        url = self.url + '/' + self.maintainer_index_name + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))

        data = res.json()['hits']['hits']
        user_company = {}
        user_email = {}
        for d in data:
            maintainer_info = d['_source'].get('maintainer_info')
            if not maintainer_info:
                continue
            for info in maintainer_info:
                if info.get('gitee_id') and info.get('organization'):
                    user_company.update({info.get('gitee_id'): info.get('organization')})
                if info.get('gitee_id') and info.get('email'):
                    user_email.update({info.get('gitee_id'): info.get('email')})
        return user_company, user_email

    def get_user_company(self):
        query = self.account_query
        url = self.url + '/' + self.account_index_name + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        data = res.json()['aggregations']['2']['buckets']
        user_company = {}
        for d in data:
            user = d['key']
            company = d['3']['buckets'][0]['key']
            user_company.update({user: company})
        return user_company

    def get_d2(self, start, end):
        query = self.d2_query % (start, end)
        url = self.url + '/' + self.gitee_all_index_name + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        data = res.json()['aggregations']['2']['buckets']
        repo_user = {}
        for d in data:
            repo_name = d['key']
            repo = repo_name.split('/')[-1]
            users = d['3']['buckets']
            user_list = []
            for user in users:
                user_list.append(user['key'])
            repo_user.update({repo: user_list})
        return repo_user

    def get_obs_meta(self, version: str):
        obs_path = self.git_clone_or_pull_repo(platform=self.platform, owner=self.obs_meta_org,
                                               repo_name=self.obs_meta_repo)
        meta_dir = obs_path if self.obs_meta_dir is None else os.path.join(obs_path, self.obs_meta_dir)
        root, dirs, _ = os.walk(meta_dir).__next__()

        repo_types = {}

        package_dirs = []
        try:
            # 注意，windows下不支持目录中包含”:“等符号
            package_path = os.path.join(root, version)
            if os.path.exists(package_path):
                _, package_dirs, _ = os.walk(package_path).__next__()
        except:
            print('package_path error')
            return

        for dir in package_dirs:
            if dir == 'delete':
                continue
            repo_path = os.path.join(root, version, dir, 'pckg-mgmt.yaml')
            repo_info = yaml.safe_load(open(repo_path)).get('packages')
            for repo in repo_info:
                repo_name = repo.get('name')
                types = repo_types.get(repo_name)
                if types is not None:
                    types.append(dir)
                    repo_types.update({repo_name: types})
                else:
                    repo_types.update({repo_name: [dir]})
        return repo_types

    def git_clone_or_pull_repo(self, platform, owner, repo_name):
        # 本地仓库目录
        owner_path = os.path.join(self.code_base_path, platform, owner)
        if not os.path.exists(owner_path):
            os.makedirs(owner_path)
        code_path = os.path.join(owner_path, repo_name)

        if platform == 'gitee':
            clone_url = 'https://%s/%s/%s' % (GITEE_BASE, owner, repo_name)
        else:
            clone_url = None

        # 本地仓库已存在执行git pull；否则执行git clone
        self.removeGitLockFile(code_path)
        if os.path.exists(code_path):
            cmd_pull = 'cd %s;git checkout .;git pull --rebase' % code_path
            os.system(cmd_pull)
        else:
            if clone_url is None:
                return
            cmd_clone = 'cd "%s";git clone %s' % (owner_path, clone_url + '.git')
            os.system(cmd_clone)

        return code_path

    # 删除git lock
    def removeGitLockFile(self, code_path):
        lock_file = code_path + '/.git/index.lock'
        if os.path.exists(lock_file):
            os.remove(lock_file)

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

    def get_composite_page(self, start, end):
        url = f'{self.esClient.url}/{self.gitee_all_index_name}/_search'
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "created_at": {
                                    "gte": start,
                                    "lte": end
                                }
                            }
                        },
                        {
                            "query_string": {
                                "analyze_wildcard": True,
                                "query": "!is_removed:1"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "repositories": {
                    "composite": {
                        "sources": [
                            {
                                "repository_name": {
                                    "terms": {
                                        "field": "gitee_repo.keyword"
                                    }
                                }
                            },
                            {
                                "developer_name": {
                                    "terms": {
                                        "field": "user_login.keyword"
                                    }
                                }
                            }
                        ],
                        "size": 1000
                    },
                    "aggs": {}
                }
            }
        }

        results = {}
        response = requests.post(url, headers=self.esClient.default_headers, verify=False, json=query)

        while True:
            # Process current page of results
            for bucket in response.json()["aggregations"]["repositories"]["buckets"]:
                repository_name = bucket["key"]["repository_name"]
                developer_name = bucket["key"]["developer_name"]
                if repository_name in results:
                    results.update({repository_name: results[repository_name].append(developer_name)})
                else:
                    results.update({repository_name: [developer_name]})

            # Check if there are more pages
            if "after_key" not in response.json()["aggregations"]["repositories"]:
                break

            # Fetch next page of results
            after_key = response.json()["aggregations"]["repositories"]["after_key"]
            query["aggs"]["repositories"]["composite"]["after"] = after_key
            response = requests.post(url, headers=self.esClient.default_headers, verify=False, json=query)

        return results

    def get_active_user(self):
        search = '''{
            "query": {
                "bool": {
                    "must": [
                        {
                            "exists": {
                                "field": "gitee_repo.keyword"
                            }
                        },
                        {
                            "exists": {
                                "field": "user_login.keyword"
                            }
                        }
                    ],
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1"
                            }
                        }
                    ]
                }
            },
            "_source": [
                "gitee_repo",
                "user_login"
            ],
            "size": 1000
        }'''
        data_dic_list = []

        def func(data):
            for item in data:
                data_dic_list.append(item['_source'])

        self.esClient.scrollSearch(self.gitee_all_index_name, search, "1m", func)
        results = {}
        for data_item in data_dic_list:
            repo = data_item.get('gitee_repo')
            if repo in results:
                results.update({repo: results[repo].append(data_item.get('user_login'))})
            else:
                results.update({repo: [data_item.get('user_login')]})
        return results

    def read_openeuler_software_level(self):
        actions = ''
        created_at = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
        with open("openeuler_level.csv", mode="r", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if reader.line_num == 1:
                    continue
                if row[1] == "baseos":
                    level = row[3] if row[3] else "L3"
                if row[1] == "everything":
                    level = "L4"
                if row[1] == "epol":
                    level = "epol"
                action = {
                    "repo": row[0],
                    "kind": row[1],
                    "level": level,
                    "responsible_team": row[4],
                    "responsible": row[5],
                    "created_at": created_at
                }
                doc_id = row[0] + '_' + row[1]
                index_data = {"index": {"_index": self.package_level_index_name, "_id": doc_id}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def update_status(self, repos):
        update_query = '''{
            "script": {
                "source": "ctx._source.final_status = params.final_status",
                "params": {
                    "final_status": "%s"
                },
                "lang": "painless"
            },
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "is_last:1 AND repo.keyword:%s"
                            }
                        }
                    ]
                }
            }
        }'''
        re_query = '''{
            "script": {
                "source": "ctx._source.final_status = ctx._source.status",
                "lang": "painless"
            },
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "is_last:1 AND repo.keyword:%s"
                            }
                        }
                    ]
                }
            }
        }'''
        update_repos = self.get_update_repos()
        for repo in repos:
            if repo in update_repos:
                print(f'update {repo}')
                query = update_query % ("合理静止", repo)
            else:
                query = re_query % repo

            self.esClient.updateByQuery(query.encode('utf-8'))

    def get_update_repos(self):
        csv_file = open("baseos_repos.csv", mode="r", encoding="utf-8")
        reader = csv.reader(csv_file)
        repos = []
        for item in reader:
            if reader.line_num == 1:
                continue
            repos.append(item[0])

        base_repos = self.get_kind_status_repos("baseos", "静止")
        update_repos = [repo for repo in repos if repo in base_repos]
        return update_repos

    def get_kind_status_repos(self, kind, status):
        search = '''{
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "is_last:1 AND kind.keyword:%s AND status.keyword:\\"%s\\""
                            }
                        }
                    ]
                }
            },
            "_source": [
                "repo"
            ]
        }''' % (kind, status)
        repos = []
        data_dic_list = []

        def func(data):
            for item in data:
                data_dic_list.append(item['_source'])

        self.esClient.scrollSearch(self.index_name, search, "1m", func)
        for data_item in data_dic_list:
            repos.append(data_item.get('repo'))
        return repos
