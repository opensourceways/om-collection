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
# Create: 2024/4/8
import csv
import datetime
import json
import logging
import time

import requests
from dateutil.relativedelta import relativedelta

from data.common import ESClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

GITEE_BASE = "gitee.com"


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
        self.pkg_version_url = config.get('pkg_version_url')
        self.package_level_index_name = config.get('package_level_index_name')
        self.repo_active = {}
        self.package_level = {}

    def run(self, from_date):
        self.tag_last_update()
        repos = self.get_repo_list()
        actions = ''
        cnt = 0
        self.get_active_dict()
        self.get_package_level()
        for repo, sig in repos.items():
            cnt += 1
            actions += self.write_repo_data(repo, sig)
            if cnt > 1000:
                self.esClient.safe_put_bulk(actions)
                actions = ''
                cnt = 0
        self.esClient.safe_put_bulk(actions)

    def get_time_range(self):
        from_date = datetime.datetime.now()
        last_date = from_date - relativedelta(years=1)
        from_time = time.mktime(last_date.timetuple()) * 1000
        end_time = time.mktime(from_date.timetuple()) * 1000
        return from_time, end_time

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
            if repo_active['users'] > 10:
                participant = {"is_positive": 1, "status": "贡献人数多", "num": repo_active['users']}
            else:
                participant = {"is_positive": 0, "status": "贡献人数少", "num": repo_active['users']}
            if repo_active['companies'] > 5:
                company = {"is_positive": 1, "status": "贡献组织多", "num": repo_active['companies']}
            else:
                company = {"is_positive": 0, "status": "贡献组织少", "num": repo_active['companies']}
        return participant, company

    def get_active_dict(self):
        time_range = self.get_time_range()
        query = '''{
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "created_at": {
                                    "gte": %d,
                                    "lte": %d,
                                    "format": "epoch_millis"
                                }
                            }
                        },
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 AND org_name.keyword:src-openeuler"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "2": {
                    "terms": {
                        "field": "gitee_repo.keyword",
                        "size": 20000,
                        "order": {
                            "_key": "desc"
                        },
                        "min_doc_count": 1
                    },
                    "aggs": {
                        "user": {
                            "cardinality": {
                                "field": "user_login.keyword"
                            }
                        },
                        "company": {
                            "cardinality": {
                                "field": "tag_user_company.keyword"
                            }
                        }
                    }
                }
            }
        }''' % (time_range[0], time_range[1])
        url = self.url + '/' + self.gitee_all_index_name + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        buckets = res.json()['aggregations']['2']['buckets']
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
        query = '''{
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 AND org_name.keyword:src-openeuler"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "2": {
                    "terms": {
                        "field": "gitee_repo.keyword",
                        "size": 20000,
                        "order": {
                            "_key": "desc"
                        },
                        "min_doc_count": 1
                    },
                    "aggs": {
                        "3": {
                            "terms": {
                                "field": "sig_names.keyword",
                                "size": 10,
                                "order": {
                                    "_key": "desc"
                                },
                                "min_doc_count": 1
                            },
                            "aggs": {}
                        }
                    }
                }
            }
        }'''
        url = self.url + '/' + self.gitee_all_index_name + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        buckets = res.json()['aggregations']['2']['buckets']
        repo_list = {}
        for bucket in buckets:
            repo_name = bucket['key']
            repo = repo_name.split('/')[-1]
            sig_buckets = bucket['3']['buckets']
            for sig_bucket in sig_buckets:
                repo_list.update({repo: sig_bucket['key']})

        return repo_list

    def get_package_update(self, repo):
        time_range = self.get_time_range()
        repo = "https://gitee.com/src-openeuler/" + repo
        query = '''{
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "created_at": {
                                    "gte": %d,
                                    "lte": %d,
                                    "format": "epoch_millis"
                                }
                            }
                        },
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 AND org_name.keyword:src-openeuler AND gitee_repo.keyword:\\"%s\\" AND is_gitee_pull_request:1"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "2": {
                    "terms": {
                        "field": "pull_state.keyword",
                        "size": 20000,
                        "order": {
                            "_key": "desc"
                        },
                        "min_doc_count": 1
                    },
                    "aggs": {}
                }
            }
        }''' % (time_range[0], time_range[1], repo)

        resp = self.esClient.esSearch(index_name=self.gitee_all_index_name, search=query)
        aggregations = resp.get('aggregations').get('2').get('buckets')
        if len(aggregations) == 0:
            package_update = {"is_positive": 0, "status": "没有PR提交"}
        else:
            package_update = self.pr_merged(aggregations)
        return package_update

    def pr_merged(self, hits):
        for hit in hits:
            if hit.get('key') == 'merged' and hit.get('doc_count') > 0:
                return {"is_positive": 1, "status": "有PR合入"}
        return {"is_positive": 0, "status": "有PR提交未合入"}

    def get_issue_update(self, repo):
        time_range = self.get_time_range()
        repo = "https://gitee.com/src-openeuler/" + repo
        query = '''{
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "created_at": {
                                    "gte": %d,
                                    "lte": %d,
                                    "format": "epoch_millis"
                                }
                            }
                        },
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "!is_removed:1 AND org_name.keyword:src-openeuler AND gitee_repo.keyword:\\"%s\\" AND is_gitee_issue:1"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "2": {
                    "terms": {
                        "field": "pull_state.keyword",
                        "size": 20000,
                        "order": {
                            "_key": "desc"
                        },
                        "min_doc_count": 1
                    },
                    "aggs": {}
                }
            }
        }''' % (time_range[0], time_range[1], repo)

        resp = self.esClient.esSearch(index_name=self.gitee_all_index_name, search=query)
        aggregations = resp.get('aggregations').get('2').get('buckets')
        issue_state = self.issue_state(aggregations)
        return issue_state

    def issue_state(self, hits):
        for hit in hits:
            if hit.get('key') == 'closed' and hit.get('doc_count') == 0:
                return {"is_positive": 0, "status": "没有Issue修复"}
            if hit.get('key') == 'open' and hit.get('doc_count') == 0:
                return {"is_positive": 1, "status": "全部Issue修复"}
        return {"is_positive": 0, "status": "有部分Issue修复"}

    def get_repo_version(self, repo):
        params = {"name": repo}
        resp = requests.get(url=self.pkg_version_url, params=params, timeout=60)
        up_version, openeuler_version = None, None
        if resp.status_code == 200:
            items = resp.json().get('items')
            for item in items:
                if item.get('tag').endswith('_up'):
                    up_version = item.get('version')
                if item.get('tag').endswith('_openeuler'):
                    openeuler_version = item.get('version')
        if up_version == openeuler_version and up_version and openeuler_version:
            version = {"is_positive": 1, "status": "最新版本", "version": openeuler_version, "up_version": up_version}
        else:
            version = {"is_positive": 0, "status": "落后版本", "version": openeuler_version, "up_version": up_version}
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
        created_at = time.strftime("%Y-%m-%d", time.localtime())
        action = self.get_repo_status(repo)
        action['created_at'] = created_at
        action['sig_names'] = sig
        action['is_last'] = 1
        action.update(self.get_repo_maintenance(action))
        if self.package_level.get(repo):
            action.update(self.package_level.get(repo))
        else:
            action.update({'level': 'Unmarked'})
        doc_id = created_at + '_' + repo
        actions = self.single_data(action, doc_id)
        return actions

    def get_repo_status(self, repo):
        participant, company = self.get_active(repo)
        cve = self.get_openeuper_cve_state(repo)
        issue = self.get_issue_update(repo)
        package_update = self.get_package_update(repo)
        package_version = self.get_repo_version(repo)
        action = {
            'repo': repo,
            'participant': participant,
            'company': company,
            'cve': cve,
            'issue': issue,
            'package_update': package_update,
            'package_version': package_version
        }
        return action

    def get_repo_maintenance(self, action):
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

    def get_package_level(self):
        search = '''{
            "size": 1000,
            "_source": [
                "repo",
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
        for data_item in data_dic_list:
            data_item['level'] = data_item['level'] if data_item.get('level') else "Unmarked"
            self.package_level[data_item.get('repo')] = data_item

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
