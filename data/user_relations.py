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

import json

from data.common import ESClient


class UserRelations:
    def __init__(self, config=None):
        self.config = config
        self.org = config.get('org')
        self.esClient = ESClient(config)
        self.maintainer_index = config.get('maintainer_index')
        self.user_index = config.get('user_index')
        self.target_index = config.get('target_index')
        self.maintainers = []

    def run(self, from_date):
        print('****** Start collection UserRelations ******')
        self.get_maintainers()
        self.get_issue_user()
        self.get_pr_user()
        print('****** Finnish collection UserRelations ******')

    def get_maintainers(self):
        search = '''{
                      "size": 0,
                      "query": {
                        "bool": {
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
                      "aggs": {
                        "committers": {
                          "terms": {
                            "field": "committer.keyword",
                            "size": 10000,
                            "min_doc_count": 1
                          }
                        }
                      }
                    }'''
        data = self.esClient.esSearch(index_name=self.maintainer_index, search=search, method='_search')
        for maintainer in data['aggregations']['committers']['buckets']:
            self.maintainers.append(maintainer['key'])

    def get_issue_user(self):
        print('****** start issue user relations ******')
        search = '''{
                      "size": 1000,
                      "_source": {
                        "includes": [
                          "created_at",
                          "user_login",
                          "issue_number",
                          "sig_names",
                          "tag_user_company"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "term": {
                                "is_gitee_issue_comment": 1
                              }
                            }
                          ]
                        }
                      }
                    }'''
        self.esClient.scrollSearch(index_name=self.user_index, search=search, scroll_duration='2m',
                                   func=self.issue_user_func)
        print('****** finish issue user relations ******')

    def issue_user_func(self, hits):
        actions = ''
        for hit in hits:
            id = hit['_id']
            source = hit['_source']
            comment_user = source['user_login']

            issue_user = self.get_user_by_issue_num(num=source['issue_number'])
            if issue_user is None:
                continue

            is_maintainer = 0
            if comment_user in self.maintainers:
                is_maintainer = 1

            if 'sig_names' in source:
                action = {
                    'created_at': source['created_at'],
                    'comment_user': comment_user,
                    'be_commented_user': issue_user,
                    'comment_type': 'issue_comment',
                    'sig_names': source['sig_names'],
                    'tag_user_company': source['tag_user_company'],
                    'is_maintainer': is_maintainer
                }
            else:
                action = {
                    'created_at': source['created_at'],
                    'comment_user': comment_user,
                    'be_commented_user': issue_user,
                    'comment_type': 'issue_comment',
                    'tag_user_company': source['tag_user_company'],
                    'is_maintainer': is_maintainer
                }
            indexData = {"index": {"_index": self.target_index, "_id": id}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_user_by_issue_num(self, num):
        search = '''{
                      "size": 1,
                      "_source": {
                        "includes": [
                          "user_login"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "term": {
                                "is_gitee_issue": 1
                              }
                            },
                            {
                              "term": {
                                "issue_number.keyword": "%s"
                              }
                            }
                          ]
                        }
                      }
                    }''' % num
        print('*** issue number: %s ***' % num)
        data = self.esClient.esSearch(index_name=self.user_index, search=search, method='_search')
        hits = data['hits']['hits']
        if len(hits) < 1:
            return None
        else:
            return hits[0]['_source']['user_login']

    def get_pr_user(self):
        print('****** start pr user relations ******')
        search = '''{
                      "size": 1000,
                      "_source": {
                        "includes": [
                          "created_at",
                          "user_login",
                          "pull_id",
                          "sig_names",
                          "tag_user_company"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "term": {
                                "is_gitee_review_comment": 1
                              }
                            }
                          ]
                        }
                      }
                    }'''
        self.esClient.scrollSearch(index_name=self.user_index, search=search, scroll_duration='2m',
                                   func=self.pr_user_func)
        print('****** finish pr user relations ******')

    def pr_user_func(self, hits):
        actions = ''
        for hit in hits:
            id = hit['_id']
            source = hit['_source']
            comment_user = source['user_login']

            pr_user = self.get_user_by_pr_id(id=source['pull_id'])
            if pr_user is None:
                continue

            is_maintainer = 0
            if comment_user in self.maintainers:
                is_maintainer = 1

            if 'sig_names' in source:
                action = {
                    'created_at': source['created_at'],
                    'comment_user': comment_user,
                    'be_commented_user': pr_user,
                    'comment_type': 'pr_comment',
                    'sig_names': source['sig_names'],
                    'tag_user_company': source['tag_user_company'],
                    'is_maintainer': is_maintainer
                }
            else:
                action = {
                    'created_at': source['created_at'],
                    'comment_user': comment_user,
                    'be_commented_user': pr_user,
                    'comment_type': 'pr_comment',
                    'tag_user_company': source['tag_user_company'],
                    'is_maintainer': is_maintainer
                }
            indexData = {"index": {"_index": self.target_index, "_id": id}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_user_by_pr_id(self, id):
        print('*** pr number: %s ***' % id)
        search = '''{
                      "size": 1,
                      "_source": {
                        "includes": [
                          "user_login"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "term": {
                                "is_gitee_pull_request": 1
                              }
                            },
                            {
                              "term": {
                                "pull_id": "%d"
                              }
                            }
                          ]
                        }
                      }
                    }''' % id
        data = self.esClient.esSearch(index_name=self.user_index, search=search, method='_search')
        hits = data['hits']['hits']
        if len(hits) < 1:
            return None
        else:
            return hits[0]['_source']['user_login']