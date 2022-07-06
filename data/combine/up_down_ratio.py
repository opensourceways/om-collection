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
import datetime
import json
import time

from data.common import ESClient

MIN_TIME = '2019-05-01'


def get_ratio(prev_count, last_count):
    if prev_count == 0 and last_count == 0:
        percent = 0.0
    elif prev_count == 0 and last_count != 0:
        percent = 1.0
    else:
        percent = (last_count - prev_count) / prev_count
    return percent


class UpDownRatio(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.query_index_name = config.get('query_index_name')
        self.query_commit_index_name = config.get('query_commit_index_name')
        self.query_website_index_name = config.get('query_website_index_name')
        self.query_download_ip_index_name = config.get('query_download_ip_index_name')
        self.from_time = config.get('from_time')
        self.esClient = ESClient(config)
        self.user_count_query_str = str(config.get('user_count_query_str'))
        self.user_count_key = str(config.get('user_count_key'))
        self.contribute_query_str = str(config.get('contribute_query_str'))
        self.contribute_key = str(config.get('contribute_key'))
        self.website_query_str = str(config.get('website_query_str'))
        self.website_key = str(config.get('website_key'))
        self.search_user_count = '''{
                                      "size": 0,
                                      "query": {
                                        "bool": {
                                          "filter": [
                                            {
                                              "range": {
                                                "created_at": {
                                                  "gte": "%s",
                                                  "lt": "%s",
                                                  "time_zone": "Asia/Shanghai"
                                                }
                                              }
                                            },
                                            {
                                              "query_string": {
                                                "analyze_wildcard": true,
                                                "query": "%s"
                                              }
                                            }
                                          ]
                                        }
                                      },
                                      "aggs": {
                                        "1": {
                                          "terms": {
                                            "field": "user_login.keyword",
                                            "size": 20000
                                          },
                                          "aggs": {
                                            "2": {
                                              "cardinality": {
                                                "field": "user_login.keyword"
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }'''
        self.search_contribute = '''{
                                      "query": {
                                        "bool": {
                                          "filter": [
                                            {
                                              "range": {
                                                "created_at": {
                                                  "gte": "%s",
                                                  "lt": "%s",
                                                  "time_zone": "Asia/Shanghai"
                                                }
                                              }
                                            },
                                            {
                                              "query_string": {
                                                "analyze_wildcard": true,
                                                "query": "%s"
                                              }
                                            }
                                          ]
                                        }
                                      }
                                    }'''
        self.search_website = '''{
                                      "size": 0,
                                      "query": {
                                        "bool": {
                                          "filter": [
                                            {
                                              "range": {
                                                "created_at": {
                                                  "gte": "%s",
                                                  "lt": "%s",
                                                  "time_zone": "Asia/Shanghai"
                                                }
                                              }
                                            },
                                            {
                                              "query_string": {
                                                "analyze_wildcard": true,
                                                "query": "%s"
                                              }
                                            }
                                          ]
                                        }
                                      },
                                      "aggs": {
                                        "pv": {
                                          "sum": {
                                            "field": "pv_count"
                                          }
                                        },
                                        "ip": {
                                          "sum": {
                                            "field": "ip_count"
                                          }
                                        }
                                      }
                                    }'''
        self.search_download_ip_count = '''{
                                      "size": 0,
                                      "query": {
                                        "bool": {
                                          "filter": [
                                            {
                                              "range": {
                                                "created_at": {
                                                  "gte": "%s",
                                                  "lt": "%s",
                                                  "time_zone": "Asia/Shanghai"
                                                }
                                              }
                                            },
                                            {
                                              "query_string": {
                                                "analyze_wildcard": true,
                                                "query": "%s"
                                              }
                                            }
                                          ]
                                        }
                                      },
                                      "aggs": {
                                        "1": {
                                          "terms": {
                                            "field": "ip.keyword",
                                            "size": 30000
                                          },
                                          "aggs": {
                                            "2": {
                                              "cardinality": {
                                                "field": "ip.keyword"
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }'''
        self.search_download_ip_count_cardinality = '''{
                                              "size": 0,
                                              "query": {
                                                "bool": {
                                                  "filter": [
                                                    {
                                                      "range": {
                                                        "created_at": {
                                                          "gte": "%s",
                                                          "lt": "%s",
                                                          "time_zone": "Asia/Shanghai"
                                                        }
                                                      }
                                                    },
                                                    {
                                                      "query_string": {
                                                        "analyze_wildcard": true,
                                                        "query": "%s"
                                                      }
                                                    }
                                                  ]
                                                }
                                              },
                                              "aggs": {
                                                "count": {
                                                  "cardinality": {
                                                    "field": "ip.keyword"
                                                  }
                                                }
                                              }
                                            }'''
        self.item_days = {"week_ratio": 7, "month_ratio": 30}

        self.download_indexes = str(config.get('download_indexes'))
        self.download_sum_fields = str(config.get('download_sum_fields'))
        self.download_coefficients = str(config.get('download_coefficients'))
        self.download_pub_coefficient = float(config.get('download_pub_coefficient'))
        self.search_download = '''{
                                      "size": 0,
                                      "query": {
                                        "bool": {
                                          "filter": [
                                            {
                                              "range": {
                                                "created_at": {
                                                  "gte": "%s",
                                                  "lt": "%s",
                                                  "time_zone": "Asia/Shanghai"
                                                }
                                              }
                                            },
                                            {
                                              "query_string": {
                                                "analyze_wildcard": true,
                                                "query": "*"
                                              }
                                            }
                                          ]
                                        }
                                      },
                                      "aggs": {
                                        "1": {
                                          "sum": {
                                            "field": "%s"
                                          }
                                        }
                                      }
                                    }'''

    def run(self, from_time):
        self.download_up_down()
        self.contribute_up_down()
        self.website_up_down()
        self.user_count_up_down()

    def user_count_up_down(self):
        querys = self.user_count_query_str.split(";")
        keys = self.user_count_key.split(";")
        for i in range(len(keys)):
            for item, days in self.item_days.items():
                key = keys[i] + '_' + item
                self.user_count_up_down_ratio(key=key, before_days=days, query_str=querys[i])

    def contribute_up_down(self):
        querys = self.contribute_query_str.split(";")
        keys = self.contribute_key.split(";")
        for i in range(len(keys)):
            for item, days in self.item_days.items():
                key = keys[i] + '_' + item
                self.contribute_up_down_ratio(key=key, before_days=days, query_str=querys[i])

    def website_up_down(self):
        querys = self.website_query_str.split(";")
        keys = self.website_key.split(";")
        for i in range(len(keys)):
            for item, days in self.item_days.items():
                key = keys[i] + '_' + item
                self.website_up_down_ratio(key=key, before_days=days, query_str=querys[i])

    def download_up_down(self):
        for item, days in self.item_days.items():
            key = 'download_count_' + item
            self.download_up_down_ratio(key=key, before_days=days)

    def user_count_up_down_ratio(self, key, before_days, query_str):
        query_index = self.query_index_name
        search_count = self.search_user_count
        if 'download' in key:
            query_index = self.query_download_ip_index_name
            search_count = self.search_download_ip_count_cardinality

        end_time = datetime.date.today()
        if self.from_time is None:
            min_time = datetime.date.today()
        else:
            min_time = datetime.datetime.strptime(self.from_time, '%Y-%m-%d').date()
        max_time = min_time

        actions = ''
        while max_time <= end_time:
            print(key + ": " + str(max_time))
            single_days_ago = max_time - datetime.timedelta(days=before_days)
            double_days_ago = max_time - datetime.timedelta(days=before_days * 2)

            double_days_ago_search = search_count % (MIN_TIME, double_days_ago, query_str)
            single_days_ago_search = search_count % (MIN_TIME, single_days_ago, query_str)
            end_search = search_count % (MIN_TIME, max_time, query_str)

            double_days_ago_data = self.esClient.esSearch(index_name=query_index, search=double_days_ago_search)
            single_days_ago_data = self.esClient.esSearch(index_name=query_index, search=single_days_ago_search)
            end_data = self.esClient.esSearch(index_name=query_index, search=end_search)

            if 'download' in key:
                double_days_ago_count = double_days_ago_data['aggregations']['count']['value']
                single_days_ago_count = single_days_ago_data['aggregations']['count']['value']
                end_count = end_data['aggregations']['count']['value']
            else:
                double_days_ago_count = len(double_days_ago_data['aggregations']['1']['buckets'])
                single_days_ago_count = len(single_days_ago_data['aggregations']['1']['buckets'])
                end_count = len(end_data['aggregations']['1']['buckets'])

            prev_count = single_days_ago_count - double_days_ago_count
            last_count = end_count - single_days_ago_count
            if prev_count == 0 and last_count == 0:
                percent = 0.0
            elif prev_count == 0 and last_count != 0:
                percent = 1.0
            else:
                percent = (last_count - prev_count) / prev_count

            action = {'created_at': str(max_time),
                      key: percent,
                      'is_' + key: 1
                      }
            id = key + '_' + str(max_time)
            index_data = {"index": {"_index": self.index_name, "_id": id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

            max_time = max_time + datetime.timedelta(days=1)

        self.esClient.safe_put_bulk(actions)

    def contribute_up_down_ratio(self, key, before_days, query_str):
        query_index = self.query_index_name
        if 'commit' in key:
            query_index = self.query_commit_index_name
        method = "_count"

        end_time = datetime.date.today()
        if self.from_time is None:
            min_time = datetime.date.today()
        else:
            min_time = datetime.datetime.strptime(self.from_time, '%Y-%m-%d').date()
        max_time = min_time

        actions = ''
        while max_time <= end_time:
            print(key + ": " + str(max_time))
            single_days_ago = max_time - datetime.timedelta(days=before_days)
            double_days_ago = max_time - datetime.timedelta(days=before_days * 2)

            double_days_ago_search = self.search_contribute % (double_days_ago, single_days_ago, query_str)
            single_days_ago_search = self.search_contribute % (single_days_ago, max_time, query_str)

            double_days_ago_data = self.esClient.esSearch(index_name=query_index,
                                                          search=double_days_ago_search, method=method)
            single_days_ago_data = self.esClient.esSearch(index_name=query_index,
                                                          search=single_days_ago_search, method=method)

            prev_count = double_days_ago_data['count']
            last_count = single_days_ago_data['count']

            if prev_count == 0 and last_count == 0:
                percent = 0.0
            elif prev_count == 0 and last_count != 0:
                percent = 1.0
            else:
                percent = (last_count - prev_count) / prev_count

            action = {'created_at': str(max_time),
                      key: percent,
                      'is_' + key: 1
                      }
            id = key + '_' + str(max_time)
            index_data = {"index": {"_index": self.index_name, "_id": id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

            max_time = max_time + datetime.timedelta(days=1)

        self.esClient.safe_put_bulk(actions)

    def website_up_down_ratio(self, key, before_days, query_str):
        end_time = datetime.date.today()
        if self.from_time is None:
            min_time = datetime.date.today()
        else:
            min_time = datetime.datetime.strptime(self.from_time, '%Y-%m-%d').date()
        max_time = min_time

        actions = ''
        while max_time <= end_time:
            print(key + ": " + str(max_time))
            single_days_ago = max_time - datetime.timedelta(days=before_days)
            double_days_ago = max_time - datetime.timedelta(days=before_days * 2)

            double_days_ago_search = self.search_website % (double_days_ago, single_days_ago, query_str)
            single_days_ago_search = self.search_website % (single_days_ago, max_time, query_str)

            double_days_ago_data = self.esClient.esSearch(index_name=self.query_website_index_name,
                                                          search=double_days_ago_search)
            single_days_ago_data = self.esClient.esSearch(index_name=self.query_website_index_name,
                                                          search=single_days_ago_search)

            double_days_ago_sum = double_days_ago_data['aggregations']
            single_days_ago_sum = single_days_ago_data['aggregations']
            ip_percent = get_ratio(double_days_ago_sum['ip']['value'], single_days_ago_sum['ip']['value'])
            pv_percent = get_ratio(double_days_ago_sum['pv']['value'], single_days_ago_sum['pv']['value'])

            ip_action = {'created_at': str(max_time),
                         'ip_' + key: ip_percent,
                         'is_ip_' + key: 1
                         }

            pv_action = {'created_at': str(max_time),
                         'pv_' + key: pv_percent,
                         'is_pv_' + key: 1
                         }

            index_data = {"index": {"_index": self.index_name, "_id": 'ip_' + key + '_' + str(max_time)}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(ip_action) + '\n'

            index_data = {"index": {"_index": self.index_name, "_id": 'pv_' + key + '_' + str(max_time)}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(pv_action) + '\n'

            max_time = max_time + datetime.timedelta(days=1)

        self.esClient.safe_put_bulk(actions)

    def download_up_down_ratio(self, key, before_days):
        indexes = self.download_indexes.split(";")
        fields = self.download_sum_fields.split(";")
        coefficients = self.download_coefficients.split(";")

        end_time = datetime.date.today()
        if self.from_time is None:
            min_time = datetime.date.today()
        else:
            min_time = datetime.datetime.strptime(self.from_time, '%Y-%m-%d').date()
        max_time = min_time

        actions = ''
        while max_time <= end_time:
            print(key + ": " + str(max_time))
            single_days_ago = max_time - datetime.timedelta(days=before_days)
            double_days_ago = max_time - datetime.timedelta(days=before_days * 2)

            prev_count = 0
            last_count = 0
            for i in range(len(indexes)):
                index = indexes[i]
                field = fields[i]
                coefficient = float(coefficients[i])
                double_days_ago_search = self.search_download % (double_days_ago, single_days_ago, field)
                single_days_ago_search = self.search_download % (single_days_ago, max_time, field)

                double_days_ago_data = self.esClient.esSearch(index_name=index, search=double_days_ago_search)
                single_days_ago_data = self.esClient.esSearch(index_name=index, search=single_days_ago_search)

                prev_count += \
                    double_days_ago_data['aggregations']['1']['value'] / coefficient / self.download_pub_coefficient
                last_count += \
                    single_days_ago_data['aggregations']['1']['value'] / coefficient / self.download_pub_coefficient

            if prev_count == 0 and last_count == 0:
                percent = 0.0
            elif prev_count == 0 and last_count != 0:
                percent = 1.0
            else:
                percent = (last_count - prev_count) / prev_count

            action = {'created_at': str(max_time),
                      key: percent,
                      'is_' + key: 1
                      }
            id = key + '_' + str(max_time)
            index_data = {"index": {"_index": self.index_name, "_id": id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

            max_time = max_time + datetime.timedelta(days=1)

        self.esClient.safe_put_bulk(actions)
