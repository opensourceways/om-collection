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
import time

import yaml
from data import common
from data.common import ESClient
from data.tag_labels.text_process import TextProcess


class IssueRuleLabel(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.orgs = config.get('orgs')
        self.index_name = config.get('index_name')
        self.rule_yaml = config.get('rule_yaml', 'issue_rule_label.yaml')
        self.stopwords_file = config.get('stopwords_file', 'cn_stopwords.txt')
        self.block_num = int(config.get('block_num', '1000'))
        self.text_process = TextProcess(text=None)
        self.self_stopwords = []
        self.label_rules = {}
        self.count_list = []
        self.hit_ids = []

    def run(self, from_time):
        # 自定义停用词词典
        self.get_stopwords()

        # 自定义规则标签字典
        self.get_label_rule()

        # 获取issue数据，使用自定义规则标签
        search = '''{
                      "size": %d,
                      "_source": {
                        "includes": [
                          "issue_title",
                          "body"
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
                              "exists": {
                                "field": "body"
                              }
                            }
                          ],
                          "must_not": [
                            {
                              "exists": {
                                "field": "rule_labels"
                              }
                            }
                          ]
                        }
                      }
                    }''' % self.block_num
        self.esClient.scrollSearch(index_name=self.index_name, search=search, scroll_duration='10m',
                                   func=self.rule_label_func)

    # 自定义停用词词典
    def get_stopwords(self):
        f = open("data/tag_labels/cn_stopwords.txt", "r", encoding='utf8')
        for word in f.readlines():
            self.self_stopwords.append(word.strip('\n'))

    # 自定义规则标签字典
    def get_label_rule(self):
        datas = yaml.safe_load(open(self.rule_yaml, encoding='utf8'))
        for data in datas['label_rules']:
            self.label_rules.update({data['label']: data['aliases']})

    # 基于匹配的标签
    def rule_label_func(self, hits):
        actions = ''
        t1 = time.time()
        for hit in hits:
            hit_id = hit['_id']
            source = hit['_source']
            title = source['issue_title']
            body = source['body'] if 'body' in source else ''
            text = title + ',' + body

            # 数据去噪
            word_list = self.text_process.body_clean(text=text)

            # 分词
            tokens = self.text_process.hanlp_text_spilt_noun(text_list=word_list, self_stopwords=self.self_stopwords)
            # print('*** issue_id: %s, end tok' % hit_id)

            # 匹配
            labels = []
            for token in tokens:
                for label, rule in self.label_rules.items():
                    if token in rule and label not in labels:
                        labels.append(label)

            if len(labels) == 0:
                continue
            update_data = {
                "doc": {
                    "rule_labels": labels,
                }
            }
            action = common.getSingleAction(self.index_name, hit_id, update_data, act="update")
            actions += action

        self.esClient.safe_put_bulk(actions)
        t2 = time.time()
        t3 = t2 - t1
        print('*** block_size: %d, time: %d' % (self.block_num, t3))

    # 基于TF-IDF的标签
    def tf_idf_func(self, hits):
        for hit in hits:
            hit_id = hit['_id']
            source = hit['_source']
            title = source['issue_title']
            body = source['body'] if 'body' in source else ''
            text = title + ',' + body

            # 数据去噪
            word_list = self.text_process.body_clean(text=text)

            # 分词
            tokens = self.text_process.hanlp_text_spilt_noun(text_list=word_list, self_stopwords=self.self_stopwords)

            # 词频语料库
            self.count_list.append(self.text_process.count_term(tokens))

            # block_num 条数据作为一个语料库的数据，进行一次关键词提取
            self.hit_ids.append(hit_id)
            if len(self.hit_ids) == self.block_num:
                # TF-IDF计算
                sorted_words = self.text_process.tf_idf(self.count_list)

                actions = ''
                for i in range(0, len(self.hit_ids)):
                    words_scores = sorted_words[i]
                    labels = []
                    for item in words_scores[:5]:
                        labels.append(item[0])
                    id = self.hit_ids[i]
                    update_data = {
                        "doc": {
                            "tfidf_labels": labels,
                        }
                    }
                    action = common.getSingleAction(self.index_name, id, update_data, act="update")
                    actions += action

                # 一个语料库完成，置空语料库
                self.hit_ids = []
                self.count_list = []

                self.esClient.safe_put_bulk(actions)
