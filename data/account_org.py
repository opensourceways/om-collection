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
import csv
import json
import os
import requests
import yaml

from data.common import ESClient


class AccountOrg(object):
    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.index_name_cla = config.get('index_name_cla')
        self.email_gitee_es = config.get('email_gitee_es')
        self.email_gitee_authorization = config.get('email_gitee_authorization')
        self.email_gitee_index = config.get('email_gitee_index')
        self.data_yaml_url = config.get('data_yaml_url', 'data.yaml')
        self.company_yaml_url = config.get('company_yaml_url', 'company.yaml')
        self.csv_url = config.get('csv_url')
        self.csv_data = {}

    def run(self, from_time):
        print("Collect AccountOrg data: start")
        self.csv_data = self.getEmailGiteeDict()
        self.getDataFromCla()
        # 用于从csv文件中刷新用户信息
        # self.getDataFromCsv()
        print("Collect AccountOrg data: finished")

    def getDataFromCla(self):
        if self.index_name_cla:
            search_json = '''{
                                  "size": 10000,
                                  "_source": {
                                    "includes": [
                                      "email",
                                      "corporation",
                                      "created_at"
                                    ]
                                  },
                                  "query": {
                                    "bool": {
                                      "must": [
                                        {
                                          "term": {
                                            "is_corporation_signing": "1"
                                          }
                                        }
                                      ]
                                    }
                                  }
                                }'''
            res = self.esClient.request_get(self.esClient.getSearchUrl(index_name=self.index_name_cla),
                                            data=search_json, headers=self.esClient.default_headers)
            if res.status_code != 200:
                print("The index not exist")
                return {}
            data = res.json()
            actions = ""
            for hits in data['hits']['hits']:
                source_data = hits['_source']
                email = source_data['email']
                domain = str(email).split("@")[1]
                if email in self.csv_data.keys():
                    gitee_ids = self.csv_data[email]
                    for gitee_id in gitee_ids:
                        actions = self.getActions(email, source_data['corporation'], gitee_id, domain,
                                                  source_data['created_at'], actions)
                else:
                    actions = self.getActions(email, source_data['corporation'], None, domain,
                                              source_data['created_at'], actions)

            self.esClient.safe_put_bulk(actions)

    def getActions(self, email, corporation, gitee_id, domain, created_at, actions):
        action = {
            "email": email,
            "organization": corporation,
            "gitee_id": gitee_id,
            "domain": domain,
            "created_at": created_at,
            "is_cla": 1
        }
        if gitee_id:
            id = email + '_' + gitee_id
        else:
            id = email
        index_data = {"index": {"_index": self.index_name, "_id": id}}
        actions += json.dumps(index_data) + '\n'
        actions += json.dumps(action) + '\n'
        return actions

    def getEmailGiteeDict(self):
        search = '"must": [{"match_all": {}}]'
        hits = self.esClient.searchEmailGitee(url=self.email_gitee_es, headers=None,
                                              index_name=self.email_gitee_index, search=search)
        data = {}
        if hits is not None and len(hits) > 0:
            for hit in hits:
                source = hit['_source']
                email = source['email']
                gitee_id = source['gitee_id']
                new_ids = []
                if email in data:
                    old_ids = data.get(email)
                    old_ids.append(gitee_id)
                    new_ids = list(set(old_ids))
                else:
                    new_ids.append(gitee_id)

                data.update({email: new_ids})
        return data

    def getDataFromYaml(self):
        dic = self.esClient.getOrgByGiteeID()
        dic1 = dic[0]
        dic2 = dic[1]
        dic3 = {}
        if self.data_yaml_url:
            datas = yaml.load_all(open('company.yaml', encoding='UTF-8')).__next__()
            for data in datas['companies']:
                key = data['company_name']
                value = data['aliases'][0]
                dic3.update({key: value})

            datas = yaml.load_all(open('data.yaml', encoding='UTF-8')).__next__()
            actions = ""
            for data in datas['users']:
                gitee_id = data['gitee_id']
                organization = data['companies'][0]['company_name']
                if organization == '' or gitee_id in dic1 or gitee_id in dic2:
                    continue
                emails = data['emails']
                if len(emails) != 0:
                    email = emails[0]
                    id = email + '_' + gitee_id
                else:
                    id = gitee_id
                    email = gitee_id

                action = {
                    "email": email,
                    "organization": dic3.get(organization),
                    "gitee_id": gitee_id,
                    "domain": None,
                    "created_at": '1999-01-01',
                    "is_cla": 1
                }
                index_data = {"index": {"_index": self.index_name, "_id": id}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'

            self.esClient.safe_put_bulk(actions)

    def getDataFromCsv(self):
        actions = ""
        csvFile = open(self.csv_url, "r")
        reader = csv.reader(csvFile)
        for item in reader:
            if reader.line_num == 1:
                continue
            organization = item[2]
            if organization == '':
                continue
            email = item[1]
            if email == '':
                id = item[0]
            else:
                id = email + '_' + item[0]
            action = {
                "email": email,
                "organization": organization,
                "gitee_id": item[0],
                "domain": None,
                "created_at": '1999-01-01',
                "is_cla": 1
            }
            index_data = {"index": {"_index": self.index_name, "_id": id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)
        csvFile.close()
