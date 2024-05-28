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
# Create: 2024/5/28
import json

import requests
import yaml

from data.common import ESClient


class GitHubAccount(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')

        self.user_yaml = config.get('user_yaml')
        self.company_yaml = config.get('company_yaml')
        self.user = config.get('user')
        self.password = config.get('password')

    def run(self, from_date):
        self.get_upstream_company(self.user_yaml, self.company_yaml)

    # upstream 需要从yaml中获取组织
    def get_upstream_company(self, user_yaml, company_yaml):
        domain_org_dict, aliases_company_dict = self.get_domain_org(company_yaml)
        user_datas = self.get_yaml_data(user_yaml)
        actions = ''
        for user in user_datas.get('users'):
            actions += self.parse_user(user, aliases_company_dict, domain_org_dict)
        self.esClient.safe_put_bulk(actions)

    def parse_user(self, user, aliases_company_dict, domain_org_dict):
        actions = ''
        github_id = user['github_id']
        gitee_id = user['gitee_id']
        username = user.get('user_name')
        for email in user['emails']:
            companies = user['companies']
            created_at = '1999-01-01'
            for company in companies:
                user_company = company['company_name']
                if not user_company:
                    continue
                if user_company in aliases_company_dict:
                    user_company = aliases_company_dict[user_company]
                actions += self.get_action(github_id, gitee_id, email, user_company, username, created_at)
                created_at = company.get('end_date')

            if not companies and email and email.split('@')[-1] in domain_org_dict:
                user_company = domain_org_dict[email.split('@')[-1]]
                actions += self.get_action(github_id, gitee_id, email, user_company, username, created_at)
        return actions

    def get_action(self, github_id, gitee_id, email, user_company, username, created_at):
        action = {
            'github_id': github_id,
            'gitee_id': gitee_id,
            'email': email,
            'company': user_company,
            'username': username,
            'created_at': created_at
        }
        index_data = {"index": {"_index": self.index_name, "_id": (username + email).replace(" ", "")}}
        actions = json.dumps(index_data) + '\n'
        actions += json.dumps(action) + '\n'
        return actions

    def get_domain_org(self, company_yaml):
        domain_org_dict = {}
        aliases_company_dict = {}
        contents = self.get_yaml_data(company_yaml)

        for company in contents['companies']:
            company_name = company['company_name']
            for alias in company['aliases']:
                aliases_company_dict.update({alias: company_name})
            for domain in company['domains']:
                domain_org_dict.update({domain: company_name})
        return domain_org_dict, aliases_company_dict

    def get_yaml_data(self, yaml_file):
        auth = (self.user, self.password)
        yaml_response = requests.get(yaml_file, auth=auth, verify=False, timeout=60)
        if yaml_response.status_code != 200:
            print('Cannot fetch online yaml file.')
            return
        try:
            contents = yaml.safe_load(yaml_response.text)
        except yaml.YAMLError as e:
            print(f'Error parsing YAML: {e}')
            return
        return contents

    def update_user_org(self, company, user):
        query = """{
          "script": {
            "source": "ctx._source['tag_user_company']=params.tag_user_company",
            "params": {
                "tag_user_company": "%s"
            },
            "lang": "painless"
          },
          "query": {
            "term": {
              "user_login.keyword": "%s"
            }
          }
        }""" % (company, user)
        self.esClient.updateByQuery(query=query.encode('utf-8'))
