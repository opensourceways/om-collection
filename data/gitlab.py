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
import sys

from collect import gitlab
from data import common


class Gitlab(object):
    def __init__(self, config=None):
        self.config = config
        self.org = config.get('org')
        self.index_name = config.get('index_name')
        self.project_group_paths = config.get('project_group_paths')
        self.gitlabClient = gitlab.GitlabClient()
        self.esClient = common.ESClient(config)

    @common.show_spend_seconds_of_this_function
    def run(self, from_date):

        project_group_path_list = self.project_group_paths.split(',')
        for project_group_path in project_group_path_list:
            self.process_all_project_star_fork_commit(group_path=project_group_path)

        print(f'Function name: {sys._getframe().f_code.co_name} has run over.')

    @common.show_spend_seconds_of_this_function
    def process_all_project_star_fork_commit(self, group_path):
        last_slash_pos = group_path.rfind('/')
        boot_url = group_path[:last_slash_pos]

        project_basicInfo_list = self.gitlabClient.get_all_project_basicInfo(boot_url)

        actions = ''
        for project_basicInfo in project_basicInfo_list:
            project_id = project_basicInfo.get('id')
            commit_totalCount = self.gitlabClient.get_whole_project_commit_count(boot_url, project_id)

            #  Assemble content_body
            content_body = {}
            content_body['org'] = self.org
            content_body['owner'] = self.org
            content_body['repo_name'] = project_basicInfo.get('name')
            content_body['repo_full_name'] = project_basicInfo.get('path_with_namespace')
            content_body["created_at"] = project_basicInfo.get('created_at')
            content_body['stargazers_count'] = project_basicInfo.get('star_count')
            content_body['forks_count'] = project_basicInfo.get('forks_count')
            content_body['watchers_count'] = -1
            content_body['commits_count'] = commit_totalCount
            content_body['platform'] = 'https://about.gitlab.com'

            action = common.getSingleAction(index_name=self.index_name, id=project_basicInfo['web_url'],
                                            body=content_body)
            actions += action

            print(f"Succeed process project_id: {project_id}\n\n")

        self.esClient.safe_put_bulk(actions)

        print(f'Function name: {sys._getframe().f_code.co_name} run over')

        return None
