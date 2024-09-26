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
# Create: 2024/8/19
from datetime import datetime, timedelta
import pytz
import json
from data import common


class GiteeSLA(object):

    def __init__(self, config=None):
        self.config = config
        self.esClient = common.ESClient(config)
        self.index_name = config.get("index_name")
        self.index_name_all = config.get("index_name_all")
        self.access_token = config.get("access_token")
        self.skip_user = config.get("skip_user")
        self.bug_tags = config.get("bug_tags").split("/")
        self.request_url = config.get("request_url")
        self.login_to_name_dict = {}

    def run(self, from_time):
        self.get_issue_item()

    def get_issue_item(self):
        search = """
        {
            "_source": [
                "id",
                "issue_number",
                "created_at",
                "closed_at",
                "user_login",
                "user_name",
                "closed_at",
                "issue_labels",
                "repository",
                "issue_title",
                "body",
                "repository",
                "org_name",
                "issue_url",
                "issue_customize_state"
            ],
            "size": 100,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "is_gitee_issue": 1
                            }
                        }
                    ],
                    "must_not": [
                        {
                            "match": {
                                "user_login": "modelfoundry-ci-bot"
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "created_at": "desc"
                }
            ]
        }
        """
        self.esClient.scrollSearch(
            index_name=self.index_name_all, search=search, scroll_duration='5m', func=self.func
        )

    def func(self, data):
        actions = ""
        cnt = 0
        for issue in data:
            issue_sla_dic = {
                # issue信息
                "issue_id": None,
                "issue_number": None,
                "repository": None,
                "user_name": None,
                "user_login": None,
                "org_name": None,
                "issue_title": None,
                "body": None,
                "issue_url": None,
                "issue_customize_state": None,
                "last_responsible_name": None,
                "last_responsible_login": None,
                "tags": None,
                "bug_tag": None,
                # 时间点
                "issue_created_at": None,
                "first_bug_tag_created_at": None,
                "last_bug_tag_created_at": None,
                "issue_closed_at": None,
                "last_responsible_created_at": None,
                "firstreplyissuetime_at": None,
                # 持续时间
                "issue_duration": None,
                "issue_resolve_bug_tag_duration": None,
                "responsible_duration": None,
                "firstreplyissuetime": None,
                "firstreplyissuetime_except_weekend": None,
                # 是否规定时间内
                "is_timely_firstreplyissue": 0,
                "is_timely_bug_resolve": 0,
            }
            # 获取issue操作日志
            owner = issue["_source"]["org_name"]
            issue_number = issue["_source"]["issue_number"]
            repo = issue["_source"]["repository"].split("/")[-1]

            issue_sla_dic["issue_number"] = issue["_source"]["issue_number"]
            issue_sla_dic["repository"] = issue["_source"]["repository"]
            issue_sla_dic["user_name"] = issue["_source"]["user_name"]
            issue_sla_dic["org_name"] = issue["_source"]["org_name"]
            issue_sla_dic["issue_title"] = issue["_source"]["issue_title"]
            issue_sla_dic["body"] = issue["_source"]["body"]
            issue_sla_dic["issue_url"] = issue["_source"]["issue_url"]
            issue_sla_dic["issue_customize_state"] = issue["_source"][
                "issue_customize_state"
            ]

            url = f"{self.request_url}/repos/{owner}/issues/{issue_number}/operate_logs"
            params = {
                "access_token": self.access_token,
                "repo": repo,
                "sort": "asc",
            }
            issue_res = self.esClient.request_get(url=url, params=params)

            # 无法获取操作日志就跳过
            if issue_res.status_code != 200:
                print(f'Get {issue_sla_dic["issue_number"]} operate_logs error:', issue_res.text)
                continue
            else:
                # 对issue的每个操作进行遍历
                for operation in issue_res.json():
                    # 获取最新责任人 & 时间点
                    self.parse_operation(operation, issue_sla_dic)

            # 责任人、通过user_name获取user_login
            issue_sla_dic["last_responsible_login"] = self.get_login_from_name(
                issue_sla_dic["last_responsible_name"]
            )
            # 获取首次评论时间
            url = f"{self.request_url}/repos/{owner}/{repo}/issues/{issue_number}/comments"
            params = {
                "access_token": self.access_token,
                "repo": repo,
                "order": "asc",
            }
            comment_res = self.esClient.request_get(url=url, params=params)
            if comment_res.status_code != 200:
                print('Get comments error: ', comment_res.text)
            else:
                for comment in comment_res.json():
                    if comment["user"]["login"] != self.skip_user:
                        issue_sla_dic["firstreplyissuetime_at"] = comment["created_at"]
                        break

            # 获取id
            issue_sla_dic["issue_id"] = issue["_source"]["id"]
            # 获取user_login
            issue_sla_dic["user_login"] = issue["_source"]["user_login"]
            # 获取tags列表
            issue_sla_dic["tags"] = issue["_source"]["issue_labels"]
            # 获取bug_tag
            issue_sla_dic["bug_tag"] = self.get_bug_tag(issue_sla_dic["tags"])

            # 获取created_at
            issue_sla_dic["issue_created_at"] = issue["_source"]["created_at"]
            # 获取closed_at
            issue_sla_dic["issue_closed_at"] = issue["_source"]["closed_at"]

            # 获取当前时间
            format = "%Y-%m-%dT%H:%M:%S%z"  # 指定时间格式
            beijing = pytz.timezone("Asia/Shanghai")  # 时区设置
            now = datetime.now().astimezone(beijing)  # 当前时间

            # issue持续时间
            if issue_sla_dic["issue_closed_at"] is None:
                issue_sla_dic["issue_duration"] = self.time_subtract(
                    now, issue_sla_dic["issue_created_at"], format
                )
            else:
                issue_sla_dic["issue_duration"] = self.time_subtract(
                    issue_sla_dic["issue_closed_at"],
                    issue_sla_dic["issue_created_at"],
                    format,
                )

            # issue bug_tag持续时间
            if issue_sla_dic["issue_closed_at"] is None:
                issue_sla_dic["issue_resolve_bug_tag_duration"] = self.time_subtract(
                    now, issue_sla_dic["first_bug_tag_created_at"], format
                )
            else:
                issue_sla_dic["issue_resolve_bug_tag_duration"] = self.time_subtract(
                    issue_sla_dic["issue_closed_at"],
                    issue_sla_dic["first_bug_tag_created_at"],
                    format,
                )

            # 责任人持续时间
            if issue_sla_dic["issue_closed_at"] is None:
                issue_sla_dic["responsible_duration"] = self.time_subtract(
                    now, issue_sla_dic["last_responsible_created_at"], format
                )
            else:
                issue_sla_dic["responsible_duration"] = self.time_subtract(
                    issue_sla_dic["issue_closed_at"],
                    issue_sla_dic["last_responsible_created_at"],
                    format,
                )

            # 首次回复持续时间
            issue_sla_dic["firstreplyissuetime"] = self.time_subtract(
                issue_sla_dic["firstreplyissuetime_at"],
                issue_sla_dic["issue_created_at"],
                format,
            )

            # 首次回复非周末
            issue_sla_dic["firstreplyissuetime_except_weekend"] = (
                self.get_firstreplyissuetime_except_weekend(
                    issue_sla_dic["firstreplyissuetime_at"],
                    issue_sla_dic["issue_created_at"],
                )
            )

            # 判断是否及时回复(24小时内)，除去周末
            if (
                issue_sla_dic.get("firstreplyissuetime_except_weekend")
                and issue_sla_dic["firstreplyissuetime_except_weekend"] <= 24 * 60 * 60
            ):
                issue_sla_dic["is_timely_firstreplyissue"] = 1

            # 根据不同bug严重程度判断解决时间是否及时
            if issue_sla_dic["bug_tag"] is not None:
                issue_sla_dic["is_timely_bug_resolve"] = self.parse_bug_resolve_timely(
                    issue_sla_dic["issue_resolve_bug_tag_duration"],
                    issue_sla_dic["bug_tag"],
                )

            doc_id = issue_sla_dic["repository"] + issue_sla_dic["issue_number"]
            indexData = {"index": {"_index": self.index_name, "_id": doc_id}}
            actions += json.dumps(indexData) + "\n"
            actions += json.dumps(issue_sla_dic) + "\n"
            cnt += 1
            if cnt == 1000:
                self.esClient.safe_put_bulk(actions)
                actions = ""
                cnt = 0
        self.esClient.safe_put_bulk(actions)

    # 将字符串转换为时间戳,做减法,返回秒数
    def time_subtract(self, time1, time2, format="%Y-%m-%dT%H:%M:%S%z"):
        if time1 == None or time2 == None:
            return None
        if isinstance(time1, str):
            time1 = datetime.strptime(time1, format)
        if isinstance(time2, str):
            time2 = datetime.strptime(time2, format)
        return (time1 - time2).total_seconds()

    # 获取首次回复非周末时间
    def get_firstreplyissuetime_except_weekend(self, first_at, created_at):
        if first_at == None or created_at == None:
            return None
        if isinstance(first_at, str):
            first_at = datetime.strptime(first_at, "%Y-%m-%dT%H:%M:%S%z")
        if isinstance(created_at, str):
            created_at = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S%z")
        first_flag = False
        created_flag = False
        while first_at.weekday() > 4:
            first_at = first_at + timedelta(days=1)
            first_flag = True
        while created_at.weekday() > 4:
            created_at = created_at + timedelta(days=1)
            created_flag = True
        if first_flag:
            first_at = first_at.replace(hour=0, minute=0, second=0, microsecond=0)
        if created_flag:
            created_at = created_at.replace(hour=0, minute=0, second=0, microsecond=0)
        weeks = (first_at - created_at).days / 7
        weeks = int(weeks)
        if first_at.weekday() < created_at.weekday():
            weeks += 1
        return (first_at - created_at).total_seconds() - weeks * 2 * 24 * 60 * 60

    # 获取bug_tag
    def get_bug_tag(self, tags):
        for tag in tags:
            if tag in self.bug_tags:
                return tag
        return None

    def parse_operation(self, operation, issue_sla_dic):
        # 获取最新责任人 & 时间点
        if operation["action_type"] == "setting_assignee":
            issue_sla_dic["last_responsible_name"] = operation["content"][7:]
            issue_sla_dic["last_responsible_created_at"] = operation["created_at"]
        elif operation["action_type"] == "change_assignee":
            issue_sla_dic["last_responsible_name"] = operation["content"].split(" ")[-1][3:]
            issue_sla_dic["last_responsible_created_at"] = operation["created_at"]
        # 获取first_bug_tag_created_at & last_bug_tag_created_at
        if operation["action_type"] == "add_label":
            if issue_sla_dic["first_bug_tag_created_at"] is None:
                issue_sla_dic["first_bug_tag_created_at"] = operation["created_at"]
                issue_sla_dic["last_bug_tag_created_at"] = operation["created_at"]
            issue_sla_dic["last_bug_tag_created_at"] = operation["created_at"]

    def get_login_from_name(self, name):
        if name is None:
            return None
        if self.login_to_name_dict.get(name):
            return self.login_to_name_dict.get(name)
        search = """
            {
                "size": 1,
                "_source": [
                    "user_login"
                ],
                "query": {
                    "term": {
                        "author_name.keyword": {
                            "value": "%s"
                        }
                    }
                }
            }""" % (
            name
        )
        try:
            res = self.esClient.esSearch(index_name=self.index_name_all, search=search)
            login = res["hits"]["hits"][0]["_source"]["user_login"]
            self.login_to_name_dict[name] = login
            return login
        except:
            print(f"{name} name search error")
            return None

    def parse_bug_resolve_timely(self, duration, bug_tag):
        if duration is None:
            return 0
        if bug_tag == "bug-suggestion":
            if duration <= 24 * 60 * 60 * 15:
                return 1
        elif bug_tag == "bug-minor":
            if duration <= 24 * 60 * 60 * 7:
                return 1
        elif bug_tag == "bug-major":
            if duration <= 24 * 60 * 60 * 3:
                return 1
        elif bug_tag == "bug-critical":
            if duration <= 24 * 60 * 60 * 2:
                return 1
        return 0
