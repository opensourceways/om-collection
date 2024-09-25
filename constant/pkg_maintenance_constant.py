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
# Create: 2024/9/5

CVE_STATUS = {
    "all_fixed": "有CVE且全部修复",
    "some_fixed": "有CVE部分修复",
    "no_fixed": "有CVE且全部未修复",
    "no_cve": "没有CVE问题"
}

VERSION_STATUS = {
    "outdated": "落后版本",
    "normal": "正常版本"
}

ISSUE_STATUS = {
    "no_fixed": "没有Issue修复",
    "all_fixed": "全部Issue修复",
    "some_fixed": "有部分Issue修复",
}

UPDATE_STATUS = {
    "pr_merged": "有PR合入",
    "no_merged": "有PR提交未合入",
    "no_pr": "没有PR提交"
}

CONTRIBUTE_STATUS = {
    "many_participants": "贡献人员多",
    "few_participants": "贡献人员少",
    "many_orgs": "贡献组织多",
    "few_orgs": "贡献组织少"
}

REPO_STATUS = {
    "no_maintenance": "没有人维护",
    "lack_of_maintenance": "缺人维护",
    "health": "健康",
    "active": "活跃",
    "inactive": "静止"
}