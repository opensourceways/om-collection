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
import json
import datetime
import re

import requests

from data.common import ESClient


class CVE(object):
    def __init__(self, config=None):
        self.cve_url = config.get("cve_url")
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.all_data_index = config.get('all_data_index')
        self.branchMappingstr = config.get('branchmapping')
        self.giteeToken = config.get('gitee_token');
        if self.branchMappingstr is not None:
            self.branchMapping = {}
            fromTo = str(self.branchMappingstr).split(',')
            for branch in fromTo:
                KV = branch.split(':');
                self.branchMapping[KV[0]] = KV[1]

    def run(self, from_time):
        self.getData()

    def getDataOld(self):
        datas = self.getCveData()
        res = []
        actions = ''
        for data in datas:
            '''{
                'cve_number':'',
                'issue_number':'',
                'nvd_score':0,
                'cve_level':'',
                'created_at':'',
                'plan_close_time':'',
                'process_time':0,
                'issue_state':'',
                'repo_name':'',
                'pr_merged_at':'',
                'branch':'',
                'loopholes_perceive_times':0,
                'first_reply_times':0,
                'loopholes_fix_times':0,
                'loopholes_times':0,
                'loopholes_patch_times':0,
                'loopholes_sa_release_times':0,
                'loopholes_response_times':0
            }'''
            issue = self.getIssueByNumber(data['issue_id'])
            if issue is None:
                continue
            resData = {}
            resData['cve_number'] = data['CVE_num']
            resData['issue_number'] = data['issue_id']
            resData['nvd_score'] = data['NVD_score']
            resData['cve_level'] = data['CVE_level']
            resData['created_at'] = issue['created_at']
            # 计划完成时间
            resData['plan_closed_time'] = data['plan_closed_time']
            # issue 处理时长
            try:
                issue_closed_at = issue['issue_closed_at']
            except Exception:
                issue_closed_at = None
            if issue_closed_at is None:
                resData['process_time'] = 0
            else:
                process_time = (datetime.datetime.strptime(issue_closed_at,
                                                           '%Y-%m-%dT%H:%M:%S+08:00') - datetime.datetime.strptime(
                    resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00')).total_seconds()
                resData['process_time'] = process_time
            resData['issue_state'] = issue['issue_state']
            resData['repo_name'] = issue['gitee_repo']

            # 漏洞感知时长  cve 创建时间-cve首次公开时间
            if len(str(data['CVE_public_time'])) <= 0:
                resData['loopholes_perceive_times'] = 0
            else:
                resData['loopholes_perceive_times'] = (
                        datetime.datetime.strptime(resData['created_at'],
                                                   '%Y-%m-%dT%H:%M:%S+08:00') + datetime.timedelta(
                    hours=-8) - datetime.datetime.strptime(
                    data['CVE_public_time'], '%Y-%m-%d')).total_seconds()
                if resData['loopholes_perceive_times'] < 0:
                    resData['loopholes_perceive_times'] = 0
            # issue 首次响应时长
            firstreplyissuetime = self.getFirstReplyTime(resData['issue_number'])
            if firstreplyissuetime is None or len(str(firstreplyissuetime)) <= 0:
                resData['first_reply_times'] = 0
            else:
                resData['first_reply_times'] = (
                        datetime.datetime.strptime(firstreplyissuetime,
                                                   '%Y-%m-%dT%H:%M:%S.000Z') - datetime.datetime.strptime(
                    resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00') + datetime.timedelta(hours=8)).total_seconds()

            # 漏洞补丁修复时长
            if len(str(data['rpm_public_time'])) <= 0:
                resData['loopholes_patch_times'] = 0
            else:
                resData['loopholes_patch_times'] = (
                        datetime.datetime.strptime(data['rpm_public_time'],
                                                   '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(
                    resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00') + datetime.timedelta(hours=8)).total_seconds()
            if resData['loopholes_patch_times'] < 0:
                resData['loopholes_patch_times'] = 0
            # SA 发布时长
            if len(str(data['SA_public_time'])) <= 0:
                resData['loopholes_sa_release_times'] = 0
            else:
                resData['loopholes_sa_release_times'] = (
                        datetime.datetime.strptime(data['SA_public_time'],
                                                   '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(
                    resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00') + datetime.timedelta(hours=8)).total_seconds()
            if resData['loopholes_sa_release_times'] < 0:
                resData['loopholes_sa_release_times'] = 0
            # 漏洞响应时长
            if len(str(data['SA_public_time'])) <= 0 or len(str(data['CVE_public_time'])) <= 0:
                resData['loopholes_response_times'] = 0
            else:
                resData['loopholes_response_times'] = (
                        datetime.datetime.strptime(data['SA_public_time'],
                                                   '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(
                    data['CVE_public_time'], '%Y-%m-%d')).total_seconds()
            # 分支
            milestone = data['milestone']
            if milestone is not None and milestone != '':
                branches = str(milestone).split(',')
                ownerRepo = str(issue['repository']).split('/')
                prs = self.getPrByIssueNumber(ownerRepo[0], ownerRepo[1], issue['issue_number'])
                branchUpdate = {}
                for p in prs:
                    branchUpdate[p['base']['label']] = p
                for br in branches:
                    brAffects = str(br).split(':')
                    brAffects[0] = self.transformBranch(brAffects[0])
                    subResData = resData.copy()
                    if branchUpdate is not None and len(prs) > 0:
                        # pr 合入时间
                        try:
                            pr = branchUpdate[brAffects[0]];
                            subResData['pr_merged_at'] = pr['merged_at']
                            # 漏洞修复时长
                            subResData['loopholes_fix_times'] = (
                                    datetime.datetime.strptime(subResData['pr_merged_at'],
                                                               '%Y-%m-%dT%H:%M:%S+08:00') - datetime.datetime.strptime(
                                subResData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00')).total_seconds()
                            # 漏洞时长
                            if len(data['rpm_public_time']) >= 1 and len(subResData['pr_merged_at']) >= 1:
                                subResData['loopholes_times'] = (
                                        datetime.datetime.strptime(data['rpm_public_time'],
                                                                   '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(
                                    subResData['pr_merged_at'], '%Y-%m-%dT%H:%M:%S+08:00') + datetime.timedelta(
                                    hours=8)).total_seconds()
                            if subResData['loopholes_times'] < 0:
                                subResData['loopholes_times'] = 0
                        except Exception:
                            subResData['pr_merged_at'] = 0

                    if len(brAffects) > 1 and brAffects[1] == '受影响':
                        subResData['is_affected'] = 1
                        subResData['branch'] = brAffects[0]
                        res.append(subResData)



                    else:
                        subResData['branch'] = brAffects[0]
                        res.append(subResData)
                    indexData = {
                        "index": {"_index": self.index_name, "_id": subResData['issue_number'] + '_' + brAffects[0]}}
                    actions += json.dumps(indexData) + '\n'
                    actions += json.dumps(subResData) + '\n'

            self.esClient.safe_put_bulk(actions)
            actions = ''

    def getIssueByNumber(self, number=None):
        search = '"must": [{"term":{"is_gitee_issue":1}},{ "term": { "issue_number.keyword":"%s"}}]' % (number)
        data = self.esClient.searchEsList(self.all_data_index, search)
        if data is None or len(data) <= 0:
            return None
        return data[0]['_source']

    def getCveData(self, currentPage=None, pageSize=None):
        params = {}
        if currentPage is not None and pageSize is not None:
            params['currentPage'] = currentPage
            params['pageSize'] = pageSize
        else:
            params['pageSize'] = 10000
        response = self.esClient.request_get(self.cve_url, params=params)
        cveData = self.esClient.getGenerator(response.text)
        return cveData['body']

    def getPrByTitle(self, title, branch):
        queryStr = '''{
      "query": {
        "bool": {
          "filter": [
            {
              "query_string": {
                "analyze_wildcard": true,
                "query": "is_gitee_pull_request:1 AND issue_title:\\"%s\\" AND head_label.keyword:\\"%s\\" AND base_label_ref.keyword:\\"%s\\""
              }
            }
          ]
        }
      }
    }''' % (title, branch, branch)
        url = self.esClient.url + "/" + self.all_data_index + "/_search"
        header = {
            "Content-Type": 'application/json',
            'Authorization': self.esClient.authorization
        }
        res = requests.post(url=url, data=queryStr.encode(encoding='utf-8 '), verify=False, headers=header, )

        responseData = json.loads(res.text)
        return responseData['hits']['hits']

    def getPrByIssueNumber(self, owner, repo, issueNumber):
        url = 'https://gitee.com/api/v5/repos/%s/issues/%s/pull_requests?access_token=%s&repo=%s' % (
            owner, issueNumber, self.giteeToken, repo)
        res = self.esClient.request_get(url=url)
        prs = json.loads(res.text)
        return prs

    def getFirstReplyTime(self, issueNumber):
        querystr = '''{
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "query_string": {
                                    "analyze_wildcard": true,
                                    "query": "issue_number.keyword:\\"%s\\" AND is_gitee_issue_comment:1 AND !author_name.keyword:\\"openeuler-ci-bot\\""
                                }
                            }
                        ]
                    }
                },
                "aggs": {
                    "1": {
                        "min": {
                            "field": "created_at"
                        }
                    }
                }
            }''' % (issueNumber)
        url = self.esClient.url + "/" + self.all_data_index + "/_search"
        header = {
            "Content-Type": 'application/json',
            'Authorization': self.esClient.authorization
        }
        res = requests.post(url=url, data=querystr, verify=False, headers=header)

        responseData = json.loads(res.text)
        try:
            result = responseData['aggregations']['1']['value_as_string']
            return result
        except Exception:
            return None

    def transformBranch(self, branch):
        for br in self.branchMapping.keys():
            if br == branch:
                return self.branchMapping[br]

        return branch

    def getData(self):
        cve_data = self.getCveData()
        actions = ''
        for cve in cve_data:
            if cve['CVE_num'] is None or cve['CVE_num'] == '':
                continue
            issue = self.getIssueByNumber(cve['issue_id'])
            if issue is None:
                continue
            res = cve
            res.update(self.getInvolvedBranch(branch=cve['milestone']))
            res['CVE_num'] = str(cve['CVE_num']).replace('漏洞处理', '')
            # 推送时间（issue的创建时间）
            res['created_at'] = issue['created_at']
            # 漏洞感知时长(小时) TODO 需要确认CVE_public_time是否能精确到时分秒
            rec_time = cve['CVE_vtopic_rec_time']
            pub_time = res['CVE_public_time']
            if pub_time is not None and pub_time != '':
                fm = '%Y-%m-%d'
                if len(pub_time) == 16:
                    fm = '%Y-%m-%d %H:%M'
                elif len(pub_time) == 19:
                    fm = '%Y-%m-%d %H:%M:%S'
                res['cve_rec_duration'] = self.getDuration(res['created_at'], '%Y-%m-%dT%H:%M:%S+08:00',
                                                           pub_time, fm)
            elif rec_time is not None and rec_time != '':
                res['cve_rec_duration'] = self.getDuration(res['created_at'], '%Y-%m-%dT%H:%M:%S+08:00',
                                                           self.format_time(str(rec_time).split('.')[0]),
                                                           '%Y-%m-%d %H:%M:%S')
            else:
                res['cve_rec_duration'] = 0

            res['user_login'] = issue['user_login']
            res['issue_state'] = issue['issue_state']
            if 'issue_customize_state' not in issue:
                issue_customize_state = ''
            else:
                issue_customize_state = issue['issue_customize_state']
            res['issue_customize_state'] = issue_customize_state
            res['issue_labels'] = issue['issue_labels']
            # 受影响软件（仓库）
            res['repository'] = str(issue['repository']).split("/")[1]
            # TODO (openeuler_score or NVD_score)
            res['cvss_score'] = cve['openeuler_score']
            # 修复时长(天) = 补丁发布时间 - 漏洞创建时间
            rpm_public_time = str(res['rpm_public_time']).split(" ")[0] if res['rpm_public_time'] is not None else None
            res['cve_close_duration'] = self.getDuration(rpm_public_time, '%Y-%m-%d', res['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S+08:00') / 24
            res['is_slo'] = self.getSlo(res['cvss_score'], res['cve_close_duration'])
            res['issue_url'] = issue['issue_url']
            res['rpm_public_time'] = rpm_public_time

            version_str = cve['version']
            if version_str is None:
                res['versions'] = []
            else:
                versions_temp = version_str.split(',')
                def version_filter(item):
                    return item != ""
                versions = list(filter(version_filter, versions_temp))
                res['versions'] = versions

            indexData = {"index": {"_index": self.index_name, "_id": res['issue_id'] + '_' + res['CVE_num']}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(res) + '\n'

        self.esClient.safe_put_bulk(actions)

    def getInvolvedBranch(self, branch):
        involved_branchs = str(branch).split(",")
        affected_branchs = []
        unaffected_branchs = []
        not_analyze_dbranchs = []
        for involved_branch in involved_branchs:
            branch_info = involved_branch.split(":")
            if len(branch_info) == 2:
                if branch_info[1] == '受影响':
                    affected_branchs.append(branch_info[0])
                elif branch_info[1] == '不受影响':
                    unaffected_branchs.append(branch_info[0])
                else:
                    not_analyze_dbranchs.append(branch_info[0])
            else:
                not_analyze_dbranchs.append(branch_info[0])

        return {"affected_branchs": affected_branchs,
                "unaffected_branchs": unaffected_branchs,
                "not_analyze_dbranchs": not_analyze_dbranchs}

    def getDuration(self, max_time, max_time_format, min_time, min_time_format):
        if max_time is not None and max_time != '' and min_time is not None and min_time != '':
            max_datetime = datetime.datetime.strptime(max_time, max_time_format)
            min_datetime = datetime.datetime.strptime(min_time, min_time_format)
            res = (max_datetime - min_datetime).total_seconds()
            return res / 60 / 60
        else:
            return 0

    def getSlo(self, cvss_score, close_duration):
        if close_duration == 0:
            return 0
        if (cvss_score >= 9 and close_duration <= 7) or \
                (9 > cvss_score >= 7 and close_duration <= 14) or \
                (7 > cvss_score >= 0.1 and close_duration <= 30):
            is_slo = 1
        else:
            is_slo = 0
        return is_slo

    def format_time(self, str):
        r = re.findall('\d+', str)
        if len(r) == 5:
            r.append("00")
        return r[0] + '-' + r[1] + '-' + r[2] + ' ' + r[3] + ':' + r[4] + ':' + r[5]
