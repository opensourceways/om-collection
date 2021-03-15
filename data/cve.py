import json
from datetime import datetime

import requests

from data.common import ESClient


class CVE(object):
    def __init__(self, config=None):
        self.cve_url = config.get("cve_url")
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.all_data_index = config.get('all_data_index')

    def run(self, from_time):
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
                process_time = (datetime.strptime(resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00') - datetime.strptime(
                    issue_closed_at, '%Y-%m-%dT%H:%M:%S+08:00')).seconds
                resData['process_time'] = process_time
            resData['issue_state'] = issue['issue_state']
            resData['repo_name'] = issue['gitee_repo']

            # 漏洞感知时长  cve 创建时间-cve首次公开时间
            if len(str(data['CVE_public_time'])) <= 0:
                resData['loopholes_perceive_times'] = 0
            else:
                resData['loopholes_perceive_times'] = (
                        datetime.strptime(resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00') - datetime.strptime(
                    data['CVE_public_time'], '%Y-%m-%d')).seconds
            # issue 响应时长
            firstreplyissuetime = self.getFirstReplyTime(resData['issue_number'])
            if firstreplyissuetime is None or len(str(firstreplyissuetime)) <= 0:
                resData['first_reply_times'] = 0
            else:
                resData['first_reply_times'] = (
                        datetime.strptime(firstreplyissuetime, '%Y-%m-%dT%H:%M:%S.000Z') - datetime.strptime(
                    resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00')).seconds

            # 漏洞补丁修复时长
            if len(str(data['rpm_public_time'])) <= 0:
                resData['loopholes_patch_times'] = 0
            else:
                resData['loopholes_patch_times'] = (
                        datetime.strptime(data['rpm_public_time'], '%Y-%m-%d %H:%M:%S') - datetime.strptime(
                    resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00')).seconds
            # SA 发布时长
            if len(str(data['SA_public_time'])) <= 0:
                resData['loopholes_sa_release_times'] = 0
            else:
                resData['loopholes_sa_release_times'] = (
                        datetime.strptime(data['SA_public_time'], '%Y-%m-%d %H:%M:%S') - datetime.strptime(
                    resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00')).seconds
            # 漏洞响应时长
            if len(str(data['SA_public_time'])) <= 0 or len(str(data['CVE_public_time'])) < 0:
                resData['loopholes_response_times'] = 0
            else:
                resData['loopholes_response_times'] = (
                        datetime.strptime(data['SA_public_time'], '%Y-%m-%d %H:%M:%S') - datetime.strptime(
                    data['CVE_public_time'], '%Y-%m-%d')).seconds
            # 分支
            milestone = data['milestone']
            if milestone is not None and milestone != '':
                branches = str(milestone).split(',')
                for br in branches:
                    brAffects = str(br).split(':')
                    subResData = resData.copy()
                    prs = self.getPrByTitle(issue['issue_title'], brAffects[0])
                    if prs is not None and len(prs) > 0:
                        # pr 合入时间
                        pr = prs[0]['_source']
                        try:
                            resData['pr_merged_at'] = pr['pull_merged_at']
                            resData['loopholes_fix_times'] = (
                                    datetime.strptime(resData['pr_merged_at'],
                                                      '%Y-%m-%dT%H:%M:%S+08:00') - datetime.strptime(
                                resData['created_at'], '%Y-%m-%dT%H:%M:%S+08:00')).seconds
                            # 待版本发布时长
                            if len(data['rpm_public_time']) >= 1 and len(resData['pr_merged_at']) >= 1:
                                resData['loopholes_times'] = (
                                        datetime.strptime(data['rpm_public_time'],
                                                          '%Y-%m-%d %H:%M:%S') - datetime.strptime(
                                    resData['pr_merged_at'], '%Y-%m-%dT%H:%M:%S+08:00')).seconds
                        except Exception:
                            resData['pr_merged_at'] = 0
                        # 漏洞修复时长

                if brAffects[1] == '受影响':
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

    def getIssueByNumber(self, number=None):
        search = '"must": [{"term":{"is_gitee_issue":1}},{ "term": { "issue_number.keyword":"%s"}}]' % (number)
        data = self.esClient.searchEsList(self.all_data_index, search)
        if data is None:
            return None
        return data['_source']

    def getCveData(self, currentPage=None, pageSize=None):
        params = {}
        if currentPage is not None and pageSize is not None:
            params['currentPage'] = currentPage
            params['pageSize'] = pageSize
        response = requests.get(self.cve_url, params=params)
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
        res = requests.post(url=url, data=queryStr, verify=False, headers=header)

        responseData = json.loads(res.text)
        return responseData['hits']['hits']

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
