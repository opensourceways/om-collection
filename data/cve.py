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
            resData[''] = data['CVE_level']

    def getIssueByNumber(self, number=None):
        search = '"must": [{"term":{"is_gitee_issue:1}},{ "term": { "issue_number.keyword":"%s"}}]' % (number)
        data = self.esClient.searchEsList(self.all_data_index, search)
        if data is None:
            return None
        jsonData = self.esClient.getGenerator(data)
        return jsonData

    def getCveData(self, currentPage=None, pageSize=None):
        params = {}
        if currentPage is not None and pageSize is not None:
            params['currentPage'] = currentPage
            params['pageSize'] = pageSize
        response = requests.get(self.cve_url, params=params)
        cveData = self.esClient.getGenerator(response.text)
        return cveData['body']
