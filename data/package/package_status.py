#  Copyright (c) 2023.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import datetime
import json

from data.common import ESClient


class packageStatus(object):
    def __init__(self, config=None):
        self.config = config
        self.url = config.get("es_url")
        self.authorization = config.get("authorization")

        self.esClient = ESClient(config)
        self.index_name = config.get("index_name")
        self.packge_repo = config.get("packge_repo")

    def run(self, from_time):
        package_status_dic = {"repo": self.packge_repo}
        res_dic_data = self.get_openeuper_cve_state(self.index_name, package_status_dic)

    def get_openeuper_cve_state(self, index_name, repo_dic=None):
        """
        Add CVE state to repo_dic
        :param index_name: index name
        :param repo_dic: repo_dic
        :return: repo_dic
        """
        repo_name = repo_dic["repo"]
        repo_dic["cve"] = {}
        search = (
            """
        {
            "track_total_hits": true,
            "size": 10000,
            "_source": [
                "repository",
                "CVE_level",
                "issue_state",
                "issue_customize_state",
                ""
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "repository": "%s"
                            }
                        },
                        {
                            "range": {
                                "created_at": {
                                    "gte": "now-1y/y",
                                    "lte": "now"
                                }
                            }
                        }
                    ]
                }
            }
        }"""
            % repo_name
        )
        scroll_duration = "1m"
        data_dic_list = []

        def func(data):
            for item in data:
                print(item)
                data_dic_list.append(item["_source"])

        self.esClient.scrollSearch(index_name, search, scroll_duration, func)
        fixed_cve_count = 0
        cve_count = data_dic_list.__len__()
        for data_dic in data_dic_list:
            if (
                data_dic["issue_state"] == "closed"
                or data_dic["issue_state"] == "rejected"
            ):
                fixed_cve_count += 1
        if fixed_cve_count == cve_count > 0:
            repo_dic["cve"]["is_positive"] = 1
            repo_dic["cve"]["status"] = "有CVE且全部修复"
        elif fixed_cve_count == cve_count == 0:
            repo_dic["cve"]["is_positive"] = 1
            repo_dic["cve"]["status"] = "没有CVE问题"
        elif cve_count > fixed_cve_count > 0:
            repo_dic["cve"]["is_positive"] = 0
            repo_dic["cve"]["status"] = "有CVE部分未修复"
        elif fixed_cve_count == 0 and cve_count > 0:
            repo_dic["cve"]["is_positive"] = 0
            repo_dic["cve"]["status"] = "有CVE全部未修复"
        return repo_dic
