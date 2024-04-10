#  Copyright (c) 2023.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

from data import common
from data.common import ESClient


class PypiDownload(object):
    def __init__(self, config=None):
        self.config = config
        self.url = config.get("es_url")
        self.authorization = config.get("authorization")

        self.esClient = ESClient(config)

        self.index_name = config.get("index_name")
        self.google_key_path = config.get("google_key_path")
        self.projects = config.get("projects")

    def run(self, start_time):
        self.download_data()

    def download_data(self):
        # 获取凭证，并创建bigquery客户端
        google_key_path = self.google_key_path
        credentials = service_account.Credentials.from_service_account_file(
            google_key_path
        )
        client = bigquery.Client(
            project=credentials.project_id, credentials=credentials
        )

        datei = datetime.datetime.strftime(
            datetime.datetime.now() - datetime.timedelta(days=1), "%Y-%m-%d"
        )

        projects_arr = self.arr_add_quotation(self.projects.split(","))
        projects_str = ",".join(projects_arr)

        query_str = """ SELECT timestamp, file.project, file.version, details.python, details.system.name 
                    FROM `bigquery-public-data.pypi.file_downloads` 
                    WHERE file.project IN (%s) 
                    AND DATE(timestamp) BETWEEN DATE('%s') AND DATE('%s') 
                """ % (
            projects_str,
            datei,
            datei,
        )

        # 获取请求数据
        res = client.query(query_str)
        res = res.result()
        # 上传数据到es
        self.upload_data(res)

    def upload_data(self, res):
        actions = ""
        for row in res:
            timestamp = row.timestamp
            # 将对象都转化为字符串和字典基础类型
            doc_id = timestamp.isoformat()
            row_dic = self.obj2dic(row)

            action = common.getSingleAction(self.index_name, doc_id, row_dic)
            actions += action

        self.esClient.safe_put_bulk(actions)

    def obj2dic(self, obj):
        """
        make object to dict,
        @return: dict:target format
        """
        return {
            "created_at": obj.timestamp.isoformat(),
            "file_project": obj.project,
            "file_version": obj.version,
            "python": obj.python,
            "system": obj.name,
        }

    def arr_add_quotation(self, arr):
        """
        add quotation to arr
        """
        new_arr = []
        for item in arr:
            new_arr.append("'" + item + "'")
        return new_arr
