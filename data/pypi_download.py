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


class pypiDownload(object):
    def __init__(self, config=None):
        self.config = config
        self.url = config.get("es_url")
        self.authorization = config.get("authorization")

        self.esClient = ESClient(config)

        self.index_name = config.get("index_name")
        self.google_key_path = config.get("google_key_path")
        self.projects = config.get("projects")

    def run(self, from_time):
        self.download_data()

    def download_data(self):

        google_key_path = self.google_key_path
        # 获取数据的时间，days是几天前
        start_date = datetime.datetime.strftime(
            datetime.datetime.now() - datetime.timedelta(days=1), "%Y-%m-%d"
        )

        credentials = service_account.Credentials.from_service_account_file(
            google_key_path
        )
        client = bigquery.Client(
            project=credentials.project_id, credentials=credentials
        )

        datei = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_data = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
        datenow = datetime.datetime.strptime(end_data, "%Y-%m-%d")
        while datei < datenow:
            print(datei)
            query_template = """
            SELECT timestamp, file.project, file.version, details.python, details.system.name 
            FROM `bigquery-public-data.pypi.file_downloads` 
            WHERE file.project IN UNNEST(@projects) 
            AND DATE(timestamp) BETWEEN DATE(@start_date) AND DATE(@end_date)
            """
            projects = self.projects

            # 准备查询参数
            query_params = [
                bigquery.ArrayQueryParameter('projects', 'STRING', projects),
                bigquery.ScalarQueryParameter('start_date', 'DATE', datei.date().isoformat()),
                bigquery.ScalarQueryParameter('end_date', 'DATE', datei.date().isoformat())
            ]

            # 获取请求数据
            # 运行参数化查询
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = query_params

            query_job = client.query(query_template, job_config=job_config)

            # 检索查询结果
            results = query_job.result()

            # 上传数据到es
            self.upload_data(results)

            datei += datetime.timedelta(days=1)

    def upload_data(self, res):
        actions = ""
        for row in res:
            timestamp = row.timestamp
            # 将对象都转化为字符串和字典基础类型
            id = timestamp.isoformat()
            row_dic = self.obj2dic(row)

            print(row_dic)
            action = common.getSingleAction(self.index_name, id, row_dic)
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