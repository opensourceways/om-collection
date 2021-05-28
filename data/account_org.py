import csv
import json
import os
import requests
import yaml

from data.common import ESClient


class AccountOrg(object):
    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.index_name_cla = config.get('index_name_cla')
        self.data_yaml_url = config.get('data_yaml_url')
        self.company_yaml_url = config.get('company_yaml_url')
        self.csv_data = {}

    def run(self, from_time):
        print("Collect AccountOrg data: start")
        self.csv_data = self.getDataFromCsv()
        self.getDataFromCla()
        # self.getDataFromYaml()
        print("Collect AccountOrg data: finished")

    def getDataFromCla(self):
        if self.index_name_cla:
            search_json = '''{
                                  "size": 10000,
                                  "_source": {
                                    "includes": [
                                      "email",
                                      "corporation",
                                      "created_at"
                                    ]
                                  },
                                  "query": {
                                    "bool": {
                                      "must": [
                                        {
                                          "term": {
                                            "is_corporation_signing": "1"
                                          }
                                        }
                                      ]
                                    }
                                  }
                                }'''
            res = requests.get(self.esClient.getSearchUrl(index_name=self.index_name_cla), data=search_json,
                               headers=self.esClient.default_headers, verify=False)
            if res.status_code != 200:
                print("The index not exist")
                return {}
            data = res.json()
            actions = ""
            for hits in data['hits']['hits']:
                source_data = hits['_source']
                email = source_data['email']
                domain = str(email).split("@")[1]
                gitee_id = None
                if email in self.csv_data.keys():
                    gitee_id = self.csv_data[email]
                action = {
                    "email": email,
                    "organization": source_data['corporation'],
                    "gitee_id": gitee_id,
                    "domain": domain,
                    "created_at": source_data['created_at'],
                    "is_cla": 1
                }
                index_data = {"index": {"_index": self.index_name, "_id": email}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'

            self.esClient.safe_put_bulk(actions)

    def getDataFromYaml(self):
        if self.data_yaml_url:
            cmd = 'wget -N %s' % self.data_yaml_url
            p = os.popen(cmd.replace('=', ''))
            p.read()
            datas = yaml.load_all(open('data.yaml', encoding='UTF-8')).__next__()
            actions = ""
            for data in datas['users']:
                gitee_id = data['gitee_id']
                organization = data['companies'][0]['company_name']
                if organization == '':
                    continue

                email = None
                if gitee_id in self.csv_data:
                    email = self.csv_data[gitee_id]
                if email is None:
                    continue
                action = {
                    "email": email,
                    "organization": organization,
                    "gitee_id": gitee_id,
                    "source": "YAML"
                }
                index_data = {"index": {"_index": self.index_name, "_id": email}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'

            self.esClient.safe_put_bulk(actions)

    def getDataFromCsv(self):
        result = {}
        csvFile = open("email_userid.csv", "r")
        reader = csv.reader(csvFile)
        for item in reader:
            if reader.line_num == 1:
                continue
            result[item[0]] = item[1]
        csvFile.close()

        return result
