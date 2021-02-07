import json

from data.common import ESClient
import requests


class Cla(object):
    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.esClient = ESClient(config)
        self.platform = config.get('platform')
        self.api_url = config.get('api_url')
        self.api_auth = config.get('api_auth')
        self.api_link = config.get('api_link')
        self.api_corporation = config.get('api_corporation')
        self.api_employee = config.get('api_employee')
        self.username = config.get('username')
        self.password = config.get('password')
        self.index_name = config.get('index_name')

    def run(self, from_time):
        print("Collect CLA data: start")
        self.getClaCorporationsSigning()
        print("Collect CLA data: finished")

    def getClaCorporationsSigning(self):
        # first: get token
        auth_url = f'{self.api_url}/{self.api_auth}/{self.platform}'
        data = json.dumps({'password': self.password, 'username': self.username})
        token_info = self.fetch(url=auth_url, method='post', data=data)
        token = token_info['data']['access_token']

        # second: get link
        headers = {'token': token}
        link_url = f'{self.api_url}/{self.api_link}'
        link_infos = self.fetch(url=link_url, method='get', headers=headers)
        link_id = ''
        for link_info in link_infos['data']:
            if link_info['org_id'] != self.orgs:
                continue
            link_id = link_info['link_id']

        # third: get corporation
        corporation_url = f'{self.api_url}/{self.api_corporation}/{link_id}'
        corporation_infos = self.fetch(url=corporation_url, method='get', headers=headers)
        print(corporation_infos)
        for corporation_info in corporation_infos['data']:
            admin_email = corporation_info['admin_email']
            corporation_name = corporation_info['corporation_name']

            # fourth: get users
            employee_url = f'{self.api_url}/{self.api_employee}/{link_id}/{admin_email}'
            employee_infos = self.fetch(url=employee_url, method='get', headers=headers)
            print(employee_infos)
            employees = employee_infos['data']
            if len(employees) == 0:
                continue

            # write to es
            self.writeClaEmployees(corporation=corporation_name, employees=employees)

    def fetch(self, url, method='get', headers=None, data=None):
        req = requests.request(method, url, data=data, headers=headers)
        if req.status_code != 200:
            print("cla api error: ", req.text)
        res = json.loads(req.text)
        return res

    def writeClaEmployees(self, corporation, employees):
        actions = ""
        for employee in employees:
            action = {
                "employee_id": employee["id"],
                "email": employee["email"],
                "name": employee["name"],
                "created_at": employee['date'],
                "updated_at": employee['date'],
                "corporation": corporation,
                "is_corporation_signing": 1,
                "is_individual_signing": 0,
            }

            index_data = {"index": {"_index": self.index_name, "_id": employee['id']}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

        self.esClient.safe_put_bulk(actions)
