import json

from data.common import ESClient
from collect.cla import ClaClient

LINK = 'link'
CORPORATION = 'corporation-signing'
EMPLOYEE = 'employee-signing'


class Cla(object):
    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.esClient = ESClient(config)
        self.claClient = ClaClient(config)
        self.api_url = self.claClient.api_url
        self.timeout = self.claClient.timeout
        self.index_name = config.get('index_name')

    def run(self, from_time):
        print("Collect CLA data: start")
        self.getClaCorporationsSigning()
        print("Collect CLA data: finished")

    def getClaCorporationsSigning(self):
        # first: get token
        token = self.claClient.get_token_cla()
        headers = {'token': token}

        # second: get link
        link_url = f'{self.api_url}/{LINK}'
        link_infos = self.claClient.fetch_cla(url=link_url, method='get', headers=headers)
        link_id = ''
        for link_info in link_infos['data']:
            if link_info['org_id'] != self.orgs:
                continue
            link_id = link_info['link_id']

        # third: get corporation
        corporation_url = f'{self.api_url}/{CORPORATION}/{link_id}'
        corporation_infos = self.claClient.fetch_cla(url=corporation_url, method='get', headers=headers)
        for corporation_info in corporation_infos['data']:
            admin_email = corporation_info['admin_email']
            corporation_name = corporation_info['corporation_name']

            # fourth: get users
            employee_url = f'{self.api_url}/{EMPLOYEE}/{link_id}/{admin_email}'
            employee_infos = self.claClient.fetch_cla(url=employee_url, method='get', headers=headers)
            employees = employee_infos['data']
            if len(employees) == 0:
                continue

            # write to es
            self.writeClaEmployees(corporation=corporation_name, employees=employees)

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
