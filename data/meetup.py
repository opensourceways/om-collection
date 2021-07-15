import csv
import json
from datetime import datetime

import requests
import xlrd

from data.common import ESClient


class Meetup(object):
    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.activities_index_name = config.get('activities_index_name')
        self.activities_url = config.get('activities_url')
        self.registrants_url = config.get('registrants_url')
        self.query_token = config.get('query_token')
        self.cell_name_index_dict = {}
        self.csv_data = {}

    def run(self, from_time):
        print("Meetup data: start")
        self.csv_data = self.getGiteeIdFromCsv()
        self.meetupWechat()
        print("Meetup data: finished")

    def meetupWechat(self):
        # 获取所有活动
        response = requests.get(self.activities_url + '?token=' + self.query_token)
        if response.status_code != 200:
            print('get activities fail')
            return
        activities = self.esClient.getGenerator(response.text)
        activity_actions = ''
        for activity in activities:
            activity_id = activity['id']
            index_data = {"index": {"_index": self.orgs + '_' + self.activities_index_name, "_id": activity_id}}
            activity_actions += json.dumps(index_data) + '\n'
            activity_actions += json.dumps(activity) + '\n'

            # 获取活动报名者
            response = requests.get(self.registrants_url + str(activity_id) + '/?token=' + self.query_token)
            if response.status_code != 200:
                print("get registrants fail, activity_id is %s" % str(activity_id))
                continue
            registrants = self.esClient.getGenerator(response.text)
            registrant_actions = ''
            for registrant in registrants['registrants']:
                meetup_name = activity['title']
                meetup_date = str(activity['date']).replace('-', '/')
                email = registrant['email']
                user_login = self.csv_data.get(email) if email is not None and email in self.csv_data else None
                key = 'is_' + meetup_name + '_meetup'

                action = {'user_name': registrant['name'],
                          'company': registrant['company'],
                          'profession': registrant['profession'],
                          'telephone_num': registrant['telephone'],
                          'email': email,
                          'meetup_name': meetup_name,
                          'meetup_date': meetup_date,
                          'user_login': user_login,
                          key: 1}
                registrant_id = meetup_name + '_' + meetup_date + '_' + email
                index_data = {"index": {"_index": self.orgs + '_' + self.index_name, "_id": registrant_id}}
                registrant_actions += json.dumps(index_data) + '\n'
                registrant_actions += json.dumps(action) + '\n'
            self.esClient.safe_put_bulk(registrant_actions)
        self.esClient.safe_put_bulk(activity_actions)

    def getGiteeIdFromCsv(self):
        result = {}
        csvFile = open("email_userid.csv", "r")
        reader = csv.reader(csvFile)
        for item in reader:
            if reader.line_num == 1:
                continue
            result[item[0]] = item[1]
        csvFile.close()

        return result

    def updateDataByQuery(self):
        query = '''{
                      "script": {
                        "source": "ctx._source['tag_user_company']='independent'"
                      },
                      "query": {
                        "term": {
                          "tag_user_company.keyword": "n/a"
                        }
                      }
                    }'''
        self.esClient.updateByQuery(query=query)

    def getCellValue(self, row_index, cell_name, sheet):
        if cell_name not in self.cell_name_index_dict:
            return ''
        cell_value = sheet.cell_value(row_index, self.cell_name_index_dict.get(cell_name))
        return cell_value

    def meetup(self):
        csvFile = open("C:\\Users\\Administrator\\Desktop\\mindspore_meetup\\Meetup参会人员.csv", "r", encoding='utf-8')
        reader = csv.reader(csvFile)
        actions = ""
        count = 0
        for item in reader:
            if reader.line_num == 1:
                continue
            count += 1
            result = {}
            company = item[1] if item[1] and item[1] != '无' else '未知企业'
            email = item[2]
            meet_up_name = item[3]
            user_login = self.csv_data.get(email) if email in self.csv_data else None
            result['user_name'] = item[0]
            result['company_name'] = company
            result['email'] = email
            result['meetup_name'] = meet_up_name

            result['user_login'] = user_login
            date = datetime.strptime(item[4], '%Y/%m/%d')
            result['created_at'] = str(date).replace(' ', 'T') + '+08:00'
            key = 'is_' + meet_up_name + '_meetup'
            result[key] = 1

            id = meet_up_name + '_' + email
            indexData = {"index": {"_index": 'mindspore_meetup_user', "_id": id}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(result) + '\n'

        csvFile.close()
        self.esClient.safe_put_bulk(actions)
        print(count)

    def meetupFromExcel(self, orgs, sheet=None, meetup_name='', meetup_date=''):
        wb = xlrd.open_workbook("C:\\Users\\Administrator\\Desktop\\openlookeng_meetup\\626-meetup触达人群统计.xlsx")
        if sheet:
            sh = wb.sheet_by_name(sheet)
        else:
            sh = wb.sheet_by_index(0)

        for i in range(sh.ncols):
            cell_name = sh.cell_value(0, i)
            self.cell_name_index_dict.update({cell_name: i})

        actions = ''
        for r in range(1, sh.nrows):
            email = self.getCellValue(r, '电子邮件', sh)
            user_login = self.csv_data.get(email) if email is not None and email in self.csv_data else None
            telephone_num = self.getCellValue(r, '手机号码', sh)
            user_name = self.getCellValue(r, '姓名', sh)
            key = 'is_' + meetup_name + '_meetup'
            action = {'user_name': user_name,
                      'company': self.getCellValue(r, '单位/公司', sh),
                      'position': self.getCellValue(r, '职位/职务', sh),
                      'telephone_num': telephone_num,
                      'email': email,
                      'meetup_name': meetup_name,
                      'meetup_date': meetup_date,
                      'user_login': user_login,
                      key: 1}
            if email != '':
                id = meetup_name + '_' + meetup_date + '_' + email
            elif telephone_num != '':
                id = meetup_name + '_' + meetup_date + '_' + telephone_num
            else:
                id = meetup_name + '_' + meetup_date + '_' + user_name
            index_data = {"index": {"_index": orgs + '_' + self.index_name, "_id": id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

        self.esClient.safe_put_bulk(actions)
        print(action)
