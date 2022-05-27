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
# Create: 2020-05
#
import csv
import json
import requests
import xlrd
from datetime import datetime

import yaml
from geopy.geocoders import Nominatim
from phone import Phone

from data.common import ESClient


class Meetup(object):
    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.esClient = ESClient(config)
        self.eamil_gitee_index = config.get('eamil_gitee_index')
        self.city_lon_lat_index = config.get('city_lon_lat_index')
        self.index_name = config.get('index_name')
        self.activities_index_name = config.get('activities_index_name')
        self.activities_url = config.get('activities_url')
        self.registrants_url = config.get('registrants_url')
        self.query_token = config.get('query_token')
        self.company_aliases_yaml = config.get('company_aliases_yaml')
        self.profession_aliases_yaml = config.get('profession_aliases_yaml')
        self.cell_name_index_dict = {}
        self.email_giteeid_dict = {}
        self.aliase_company_dict = {}
        self.aliase_profession_dict = {}
        self.city_lon_lat_dict = {}
        self.csv_data = {}

    def run(self, from_time):
        print("Meetup data collect: start")
        self.getCityLonLatDict()

        email_giteeid = self.getEmailGiteeDict()
        self.updateHistorGiteeid(email_giteeid=email_giteeid)

        aliase_company = self.getAliaseCompanyDict()
        self.updateHistorCompany(aliase_company=aliase_company)

        aliase_profession = self.getAliaseProfessionDict()
        self.updateHistorProfession(aliase_profession=aliase_profession)

        self.meetupWechat()
        print("Meetup data collect: finished")

    def meetupWechat(self):
        # 获取所有活动
        response = self.esClient.request_get(self.activities_url + '?token=' + self.query_token)
        if response.status_code != 200:
            print('get activities fail, code=%d' % response.status_code)
            return
        activities = self.esClient.getGenerator(response.text)
        activity_actions = ''
        for activity in activities:
            activity_id = activity['id']
            index_data = {"index": {"_index": self.activities_index_name, "_id": activity_id}}
            activity_actions += json.dumps(index_data) + '\n'
            activity_actions += json.dumps(activity) + '\n'

            # 获取活动报名者
            response = self.esClient.request_get(self.registrants_url + str(activity_id) + '/?token=' + self.query_token)
            if response.status_code != 200:
                print("get registrants fail, code=%d, activity_id is %s" % (response.status_code, str(activity_id)))
                continue
            registrants = self.esClient.getGenerator(response.text)
            registrant_actions = ''
            for registrant in registrants['registrants']:
                meetup_name = activity['title']
                meetup_date = str(activity['date']).replace('-', '/')
                # email -> giteeid
                email = registrant['email']
                user_login = self.email_giteeid_dict.get(
                    email) if email is not None and email in self.email_giteeid_dict else None
                # company -> tag_user_company
                company = registrant['company']
                if company is None or company == '':
                    tag_user_company = '未知企业'
                elif company in self.aliase_company_dict:
                    tag_user_company = self.aliase_company_dict.get(company)
                else:
                    tag_user_company = company
                # profession_org -> profession
                profession_org = registrant['profession']
                profession = self.aliase_profession_dict.get(
                    profession_org) if profession_org is not None and profession_org in self.aliase_profession_dict else profession_org
                # phone_num -> geo
                phone_num = registrant['telephone']
                geo = self.getGeographicalByPhone(phone_num=phone_num)
                key = 'is_' + meetup_name + '_meetup'

                action = {'user_name': registrant['name'],
                          'company': registrant['company'],
                          'tag_user_company': tag_user_company,
                          'profession': profession,
                          'position': profession_org,
                          'telephone_num': phone_num,
                          'email': email,
                          'meetup_name': meetup_name,
                          'meetup_date': meetup_date,
                          'user_login': user_login,
                          'sign': registrant['sign'],
                          key: 1}
                if geo is not None:
                    city = geo['city']
                    # city -> lon & lat
                    if city in self.city_lon_lat_dict:
                        lon_lat = str(self.city_lon_lat_dict.get(city)).split("-")
                        action.update({'lon': lon_lat[0], 'lat': lon_lat[1]})
                    action.update(geo)
                registrant_id = meetup_name + '_' + meetup_date + '_' + email
                index_data = {"index": {"_index": self.index_name, "_id": registrant_id}}
                registrant_actions += json.dumps(index_data) + '\n'
                registrant_actions += json.dumps(action) + '\n'
            self.esClient.safe_put_bulk(registrant_actions)
        self.esClient.safe_put_bulk(activity_actions)

    def getEmailGiteeDict(self):
        search = '"must": [{"match_all": {}}]'
        hits = self.esClient.searchEsList(index_name=self.eamil_gitee_index, search=search)
        data = {}
        if hits is not None and len(hits) > 0:
            for hit in hits:
                source = hit['_source']
                data.update({source['email']: source['gitee_id']})
        return data

    def getAliaseCompanyDict(self):
        aliase_company_dict = {}
        res = self.esClient.request_get(url=self.company_aliases_yaml)
        if res.status_code == 200:
            data = yaml.safe_load(res.text)
            for org in data['companies']:
                for aliase in org['aliases']:
                    aliase_company_dict.update({aliase: org['company_name']})
        return aliase_company_dict

    def getAliaseProfessionDict(self):
        aliase_profession_dict = {}
        res = self.esClient.request_get(url=self.profession_aliases_yaml)
        if res.status_code == 200:
            data = yaml.safe_load(res.text)
            for org in data['professions']:
                for aliase in org['aliases']:
                    aliase_profession_dict.update({aliase: org['profession']})
        return aliase_profession_dict

    def getCityLonLatDict(self):
        search = '"must": [{"match_all": {}}]'
        hits = self.esClient.searchEsList(index_name=self.city_lon_lat_index, search=search)
        if hits is not None and len(hits) > 0:
            for hit in hits:
                source = hit['_source']
                lon_lat = str(source['longitude']) + "-" + str(source['latitude'])
                self.city_lon_lat_dict.update({source['city_short']: lon_lat})

    def updateCityLonLat(self):
        for city, lon_lat in self.city_lon_lat_dict.items():
            ll = str(lon_lat).split("-")
            query = '''{
                          "script": {
                            "source": "ctx._source['lon']=params['lon'];ctx._source['lat']=params['lat']",
                            "params": {
                                  "lon": "%s",
                                  "lat": "%s"
                                }
                          },
                          "query": {
                            "term": {
                              "city.keyword": "%s"
                            }
                          }
                        }''' % (ll[0], ll[1], city)
            self.esClient.updateByQuery(query=query.encode('utf-8'))

    def updateHistorGiteeid(self, email_giteeid):
        if self.email_giteeid_dict == email_giteeid:
            print('there is no changes of email/giteeid')
            return
        diff = email_giteeid.items() - self.email_giteeid_dict.items()
        self.updateDataByQuery(change_data_dict=diff, update_field='user_login', query_field='email')
        self.email_giteeid_dict = email_giteeid

    def updateHistorCompany(self, aliase_company):
        if self.aliase_company_dict == aliase_company:
            print('there is no changes of company/aliase')
            return
        diff = aliase_company.items() - self.aliase_company_dict.items()
        self.updateDataByQuery(change_data_dict=diff, update_field='tag_user_company', query_field='company')
        self.aliase_company_dict = aliase_company

    def updateHistorProfession(self, aliase_profession):
        if self.aliase_profession_dict == aliase_profession:
            print('there is no changes of profession/aliase')
            return
        diff = aliase_profession.items() - self.aliase_profession_dict.items()
        self.updateDataByQuery(change_data_dict=diff, update_field='profession', query_field='position')
        self.aliase_profession_dict = aliase_profession

    def updateDataByQuery(self, change_data_dict, update_field, query_field):
        for email, gitee_id in change_data_dict:
            query = '''{
                          "script": {
                            "source": "ctx._source['%s']=params['giteeid']",
                            "params": {
                                  "giteeid": "%s"
                                }
                          },
                          "query": {
                            "term": {
                              "%s.keyword": "%s"
                            }
                          }
                        }''' % (update_field, gitee_id, query_field, email)
            self.esClient.updateByQuery(query=query.encode('utf-8'))

    def writeEmailGiteeToES(self):
        result = {}
        csvFile = open("email_userid.csv", "r")
        reader = csv.reader(csvFile)
        actions = ""
        for item in reader:
            if reader.line_num == 1:
                continue
            result[item[0]] = item[1]
            action = {
                'email': item[0],
                'gitee_id': item[1]
            }

            index_data = {"index": {"_index": self.eamil_gitee_index, "_id": item[0]}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)
        csvFile.close()

        return result

    def getCellValue(self, row_index, cell_name, sheet):
        if cell_name not in self.cell_name_index_dict:
            return ''
        cell_value = sheet.cell_value(row_index, self.cell_name_index_dict.get(cell_name))
        return cell_value

    def getGeographicalByPhone(self, phone_num):
        try:
            data = Phone().find(phone_num)
        except Exception as ex:
            print(ex)
            data = None
        return data

    def meetup(self):
        csvFile = open("Meetup参会人员.csv", "r", encoding='utf-8')
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
            user_login = self.email_giteeid_dict.get(email) if email in self.email_giteeid_dict else None
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

    def meetupFromExcel(self, sheet=None, meetup_name='', meetup_date=''):
        wb = xlrd.open_workbook("626-meetup触达人群统计.xlsx")
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
            user_login = self.email_giteeid_dict.get(email) if email is not None and email in self.email_giteeid_dict else None
            company = self.getCellValue(r, '单位/公司', sh)
            if company is None or company == '':
                tag_user_company = '未知企业'
            elif company in self.aliase_company_dict:
                tag_user_company = self.aliase_company_dict.get(company)
            else:
                tag_user_company = company
            telephone_num = self.getCellValue(r, '手机号码', sh)
            if type(telephone_num) == float:
                telephone_num = str(int(telephone_num))
            geo = self.getGeographicalByPhone(phone_num=telephone_num)
            user_name = self.getCellValue(r, '姓名', sh)
            key = 'is_' + meetup_name + '_meetup'
            action = {'user_name': user_name,
                      'company': company,
                      'tag_user_company': tag_user_company,
                      'position': self.getCellValue(r, '职位/职务', sh),
                      'profession': self.getCellValue(r, '职位/职务', sh),
                      'telephone_num': telephone_num,
                      'email': email,
                      'meetup_name': meetup_name,
                      'meetup_date': meetup_date,
                      'user_login': user_login,
                      key: 1}
            if geo is not None:
                city = geo['city']
                # city -> lon & lat
                if city in self.city_lon_lat_dict:
                    lon_lat = str(self.city_lon_lat_dict.get(city)).split("-")
                    action.update({'lon': lon_lat[0], 'lat': lon_lat[1]})
                action.update(geo)
            if email != '':
                id = meetup_name + '_' + meetup_date + '_' + email
            elif telephone_num != '':
                id = meetup_name + '_' + meetup_date + '_' + telephone_num
            else:
                id = meetup_name + '_' + meetup_date + '_' + user_name
            index_data = {"index": {"_index": self.index_name, "_id": id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

        self.esClient.safe_put_bulk(actions)

    def city_lon_lat(self):
        csvFile = open("city - 副本.csv", "r", encoding='utf-8')
        reader = csv.reader(csvFile)
        actions = ""
        count = 0
        for item in reader:
            if reader.line_num == 1:
                continue
            count += 1
            result = {'province': item[0], 'province_short': item[1], 'city_short': item[2], 'city': item[3]}

            gps = Nominatim(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
            location = gps.geocode(item[3])
            result['longitude'] = location.longitude
            result['latitude'] = location.latitude

            indexData = {"index": {"_index": 'city_lon_lat', "_id": item[2]}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(result) + '\n'

        csvFile.close()
        self.esClient.safe_put_bulk(actions)
        print(count)
