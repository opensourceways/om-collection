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
import datetime
import json
import requests
import pymysql
import time
import traceback
from data.common import ESClient
from data import common


class TransformData(object):

    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.host = config.get('host')
        self.user = config.get('user')
        self.password = config.get('password')
        self.database = config.get('database')
        self.table = config.get('table')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')

    def run(self, from_time):
        self.cla_mysql_to_es(self.host, self.user, self.password, self.database, self.table)

    def exeMysql(self, host, user, pwd, name, qury, des=False):

        db = pymysql.connect(host=host, user=user, password=pwd, port=3306, database=name, charset='utf8')

        cursor = db.cursor()

        sql = qury

        try:
            cursor.execute(sql)
        except:
            print(traceback.format_exc())
            return ''

        results = cursor.fetchall()
        if des:
            db.close()
            print(cursor.description)
            return cursor.description

        db.close()
        # print(results)
        return results

    def cla_mysql_to_es(self, host, user, pwd, database, table):
        qury = 'select * from %s' % table
        titles = self.exeMysql(host, user, pwd, database, qury, True)
        datas = self.exeMysql(host, user, pwd, database, qury)
        datap = ''
        print(len(datas))
        for data in datas:
            index = 0
            body = {'database_name': database, 'table_name': table}
            while True:
                if index == len(data):
                    break
                body.update({titles[index][0]: str(data[index])})
                index += 1
            ID = data[0]
            body['created_at'] = body['created_at'].replace(' ', 'T') + "+08:00"
            if body['date'] == '':
                body['date'] = body['created_at']
            datar = common.getSingleAction(self.index_name, ID, body)
            datap += datar
        self.esClient.safe_put_bulk(datap)

