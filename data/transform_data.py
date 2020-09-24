import datetime
import json
import requests
import pymysql
import time
import traceback


class TransformData(object):

    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.host = config.get('host')
        self.user = config.get('user')
        self.password = config.get('password')
        self.database = config.get('database')
        self.table = config.get('table')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')

    def run(self):
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
        print(results)
        return results

    def safe_put_bulk(self, bulk_json, header=None, url=None):
        """Bulk items to a target index `url`. In case of UnicodeEncodeError,
        the bulk is encoded with iso-8859-1.

        :param url: target index where to bulk the items
        :param bulk_json: str representation of the items to upload
        """
        if not bulk_json:
            return
        _header = {
            "Content-Type": 'application/x-ndjson',
            'Authorization': self.authorization
        }
        if header:
            _header = header

        _url = self.url
        if url:
            _url = url

        try:
            res = requests.post(_url + "/_bulk", data=bulk_json,
                                headers=_header, verify=False)
            res.raise_for_status()
        except UnicodeEncodeError:

            # Related to body.encode('iso-8859-1'). mbox data
            bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
            res = requests.put(url, data=bulk_json, headers=headers)
            res.raise_for_status()

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""
        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData) + '\n'
        action += json.dumps(body) + '\n'
        return action

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
            datar = self.getSingleAction(self.index_name, ID, body)
            datap += datar
        self.safe_put_bulk(datap)

