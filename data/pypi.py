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
import re
import pypistats

from data.common import ESClient
from data import common


class CollectPypi(object):

    def __init__(self, config=None):
        self.config = config
        self.index_name_pypi = config.get('index_name_pypi')
        self.pypi_orgs = config.get('pypi_orgs')
        self.esClient = ESClient(config)

        self.url = config.get('es_url')
        self.authorization = config.get('authorization')

    def run(self, time=None):
        if self.pypi_orgs:
            orgs = str(self.pypi_orgs).split(",")
            startTime = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=6), "%Y-%m-%d")
            for org in orgs:
                print('...start collect pypi download info of %s ...' % org)
                self.get_pypi_overall(startTime, org)
                self.get_pypi_python_major(startTime, org)
                self.get_pypi_python_minor(startTime, org)
                self.get_pypi_system(startTime, org)

    def get_pypi_overall(self, start_date, package):
        datei = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            print(datei)
            if datei == datenow:
                break
            try:
                overall = pypistats.overall(package, start_date=datei.strftime("%Y-%m-%d"),
                                            end_date=datei.strftime("%Y-%m-%d"), format="rst")
            except:
                print('No pypi download info...')
                datei += datetime.timedelta(days=1)
                continue
            With_Mirrors = self.get_data_num_pypi(overall, "with_mirrors")
            Without_Mirrors = self.get_data_num_pypi(overall, "without_mirrors")
            Total = self.get_data_num_pypi(overall, "Total", True)
            dataw = {"With_Mirrors": With_Mirrors, "Without_Mirrors": Without_Mirrors, "Total": Total,
                     "package": package + "_overall_download",
                     "created_at": datei.strftime("%Y-%m-%d") + "T23:00:00+08:00"}
            ID = package + "_pypi_overall_" + datei.strftime("%Y-%m-%d")
            data = common.getSingleAction(self.index_name_pypi, ID, dataw)
            self.esClient.safe_put_bulk(data)
            datei += datetime.timedelta(days=1)

    def get_pypi_python_major(self, start_date, package):
        datei = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            print(datei)
            if datei == datenow:
                break
            try:
                major = pypistats.python_major(package, start_date=datei.strftime("%Y-%m-%d"),
                                               end_date=datei.strftime("%Y-%m-%d"), format="rst")
            except:
                print('No pypi download info...')
                datei += datetime.timedelta(days=1)
                continue
            Python3 = self.get_data_num_pypi(major, "3")
            null = self.get_data_num_pypi(major, "null")
            Total = self.get_data_num_pypi(major, "Total", True)
            dataw = {"Python3": Python3, "Others(null)": null, "Total": Total,
                     "package": package + "_python_major_download",
                     "created_at": datei.strftime("%Y-%m-%d") + "T23:00:00+08:00"}
            ID = package + "_pypi_python_major_" + datei.strftime("%Y-%m-%d")
            data = common.getSingleAction(self.index_name_pypi, ID, dataw)
            self.esClient.safe_put_bulk(data)
            datei += datetime.timedelta(days=1)

    def get_pypi_python_minor(self, start_date, package):
        datei = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            print(datei)
            if datei == datenow:
                break
            try:
                minor = pypistats.python_minor(package, start_date=datei.strftime("%Y-%m-%d"),
                                               end_date=datei.strftime("%Y-%m-%d"), format="rst")
            except:
                print('No pypi download info...')
                datei += datetime.timedelta(days=1)
                continue
            Python37 = self.get_data_num_pypi(minor, "3\.7")
            null = self.get_data_num_pypi(minor, "null")
            Total = self.get_data_num_pypi(minor, "Total", True)
            dataw = {"Python37": Python37, "Others(null)": null, "Total": Total,
                     "package": package + "_python_minor_download",
                     "created_at": datei.strftime("%Y-%m-%d") + "T23:00:00+08:00"}
            ID = package + "_pypi_python_minor_" + datei.strftime("%Y-%m-%d")
            data = common.getSingleAction(self.index_name_pypi, ID, dataw)
            self.esClient.safe_put_bulk(data)
            datei += datetime.timedelta(days=1)

    def get_pypi_system(self, start_date, package):
        datei = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        while True:
            datenow = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
                                                 "%Y-%m-%d")
            print(datei)
            if datei == datenow:
                break
            try:
                system = pypistats.system(package, start_date=datei.strftime("%Y-%m-%d"),
                                          end_date=datei.strftime("%Y-%m-%d"), format="rst")
            except:
                print('No pypi download info...')
                datei += datetime.timedelta(days=1)
                continue
            Windows = self.get_data_num_pypi(system, "Windows")
            Linux = self.get_data_num_pypi(system, "Linux")
            null = self.get_data_num_pypi(system, "null")
            Total = self.get_data_num_pypi(system, "Total", True)
            dataw = {"Windows": Windows, "Others(null)": null, "Linux": Linux, "Total": Total,
                     "package": package + "_system_download",
                     "created_at": datei.strftime("%Y-%m-%d") + "T23:00:00+08:00"}
            ID = package + "_pypi_system_" + datei.strftime("%Y-%m-%d")
            data = common.getSingleAction(self.index_name_pypi, ID, dataw)
            self.esClient.safe_put_bulk(data)
            datei += datetime.timedelta(days=1)

    def get_data_num_pypi(self, data, mark, bm=False):
        if "%" in data:
            if bm:
                content = re.search(mark + '\s+(\d+)', data)
                num = content.group(1) if content else 0
            else:
                content = re.search(mark + '\s+\d+\.\d+%\s+(\d+)', data)
                num = content.group(1) if content else 0
        else:
            content = re.search(mark + '\s+(\d+)', data)
            num = content.group(1) if content else 0
        return int(num)
