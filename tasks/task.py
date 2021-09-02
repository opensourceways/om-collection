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


import logging
import threading
import sys
import time
from configparser import ConfigParser

from tasks.utils import import_object
from datetime import datetime

logger = logging.getLogger(__name__)

BACKEND_MAPPING = {
    'baidutongji': 'data.baidutongji.BaiduTongji',
    'gitee': 'data.gitee.Gitee',
    'gitee_event': 'data.gitee_event.GiteeEvent',
    'nginx': 'data.nginx.Nginx',
    'obs': 'data.obs.OBS',
    'mailman': 'data.mailman.MailMan',
    'sig': 'data.sig.SIG',
    'github_down': 'data.github_down.GitHubDown',
    'github_swf': 'data.github_swf.GitHubSWF',
    'huawei_swr': 'data.huawei_swr.HUAWEISWR',
    'users': 'data.combine.users.Users',
    'huaweicloud': 'data.huaweicloud.HuaweiCloud',
    'bilibili': 'data.bilibili.BILIBILI',
    'collect_data': 'data.collect_data.CollectData',
    'transform_data': 'data.transform_data.TransformData',
    'git_commit': 'data.git_commit.GitCommit',
    'meetings': 'data.meetings.Meetings',
    'report_email': 'data.report_email.ReportEmail',
    'cve': 'data.cve.CVE',
    'cla': 'data.cla.Cla',
    'polymerization': 'data.combine.polymerization.Polymerization',
    'account_org': 'data.account_org.AccountOrg',
    'git': 'data.git_commit.GitCommit',
    'ru_download': 'data.ru_download.RuDownload',
    'meetup': 'data.meetup.Meetup',
    'github_pr_issue': 'data.github_pr_issue.GitHubPrIssue',
    'try_me': 'data.tryme.TryMe',
    'blue_zone_user': 'data.blue_zone_user.BlueZoneUser',
    'googleanalytic_user': 'data.googleanalytic_user.GOOGLEANALYTICUSER',
    'up_down_ratio': 'data.combine.up_down_ratio.UpDownRatio'
}


class George:

    def __init__(self):
        """ config is a Config object """
        # self.from_data = config.from_data
        self.config = ConfigParser()
        self.config.read('config.ini', 'UTF-8')
        self.sections = self.config.sections()
        self.from_data = self.config.get('general', 'from_data')
        self.sleep_time = self.config.getint('general', 'sleep_time')

    def start(self):
        logger.info("----------------------------")
        logger.info("Starting engine ...")
        logger.info("- - - - - - - - - - - - - - - ")

        drivers = []
        for backend in self.sections:
            if backend in BACKEND_MAPPING:
                driver = import_object(BACKEND_MAPPING[backend], self.getBackendConfig(backend))
                drivers.append(driver)

        starTime = self.from_data
        while True:
            if starTime is None:
                print("[common] Start to run from itself time")
            else:
                print("[common] Start to run from ", starTime)
            for driver in drivers:
                driver.run(starTime)

            print("try to run again....waiting for %d seconds, from %s" % (self.sleep_time, self.from_data))
            time.sleep(self.sleep_time)
            starTime = None
            print("try to run again")

        logger.info("Finished engine ...")

    def getBackendConfig(self, backend_name):
        backend_conf = {}
        for key, value in self.config.items('general'):
            backend_conf[key] = value

        for key, value in self.config.items(backend_name):
            backend_conf[key] = value
        return backend_conf
