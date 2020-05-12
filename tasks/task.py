#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020 Technologies Co., Ltd.
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
import queue
import threading
import sys
import time
from configparser import ConfigParser

from tasks.utils import import_object
from datetime import datetime
logger = logging.getLogger(__name__)


BACKEND_MAPPING = {
    'baidutongji': 'data.baidutongji.BaiduTongji'
}

class George:

    def __init__(self):
        """ config is a Config object """
        # self.from_data = config.from_data
        self.config = ConfigParser()
        self.config.read('config.ini')
        self.sections = self.config.sections()
        self.from_data = self.config.get('general', 'from_data')
        self.sleep_time = self.config.getint('general', 'sleep_time')
        print(self.sleep_time)
        print(type(self.sleep_time))


    def start(self):
        logger.info("----------------------------")
        logger.info("Starting engine ...")
        logger.info("- - - - - - - - - - - - - - ")

        drivers = []
        for backend in self.sections:
            if backend in BACKEND_MAPPING:
                driver = import_object(BACKEND_MAPPING[backend], self.getBackendConfig(backend))
                drivers.append(driver)

        starTime = self.from_data
        while True:
            print("start to run from ", starTime)
            for driver in drivers:
                driver.run(starTime)

            time.sleep(self.sleep_time)
            starTime = None
            print("try to run again from ", starTime)

        logger.info("Finished engine ...")


    def getBackendConfig(self, backend_name):
        backend_conf = {}
        for key, value in self.config.items('general'):
            backend_conf[key] = value

        for key, value in self.config.items(backend_name):
            backend_conf[key] = value
        return backend_conf
