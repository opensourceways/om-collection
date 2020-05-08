#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 The community Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
