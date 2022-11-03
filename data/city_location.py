#  Copyright (c) 2022.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

import json
import time
from data.common import ESClient


class CityLocation(object):

    def __init__(self, config=None):
        self.esClient = ESClient(config)
        self.config = config

        self.index_name = config.get('index_name')
        self.company_loc_url = config.get('company_loc_url')

    def run(self, from_time):
        startTime = time.time()
        self.getCompanyLocationInfo()
        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S", time.gmtime(endTime - startTime))
        print("Collect company location data finished after %s" % spent_time)

    def getCompanyLocationInfo(self):
        actions = ''
        if self.company_loc_url is None:
            return None

        data = self.esClient.request_get(self.company_loc_url)
        reader = data.text.split('\n')
        for item in reader:
            company_info = item.strip().split(';')
            company = company_info[0]
            if company == '' or company == '公司名称':
                continue
            try:
                location = company_info[1]
                center = company_info[2]
                action = {
                    'company': company,
                    'company_location': location,
                    'innovation_center': center,
                }
                loc = self.esClient.getLocationbyCity(location)
                if loc:
                    action.update(loc)
                indexData = {"index": {"_index": self.index_name, "_id": company}}
                actions += json.dumps(indexData) + '\n'
                actions += json.dumps(action) + '\n'
            except IndexError:
                continue
        self.esClient.safe_put_bulk(actions)
