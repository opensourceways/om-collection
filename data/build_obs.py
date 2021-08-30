import json
import datetime

import requests

from data.common import ESClient


class OBS(object):
    def __init__(self, config=None):

        self.config = config
        self.index_name = config.get('index_name')
        self.target_index_name = config.get('target_index_name')
        self.esClient = ESClient(config)

    def run(self, datatime=None):

        packagenames = self.esClient.getObsAllPackageName()


        for i in packagenames["aggregations"]["each_project"]["buckets"]:


            packagename = i["key"]

            # if packagename < "ukui-sidebar":
            #     continue


            print(packagename)


            res = self.esClient.getObsSumAndCount(packagename)
            actions = ''
            for i in res["aggregations"]["each_month"]["buckets"]:
                time = i["key_as_string"]
                for j in i["each_project"]["buckets"]:
                    project_name = j["key"]
                    for l in j["each_hostarch"]["buckets"]:
                        cpu_name = l["key"]
                        count = l["doc_count"]
                        avg_time = l["avg_duration"]["value"]
                        sum = count * avg_time
                        indexData = {"index": {"_index": self.target_index_name,
                                               "_id": project_name + '_' + cpu_name + '_' + packagename + '_' + time}}

                        subResData = {"project": project_name, "cpu": cpu_name, "package_name": packagename,
                                      "count": count,
                                      "sum_time": sum, "average": avg_time, "time": time}

                        actions += json.dumps(indexData) + '\n'
                        actions += json.dumps(subResData) + '\n'
            self.esClient.safe_put_bulk(actions)
