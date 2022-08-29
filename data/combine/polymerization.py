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

import time
from collections import Counter
import json
from data.common import ESClient


class Polymerization(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.query_index_name = config.get('query_index_name')
        self.query = config.get('query')
        self.key_prefix = config.get('key_prefix')
        self.from_d = config.get('polymerization_from_time')
        self.count_key = config.get('count_key')
        self.is_get_total_count = config.get('is_get_total_count')
        self.is_tag_first_doc = config.get('is_tag_first_doc')
        self.esClient = ESClient(config)
        self.collections = config.get('collections')
        self.is_mix_download = config.get('is_mix_download')
        self.get_total_count_oversea = config.get('get_total_count_oversea')

    def run(self, from_time):
        startTime = time.time()

        querys, key_prefixs, count_keys = None, None, None
        if self.query:
            querys = self.query.split(";")
        if self.key_prefix:
            key_prefixs = self.key_prefix.split(";")
        if self.count_key:
            count_keys = self.count_key.split(";")

        if self.is_mix_download == "true":
            if self.get_total_count_oversea == "true":
                self.getDownloadMixOversea()
            if self.is_get_total_count == "true":
                self.getDownloadMix()
            if self.is_tag_first_doc == "true":
                self.tagIpDownloadMix()
        else:
            if self.is_get_total_count == "true":
                self.getTotalCount(querys, key_prefixs, count_keys)
            if self.is_tag_first_doc == "true":
                self.tagFirstDoc(querys, key_prefixs, count_keys)

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect Polymerization data: finished after ", spent_time)

    def getTotalCount(self, querys, key_prefixs, count_keys):
        for i in range(len(count_keys)):
            query = querys[i] if querys else None
            self.esClient.setToltalCount(self.from_d, query=query, count_key=count_keys[i],
                                         key_prefix=key_prefixs[i])

    def tagFirstDoc(self, querys, key_prefixs, count_keys):
        for i in range(len(count_keys)):
            query = querys[i] if querys else None
            self.esClient.setFirstItem(key_prefix=key_prefixs[i], query=query, key=count_keys[i],
                                       query_index_name=self.query_index_name)

    def getDownloadMix(self):
        j = json.loads(self.collections)
        counter = Counter({})
        for coll in j['collections']:
            query, key_prefix, count_key, query_es, es_authorization = None, None, None, None, None
            if 'query' in coll:
                query = coll['query']
            if 'count_key' in coll:
                count_key = coll['count_key']
            if 'query_es' in coll:
                query_es = coll['query_es']
            if 'es_authorization' in coll:
                es_authorization = coll['es_authorization']

            query_index_name = coll['query_index_name']
            self.esClient.query_index_name = query_index_name
            polymerization_from_time = coll['polymerization_from_time']
            if query is not None:
                query = str(query).replace('"', '\\"')
            if query_es is not None and es_authorization is not None:
                time_count_dict = self.esClient.splitMixDockerHub(from_date=polymerization_from_time,
                                                                  count_key=count_key, query=query,
                                                                  query_index_url=query_es,
                                                                  query_index_name=query_index_name,
                                                                  es_authorization=es_authorization)
                self.esClient.writeMixDownload(time_count_dict, "day_download")
            else:
                time_count_dict = self.esClient.getTotalCountMix(polymerization_from_time, query=query,
                                                                 count_key=count_key, key_prefix=key_prefix)
                # the same key, add value
                counter = counter + Counter(time_count_dict)

        self.esClient.writeMixDownload(dict(counter), "all_download")

    def tagIpDownloadMix(self):
        j = json.loads(self.collections)
        com_keys = {}.keys()
        dicts = []
        query, key_prefix, count_key, query_es = None, None, None, None
        for coll in j['collections']:
            if 'query' in coll:
                query = coll['query']
            count_key = coll['count_key']
            query_index_name = coll['query_index_name']
            self.esClient.query_index_name = query_index_name
            if query is not None:
                query = str(query).replace('"', '\\"')
            ip_first_dict = self.esClient.getFirstItemMix(query=query, key=count_key,
                                                          query_index_name=query_index_name)
            dicts.append(ip_first_dict)
            com_keys = com_keys | ip_first_dict.keys()

        # the same key, min value
        ip_first_comb_dict = {key: min(
            [dicts[0].get(key, float('inf')), dicts[1].get(key, float('inf')), dicts[2].get(key, float('inf'))]) for key
            in com_keys}
        self.esClient.writeFirstDownload(ip_first_comb_dict)

        # total ip
        self.esClient.query_index_name = self.index_name
        ip_all_dict = self.esClient.getTotalCountMix(self.from_d, query="is_first_download:1",
                                                     count_key='ip.keyword')
        self.esClient.writeMixDownload(ip_all_dict, "all_ip")

    def getDownloadMixOversea(self):
        j = json.loads(self.collections)
        counter = Counter({})
        data_dict = {
            'sea': counter,
            'oversea': counter
        }
        for coll in j['collections']:
            query, key_prefix, count_key, query_es, es_authorization = None, None, None, None, None
            key = 'sea'
            if 'query' in coll:
                query = coll['query']
            if 'count_key' in coll:
                count_key = coll['count_key']
            if 'query_es' in coll:
                query_es = coll['query_es']
            if 'es_authorization' in coll:
                es_authorization = coll['es_authorization']
            if 'oversea' in coll:
                key = 'oversea'

            query_index_name = coll['query_index_name']
            self.esClient.query_index_name = query_index_name
            polymerization_from_time = coll['polymerization_from_time']
            if query is not None:
                query = str(query).replace('"', '\\"')
            if query_es is not None and es_authorization is not None:
                time_count_dict = self.esClient.splitMixDockerHub(from_date=polymerization_from_time,
                                                                  count_key=count_key, query=query,
                                                                  query_index_url=query_es,
                                                                  query_index_name=query_index_name,
                                                                  es_authorization=es_authorization)
                # self.esClient.writeMixDownload(time_count_dict, "day_download", oversea)
            elif 'max_count' in coll:
                time_count_dict = self.esClient.getTotalXiheDown(from_date=polymerization_from_time,
                                                                 count_key=count_key, query=query,
                                                                 query_index_name=query_index_name, )
            else:
                time_count_dict = self.esClient.getTotalCountMix(polymerization_from_time, query=query,
                                                                 count_key=count_key, key_prefix=key_prefix)

            # the same key, add value
            data_dict.update({key: data_dict.get(key) + Counter(time_count_dict)})

        for key in data_dict:
            oversea = 1 if key == 'oversea' else 0
            self.esClient.writeMixDownload(dict(data_dict.get(key)), "all_download", oversea)
