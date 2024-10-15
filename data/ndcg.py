import datetime
import hashlib
import json

from algorithm.ndcg import NDCG
from data.common import ESClient
import pandas as pd
import numpy as np


class Ndcg(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.orgs = config.get('orgs')
        self.source_index = config.get('source_index')
        self.target_index = config.get('target_index')
        self.start_time = config.get('start_time')
        self.search_datas = []

    def run(self, from_time):
        print("Ndcg calc: staring")
        self.getData()

    def getDataFunc(self, hits):
        for hit in hits:
            source = hit['_source']
            properties = source['properties']
            if properties.get('search_rank_num') is None:
                continue

            data_json = {'search_rank_num': int(properties['search_rank_num']),
                         'created_at': source['created_at'],
                         'search_event_id': properties.get('search_event_id'),
                         'search_key': properties.get('search_key'),
                         'search_rank_url': properties.get('search_result_url')}
            self.search_datas.append(data_json)

    def getDate(self):
        today = datetime.date.today()
        if self.start_time:
            s_time = datetime.datetime.strptime(self.start_time, "%Y-%m-%d").date()
        else:
            s_time = today - datetime.timedelta(days=1)
        return s_time, today

    def getData(self):
        start_time, end_time = self.getDate()
        query = '''{
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "range": {
                                "created_at": {
                                  "gte": "%s",
                                  "lte": "%s"
                                }
                              }
                            },
                            {
                              "term": {
                                "event.keyword": "selectSearchResult"
                              }
                            }
                          ]
                        }
                      },
                      "size": 1000
                    }''' % (start_time, end_time)
        self.esClient.scrollSearch(index_name=self.source_index, search=query, func=self.getDataFunc)
        if not self.search_datas:
            print(f'{start_time} - {end_time}: No selectSearchResult data')
            return
        df = pd.json_normalize(self.search_datas)

        dfg = df.groupby('search_event_id').apply(self.agg_func)
        dfg.reset_index(drop=True, inplace=True)
        print(dfg.head(10))
        self.push_to_es(dfg)

    def agg_func(self, df_agg):
        df_agg['search_key'] = df_agg['search_key'].apply(lambda _: ','.join(_) if isinstance(_, list) else _)
        df = df_agg.drop_duplicates(keep='last', inplace=False)
        df.sort_values('created_at', inplace=True)

        rank_nums = df['search_rank_num'].tolist()
        rank_num_sort = ",".join(str(i) for i in rank_nums)
        rank_dict = df.groupby('search_rank_num').size().to_dict()

        # 多次打开同一条在5分基础上加上ln(p),p为次数；最后一次10分
        df['rel'] = df['search_rank_num'].map(lambda a: np.log(rank_dict.get(a)) + 5)
        df.reset_index(drop=True, inplace=True)
        df.loc[len(df) - 1, 'rel'] = 10

        # 计算一次搜索的NDCG
        df_dis = df.drop_duplicates('search_rank_num', keep='last', inplace=False)
        index_rel_dict = df_dis.set_index(['search_rank_num'])['rel'].to_dict()
        ndcg = NDCG().calc_ndcg_dict(rel_dic=index_rel_dict)

        df_out = df_dis.head(1)[['search_event_id', 'created_at', 'search_key', 'search_rank_url']]
        df_out['ndcg'] = ndcg
        df_out['rank_num_sort'] = rank_num_sort
        return df_out

    def push_to_es(self, df):
        records = df.to_json(orient='records')
        actions = ''
        for record in json.loads(records):
            id_str = record['search_event_id'] + record['created_at'] + record['search_key']
            index_id = hashlib.md5(id_str.encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.target_index, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(record) + '\n'
        self.esClient.safe_put_bulk(actions)
