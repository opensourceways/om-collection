import json
import time

from data.common import ESClient


class CompanyLocation(object):

    def __init__(self, config=None):
        self.config = config
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.collections = config.get('collections')
        self.companyLocationDic = {}

    def run(self, from_time):
        self.companyLocationDic = self.esClient.getCompanyLocationInfo()
        self.getReindexMix()

    def update_city_info(self, company, index, query_es, es_authorization):
        time.sleep(1)
        loc = self.companyLocationDic.get(company)
        update_json = '''
        {
            "script": {
                "source": "ctx._source.company_location=\\"%s\\";ctx._source.innovation_center=\\"%s\\""                    
            },
            "query": {
                "term": {
                    "tag_user_company.keyword": "%s"
                }
            }
        } ''' % (loc.get('company_location'), loc.get('innovation_center'), company)
        self.esClient.updateByQuery(update_json.encode('utf-8'), index, query_es, es_authorization)
        print('update city info of %s over' % company)

    def update_loc_info(self, company, index, query_es, es_authorization):
        time.sleep(1)
        loc = self.companyLocationDic.get(company).get('location')
        if loc is None:
            return
        update_json = '''
        {
            "script": {
                "source": "ctx._source.location=params",
                "params": {
                    "lon": %f,
                    "lat": %f
                }             
            },
            "query": {
                "term": {
                    "tag_user_company.keyword": "%s"
                }
            }
        } ''' % (loc.get('lon'), loc.get('lat'), company)
        self.esClient.updateByQuery(update_json.encode('utf-8'), index, query_es, es_authorization)
        print('update loc info of %s over' % company)

    def reindex_sig_repo_info(self, query_es, es_authorization, query_index_name, index_name):
        companys = self.companyLocationDic.keys()
        for company in companys:
            reindex_json = '''{
                "source": {
                    "index": "%s",
                    "query": {
                        "term": {
                            "tag_user_company.keyword": "%s"
                        }
                    }
                },
                "dest": {
                    "index": "%s"
                }
            }''' % (query_index_name, company, index_name)
            data_num = self.esClient.reindex(reindex_json.encode('utf-8'), query_es, es_authorization)
            if data_num == 0:
                continue
            print('reindex: %s -> %d over' % (company, data_num))
            self.update_city_info(company, index_name, query_es, es_authorization)
            self.update_loc_info(company, index_name, query_es, es_authorization)

    def getReindexMix(self):
        j = json.loads(self.collections)
        for coll in j['collections']:
            query_es, es_authorization = None, None
            if 'query_es' in coll:
                query_es = coll['query_es']
            if 'es_authorization' in coll:
                es_authorization = coll['es_authorization']

            query_index_name = coll['query_index_name']
            index_name = coll['index_name']
            print('start to collect %s ...' % index_name)
            self.reindex_sig_repo_info(query_es, es_authorization, query_index_name, index_name)
            print('collect over: %s' % index_name)

