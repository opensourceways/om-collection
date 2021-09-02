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


import json
import time
from collections import defaultdict

from dateutil import parser
import types
import re
import dateutil.parser
import dateutil.rrule
import dateutil.tz
import threading
import traceback
from json import JSONDecodeError
from urllib.parse import quote
from datetime import timedelta, datetime
import urllib3
from collect.gitee import GiteeClient

urllib3.disable_warnings()
import requests
import os
import yaml


class ESClient(object):

    def __init__(self, config=None):
        self.url = config.get('es_url')
        self.from_date = config.get('from_date')
        self.index_name = config.get('index_name')
        self.sig_index = config.get('sig_index')
        self.query_index_name = config.get('query_index_name')
        self.authorization = config.get('authorization')
        self.default_headers = {
            'Content-Type': 'application/json'
        }
        self.internalUsers = []
        self.internalUsers = self.getItselfUsers()
        self.internal_users = config.get('internal_users', 'users')
        self.enterpriseUsers = []
        self.internal_company_name = config.get('internal_company_name', 'internal_company')
        self.is_gitee_enterprise = config.get('is_gitee_enterprise')
        self.enterpriseUsers = []
        self.gitee_token = config.get('gitee_token')
        self.is_update_tag_company = config.get('is_update_tag_company', 'false')
        self.is_update_tag_company_cla = config.get('is_update_tag_company_cla', 'false')
        self.data_yaml_url = config.get('data_yaml_url')
        self.data_yaml_path = config.get('data_yaml_path')
        self.company_yaml_url = config.get('company_yaml_url')
        self.company_yaml_path = config.get('company_yaml_path')
        self.index_name_cla = config.get('index_name_cla')
        self.index_name_org = config.get('index_name_org')
        self.giteeid_company_dict = {}
        self.giteeid_company_change_dict = defaultdict(dict)
        if self.authorization:
            self.default_headers['Authorization'] = self.authorization

    def getRepoSigs(self):
        dict_comb = defaultdict(dict)
        if self.sig_index:
            search = '''{
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "is_sig_original": "1"
          }
        }
      ]
    }
  }
}'''
            res = requests.get(self.getSearchUrl(index_name=self.sig_index), data=search,
                               headers=self.default_headers, verify=False)
            if res.status_code != 200:
                print("The index not exist")
                return dict_comb
            data = res.json()
            for d in data['hits']['hits']:
                source = d['_source']
                sig = source['sig_name']
                repos = source['repos']
                dt = defaultdict(dict)
                for repo in repos:
                    dt.update({repo: [sig]})
                combined_keys = dict_comb.keys() | dt.keys()
                dict_comb = {key: dict_comb.get(key, []) + dt.get(key, []) for key in combined_keys}
        return dict_comb

    def getAllGiteeRepo(self):
        search_json = '''{
  "size": 0,
  "aggs": {
    "repos": {
      "terms": {
        "field": "gitee_repo.keyword",
        "size": 20000,
        "order": {
          "_key": "desc"
        },
        "min_doc_count": 0
      }
    }
  }
}'''
        res = requests.get(self.getSearchUrl(index_name=self.index_name), data=search_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("The index not exist")
            return {}
        return res.json()

    def getOrgByEmail(self):
        if self.index_name_org is None:
            return

        email_org_dict = {}
        search_json = '''{
                          "size": 10000,
                          "_source": {
                            "includes": [
                              "email",
                              "organization"
                            ]
                          },
                          "query": {
                            "bool": {
                              "must": [
                                {
                                  "term": {
                                    "is_cla": "1"
                                  }
                                }
                              ]
                            }
                          }
                        }'''
        res = requests.get(self.getSearchUrl(index_name=self.index_name_org), data=search_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("The index not exist")
            return {}
        data = res.json()
        for hits in data['hits']['hits']:
            source_data = hits['_source']
            email_org_dict.update({source_data['email']: source_data['organization']})
        return email_org_dict

    def getOrgByGiteeID(self):
        dic = {}
        giteeid_orgs_dict = defaultdict(dict)

        if self.index_name_org is None:
            return dic, giteeid_orgs_dict

        search_json = '''{
                          "size": 10000,
                          "_source": {
                            "includes": [
                              "gitee_id",
                              "organization",
                              "created_at"
                            ]
                          },
                          "query": {
                            "bool": {
                              "must": [
                                {
                                  "term": {
                                    "is_cla": "1"
                                  }
                                }
                              ]
                            }
                          }
                        }'''
        res = requests.get(self.getSearchUrl(index_name=self.index_name_org), data=search_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("The index not exist")
            return {}
        data = res.json()
        for hits in data['hits']['hits']:
            source_data = hits['_source']
            gitee_id = source_data['gitee_id']
            if gitee_id is None:
                continue
            value = {source_data['created_at']: source_data['organization']}
            giteeid_orgs_dict[gitee_id].update(value)

        dic_change = giteeid_orgs_dict.copy()
        for key, vMap in giteeid_orgs_dict.items():
            if len(vMap) > 1:
                last_time = max(vMap.keys())
                dic.update({key: vMap[last_time]})
            else:
                dic.update({key: list(vMap.values())[0]})
                dic_change.pop(key)

        return dic, dic_change

    def getuserInfoFromCla(self):
        if self.is_update_tag_company_cla != 'true' or self.index_name_cla is None:
            return {}

        giteeid_company_dict = {}
        search_json = '''{
                          "size": 10000,
                          "_source": {
                            "includes": [
                              "employee_id",
                              "corporation",
                              "is_admin_added"
                            ]
                          },
                          "query": {
                            "bool": {
                              "must": [
                                {
                                  "term": {
                                    "is_corporation_signing": "1"
                                  }
                                }
                              ]
                            }
                          }
                        }'''
        res = requests.get(self.getSearchUrl(index_name=self.index_name_cla), data=search_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("The index not exist")
            return {}
        data = res.json()
        for hits in data['hits']['hits']:
            source_data = hits['_source']
            corporation_admin_added = source_data['corporation'] + "_adminAdded_" + str(source_data['is_admin_added'])
            giteeid_company_dict.update({source_data['employee_id']: corporation_admin_added})

        return giteeid_company_dict

    def getUserInfoFromFile(self):
        if self.is_update_tag_company != 'true':
            return {}

        domain_company_dict = {}
        giteeid_company_dict = {}
        if self.data_yaml_url and self.company_yaml_url:
            cmd = 'wget -N %s' % self.data_yaml_url
            p = os.popen(cmd.replace('=', ''))
            p.read()
            datas = yaml.load_all(open(self.data_yaml_path, encoding='UTF-8')).__next__()

            cmd = 'wget -N %s' % self.company_yaml_url
            p = os.popen(cmd.replace('=', ''))
            p.read()
            companies = yaml.load_all(open(self.company_yaml_path, encoding='UTF-8')).__next__()
            p.close()

            for company in companies['companies']:
                for domain in company['domains']:
                    domain_company_dict.update({domain: company['company_name']})

            for data in datas['users']:
                if data['companies'][0]['company_name'] != '':
                    giteeid_company_dict.update({data['gitee_id']: data['companies'][0]['company_name']})
                elif data['emails']:
                    for email in data['emails']:
                        domain = str(email).split('@')[1]
                        if domain in domain_company_dict:
                            giteeid_company_dict.update({data['gitee_id']: domain_company_dict.get(domain)})
                else:
                    giteeid_company_dict.update({data['gitee_id']: "independent"})

        return giteeid_company_dict

    def getUserInfo(self, login):
        userExtra = {}
        if self.is_gitee_enterprise == 'true':
            if login in self.enterpriseUsers:
                userExtra["tag_user_company"] = self.internal_company_name
                userExtra["is_project_internal_user"] = 1
            else:
                userExtra["tag_user_company"] = "independent"
                userExtra["is_project_internal_user"] = 0
        else:
            if login in self.internalUsers:
                userExtra["tag_user_company"] = self.internal_company_name
                userExtra["is_project_internal_user"] = 1
            else:
                userExtra["tag_user_company"] = "independent"
                userExtra["is_project_internal_user"] = 0
        if self.is_update_tag_company == 'true' and login in self.giteeid_company_dict:
            if self.giteeid_company_dict.get(login) is None:
                userExtra["tag_user_company"] = 'independent'
            else:
                sp = self.giteeid_company_dict.get(login).split("_adminAdded_", 1)
                company = sp[0]
                userExtra["tag_user_company"] = company
                if len(sp) == 2:
                    userExtra["is_admin_added"] = sp[1]
                else:
                    userExtra["is_admin_added"] = 0

            if userExtra["tag_user_company"] == self.internal_company_name:
                userExtra["is_project_internal_user"] = 1
            else:
                userExtra["is_project_internal_user"] = 0

        return userExtra

    def getItselfUsers(self, filename="users"):
        try:
            f = open(filename, 'r', encoding="utf-8")
        except:
            return []

        users = []
        for line in f.readlines():
            if line != "\n":
                users.append(line.split('\n')[0])
        print(users)
        print(len(users))
        return users

    def getEnterpriseUser(self):
        if self.is_gitee_enterprise != "true":
            return

        self.orgs = self.getOrgs(config.get('orgs'))
        client = GiteeClient(self.orgs[0], "", self.gitee_token)

        data = self.getGenerator(client.enterprise_members())
        for d in data:
            user = d.get("user").get("login")
            print(user)
            self.enterpriseUsers.append(user)

    def getOrgs(self, orgsStr):
        orgs = []
        if orgsStr:
            orgs = orgsStr.split(",")
            print(orgs)
        else:
            print("The 'orgs' field must be set")
        return orgs

    def getGenerator(self, response):
        data = []
        try:
            while 1:
                if isinstance(response, types.GeneratorType):
                    res_data = next(response)
                    if isinstance(res_data, str):
                        data += json.loads(res_data.encode('utf-8'))
                    else:
                        data += json.loads(res_data.decode('utf-8'))
                else:
                    data = json.loads(response)
                    break
        except StopIteration:
            return data
        except JSONDecodeError:
            print("Gitee get JSONDecodeError, error: ", response)

        return data

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

        total_num = bulk_json.count("\n")
        if total_num > 10000:
            sub_bulk_json = bulk_json.split("\n")
            bulk_json_temp = ''
            for data in sub_bulk_json:
                bulk_json_temp += data + '\n'
                if bulk_json_temp.count('\n') >= 10000:
                    try:
                        res = requests.post(_url + "/_bulk", data=bulk_json_temp,
                                            headers=_header, verify=False)
                        print(res)
                        bulk_json_temp = ""
                        res.raise_for_status()
                    except UnicodeEncodeError:
                        # Related to body.encode('iso-8859-1'). mbox data
                        logger.warning("Encondig error ... converting bulk to iso-8859-1")
                        bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
                        res = requests.put(url, data=bulk_json_temp, headers=headers)
                        res.raise_for_status()
            if bulk_json_temp is not None and bulk_json_temp != '\n' and len(bulk_json_temp) > 0:
                try:
                    res = requests.post(_url + "/_bulk", data=bulk_json_temp,
                                        headers=_header, verify=False)
                    print(res)
                    bulk_json_temp = ""
                    res.raise_for_status()
                except UnicodeEncodeError:
                    # Related to body.encode('iso-8859-1'). mbox data
                    logger.warning("Encondig error ... converting bulk to iso-8859-1")
                    bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
                    res = requests.put(url, data=bulk_json_temp, headers=headers)
                    res.raise_for_status()
        else:
            try:
                res = requests.post(_url + "/_bulk", data=bulk_json,
                                    headers=_header, verify=False)
                res.raise_for_status()
                print(res)
            except UnicodeEncodeError:
                # Related to body.encode('iso-8859-1'). mbox data
                logger.warning("Encondig error ... converting bulk to iso-8859-1")
                bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
                res = requests.put(url, data=bulk_json, headers=headers)
                res.raise_for_status()

    def searchEsList(self, index_name, search=None):
        url = self.url + '/' + index_name + '/search'
        data = '''{"size":10000,"query": {"bool": {%s}}}''' % search
        try:
            res = json.loads(
                requests.get(url=url, headers=self.default_headers, verify=False, data=data.encode('utf-8')).content)
        except:
            print(traceback.format_exc())
        return res['hits']['hits']

    def getRepoMaintainer(self, index_name, repo=None):
        result = {}
        if not index_name:
            return result
        repo = str(repo).replace('/', '\\\/')
        url = self.url + '/' + index_name + '/_search'
        data = '''{
    "size": 0,
    "query": {
        "bool": {
            "filter": [
                
                {
                    "query_string": {
                        "analyze_wildcard": true,
                        "query": "repo_name.keyword:%s"
                    }
                }
            ]
        }
    },
    "aggs": {
        "2": {
            "terms": {
                "field": "committer.keyword",
                "size": 10000,
                "order": {
                    "_key": "desc"
                },
                "min_doc_count": 1
            },
            "aggs": {}
        }
    }
}''' % repo
        try:
            res = json.loads(
                requests.get(url=url, headers=self.default_headers, verify=False, data=data.encode('utf-8')).content)
        except:
            print(traceback.format_exc())
        maintainerdata = res['aggregations']['2']['buckets']
        if maintainerdata:
            mtstr = ""
            for m in maintainerdata:
                mtstr = mtstr + str(m['key']) + ","
            mtstr = mtstr[:len(mtstr) - 1]
            result['Maintainer'] = mtstr
        return result

    def getRepoSigCount(self, index_name, repo=None):
        result = {}
        if not index_name:
            return result
        repo = str(repo).replace('/', '\\\/')
        querystr = '''{
    "size": 0,
    "query": {
        "bool": {
            "filter": [
                {
                    "query_string": {
                        "analyze_wildcard": true,
                        "query": "repo_name.keyword:%s"
                    }
                }
            ]
        }
    },
    "aggs": {
        "2": {
            "terms": {
                "field": "repo_name.keyword",
                "size": 10000,
                "order": {
                    "_key": "desc"
                },
                "min_doc_count": 1
            },
            "aggs": {
                "1": {
                    "cardinality": {
                        "field": "sig_name.keyword"
                    }
                }
            }
        }
    }
}''' % (repo)
        url = self.url + '/' + index_name + '/_search'
        try:
            res = json.loads(
                requests.get(url=url, headers=self.default_headers, verify=False,
                             data=querystr.encode('utf-8')).content)
            resultdata = res['aggregations']['2']['buckets']
            count = 0
            for re in resultdata:
                count += int(re['1']['value'])
            result['sigcount'] = count
            return result
        except:
            print(traceback.format_exc())
            result['sigcount'] = 0
            return result

    def getRepoSigNames(self, index_name, repo=None):
        if not index_name:
            return ""
        querystr = '''{
    "query": {
        "term":{
        "repo_name.keyword": "%s"
        }
    }
}''' % (repo)
        url = self.url + '/' + index_name + '/_search'
        try:
            res = json.loads(
                requests.get(url=url, headers=self.default_headers, verify=False,
                             data=querystr.encode('utf-8')).content)
            resultdata = res['hits']['hits']
            restr = ''
            for re in resultdata:
                restr = restr + "," + re['_source']['sig_name']
            tuplelist = restr[1:].split(",")
            tuplelist = list(set(tuplelist))
            resultstr = ""
            for s in tuplelist:
                resultstr = resultstr + ',' + s
            if resultstr != "":
                return resultstr[1:]
            return ""
        except:
            print(traceback.format_exc())
            return ""

    def geTimeofVersion(self, version, repo, index):
        url = self.url + '/' + index + '/_doc/' + repo + version
        try:
            res = requests.get(url=url, headers=self.default_headers, verify=False)
            if res.status_code == 404:
                return None
            else:
                content = json.loads(res.content)
                return content['_source']['version_time']
        except:
            print(traceback.format_exc())
            return None

    def post_delete_index_name(self, index_name=None, header=None, url=None):
        if not index_name:
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
            res = requests.delete(_url + "/" + index_name, headers=_header, verify=False)
            res.raise_for_status()
            print(res)
        except:
            print(traceback.format_exc())

    def post_delete_delete_by_query(self, bulk_json, index_name, header=None, url=None):
        if not bulk_json:
            return

        if not index_name:
            return

        _header = {
            "Content-Type": 'application/json',
            'Authorization': self.authorization
        }
        if header:
            _header = header

        _url = self.url
        if url:
            _url = url

        try:
            res = requests.post(_url + "/" + index_name + '/_delete_by_query', headers=_header, verify=False,
                                data=bulk_json)
            res.raise_for_status()
            print(res)
        except:
            print(traceback.format_exc())

    def getStartTime(self):
        # 2020-04-29T15:59:59.000Z
        last_time = self.getLastTime()
        if last_time is None:
            return self.from_date
        # 20200429
        last_time = last_time.split("T")[0].replace("-", "")

        f = datetime.strptime(last_time, "%Y%m%d") + timedelta(days=-1)
        # 20200428
        starTime = f.strftime("%Y%m%d")
        return starTime

    def searchEmailGitee(self, url, headers, index_name, search=None):
        url = url + '/' + index_name + '/_search'
        data = '''{"size":10000,"query": {"bool": {%s}}}''' % search
        try:
            res = json.loads(
                requests.get(url=url, headers=headers, verify=False, data=data.encode('utf-8')).content)
            return res['hits']['hits']
        except:
            print(traceback.format_exc())
            return None

    def searchEsList(self, index_name, search=None):
        url = self.url + '/' + index_name + '/_search'
        data = '''{"size":10000,"query": {"bool": {%s}}}''' % search
        try:
            res = json.loads(
                requests.get(url=url, headers=self.default_headers, verify=False, data=data.encode('utf-8')).content)
            return res['hits']['hits']
        except:
            print(traceback.format_exc())
            return None

    def getLastFormatTime(self):
        # 2020-04-29T15:59:59.000Z
        last_time = self.getLastTime()
        if last_time is None:
            return self.from_date
        # 20200429
        last_time = last_time.split("T")[0].replace("-", "")

        f = datetime.strptime(last_time, "%Y%m%d")
        lastTime = f.strftime("%Y%m%d")
        return lastTime

    def getLastTime(self, field="created_at"):
        data_agg = '''
                "aggs": {
                    "1": {
                      "max": {
                        "field": "%s"
                      }
                    }
                }
            ''' % field

        data_json = '''
            { "size": 0, %s
            } ''' % data_agg
        res = requests.get(self.getSearchUrl(), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("The field (%s) not exist." % field)
            return None
        data = res.json()
        # print(data)
        result_num = data['hits']['total']['value']
        if result_num == 0:
            return
        # get min create at value
        created_at_value = data['aggregations']['1']['value_as_string']
        return created_at_value

    def checkFieldExist(self, field="_id", filter=None):
        query_data = '''
                "query": {
                    "terms": {
                      "%s": ["%s"]
                    }
                }
            ''' % (field, filter)

        data_json = '''
            { "size": 0, %s
            } ''' % query_data

        self.default_headers = {
            'Authorization': self.authorization,
            'Content-Type': 'application/json'
        }
        res = requests.get(self.getSearchUrl(), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("The resource not exist")
            return False
        data = res.json()

        result_num = data['hits']['total']['value']
        if result_num == 0:
            return False
        # get min create at value
        # created_at_value = data['aggregations']['1']['value_as_string']
        return True

    def get_last_item_field(self, field, filters_=[], offset=False):
        """Find the offset/date of the last item stored in the index.
        """
        last_value = None

        if filters_ is None:
            filters_ = []

        terms = []
        for filter_ in filters_:
            if not filter_:
                continue
            term = '''{"term" : { "%s" : "%s"}}''' % (
                filter_['name'], filter_['value'])
            terms.append(term)

        data_query = '''"query": {"bool": {"filter": [%s]}},''' % (
            ','.join(terms))

        data_agg = '''
            "aggs": {
                "1": {
                  "max": {
                    "field": "%s"
                  }
                }
            }
        ''' % field

        data_json = '''
        { "size": 0, %s  %s
        } ''' % (data_query, data_agg)

        print(data_json)

        self.default_headers = {
            'Authorization': self.authorization,
            'Content-Type': 'application/json'
        }
        data = requests.get(self.getSearchUrl(),
                            data=data_json,
                            headers=self.default_headers, verify=False)
        res = data.json()
        print(res)

        if "value_as_string" in res["aggregations"]["1"]:
            last_value = res["aggregations"]["1"]["value_as_string"]
            last_value = str_to_datetime(last_value)
        else:
            last_value = res["aggregations"]["1"]["value"]
            if last_value:
                try:
                    last_value = unixtime_to_datetime(last_value)
                except InvalidDateError:
                    last_value = unixtime_to_datetime(last_value / 1000)

        return last_value

    def getSearchUrl(self, url=None, index_name=None):
        if index_name is None:
            index_name = self.index_name

        if url is None:
            url = self.url
        return url + "/" + index_name + "/_search"

    def get_last_date(self, field, filters_=[]):
        """Find the date of the last item stored in the index
        """
        last_date = self.get_last_item_field(field, filters_=filters_)

        return last_date

    def get_incremental_date(self):
        """Field with the date used for incremental analysis."""

        return "updated_at"

    def get_last_update_from_es(self, _filters=[]):
        last_update = self.get_last_date(self.get_incremental_date(), _filters)

        return last_update

    def get_from_date(self, filters=[]):
        last_update = self.get_last_update_from_es(filters)
        last_update = last_update

        # if last_update is None:
        #     last_update = str_to_datetime("2020-04-26T14:26+08:00")
        #
        # print("last update time:", last_update)
        # print("last update time type:", type(last_update))
        return last_update

    def getTotalAuthorName(self, field="user_login.keyword", size=1500):
        data_json = '''{"size": 0,
                     "aggs": {
                         "uniq_gender": {
                             "terms": {
                                 "field": "%s",
                                 "size": %d
                             }
                         }
                     }
                     }''' % (field, size)
        print(data_json)
        res = requests.get(self.getSearchUrl(), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("The author name not exist")
            return []
        data = res.json()
        return data['aggregations']['uniq_gender']['buckets']

    def setIsFirstCountributeItem(self, user_login):
        # get min created at value by author name
        # data_query = '''"query": {
        #     "bool": {
        #         "must": [
        #             {"match": {"user_login": "%s"}}
        #         ]
        #     }
        # },''' % (
        #     user_login)

        data_query = '''"query": {
            "bool": {
                "must": [
                    {
                        "match": {"user_login.keyword": "%s"}
                    },
                    {
                        "bool": {
                            "should": [
                                {"match": {"is_gitee_pull_request": 1}},
                                {"match": {"is_gitee_issue": 1}},
                                {"match": {"is_gitee_issue_comment": 1}},
                                {"match": {"is_gitee_review_comment": 1}},
                                {"match": {"is_gitee_comment": 1}},
                                {"match": {"is_gitee_fork": 1}}
                            ]
                        }
                        }
                    ]
                }
            },''' % (user_login)
        data_agg = '''
                "aggs": {
                    "1": {
                      "min": {
                        "field": "created_at"
                      }
                    }
                }
            '''

        data_json = '''
            { "size": 2, %s  %s
            } ''' % (data_query, data_agg)
        res = requests.get(self.getSearchUrl(), data=data_json,
                           headers=self.default_headers, verify=False)
        res = res.json()
        result_num = res['hits']['total']['value']
        if result_num == 0:
            print("author name(%s) not exist." % (user_login))
            return
        # get min create at value
        created_at_value = res['aggregations']['1']['value_as_string']

        # get author name item id with min created_at
        data_json = '''{"size": 1, "query": {"bool": {"must": [{"match": {"user_login.keyword": "%s"}}, {"match": {"created_at": "%s"}}]}}}''' % (
            user_login, created_at_value)
        res = requests.get(self.getSearchUrl(), data=data_json,
                           headers=self.default_headers, verify=False)

        if res.status_code != 200:
            print("get user(%s) id  fail" % user_login)
            return
        res = res.json()
        id = res['hits']['hits'][0]['_id']

        if "_star" in id or "_watch" in id:
            return

        update_data = '''{
            "doc": {
                "is_first_contribute": 1
            }
        }'''
        url = self.url + "/" + self.index_name + '/_update/' + quote(id, safe='')
        res = requests.post(url, data=update_data,
                            headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("update user name fail:", res.text)
            return
        print("update user(%s) id(%s) is_first_contribute  success" % (user_login, id))

    def updateRepoCreatedName(self, org_name, repo_name, author_name, user_login, is_internal=False):
        data_query = '''"query": {"bool": {"must": [{"match": {"is_gitee_repo": 1}},{"match": {"repository.keyword": "%s"}}]}}''' % (
                org_name + "/" + repo_name)

        data_json = '''
        { "size": 1, %s
        } ''' % (data_query)

        res = requests.get(self.getSearchUrl(), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            return
        res = res.json()
        result_num = res['hits']['total']['value']
        if result_num != 1:
            print("repo(%s) not exist or has more than one resources." % (
                    org_name + "/" + repo_name))
            return

        id = res['hits']['hits'][0]['_id']
        if not id:
            print("ID not exist")
            return

        original_author_name = res['hits']['hits'][0]['_source'][
            'author_name']
        if original_author_name == author_name:
            print("the repo(%s) author name(%s) is the same as community record" %
                  (org_name + "/" + repo_name, author_name))
            return

        print("Change repo(%s) author name from (%s) to (%s)" %
              (org_name + "/" + repo_name, original_author_name, author_name))
        update_data = {
            "doc": {
                "author_name": author_name,
                "user_login": user_login
            }
        }
        if is_internal == True:
            update_data["doc"]["is_project_internal_user"] = 1
            update_data["doc"]["tag_user_company"] = self.internal_company_name
        else:
            update_data["doc"]["is_project_internal_user"] = None
            update_data["doc"]["tag_user_company"] = None
        self.update(id, update_data)

    def update(self, id, update_data):
        url = self.url + "/" + self.index_name + '/_update/' + quote(id,
                                                                     safe='')
        res = requests.post(url, data=json.dumps(update_data),
                            headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("update repo author name failed:", res.text)
            return

    def reindex(self, reindex_json):
        url = self.url + '/' + '_reindex'
        res = requests.post(url, headers=self.default_headers, verify=False, data=reindex_json)
        if res.status_code != 200:
            return 0
        data = res.json()
        data_total = data['total']
        return data_total

    def get_sig_maintainers(self, index_name):
        search_json = '''{
                      "size": 0,
                      "aggs": {
                        "maintainers": {
                          "terms": {
                            "field": "maintainers.keyword",
                            "size": 10000
                          }
                        }
                      }
                    }'''
        maintainers = []
        url = self.url + "/" + index_name + '/_search'
        res = requests.post(url, headers=self.default_headers, verify=False, data=search_json)
        if res.status_code != 200:
            return maintainers
        data = res.json()
        for bucket in data["aggregations"]['maintainers']['buckets']:
            maintainers.append(bucket['key'])
        return maintainers

    def updateByQuery(self, query):
        url = self.url + '/' + self.index_name + '/_update_by_query?conflicts=proceed'
        requests.post(url, headers=self.default_headers, verify=False, data=query)

    def getUniqueCountByDate(self, field, from_date, to_date,
                             url=None, index_name=None):
        data_json = '''{"size": 0,
         "query": {
             "range": {
                 "created_at": {
                     "gte": "%s",
                     "lte": "%s"
                 }
             }
         },
         "aggs": {
             "sum": {
                 "cardinality": {
                     "field": "%s"
                 }
             }
         }
         }''' % (from_date, to_date, field)

        res = requests.get(self.getSearchUrl(url, index_name), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("The field (%s) not exist from time(%s) to (%s)"
                  % (field, from_date, to_date))
            return None

        data = res.json()
        # print(data["aggregations"]["sum"]["value"])
        return data["aggregations"]["sum"]["value"]

    def giteeEventMaxId(self, repo_full_name):
        data_json = '''{
              "size": 0,
              "query": {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "repo.full_name.keyword": "%s"
                      }
                    }
                  ]
                }
              },
              "aggs": {
                "max_id": {
                  "max": {
                    "field": "id"
                  }
                }
              }
            }''' % repo_full_name
        self.setFirstItem()
        res = requests.get(self.getSearchUrl(index_name=self.index_name), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            return 0
        max_id = res.json()['aggregations']['max_id']['value']
        if max_id is None or max_id == 'null':
            return 0
        return max_id

    def getCountByTermDate(self, term=None, field=None, from_date=None, to_date=None,
                           url=None, index_name=None, query=None, query_index_name=None):
        if query:
            if field:
                agg_json = '''"aggs": {"2": {"terms": {"field": "%s","size": 20000,"min_doc_count": 1}}}''' % field
            else:
                agg_json = '''"aggs": {"2": {"date_histogram": {"interval": "10000d","field": "created_at","min_doc_count": 0}}}'''

            data_json = '''{
                "size": 0,
                "query": {
                    "bool":{
                        "filter":[
                            {"range": {
                                "created_at": {
                                    "gte": "%s",
                                    "lte": "%s"
                                }
                            }},
                            {"query_string":{
                                "analyze_wildcard":true,
                                "query": "%s"
                            }}]
                    }
                },
                %s
            }''' % (from_date, to_date, query, agg_json)
        elif term:
            data_json = '''{
                "size": 3,
                "query": {
                    "range": {
                        "created_at": {
                            "gte": "%s",
                            "lte": "%s"
                        }
                    }
                },
                "aggs": {
                    "list": {
                        "terms": {
                            "field": "%s"
                        },
                        "aggs": {
                            "sum": {
                                "sum": {
                                    "field": "%s"
                                }
                            }
                        }
                    }
                }
            }''' % (from_date, to_date, term, field)
        else:
            data_json = '''{
                "size": 0,
                "query": {
                    "range": {
                        "created_at": {
                            "gte": "%s",
                            "lte": "%s"
                        }
                    }
                },
                "aggs": {
                    "sum": {
                        "sum": {
                            "field": "%s"
                        }
                    }
                }
            }''' % (from_date, to_date, field)

        if query_index_name is None:
            query_index_name = index_name
        res = requests.get(self.getSearchUrl(url, query_index_name), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("The field (%s) not exist from time(%s) to (%s), err=%s"
                  % (field, from_date, to_date, res))
            return None

        data = res.json()
        print(data)
        count = 0
        if query:
            if not field:
                for bucket in data["aggregations"]["2"]["buckets"]:
                    count += bucket["doc_count"]
            else:
                count = len(data["aggregations"]["2"]["buckets"])

        elif term is None:
            count = data["aggregations"]["sum"]["value"]
        else:
            for b in data["aggregations"]["list"]["buckets"]:
                count += b["sum"]["value"]
        # count = data["aggregations"]["sum"]["value"]
        # print(count)
        return count

    def getCountByDateRange(self, matchs, from_date, to_date, interval=1):
        terms = []
        for match in matchs:
            if not match:
                continue
            term = '''{"match" : { "%s" : %d}}''' % (
                match['name'], match['value'])
            terms.append(term)

        data_query = '''"query": {"bool": {"must": [%s]}},''' % (
            ','.join(terms))

        # "aggs":{
        #     "range": {
        #         "date_range": {
        #             "field": "created_at",
        #             "ranges": [
        #                 {"from": "2019-01-07T00:00:00+08:00",
        #                  "to": "2020-06-09T23:00:00+08:00"},
        #                 {"from": "2019-01-07T00:00:00+08:00",
        #                  "to": "2020-06-08T00:00:00+08:00"},
        #                 {"from": "2019-01-07T00:00:00+08:00",
        #                  "to": "2020-06-07T00:00:00+08:00"}
        #             ]
        #         }
        #     }
        # }
        ranges = []
        tmp_date = from_date
        while 1:
            if tmp_date > to_date:
                break

            term = '''{"from" : "%s", "to": "%s"}''' % (
                from_date.strftime("%Y-%m-%dT00:00:00+08:00"), tmp_date.strftime("%Y-%m-%dT23:59:59+08:00"))
            ranges.append(term)
            tmp_date = tmp_date + timedelta(days=interval)

        data_agg = '''
                "aggs": {
                    "range": {
                      "date_range": {
                        "field": "created_at",
                        "ranges": [%s]
                      }
                    }
                }
            ''' % ','.join(ranges)

        data_json = '''
            { "size": 0, %s  %s
            } ''' % (data_query, data_agg)

        print(data_json)
        res = requests.get(self.getSearchUrl(), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("Get result error:", res.status_code)
            return None

        data = res.json()
        data = data.get("aggregations").get("range").get("buckets")
        return data

    def initLocationGeoIPIndex(self):
        body = {
            "description": "Add geoip info",
            "processors": [
                {
                    "geoip": {
                        "field": "ip"
                    }
                }
            ]
        }

        url = self.url + "/_ingest/pipeline/geoip"
        response = requests.request("PUT", url, headers=self.default_headers, data=json.dumps(body), verify=False)
        if response.status_code != 200:
            return None

    def getLocationByIP(self, ip):
        # initLocationGeoIPIndex()
        payload = "{\n\t\"ip\": \"%s\"\n}" % ip
        r = requests.put(self.url + '/my_index/_doc/my_id?pipeline=geoip',
                         data=payload, headers=self.default_headers,
                         verify=False)
        if r.status_code != 200:
            print("get location failed, err=", r.text)
            return {}

        res = requests.get(self.url + '/my_index/_doc/my_id',
                           headers=self.default_headers, verify=False)
        if r.status_code != 200:
            print("get location failed, err=", r.text)
            return {}
        '''
        "geoip": {
                "continent_name": "Asia",
                "region_iso_code": "CN-ZJ",
                "city_name": "Hangzhou",
                "country_iso_code": "CN",
                "region_name": "Zhejiang",
                "location": {
                    "lon": 120.1614,
                    "lat": 30.2936
                }
            },
            "ip": "122.235.249.147"
        '''
        j = res.json()
        data = j['_source'].get('geoip')

        if data is None:
            return {}
        return data

    def getItemsByMatchs(self, matchs, size=500, aggs=None):
        '''
        {
            "size": 497,
            "query": {
                "bool": {
                    "must": [
                        {"match":
                            {
                                "is_gitee_fork": 1
                            }
                        },
                        {"match":
                            {
                                "gitee_repo.keyword": "https://gitee.com/mindspore/mindspore"
                            }
                        }
                    ]
                }
            }
        }
        '''
        if matchs is None:
            matchs = []

        terms = []
        for match in matchs:
            if not match:
                continue
            term = '''{"match" : { "%s" : "%s"}}''' % (
                match['name'], match['value'])
            terms.append(term)

        data_query = '''"query": {"bool": {"must": [%s]}}''' % (
            ','.join(terms))

        if aggs:
            data_json = '''
            { "size": %d, %s, %s
            } ''' % (size, data_query, aggs)
        else:
            data_json = '''
            { "size": %d, %s
            } ''' % (size, data_query)
        data = requests.get(self.getSearchUrl(),
                            data=data_json.encode('utf-8'),
                            headers=self.default_headers, verify=False)
        if data.status_code != 200:
            print("match data failed, err=", data.text)
            return {}
        res = data.json()
        return res

    def updateToRemoved(self, id):
        update_data = '''{
                    "doc": {
                        "is_removed": 1
                    }
                }'''
        url = self.url + "/" + self.index_name + '/_update/' + quote(id, safe='')
        res = requests.post(url, data=update_data,
                            headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("set fork is_removed value to 1 fail:", res.text)
            return
        print("set fork is_removed value to 1 success")

    def splitMixDockerHub(self, from_date, count_key, query=None,
                          query_index_url=None, query_index_name=None):
        fromTime = datetime.strptime(from_date, "%Y%m%d")
        to = datetime.strptime(datetime.today().strftime("%Y%m%d"), "%Y%m%d")

        data_json = '''{
                         "size": 0,
                         "query": {
                           "bool": {
                             "filter": [
                               {
                                 "range": {
                                   "metadata__updated_on": {
                                     "gte": "%s",
                                     "lte": "%s"
                                   }
                                 }
                               },
                               {
                                 "query_string": {
                                   "analyze_wildcard": true,
                                   "query": "%s"
                                 }
                               }
                             ]
                           }
                         },
                         "aggs": {
                           "2": {
                             "date_histogram": {
                               "interval": "1d",
                               "field": "metadata__updated_on",
                               "min_doc_count": 0
                             },
                             "aggs": {
                               "maxCount": {
                                 "max": {
                                   "field": "%s"
                                 }
                               }
                             }
                           }
                         }
                       }''' % (fromTime.strftime("%Y-%m-%dT00:00:00+00:00"),
                               to.strftime("%Y-%m-%dT23:59:59+00:00"), query, count_key)

        res = requests.get(self.getSearchUrl(query_index_url, query_index_name), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            return 0
        data = res.json()
        buckets = data['aggregations']['2']['buckets']
        time_count_dict = {}
        for bucket in buckets:
            timestamp = bucket['key']
            max_count = bucket['maxCount']['value']
            if max_count is None:
                max_count = 0
            time_count_dict.update({timestamp: max_count})

        last_not_0_count = 0
        count_dict = {}
        for key, value in time_count_dict.items():
            dt = time.strftime("%Y%m%d", time.localtime(key / 1000))
            created_at = datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%dT23:59:59+08:00")
            if datetime.strptime(dt, "%Y%m%d") == to:
                created_at = datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%dT00:00:01+08:00")

            before_value = time_count_dict.get(key - 86400000)
            if value == 0 or before_value is None:
                count_dict.update({created_at: value})
            elif before_value != 0:
                count_dict.update({created_at: value - before_value})
                last_not_0_count = value
            else:
                count_dict.update({created_at: value - last_not_0_count})

        return count_dict

    def getTotalCountMix(self, from_date, count_key,
                         field=None, query=None, key_prefix=None):
        starTime = datetime.strptime(from_date, "%Y%m%d")
        fromTime = datetime.strptime(from_date, "%Y%m%d")
        to = datetime.today().strftime("%Y%m%d")

        time_count_dict = {}
        while fromTime.strftime("%Y%m%d") <= to:
            print(fromTime)

            created_at = fromTime.strftime("%Y-%m-%dT23:59:59+08:00")
            if fromTime.strftime("%Y%m%d") == to:
                created_at = fromTime.strftime("%Y-%m-%dT00:00:01+08:00")

            c = self.getCountByTermDate(
                field,
                count_key,
                starTime.strftime("%Y-%m-%dT00:00:00+08:00"),
                fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
                index_name=self.index_name,
                query_index_name=self.query_index_name,
                query=query)
            if c is None:
                c = 0
            time_count_dict.update({created_at: c})
            fromTime = fromTime + timedelta(days=1)
        return time_count_dict

    def writeMixDownload(self, time_count_dict, item):
        actions = ''
        for key, value in time_count_dict.items():
            user = {
                item: value,
                "created_at": key,
                "is_%s" % item: 1
            }
            id = key.split("T")[0] + item
            action = getSingleAction(self.index_name, id, user)
            actions += action
        self.safe_put_bulk(actions)

    def writeFirstDownload(self, ip_time_dict):
        actions = ''
        for key, value in ip_time_dict.items():
            user = {
                "ip": key,
                "created_at": time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time.localtime(value)),
                "is_first_download": 1
            }
            action = getSingleAction(self.index_name, key, user)
            actions += action
        self.safe_put_bulk(actions)

    def setToltalCount(self, from_date, count_key,
                       field=None, query=None, key_prefix=""):
        starTime = datetime.strptime(from_date, "%Y%m%d")
        fromTime = datetime.strptime(from_date, "%Y%m%d")
        to = datetime.today().strftime("%Y%m%d")

        actions = ""
        while fromTime.strftime("%Y%m%d") <= to:
            print(fromTime)

            created_at = fromTime.strftime("%Y-%m-%dT23:59:59+08:00")
            if fromTime.strftime("%Y%m%d") == to:
                created_at = fromTime.strftime("%Y-%m-%dT00:00:01+08:00")

            c = self.getCountByTermDate(
                field,
                count_key,
                starTime.strftime("%Y-%m-%dT00:00:00+08:00"),
                fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
                index_name=self.index_name,
                query_index_name=self.query_index_name,
                query=query)
            # return
            if c is not None:
                user = {
                    "all_" + key_prefix + count_key: c,
                    "updated_at": fromTime.strftime(
                        "%Y-%m-%dT00:00:00+08:00"),
                    "created_at": created_at,
                    # "metadata__updated_on": fromTime.strftime("%Y-%m-%dT23:59:59+08:00"),
                    "is_all" + key_prefix + count_key: 1
                }
                id = fromTime.strftime(
                    "%Y-%m-%dT00:00:00+08:00") + "all_" + key_prefix + count_key
                action = getSingleAction(self.index_name, id, user)
                actions += action
            fromTime = fromTime + timedelta(days=1)

        self.safe_put_bulk(actions)

    def getEsIds(self, index_name):
        search_json = '''{
                            "size": 10000,
                            "_source": false,
                            "query": {
                              "match_all": {}
                            }
                          }'''
        res = requests.post(self.getSearchUrl(index_name=index_name), data=search_json, headers=self.default_headers,
                            verify=False)
        if res.status_code != 200:
            print("The search not exist")
            return []

        ids = []
        data = res.json()
        for hit in data['hits']['hits']:
            ids.append(hit['_id'])

        return ids

    def deleteById(self, id, index_name):
        search_json = '''{
                           "query": {
                             "bool": {
                               "must": [
                                 {
                                   "term": {
                                     "_id": "%s"
                                   }
                                 }
                               ]
                             }
                           }
                         }''' % id
        self.post_delete_delete_by_query(index_name=index_name, bulk_json=search_json.encode("utf-8"))

    def getFirstItemByKey(self, query=None, key=None, query_index_name=None):
        data_json = '''{
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"query_string": {
                            "analyze_wildcard": true,
                            "query": "%s"
                        }}]
                }
            },
            "aggs": {
                "group_by_login": {
                "terms": {
                    "field": "%s",
                    "size": 20000
                },
                "aggs": {
                    "login_start": {"top_hits": {"size": 1, "sort": [{"created_at": {"order": "asc"}}]}}

                }
            }}
        }''' % (query, key)

        if query_index_name is None:
            query_index_name = self.index_name
        res = requests.get(self.getSearchUrl(index_name=query_index_name), data=data_json,
                           headers=self.default_headers, verify=False)
        if res.status_code != 200:
            print("get First Item By Key(%s), err=%s"
                  % (key, res))
            return None

        data = res.json()

        buckets = data["aggregations"]["group_by_login"]["buckets"]
        if len(buckets) == 0:
            return None
        return buckets

    def getFirstItemMix(self, key_prefix=None, query=None, key=None, query_index_name=None):
        if not key:
            return
        buckets = self.getFirstItemByKey(query, key, query_index_name)
        if not buckets:
            return

        ip_first_dict = {}
        for items in buckets:
            item = items["login_start"]["hits"]["hits"]
            if len(item) == 0:
                continue
            else:
                ip = item[0].get("_source").get("ip")
                created_at = parser.parse(item[0].get("_source").get("created_at")).timestamp()
                ip_first_dict.update({ip: created_at})
        return ip_first_dict

    def setFirstItem(self, key_prefix=None, query=None, key=None, query_index_name=None):
        actions = ""
        if not key:
            return
        buckets = self.getFirstItemByKey(query, key, query_index_name)
        if not buckets:
            return
        for items in buckets:
            item = items["login_start"]["hits"]["hits"]
            if len(item) == 0:
                continue
            else:
                user = {
                    "updated_at": item[0].get("_source").get("updated_at"),
                    "created_at": item[0].get("_source").get("created_at"),
                    "is_first" + key_prefix + key: 1
                }
                if key:
                    gitee_id = key.split(".keyword")[0]
                    id = item[0]["_source"].get(gitee_id) + "_is_" + key_prefix
                else:
                    id = item[0].get("_id") + "_is_" + key_prefix
                action = getSingleAction(self.index_name, id, user)
                actions += action

        self.safe_put_bulk(actions)

    def scrollSearch(self, index_name, search=None, scroll_duration='1m', func=None):
        url = self.url + '/' + index_name + '/_search?scroll=' + scroll_duration
        res = requests.get(url=url, headers=self.default_headers, verify=False, data=search.encode('utf-8'))
        if res.status_code != 200:
            print('requests error')
        res_data = res.json()
        data = res_data['hits']['hits']
        print('scroll data count: %s' % len(data))
        func(data)

        scroll_id = res_data['_scroll_id']
        while scroll_id is not None and len(data) != 0:
            url = self.url + '/_search/scroll'
            search = '''{
                          "scroll": "%s",
                          "scroll_id": "%s"
                        }''' % (scroll_duration, scroll_id)
            res = requests.get(url=url, headers=self.default_headers, verify=False, data=search.encode('utf-8'))
            if res.status_code != 200:
                print('requests error')
            res_data = res.json()
            scroll_id = res_data['_scroll_id']
            data = res_data['hits']['hits']
            print('scroll data count: %s' % len(data))
            func(data)
        print('scroll over')

    def esSearch(self, index_name, search=None, method='_search'):
        if search is None:
            return None
        url = self.url + '/' + index_name + '/' + method
        req = requests.post(url=url, headers=self.default_headers, verify=False, data=search.encode('utf-8'))
        if req.status_code != 200:
            print('requests error')
            return None
        return json.loads(req.content)


def get_date(time):
    if time:
        return time.split("+")[0]
    else:
        return None


def get_time_to_first_attention(item):
    """Get the first date at which a comment or reaction was made to the issue by someone
    other than the user who created the issue
    """
    comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']
                     if item['user']['login'] != comment['user']['login']]
    if comment_dates:
        return min(comment_dates)
    return None


def get_first_contribute_at(user, current_date):
    if user in gloabl_item:
        if str_to_datetime(current_date) < str_to_datetime(gloabl_item[user]):
            gloabl_item[user] = current_date
            return current_date
        else:
            return None
    else:
        gloabl_item[user] = current_date
        return current_date


def get_time_diff_days(start, end):
    ''' Number of days between two dates in UTC format  '''

    if start is None or end is None:
        return None

    if type(start) is not datetime:
        start = dateutil.parser.parse(start).replace(tzinfo=None)
    if type(end) is not datetime:
        end = dateutil.parser.parse(end).replace(tzinfo=None)

    seconds_day = float(60 * 60 * 24)
    diff_days = (end - start).total_seconds() / seconds_day
    diff_days = float('%.2f' % diff_days)

    return diff_days


def datetime_utcnow():
    """Handy function which returns the current date and time in UTC."""
    return datetime.now()


def str_to_datetime(ts):
    """Format a string to a datetime object.
    This functions supports several date formats like YYYY-MM-DD,
    MM-DD-YYYY, YY-MM-DD, YYYY-MM-DD HH:mm:SS +HH:MM, among others.
    When the timezone is not provided, UTC+0 will be set as default
    (using `dateutil.tz.tzutc` object).
    :param ts: string to convert
    :returns: a datetime object
    :raises IvalidDateError: when the given string cannot be converted
        on a valid date
    """

    def parse_datetime(ts):
        dt = dateutil.parser.parse(ts)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=dateutil.tz.tzutc())
        return dt

    if not ts:
        raise InvalidDateError(date=str(ts))

    try:
        # Try to remove additional information after
        # timezone section because it cannot be parsed,
        # like in 'Wed, 26 Oct 2005 15:20:32 -0100 (GMT+1)'
        # or in 'Thu, 14 Aug 2008 02:07:59 +0200 CEST'.
        m = re.search(r"^.+?\s+[\+\-\d]\d{4}(\s+.+)$", ts)
        if m:
            ts = ts[:m.start(1)]

        try:
            dt = parse_datetime(ts)
        except ValueError as e:
            # Try to remove the timezone, usually it causes
            # problems.
            m = re.search(r"^(.+?)\s+[\+\-\d]\d{4}.*$", ts)

            if m:
                dt = parse_datetime(m.group(1))
                print("Date %s does not have a valid timezone. "
                      "Date converted removing timezone info" % ts)
                return dt

            raise e

        try:
            # Check that the offset is between -timedelta(hours=24) and
            # timedelta(hours=24). If it is not the case, convert the
            # date to UTC and remove the timezone info.
            _ = dt.astimezone(dateutil.tz.tzutc())
        except ValueError:
            print("Date %s does not have a valid timezone; timedelta not in range. "
                  "Date converted to UTC removing timezone info" % ts)
            dt = dt.replace(tzinfo=dateutil.tz.tzutc()).astimezone(dateutil.tz.tzutc())

        return dt

    except ValueError as e:
        raise InvalidDateError(date=str(ts))


def getSingleAction(index_name, id, body, act="index"):
    action = ""
    indexData = {
        act: {"_index": index_name, "_id": id}}
    # indexData = {
    #     act: {"_index": index_name, "_type": "items", "_id": id}}
    action += json.dumps(indexData) + '\n'
    action += json.dumps(body) + '\n'
    return action


def writeDataThread(thread_func_args, max_thread_num=20):
    threads = []

    for key, values in thread_func_args.items():
        for v in values:
            if not isinstance(v, tuple):
                args = (v,)
            else:
                args = v
            t = threading.Thread(
                target=key,
                args=args)
            threads.append(t)
            t.start()

            if len(threads) % max_thread_num == 0:
                for t in threads:
                    t.join()
                print("finish threads num:", max_thread_num)
                threads = []

    for t in threads:
        t.join()


def getGenerator(response):
    data = []
    try:
        while 1:
            if isinstance(response, types.GeneratorType):
                data += json.loads(next(response).encode('utf-8'))
            else:
                data = json.loads(response)
                break
    except StopIteration:
        # print(response)
        print("...end")
    except JSONDecodeError:
        print("Gitee get JSONDecodeError, error: ", response)

    return data
