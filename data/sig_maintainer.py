import os
from collections import defaultdict
from bs4 import BeautifulSoup
import re
import git
import datetime
import json
import requests
import time
import yaml
from configparser import ConfigParser
from data import common
from data.common import ESClient
from data.gitee import Gitee
import pypistats
import traceback
from collect.gitee import GiteeClient
import subprocess


class SigMaintainer(object):

    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')

        self.url = config.get('es_url')
        self.authorization = config.get('authorization')

        self.esClient = ESClient(config)
        self.gitee = Gitee(config)
        self.org = config.get("org")
        self.sigs_dir = config.get('sigs_dir')
        self.sigs_url = config.get('sigs_url')
        self.index_name_sigs = config.get('index_name_sigs')
        self.index_name_sigs_repos = config.get('index_name_sigs_repos')

        self.gitee_token = config.get('gitee_token')
        self.sigs_source = config.get('sigs_source')
        self.headers = {'Content-Type': 'application/json', "Authorization": config.get('authorization')}
        self.sig_repo_name = config.get('sig_repo_name')
        self.sigs_dirs_path = config.get('sigs_dirs_path')
        self.from_data = config.get("from_data")
        self.sig_mark = config.get("sig_mark")

    def run(self, starttime=None):
        if self.index_name_sigs and self.sig_mark:
            self.get_sigs()
            last_maintainers = self.esClient.get_sig_maintainers(self.index_name_sigs_repos)
            maintainer_sigs_dict = self.get_sigs_original()
            # time.sleep(20)
            # self.reindex_maintainer_gitee_all(last_maintainers, maintainer_sigs_dict)

    def safe_put_bulk(self, bulk_json, header=None, url=None):
        """Bulk items to a target index `url`. In case of UnicodeEncodeError,
        the bulk is encoded with iso-8859-1.

        :param header:
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

        try:
            res = requests.post(_url + "/_bulk", data=bulk_json,
                                headers=_header, verify=False)
            res.raise_for_status()
        except UnicodeEncodeError:

            # Related to body.encode('iso-8859-1'). mbox data
            bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
            res = requests.put(url, data=bulk_json, headers=_header)
            res.raise_for_status()

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""
        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData) + '\n'
        action += json.dumps(body) + '\n'
        return action

    def reindex_maintainer_gitee_all(self, history_maintainers, maintainer_sigs_dict):
        dic = self.esClient.getOrgByGiteeID()
        giteeid_company_dict = dic[0]
        maintainers = self.esClient.get_sig_maintainers(self.index_name_sigs_repos)
        print('giteeid_company_dict = ', len(giteeid_company_dict))
        print('maintainers = ', maintainers)
        actions = ''
        for maintainer in maintainers:
            reindex_json = '''{
                                  "source": {
                                    "index": "%s",
                                    "query": {
                                      "term": {
                                        "user_login.keyword": "%s"
                                      }
                                    }
                                  },
                                  "dest": {
                                    "index": "%s"
                                  }
                                }''' % (self.sigs_source, maintainer, self.index_name)
            # copy self.sigs_source to self.index_name
            data_num = self.esClient.reindex(reindex_json)
            print('reindex: %s -> %d' % (maintainer, data_num))
            if data_num == 0:
                # search = '"must": { "match": { "committer":"%s"}}' % (maintainer)
                # res = self.esClient.searchEsList(self.index_name_sigs, search)
                # print('res = ', res)
                # for r in res:
                #     r_data = r['_source']
                #     action = {
                #         "user_login": maintainer,
                #         "tag_user_company": r_data['tag_user_company'],
                #         "is_project_internal_user": r_data['is_project_internal_user'],
                #         "is_no_contribute_before": 1,
                #         "created_at": r_data['created_at']
                #     }
                if maintainer in giteeid_company_dict:
                    company = giteeid_company_dict.get(maintainer)
                else:
                    company = 'independent'
                internal_user = 1 if company == "huawei" else 0
                action = {
                    "user_login": maintainer,
                    "tag_user_company": company,
                    "is_project_internal_user": internal_user,
                    "is_no_contribute_before": 1,
                    "created_at": "2022-02-08T00:00:00+08:00"
                }
                indexData = {"index": {"_index": self.index_name, "_id": maintainer}}
                actions += json.dumps(indexData) + '\n'
                actions += json.dumps(action) + '\n'
        self.safe_put_bulk(actions)

        # tag removed maintainers
        removed_maintainers = set(history_maintainers).difference(set(maintainers))
        for maintainer in removed_maintainers:
            query = """{
                          "script": {
                            "source": "ctx._source['is_removed_maintainer']=1"
                          },
                          "query": {
                            "term": {
                              "user_login.keyword": "%s"
                            }
                          }
                        }""" % maintainer
            self.esClient.updateByQuery(query=query)
        # tag maintainer`s sigs
        for maintainer in maintainers:
            sigs = maintainer_sigs_dict.get(maintainer)
            query = """{
                          "script": {
                            "source": "ctx._source['maintainer_sigs']=params.sig",
                            "lang": "painless",
                            "params": {
                              "sig": %s
                            }
                          },
                          "query": {
                            "term": {
                              "user_login.keyword": "%s"
                            }
                          }
                        }""" % (str(sigs).replace("\'", "\""), maintainer)

            self.esClient.updateByQuery(query=query)
            print('%s: %s' % (maintainer, sigs))
        print('maintainers size = ', len(maintainers))

    def mark_removed_sigs(self, dirs):
        search = '''{
                    	"size": 0,
                    	"query": {
                    		"bool": {
                    			"filter": [
                    				{
                    					"query_string": {
                    						"analyze_wildcard": true,
                    						"query": "*"
                    					}
                    				}
                    			]
                    		}
                    	},
                    	"aggs": {
                    		"2": {
                    			"terms": {
                    				"field": "sig_name.keyword",
                    				"size": 10000,
                    				"order": {
                    					"_key": "desc"
                    				},
                    				"min_doc_count": 0
                    			},
                    			"aggs": {}
                    		}
                    	}
                    }'''
        url = self.url + '/' + self.index_name_sigs + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=search)
        data = res.json()

        removed_sigs = ['sig-template']
        for sig in data['aggregations']['2']['buckets']:
            sig_name = sig['key']
            if sig_name not in dirs:
                removed_sigs.append(sig_name)

        for s in removed_sigs:
            mark = '''{
                            "script": {
                                "source":"ctx._source['is_removed']=1"
                            },
                            "query": {
                                "term": {
                                    "sig_name.keyword":"%s"
                                }
                            } 
                        }''' % s
            url = self.url + '/' + self.index_name_sigs + '/_update_by_query'
            requests.post(url, headers=self.esClient.default_headers, verify=False, data=mark)
            print('<%s> has been marked for removal' % s)
        print(res)

    def get_sig_repos(self, dir):
        sig_repo_list = []
        sig_repo_path = self.sigs_dirs_path + '/' + dir
        # print('sig_repo_path = ', sig_repo_path)
        repo_path_dirs = os.walk(sig_repo_path).__next__()[1]

        if 'openeuler' in repo_path_dirs:
            repo_path_dir = sig_repo_path + '/' + 'openeuler'
            repo_paths = os.walk(repo_path_dir).__next__()[1]
            for repo_path in repo_paths:
                yaml_dir_path = repo_path_dir + '/' + repo_path
                yaml_dir = os.walk(yaml_dir_path).__next__()[2]
                for file in yaml_dir:
                    yaml_path = yaml_dir_path + '/' + file
                    repo_name = 'openeuler/' + yaml.load_all(open(yaml_path),
                                                             Loader=yaml.Loader).__next__()['name']
                    sig_repo_list.append(repo_name)

        if 'src-openeuler' in repo_path_dirs:
            repo_path_dir = sig_repo_path + '/' + 'src-openeuler'
            repo_paths = os.walk(repo_path_dir).__next__()[1]
            for repo_path in repo_paths:
                yaml_dir_path = repo_path_dir + '/' + repo_path
                yaml_dir = os.walk(yaml_dir_path).__next__()[2]
                for file in yaml_dir:
                    yaml_path = yaml_dir_path + '/' + file
                    repo_name = 'src-openeuler/' + yaml.load_all(open(yaml_path),
                                                                 Loader=yaml.Loader).__next__()['name']
                    sig_repo_list.append(repo_name)
        return sig_repo_list

    def get_sigs(self):

        path = self.sigs_dir
        url = self.sigs_url

        if not os.path.exists(path):
            os.makedirs(path)

        gitpath = path + self.sig_repo_name

        if not os.path.exists(gitpath):
            cmdclone = 'cd %s;git clone %s' % (path, url)
            os.system(cmdclone)
        else:
            cmdpull = 'cd %s;git pull' % gitpath
            os.system(cmdpull)

        # sigs
        dic = self.esClient.getOrgByGiteeID()
        giteeid_company_dict = dic[0]

        dirs = os.walk(self.sigs_dirs_path).__next__()[1]
        for dir in dirs:
            repo_path = self.sigs_dirs_path + '/' + dir
            cmdlog = 'cd %s;git log -p README.md' % repo_path
            log_popen = subprocess.Popen(cmdlog, stdout=subprocess.PIPE, shell=True)
            log = bytes.decode(log_popen.stdout.read(), encoding="utf-8", errors='ignore')
            loglist = log.split('\n')
            n = 0
            rs = []
            for index in range(len(loglist)):
                if re.search(r'^commit .*', loglist[index]):
                    rs.append('\n'.join(loglist[n:index]))
                    n = index
            rs.append('\n'.join(loglist[n:]))
            times = None
            for r in rs:
                # if re.search(r'.*README.md', r):
                if re.search(r'^commit .*', r):
                    date = re.search(r'Date: (.*)\n', r).group(1)
                    # time_struct = time.strptime(date[2:], '%a %b %d %H:%M:%S %Y')
                    time_struct = time.strptime(date.strip()[:-6], '%a %b %d %H:%M:%S %Y')
                    times = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time_struct)
                    break

            cmdowner = 'cd %s;git log -p OWNERS' % repo_path
            owners_popen = subprocess.Popen(cmdowner, stdout=subprocess.PIPE, shell=True)
            owners = bytes.decode(owners_popen.stdout.read(), encoding="utf-8")
            ownerslist = owners.split('\n')
            n2 = 0
            rs2 = []
            for index in range(len(ownerslist)):
                if re.search(r'^commit .*', ownerslist[index]):
                    rs2.append('\n'.join(ownerslist[n2:index]))
                    n2 = index
            rs2.append('\n'.join(ownerslist[n2:]))
            onwer_file = repo_path + '/' + 'OWNERS'
            onwers = yaml.load_all(open(onwer_file), Loader=yaml.Loader).__next__()
            datas = ''
            try:
                for key, val in onwers.items():
                    key = key.lower()
                    if key == "committer":
                        key = "committers"
                    for onwer in val:
                        # search = '"must": [{ "match": { "sig_name":"%s"}},{ "match": { "committer":"%s"}}]' % (
                        # dir, onwer)
                        # ID_list = [r['_id'] for r in
                        #            self.esClient.searchEsList("openeuler_sigs_committers_20210318", search)]
                        times_onwer = None
                        for r in rs2:
                            if re.search(r'\+\s*-\s*%s' % onwer, r):
                                date = re.search(r'Date:\s*(.*)\n', r).group(1)
                                # time_struct = time.strptime(date, '%a %b %d %H:%M:%S %Y')
                                time_struct = time.strptime(date.strip()[:-6], '%a %b %d %H:%M:%S %Y')
                                times_onwer = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time_struct)

                        repo_mark = True
                        # repos = []
                        # sig_repo_path = self.sigs_dirs_path + '/' + dir
                        # # print('sig_repo_path = ', sig_repo_path)
                        # repo_path_dirs = os.walk(sig_repo_path).__next__()[1]
                        #
                        # if 'openeuler' in repo_path_dirs:
                        #     repo_path_dir = sig_repo_path + '/' + 'openeuler'
                        #     repo_paths = os.walk(repo_path_dir).__next__()[1]
                        #     for repo_path in repo_paths:
                        #         yaml_dir_path = repo_path_dir + '/' + repo_path
                        #         yaml_dir = os.walk(yaml_dir_path).__next__()[2]
                        #         for file in yaml_dir:
                        #             yaml_path = yaml_dir_path + '/' + file
                        #             repo_name = 'openeuler/' + yaml.load_all(
                        #                 open(yaml_path), Loader=yaml.Loader).__next__()['name']
                        #             repos.append(repo_name)
                        #
                        # if 'src-openeuler' in repo_path_dirs:
                        #     repo_path_dir = sig_repo_path + '/' + 'src-openeuler'
                        #     repo_paths = os.walk(repo_path_dir).__next__()[1]
                        #     for repo_path in repo_paths:
                        #         yaml_dir_path = repo_path_dir + '/' + repo_path
                        #         yaml_dir = os.walk(yaml_dir_path).__next__()[2]
                        #         for file in yaml_dir:
                        #             yaml_path = yaml_dir_path + '/' + file
                        #             repo_name = 'src-openeuler/' + yaml.load_all(
                        #                 open(yaml_path), Loader=yaml.Loader).__next__()['name']
                        #             repos.append(repo_name)

                        repos = self.get_sig_repos(dir)
                        for repo in repos:
                            ID = self.org + '_' + dir + '_' + repo + '_' + key + '_' + onwer
                            # if ID in ID_list:
                            #     ID_list.remove(ID)
                            dataw = {"sig_name": dir,
                                     "repo_name": repo,
                                     "committer": onwer,
                                     "created_at": times,
                                     "committer_time": times_onwer,
                                     "is_sig_repo_committer": 1,
                                     "owner_type": key}
                            if onwer in giteeid_company_dict:
                                company = giteeid_company_dict.get(onwer)
                            else:
                                company = 'independent'
                            internal_user = 1 if company == "huawei" else 0
                            userExtra = {"tag_user_company": company,
                                         "is_project_internal_user": internal_user}
                            dataw.update(userExtra)
                            datar = self.getSingleAction(self.index_name_sigs, ID, dataw)
                            datas += datar
                            repo_mark = False

                        if repo_mark:
                            ID = self.org + '_' + dir + '_null_' + key + '_' + onwer
                            # if ID in ID_list:
                            #     ID_list.remove(ID)
                            dataw = {"sig_name": dir,
                                     "repo_name": None,
                                     "committer": onwer,
                                     "created_at": times,
                                     "committer_time": times_onwer,
                                     "is_sig_repo_committer": 1,
                                     "owner_type": key}
                            if onwer in giteeid_company_dict:
                                company = giteeid_company_dict.get(onwer)
                            else:
                                company = 'independent'
                            internal_user = 1 if company == "huawei" else 0
                            userExtra = {"tag_user_company": company,
                                         "is_project_internal_user": internal_user}
                            dataw.update(userExtra)
                            datar = self.getSingleAction(self.index_name_sigs, ID, dataw)
                            datas += datar
                        # for id in ID_list:
                        #     data = '''{"size":10000,"query": {"bool": {"match": [{ "term": { "_id":"%s"}}]}}}''' % id
                        #     self.esClient.post_delete_delete_by_query(data, self.index_name_sigs)
                        #     print('delete ID: %s' % id)
                self.safe_put_bulk(datas)
                print("this sig done: %s" % dir)
                time.sleep(1)
            except:
                print(traceback.format_exc())

        # self.mark_removed_sigs(dirs=dirs)

    def get_sigs_original(self):
        dirs = os.walk(self.sigs_dirs_path).__next__()[1]
        sig_repos_dict = {}
        for dir in dirs:
            # sig_repo_list = []
            # sig_repo_path = self.sigs_dirs_path + '/' + dir
            # repo_path_dirs = os.walk(sig_repo_path).__next__()[1]
            #
            # if 'openeuler' in repo_path_dirs:
            #     repo_path_dir = sig_repo_path + '/' + 'openeuler'
            #     repo_paths = os.walk(repo_path_dir).__next__()[1]
            #     for repo_path in repo_paths:
            #         yaml_dir_path = repo_path_dir + '/' + repo_path
            #         yaml_dir = os.walk(yaml_dir_path).__next__()[2]
            #         for file in yaml_dir:
            #             yaml_path = yaml_dir_path + '/' + file
            #             repo_name = 'openeuler/' + yaml.load_all(open(yaml_path),
            #                                                      Loader=yaml.Loader).__next__()['name']
            #             sig_repo_list.append(repo_name)
            #
            # if 'src-openeuler' in repo_path_dirs:
            #     repo_path_dir = sig_repo_path + '/' + 'src-openeuler'
            #     repo_paths = os.walk(repo_path_dir).__next__()[1]
            #     for repo_path in repo_paths:
            #         yaml_dir_path = repo_path_dir + '/' + repo_path
            #         yaml_dir = os.walk(yaml_dir_path).__next__()[2]
            #         for file in yaml_dir:
            #             yaml_path = yaml_dir_path + '/' + file
            #             repo_name = 'src-openeuler/' + yaml.load_all(open(yaml_path),
            #                                                          Loader=yaml.Loader).__next__()['name']
            #             sig_repo_list.append(repo_name)

            sig_repo_list = self.get_sig_repos(dir)
            sig_repos_dict.update({dir: sig_repo_list})
        # print(sig_repos_dict)

        actions = ''
        dict_comb = defaultdict(dict)
        for dir in dirs:
            # get repos
            repositories = []
            if dir in sig_repos_dict:
                repos = sig_repos_dict.get(dir)
                for repo in repos:
                    if str(repo).__contains__('/'):
                        repositories = repos
                        break
                    else:
                        repositories.append(self.org + '/' + repo)
            # get maintainers
            try:
                onwer_file = self.sigs_dirs_path + '/' + dir + '/' + 'OWNERS'
                onwers = yaml.load_all(open(onwer_file), Loader=yaml.Loader).__next__()
                maintainers = onwers['maintainers']
            except FileNotFoundError:
                maintainers = []
            # get maintainer sigs dict
            dt = defaultdict(dict)
            for maintainer in maintainers:
                dt.update({maintainer: [dir]})
            combined_keys = dict_comb.keys() | dt.keys()
            dict_comb = {key: dict_comb.get(key, []) + dt.get(key, []) for key in combined_keys}
            # sig actions
            action = {
                "sig_name": dir,
                "repos": repositories,
                "is_sig_original": 1,
                "maintainers": maintainers,
                "created_at": "2021-12-01T00:00:00+08:00"
            }
            indexData = {"index": {"_index": self.index_name_sigs_repos, "_id": dir}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
        self.safe_put_bulk(actions)
        return dict_comb