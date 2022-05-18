import os
import re
import json
import requests
import time
import yaml
import subprocess

from collections import defaultdict
from data.common import ESClient
from data.gitee import Gitee



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

        self.gitee_token = config.get('gitee_token')
        self.sigs_source = config.get('sigs_source')
        self.headers = {'Content-Type': 'application/json', "Authorization": config.get('authorization')}
        self.sig_repo_name = config.get('sig_repo_name')
        self.sigs_dirs_path = config.get('sigs_dirs_path')
        self.from_data = config.get("from_data")
        self.get_repo_name_without_sig = config.get("get_repo_name_without_sig")
        self.sig_mark = config.get("sig_mark")
        self.exists_ids = []
        # self.index_name_maintainer_info = config.get('index_name_maintainer_info')

    def run(self, from_time):
        if self.index_name_sigs and self.sig_mark:
            self.get_all_id()
            self.get_sigs()
            maintainer_sigs_dict = self.get_sigs_original()

    def getSingleAction(self, index_name, id, body, act="index"):
        action = ""
        indexData = {
            act: {"_index": index_name, "_id": id}}
        action += json.dumps(indexData) + '\n'
        action += json.dumps(body) + '\n'
        return action

    def mark_removed_sigs(self, dirs, index):
        time.sleep(5)
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
        url = self.url + '/' + index + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=search)
        data = res.json()

        removed_sigs = ['sig-template', 'Template']
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
            url = self.url + '/' + index + '/_update_by_query'
            requests.post(url, headers=self.esClient.default_headers, verify=False, data=mark)
            print('<%s> has been marked for removal' % s)

    def get_sig_repos(self, dir):
        sig_repo_list = []
        sig_repo_path = self.sigs_dirs_path + '/' + dir
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

    def get_sig_repos_opengauss(self):
        sig_yaml_path = self.sigs_dir + self.sig_repo_name + '/sigs.yaml'
        data = yaml.load_all(open(sig_yaml_path), Loader=yaml.Loader).__next__()['sigs']
        sig_repos_dict = {}
        for d in data:
            repos = d['repositories']
            repositories = []
            for repo in repos:
                if self.get_repo_name_without_sig:
                    repositories.append(repo)
                    break
                else:
                    repositories.append(self.org + '/' + repo)
            sig_repos_dict.update({d['name']: repositories})

        return sig_repos_dict

    def get_id_func(self, hit):
        for data in hit:
            self.exists_ids.append(data['_id'])

    def get_all_id(self):
        search = '''{
                      "size": 10000,
                      "_source": {
                        "includes": [
                          "committer"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "term": {
                                "is_sig_repo_committer": 1
                              }
                            }
                          ]
                        }
                      }
                    }'''
        self.esClient.scrollSearch(self.index_name_sigs, search=search, func=self.get_id_func)

    def mark_removed_ids(self):
        for removed_id in self.exists_ids:
            mark = '''{
                            "script": {
                                "source":"ctx._source['is_removed']=1"
                            },
                            "query": {
                                "term": {
                                    "_id":"%s"
                                }
                            }
                        }''' % removed_id
            url = self.url + '/' + self.index_name_sigs + '/_update_by_query'
            requests.post(url, headers=self.esClient.default_headers, verify=False, data=mark)

    def get_readme_log(self, repo_path):
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
            if re.search(r'^commit .*', r):
                date = re.search(r'Date: (.*)\n', r).group(1)
                # time_struct = time.strptime(date[2:], '%a %b %d %H:%M:%S %Y')
                time_struct = time.strptime(date.strip()[:-6], '%a %b %d %H:%M:%S %Y')
                times = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time_struct)
        return times

    def get_owner_log(self, repo_path):
        cmdowner = 'cd %s;git log -p OWNERS' % repo_path
        owners_popen = subprocess.Popen(cmdowner, stdout=subprocess.PIPE, shell=True)
        owners = bytes.decode(owners_popen.stdout.read(), encoding="utf-8")
        ownerslist = owners.split('\n')
        n = 0
        rs = []
        for index in range(len(ownerslist)):
            if re.search(r'^commit .*', ownerslist[index]):
                rs.append('\n'.join(ownerslist[n:index]))
                n = index
        rs.append('\n'.join(ownerslist[n:]))
        return rs

    def get_sig_info_log(self, repo_path):
        cmdlog = 'cd %s;git log -p sig-info.yaml' % repo_path
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
        return rs

    def get_sig_info(self, owner_type, dir):
        repo_path = self.sigs_dirs_path + '/' + dir
        rs = self.get_sig_info_log(repo_path)

        sig_info = self.sigs_dirs_path + '/' + dir + '/' + 'sig-info.yaml'
        info = yaml.load_all(open(sig_info), Loader=yaml.Loader).__next__()
        datas = ''
        repositories = info.get('repositories')
        repos = []
        for repo in repositories:
            for r in repo['repo']:
                repos.append(r)
        users = []
        if owner_type in info and info[owner_type] is not None:
            users_info = info[owner_type]
            for user in users_info:
                users.append(user['gitee_id'])
        for user in users:
            times_owner = None
            times = self.get_readme_log(repo_path)
            for r in rs:
                if r == '':
                    continue
                if re.search(r'\+-\s*gitee_id:\s*%s\n' % user, r):
                    date = re.search(r'Date:\s*(.*)\n', r).group(1)
                    # time_struct = time.strptime(date, '%a %b %d %H:%M:%S %Y')
                    time_struct = time.strptime(date.strip()[:-6], '%a %b %d %H:%M:%S %Y')
                    times_owner = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time_struct)
            repo_mark = True
            for repo in repos:
                ID = self.org + '_' + dir + '_' + repo + '_' + owner_type + '_' + user
                if ID in self.exists_ids:
                    self.exists_ids.remove(ID)
                dataw = {"sig_name": dir,
                         "repo_name": repo,
                         "committer": user,
                         "user_login": user,
                         "created_at": times,
                         "committer_time": times_owner,
                         "is_sig_repo_committer": 1,
                         "owner_type": owner_type}
                userExtra = self.esClient.getUserInfo(user)
                dataw.update(userExtra)
                datar = self.getSingleAction(self.index_name_sigs, ID, dataw)
                datas += datar
                repo_mark = False
            if repo_mark:
                ID = self.org + '_' + dir + '_null_' + owner_type + '_' + user
                if ID in self.exists_ids:
                    self.exists_ids.remove(ID)
                dataw = {"sig_name": dir,
                         "repo_name": None,
                         "committer": user,
                         "user_login": user,
                         "created_at": times,
                         "committer_time": times_owner,
                         "is_sig_repo_committer": 1,
                         "owner_type": owner_type}
                userExtra = self.esClient.getUserInfo(user)
                dataw.update(userExtra)
                datar = self.getSingleAction(self.index_name_sigs, ID, dataw)
                datas += datar
        return datas

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

        dic = self.esClient.getOrgByGiteeID()
        self.esClient.giteeid_company_dict = dic[0]
        self.gitee.internalUsers = self.gitee.getItselfUsers(self.gitee.internal_users)

        dirs = os.walk(self.sigs_dirs_path).__next__()[1]
        sig_repos_dict = {}
        if self.org == 'openeuler':
            for dir in dirs:
                sig_repo_list = self.get_sig_repos(dir)
                sig_repos_dict.update({dir: sig_repo_list})
        if self.org == 'opengauss':
            sig_repos_dict = self.get_sig_repos_opengauss()

        for dir in dirs:
            try:
                repo_path = self.sigs_dirs_path + '/' + dir
                owner_file = repo_path + '/' + 'OWNERS'
                owner_logins = yaml.load_all(open(owner_file), Loader=yaml.Loader).__next__()
                times = self.get_readme_log(repo_path)
                rs = self.get_owner_log(repo_path)
                datas = ''
                for key, val in owner_logins.items():
                    key = key.lower()
                    if key == "committer":
                        key = "committers"
                    for owner in val:
                        times_owner = None
                        for r in rs:
                            if re.search(r'\+\s*-\s*%s' % owner, r):
                                date = re.search(r'Date:\s*(.*)\n', r).group(1)
                                # time_struct = time.strptime(date, '%a %b %d %H:%M:%S %Y')
                                time_struct = time.strptime(date.strip()[:-6], '%a %b %d %H:%M:%S %Y')
                                times_owner = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time_struct)

                        repo_mark = True
                        repos = []
                        if dir in sig_repos_dict:
                            repos = sig_repos_dict.get(dir)
                        for repo in repos:
                            ID = self.org + '_' + dir + '_' + repo + '_' + key + '_' + str(owner)
                            if ID in self.exists_ids:
                                self.exists_ids.remove(ID)
                            dataw = {"sig_name": dir,
                                     "repo_name": repo,
                                     "committer": owner,
                                     "user_login": owner,
                                     "created_at": times,
                                     "committer_time": times_owner,
                                     "is_sig_repo_committer": 1,
                                     "owner_type": key}
                            userExtra = self.esClient.getUserInfo(owner)
                            dataw.update(userExtra)
                            datar = self.getSingleAction(self.index_name_sigs, ID, dataw)
                            datas += datar
                            repo_mark = False

                        if repo_mark:
                            ID = self.org + '_' + dir + '_null_' + key + '_' + owner
                            if ID in self.exists_ids:
                                self.exists_ids.remove(ID)
                            dataw = {"sig_name": dir,
                                     "repo_name": None,
                                     "committer": owner,
                                     "user_login": owner,
                                     "created_at": times,
                                     "committer_time": times_owner,
                                     "is_sig_repo_committer": 1,
                                     "owner_type": key}
                            userExtra = self.esClient.getUserInfo(owner)
                            dataw.update(userExtra)
                            datar = self.getSingleAction(self.index_name_sigs, ID, dataw)
                            datas += datar
                self.esClient.safe_put_bulk(datas)
                print("this sig done: %s" % dir)
            except FileNotFoundError:
                print('OWNER of %s is not exist. Using sig-info.yaml!' % dir)
                datas = self.get_sig_info('maintainers', dir)
                datas += self.get_sig_info('committers', dir)
                self.esClient.safe_put_bulk(datas)
                print("this sig done: %s" % dir)

        self.mark_removed_sigs(dirs=dirs, index=self.index_name_sigs)
        self.mark_removed_ids()

    def get_sigs_original(self):
        dirs = os.walk(self.sigs_dirs_path).__next__()[1]
        sig_repos_dict = {}
        if self.org == 'openeuler':
            for dir in dirs:
                sig_repo_list = self.get_sig_repos(dir)
                sig_repos_dict.update({dir: sig_repo_list})
        if self.org == 'opengauss':
            sig_repos_dict = self.get_sig_repos_opengauss()

        actions = ''
        dict_comb = defaultdict(dict)
        for dir in dirs:
            repo_path = self.sigs_dirs_path + '/' + dir
            times = self.get_readme_log(repo_path)
            # get repos
            repositories = []
            if dir in sig_repos_dict:
                repositories = sig_repos_dict.get(dir)
            # get maintainers
            try:
                owner_file = self.sigs_dirs_path + '/' + dir + '/' + 'OWNERS'
                owners = yaml.load_all(open(owner_file), Loader=yaml.Loader).__next__()
                maintainers = owners['maintainers']
            except FileNotFoundError:
                maintainers = []
            # # get maintainer sigs dict
            # dt = defaultdict(dict)
            # for maintainer in maintainers:
            #     dt.update({maintainer: [dir]})
            # combined_keys = dict_comb.keys() | dt.keys()
            # dict_comb = {key: dict_comb.get(key, []) + dt.get(key, []) for key in combined_keys}
            # sig actions
            action = {
                "sig_name": dir,
                "repos": repositories,
                "is_sig_original": 1,
                "maintainers": maintainers,
                "created_at": times
            }
            try:
                sig_info = self.sigs_dirs_path + '/' + dir + '/' + 'sig-info.yaml'
                info = yaml.load_all(open(sig_info), Loader=yaml.Loader).__next__()
                for i in info:
                    if i == 'description':
                        action.update({i: info.get(i)})
                    if i == 'maintainers':
                        maintainer_info = info.get(i)
                        # maintainers = []
                        # for maintainer in maintainer_info:
                        #     maintainers.append(maintainer['gitee_id'])
                        action.update({'maintainer_info': maintainer_info})
                        # action.update({"maintainers": maintainers})
            except FileNotFoundError:
                print('sig-info.yaml of %s is not exist.' % dir)
            indexData = {"index": {"_index": self.index_name_sigs, "_id": dir}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)
        return dict_comb
