#  Copyright (c) 2023.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import base64
import json
import os

import requests

from data.common import ESClient


class GiteePrVersion(object):

    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.index_name = config.get('index_name')
        self.index_name_gitee = config.get('index_name_gitee')
        self.gitee_token = config.get('gitee_token')
        self.esClient = ESClient(config)
        self.obs_meta_org = config.get('obs_meta_org')
        self.obs_meta_repo = config.get('obs_meta_repo')
        self.obs_meta_dir = config.get('obs_meta_dir')
        self.obs_versions = config.get('obs_versions')
        self.code_base_path = config.get('code_base_path')
        self.cloc_bin_path = config.get('cloc_bin_path')
        self.username = config.get('username')
        self.password = config.get('password')

    def run(self, from_time):
        repo_versions = self.get_obs_meta()
        print(repo_versions)
        self.reindex_pr(repo_versions)
        self.refresh_robot_pr(repo_versions)

    def reindex_pr(self, repo_versions):
        for version in repo_versions:
            reindex_json = '''{
                "source": {
                    "index": "%s",
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "query_string": {
                                        "analyze_wildcard": true,
                                        "query": "is_gitee_pull_request:1 AND base_label.keyword:%s AND !tag_user_company.keyword:robot"
                                    }
                                }
                            ]
                        }
                    }
                },
                "dest": {
                    "index": "%s"
                }
            }''' % (self.index_name_gitee, version, self.index_name)
            data_num = self.esClient.reindex(reindex_json.encode('utf-8'))
            if data_num == 0:
                continue
            print('reindex: %s -> %d over' % (version, data_num))

    def refresh_robot_pr(self, repo_versions):
        for version in repo_versions:
            print('start version: ', version)
            query = '''{
                "size": 5000,
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "query_string": {
                                    "analyze_wildcard": true,
                                    "query": "is_gitee_pull_request:1 AND base_label.keyword:%s AND tag_user_company.keyword:robot"
                                }
                            }
                        ]
                    }
                },
                "aggs": {}
            }''' % version
            self.esClient.scrollSearch(self.index_name_gitee, search=query, func=self.get_pr_func)

    def get_pr_func(self, hits):
        actions = ''
        for hit in hits:
            pr_details = hit.get('_source')
            body = pr_details.get('body')
            if body and 'Origin pull request:' in body:
                try:
                    prs = body.split('Origin pull request:')
                    origin_pr = prs[1].split('###')[0].strip()
                    user = self.get_origin_pr_author(origin_pr)
                    if user:
                        pr_details.update(user)
                except:
                    print('error')

            index_data = {"index": {"_index": self.index_name, "_id": pr_details['id']}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(pr_details) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_origin_pr_author(self, pull_url):
        query = '''{
            "size": 10,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "query_string": {
                                "analyze_wildcard": true,
                                "query": "pull_url.keyword:\\"%s\\" AND is_gitee_pull_request:1"
                            }
                        }
                    ]
                }
            },
            "aggs": {}
        }''' % pull_url
        url = self.url + '/' + self.index_name_gitee + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        if res.status_code != 200:
            return
        data = res.json()['hits']['hits']
        user_info = None
        for d in data:
            user_info = {
                'user_login': d['_source']['user_login'],
                'user_id': d['_source']['user_id'],
                'user_name': d['_source']['user_name'],
                'tag_user_company': d['_source']['tag_user_company'],
                'is_project_internal_user': d['_source']['is_project_internal_user'],
                'is_admin_added': d['_source']['is_admin_added']
            }
        return user_info

    def get_obs_meta(self):
        obs_path = self.git_clone_or_pull_repo(owner=self.obs_meta_org, repo_name=self.obs_meta_repo)
        meta_dir = obs_path if self.obs_meta_dir is None else os.path.join(obs_path, self.obs_meta_dir)
        root, dirs, _ = os.walk(meta_dir).__next__()
        if self.obs_versions:
            obs_versions = self.obs_versions.split(";")
            inter_versions = list(set(obs_versions).intersection(set(dirs)))
        else:
            def check_version(s):
                return s.startswith("openEuler-")

            inter_versions = list(filter(check_version, dirs))
        return inter_versions

    def git_clone_or_pull_repo(self, owner, repo_name):
        # 本地仓库目录
        owner_path = self.code_base_path + 'gitee/' + owner + '/'
        if not os.path.exists(owner_path):
            os.makedirs(owner_path)
        code_path = owner_path + repo_name

        username = base64.b64decode(self.username).decode()
        passwd = base64.b64decode(self.password).decode()
        clone_url = 'https://%s:%s@gitee.com/%s/%s' % (username, passwd, owner, repo_name)

        # 本地仓库已存在执行git pull；否则执行git clone
        self.removeGitLockFile(code_path)
        if os.path.exists(code_path):
            cmd_pull = 'cd %s;git checkout .;git pull' % code_path
            os.system(cmd_pull)
        else:
            if clone_url is None:
                return
            cmd_clone = 'cd %s;git clone %s' % (owner_path, clone_url + '.git')
            os.system(cmd_clone)
        return code_path

    # 删除git lock
    def removeGitLockFile(self, code_path):
        lock_file = code_path + '/.git/index.lock'
        if os.path.exists(lock_file):
            os.remove(lock_file)
