#  Copyright (c) 2022.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

import json
import logging

import requests
import yaml
from data.common import ESClient
from collect.github import GithubClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class EcosystemRepo(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.headers = {}
        self.github_authorization = config.get('github_authorization')
        self.url = config.get('es_url')
        self.from_data = config.get("from_data")
        self.headers = {'Content-Type': 'application/json', 'Authorization': config.get('authorization')}
        self.yaml_path = config.get('yaml_path')
        self.yaml_path_en = config.get('yaml_path_en')
        self.exists_ids = []

    def run(self, from_date):
        self.get_all_id()
        actions = ''
        yaml_paths = self.yaml_path.split(',')
        yaml_path_ens = self.yaml_path_en.split(',')
        for i in range(len(yaml_paths)):
            actions += self.get_yaml_info(yaml_paths[i], yaml_path_ens[i])
        self.esClient.safe_put_bulk(actions)

        # tag removed repo info
        self.mark_removed_ids()

    def get_star_count(self, owner, repo):
        client = GithubClient(owner, repo, self.github_authorization)
        repo_star_user_list = client.getStarDetails(owner=owner)
        print('%s star count: %d' % (repo, len(repo_star_user_list)))
        return len(repo_star_user_list)

    def get_yaml_info(self, yaml_path, yaml_path_en):
        actions = self.get_repo_info(yaml_path, "zh")
        actions += self.get_repo_info(yaml_path_en, "en")
        return actions

    def get_repo_info(self, yaml_path, lang):
        actions = ''
        yaml_response = self.esClient.request_get(yaml_path)
        if yaml_response.status_code != 200:
            print('Cannot fetch online yaml file.')
            return None
        text = yaml_response.text
        yaml_data = yaml.load(text, Loader=yaml.Loader)

        repo_list = yaml_data.get('list')
        repo_type = yaml_data.get('type')
        description = yaml_data.get('description')
        name = yaml_data.get('name')
        for r in repo_list:
            try:
                repo = r.get('name')
                html_url = r.get('links')
                introduction = r.get('introduction')
                date = r.get('date').strftime("%Y-%m-%dT08:00:00+08:00")
                owner = html_url.split('/')[-2]
                repo_path = html_url.split('/')[-1]
                star = self.get_star_count(owner, repo_path)
                action = {
                    'name': name,
                    'type': repo_type,
                    'description': description,
                    'repo': repo,
                    'html_url': html_url,
                    'introduction': introduction,
                    'star': star,
                    'date': date,
                    'lang': lang
                }
                id = repo_type + '_' + owner + '_' + repo + '_' + lang
                indexData = {"index": {"_index": self.index_name, "_id": id}}
                actions += json.dumps(indexData) + '\n'
                actions += json.dumps(action) + '\n'
                if id in self.exists_ids:
                    self.exists_ids.remove(id)
            except Exception as e:
                logger.info('exception: ', e)
        return actions

    def get_id_func(self, hit):
        for data in hit:
            self.exists_ids.append(data['_id'])

    def get_all_id(self):
        search = ''
        self.esClient.scrollSearch(self.index_name, search=search, func=self.get_id_func)

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
            url = self.url + '/' + self.index_name + '/_update_by_query'
            requests.post(url, headers=self.esClient.default_headers, verify=False, data=mark)
