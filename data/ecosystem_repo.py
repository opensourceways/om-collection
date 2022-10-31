#  Copyright (c) 2022.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import datetime
import json
import yaml
from data.common import ESClient
from collect.github import GithubClient


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

    def run(self, from_date):
        actions = ''
        yaml_paths = self.yaml_path.split(',')
        for yaml_path in yaml_paths:
            actions += self.get_yaml_info(yaml_path)
        self.esClient.safe_put_bulk(actions)

    def get_star_count(self, owner, repo):
        client = GithubClient(owner, repo, self.github_authorization)
        repo_star_user_list = client.getStarDetails(owner=owner)
        print('%s star count: %d' % (repo, len(repo_star_user_list)))
        return len(repo_star_user_list)

    def get_yaml_info(self, yaml_path):
        actions = ''
        yaml_response = self.esClient.request_get(yaml_path)
        if yaml_response.status_code != 200:
            print('Cannot fetch online yaml file.')
            return None
        text = yaml_response.text
        yaml_data = yaml.load(text, Loader=yaml.Loader)

        repo_list = yaml_data.get('list')
        repo_type = yaml_data.get('type')
        for r in repo_list:
            name = r.get('name')
            html_url = r.get('links')
            introduction = r.get('introduction')
            date = r.get('date').strftime("%Y-%m-%dT08:00:00+08:00")
            owner = html_url.split('/')[3]
            repo = html_url.split('/')[4]
            star = self.get_star_count(owner, repo)
            action = {
                'type': repo_type,
                'repo': name,
                'html_url': html_url,
                'introduction': introduction,
                'star': star,
                'date': date
            }
            id = repo_type + '_' + owner + '_' + repo
            indexData = {"index": {"_index": self.index_name, "_id": id}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
        return actions
