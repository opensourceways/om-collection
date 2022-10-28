#  Copyright (c) 2022.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import datetime
import json
from data.common import ESClient

BASE_URL = "https://hub.docker.com/v2"


class DockerHub(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.query = config.get('query')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.owner = config.get('owner')
        self.repos = config.get('repos')

    def run(self, start):
        repos = self.repos.split(',')
        actions = ''
        for repo in repos:
            actions += self.write_pull_count(repo)
        self.esClient.safe_put_bulk(actions)

    def write_pull_count(self, repo):
        actions = ''
        created_at = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S+08:00")
        url = self.urijoin(BASE_URL, 'repositories', self.owner, repo)
        res = self.esClient.request_get(url)
        data = res.json()
        id_str = repo + '-' + self.owner
        action = {
            'id': id_str,
            'pull_count': data.get('pull_count'),
            'repo': repo,
            'metadata__updated_on': created_at,
            'owner': self.owner
        }
        indexData = {"index": {"_index": self.index_name, "_id": id_str + created_at}}
        actions += json.dumps(indexData) + '\n'
        actions += json.dumps(action) + '\n'
        return actions

    @staticmethod
    def urijoin(*args):
        return '/'.join(map(lambda x: str(x).strip('/'), args))
