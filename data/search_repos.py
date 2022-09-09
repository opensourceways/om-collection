#  Copyright (c) 2022.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import json

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data.common import ESClient


class SearchRepos(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.index_name_gitee = config.get('index_name_gitee')
        self.index_name_github = config.get('index_name_github')
        self.gitee_token = config.get('gitee_token')
        self.github_token = config.get('github_token')
        self.tokens = config.get('tokens')
        self.repos = config.get('repos').split(',')
        self.is_get_gitee = config.get('is_get_gitee')
        self.is_get_github = config.get('is_get_github')

    def run(self, from_time):
        print("Collect repos data: start")
        for repo in self.repos:
            if self.is_get_gitee == 'true':
                self.get_gitee_repos(repo)
            if self.is_get_github == 'true':
                self.get_github_repos(repo)
        print("Collect repos data: finished")

    def get_gitee_repos(self, name):
        client = GiteeClient(None, None, self.gitee_token)
        response = client.gitee_search_repo(name)
        datas = self.esClient.getGenerator(response)
        self.write_repos(name, datas, 'gitee', self.index_name_gitee)

    def get_github_repos(self, name):
        datas = []
        client = GithubClient(org=None, repository=None, token=self.github_token)
        repos = client.git_search_repo(name)
        for repo in repos:
            datas.extend(repo.get('items'))
        self.write_repos(name, datas, 'github', self.index_name_github)

    def write_repos(self, search, datas, platform, index_name):
        actions = ''
        for data in datas:
            repo_detail = {
                "search": search,
                "created_at": data["created_at"],
                "updated_at": data["updated_at"],
                "owner_login": data['owner']['login'],
                "user_id": data['owner']['id'],
                "user_login": data['owner']['login'],
                "repository": data["full_name"],
                "public": data.get("public"),
                "private": data.get("private"),
                "{}_repo".format(platform): data["html_url"],
                "description": data["description"],
                "is_{}_repo".format(platform): 1
            }
            index_id = platform + '_' + search + '_' + data["full_name"]
            index_data = {"index": {"_index": index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_detail) + '\n'
        self.esClient.safe_put_bulk(actions)
