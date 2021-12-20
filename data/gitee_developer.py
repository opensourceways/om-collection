import json

from data.common import ESClient
from collect.gitee import GiteeClient


class GiteeDeveloper(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name_committer = config.get('index_name_committer')
        self.esClient = ESClient(config)
        self.owners = config.get('owner')
        self.access_token = config.get('token')
        self.repository = None
        self.base_url = config.get('base_url')
        self.since = config.get('since')
        self.until = config.get('until')
        # self.gitee_api = GiteeClient(self.owner, self.repository, self.access_token, self.base_url)

    def run(self, from_time):
        print("Collect tencent surveys data: start")
        self.collect_developer_details()
        print("Collect tencent surveys data: finished")

    def collect_developer_details(self):
        actions = ""
        owners = self.owners.split(',')
        print(owners)
        for owner in owners:
            print("...start owner: %s..." % owner)
            gitee_api = GiteeClient(owner, self.repository, self.access_token, self.base_url)
            repo_page = 0
            while True:
                repo_page += 1
                repos = gitee_api.get_repos(cur_page=repo_page).json()
                if len(repos) == 0:
                    break
                print("repo_page: %i" % repo_page)
                for repo in repos:
                    # print(repo)
                    repo_path = repo['path']
                    print("start repo: %s" % repo_path)
                    page = 0
                    while True:
                        page += 1
                        print(self.since)
                        print(type(self.since))
                        commits = gitee_api.get_commits(repo_path, cur_page=page, since=self.since, until=self.until)
                        commits_legacy = commits.json()
                        if len(commits_legacy) == 0:
                            print("commit_page: %i finish..." % page)
                            break
                        for commit in commits_legacy:
                            if commit['author'] is None:
                                continue
                            if 'id' not in commit['author']:
                                print(commit['author']['login'])
                                continue
                            action = {
                                'email': commit['commit']['author']['email'],
                                'login': commit['author']['login'],
                                'id': commit['author']['id']
                            }
                            index_data_survey = {"index": {"_index": self.index_name_committer, "_id": action['email']}}
                            actions += json.dumps(index_data_survey) + '\n'
                            actions += json.dumps(action) + '\n'

                        for commit in commits_legacy:
                            if commit['committer'] is None:
                                continue
                            action = {
                                'email': commit['commit']['committer']['email'],
                                'login': commit['committer']['login'],
                                'id': commit['committer']['id']
                            }
                            index_data_survey = {"index": {"_index": self.index_name_committer, "_id": action['email']}}
                            actions += json.dumps(index_data_survey) + '\n'
                            actions += json.dumps(action) + '\n'

        self.esClient.safe_put_bulk(actions)
