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
        self.flag = 0

    def run(self, from_time):
        print("Collect gitee committers data: start")
        self.collect_developer_details()
        print("Collect gitee committers data: finished")

    def get_repo_branches(self, owner, repo_path):
        gitee = GiteeClient(owner, repo_path, self.access_token, self.base_url)
        branches = gitee.getSingleReopBranch()
        branches_info = self.esClient.getGenerator(branches)
        branches_name = []
        for b in branches_info:
            branches_name.append(b['name'])
        return branches_name

    def get_developer_info(self, owner, page, repo_path, branch):
        actions = ""
        gitee_api = GiteeClient(owner, self.repository, self.access_token, self.base_url)
        commits = gitee_api.get_commits(repo_path, branch, cur_page=page, since=self.since, until=self.until)
        if commits.status_code != 200:
            print('HTTP get commits error!')
            return actions
        commits_legacy = commits.json()
        if len(commits_legacy) == 0:
            print("commit_page: %i finish..." % page)
            self.flag = 1
            return actions
        for commit in commits_legacy:
            if commit['author'] is not None and 'id' in commit['author']:
                action = {
                    'email': commit['commit']['author']['email'],
                    'gitee_id': commit['author']['login'],
                    'id': commit['author']['id'],
                    'created_at': commit['commit']['author']['date']
                }
                index_data_survey = {"index": {"_index": self.index_name_committer, "_id": action['email']}}
                actions += json.dumps(index_data_survey) + '\n'
                actions += json.dumps(action) + '\n'

            if commit['committer'] is not None and 'id' in commit['committer']:
                action = {
                    'email': commit['commit']['committer']['email'],
                    'gitee_id': commit['committer']['login'],
                    'id': commit['committer']['id'],
                    'created_at': commit['commit']['committer']['date']
                }
                index_data_survey = {"index": {"_index": self.index_name_committer, "_id": action['email']}}
                actions += json.dumps(index_data_survey) + '\n'
                actions += json.dumps(action) + '\n'
        return actions

    def collect_developer_details(self):
        owners = self.owners.split(',')
        for owner in owners:
            print("...start owner: %s..." % owner)
            gitee_api = GiteeClient(owner, self.repository, self.access_token, self.base_url)
            repo_page = 0
            while True:
                repo_page += 1
                response = gitee_api.get_repos(cur_page=repo_page)
                if response.status_code != 200:
                    print('HTTP get repos error!')
                    continue
                repos = response.json()
                if len(repos) == 0:
                    break
                print("repo_page: %i" % repo_page)
                for repo in repos:
                    actions = ""
                    repo_path = repo['path']
                    print("start repo: %s" % repo_path)
                    branches_name = self.get_repo_branches(owner, repo_path)
                    if len(branches_name) == 0:
                        break
                    for branch in branches_name:
                        if branch == "init":
                            continue
                        print("start branch: %s" % branch)
                        page = 0
                        self.flag = 1
                        while True:
                            page += 1
                            action = self.get_developer_info(owner, page, repo_path, branch)
                            actions += action
                            if self.flag == 1:
                                break
                    print("collect repo: %s over." % repo_path)
                    self.esClient.safe_put_bulk(actions)
