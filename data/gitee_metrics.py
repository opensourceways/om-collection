import json
import time

from data.common import ESClient
from collect.gitee import GiteeClient

METRICS = ["vitality", "community", "health", "trend", "influence"]
METRICS_PERCENT = ["vitality_percent", "community_percent", "health_percent", "trend_percent", "influence_percent"]


class GiteeMetrics(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)
        self.owners = config.get('owner')
        self.access_token = config.get('token')
        self.repository = None

    def run(self, from_time):
        print('Collect repo gitee metrics and rank : starting...')
        self.collect_developer_details()
        print('Collect repo gitee metrics and rank : finished...')

    def gitee_repo_metrics(self, owner, repo_path):
        gitee = GiteeClient(owner, repo_path, self.access_token)
        res = gitee.gitee_metrics(owner, repo_path)
        return res

    def gitee_repo_rank(self, owner, repo_path):
        gitee = GiteeClient(owner, repo_path, self.access_token)
        res = gitee.gitee_rank(owner, repo_path)
        return res

    def collect_developer_details(self):
        owners = self.owners.split(',')
        for owner in owners:
            print("...start owner: %s..." % owner)
            gitee_api = GiteeClient(owner, self.repository, self.access_token)
            repo_page = 0
            actions = ""
            while True:
                repo_page += 1
                response = gitee_api.get_repos(cur_page=repo_page)
                if response.status_code != 200:
                    print('HTTP get repos error!')
                    continue
                repos = response.json()
                if len(repos) == 0:
                    print("...All repos collect finished...")
                    break
                print("repo_page: %i" % repo_page)

                for repo in repos:
                    repo_path = repo['path']
                    print("start repo: %s" % repo_path)

                    metrics_res = self.gitee_repo_metrics(owner, repo_path)
                    if metrics_res.status_code != 200:
                        print('metrics = ', metrics_res.json())
                        continue

                    rank_res = self.gitee_repo_rank(owner, repo_path)
                    if rank_res.status_code != 200:
                        print('rank = ', rank_res.json())
                        continue
                    rank = rank_res.json()

                    metrics = metrics_res.json()
                    created_time = time.time()
                    time_array = time.localtime(int(created_time))
                    str_date = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time_array)

                    repo_info = {
                        'repo': metrics.get('repo'),
                        'created_at': str_date
                    }
                    repo_info.update(rank)

                    for m in METRICS:
                        action = {
                            'meticename': m,
                            'metice_percentname': m,
                            'metice_percentvalue': metrics.get(m),
                            "meticevalue": metrics.get(m),
                            'total_score': metrics.get('total_score')
                        }
                        action.update(repo_info)
                        idstr = str(metrics.get('repo')['id']) + m + str_date
                        index_data_survey = {"index": {"_index": self.index_name, "_id": idstr}}
                        actions += json.dumps(index_data_survey) + '\n'
                        actions += json.dumps(action) + '\n'
                    for m in METRICS_PERCENT:
                        action = {
                            'metice_percentname': m,
                            'metice_percentvalue': metrics.get(m),
                            'total_score': metrics.get('total_score')
                        }
                        action.update(repo_info)
                        idstr = str(metrics.get('repo')['id']) + m + str_date
                        index_data_survey = {"index": {"_index": self.index_name, "_id": idstr}}
                        actions += json.dumps(index_data_survey) + '\n'
                        actions += json.dumps(action) + '\n'
                    print('...repo(%s) collect over...' % repo_path)

                # per_page(repos)
                self.esClient.safe_put_bulk(actions)
            print('...owner(%s) collect over...' % owner)

