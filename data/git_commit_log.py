import datetime
import hashlib
import json
import os
import types
from json import JSONDecodeError

import git

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data.common import ESClient

GITEE_BASE_URL = "https://gitee.com/"
GITHUB_BASE_URL = "https://github.com/"


class GitCommitLog(object):
    def __init__(self, config=None):
        self.config = config
        self.org = config.get('org')
        self.index_name = config.get('index_name')
        self.code_base_path = config.get('code_base_path')
        self.platform_owner_token = config.get('platform_owner_token')
        self.start_date = config.get('start_date')
        self.end_date = config.get('end_date')
        self.before_days = config.get('before_days')
        self.user_commit_name = config.get('user_commit_name')
        self.repo_branch = config.get('repo_branch')
        self.esClient = ESClient(config)

    def run(self, from_time):
        print("Git commit log collect: start")
        # 配置默认获取最近 <before_days> 天的数据
        if self.start_date is None and self.before_days:
            self.start_date = datetime.date.today() + datetime.timedelta(days=-int(self.before_days))

        # 代码托管平台 gitee or github
        for items in self.platform_owner_token.split(';'):
            vs = items.split('->')
            platform = vs[0]
            owner = vs[1]
            token = None if vs[2] == '' else vs[2]

            # 指定了仓库则获取指定仓库数据，否则获取owner下的所有仓库
            repos = []
            if self.repo_branch:
                repos = self.repo_branch.split(';')
            else:
                if platform == 'gitee':
                    repos = self.gitee_repos(owner=owner, token=token)
                elif platform == 'github':
                    repos = self.github_repos(owner=owner, token=token)

            for repo in repos:
                rb = repo.split('->')
                self.getLog(platform, owner, repo_name=rb[0], branch_name=rb[1])

    def getLog(self, platform, owner, repo_name, branch_name):
        # 本地仓库目录
        owner_path = self.code_base_path + platform + '/' + owner + '/'
        if not os.path.exists(owner_path):
            os.makedirs(owner_path)
        code_path = owner_path + repo_name

        if platform == 'gitee':
            remote_repo = GITEE_BASE_URL + owner + '/' + repo_name
        elif platform == 'github':
            remote_repo = GITHUB_BASE_URL + owner + '/' + repo_name
        else:
            remote_repo = None

        # 本地仓库已存在执行git pull；否则执行git clone
        if os.path.exists(code_path):
            cmd_pull = 'cd %s;git pull' % code_path
            os.system(cmd_pull)
        else:
            if remote_repo is None:
                return
            cmd_clone = 'cd %s;git clone %s' % (owner_path, remote_repo + '.git')
            os.system(cmd_clone)

        repo = git.Repo(code_path)
        if branch_name != '':
            # checkout到指定分支获取数据
            print('*** start repo: %s/%s; branch: %s ***' % (owner, repo_name, branch_name))
            repo.git.checkout(branch_name)
            commits = list(repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name))
            self.parse_commits(commits, platform, owner, branch_name, remote_repo)
        else:
            # 遍历所有分支，获取数据
            for branch in repo.git.branch('-r').split('\n'):
                if branch.startswith('  origin/HEAD ->'):
                    continue
                branch_name = branch.split('/')[1]
                print('*** start repo: %s/%s; branch: %s ***' % (owner, repo_name, branch_name))
                repo.git.checkout(branch_name)
                commits = list(
                    repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name))
                self.parse_commits(commits, platform, owner, branch_name, remote_repo)

    def parse_commits(self, commits, platform, owner, branch, repo_url):
        print(' -> commit count: %d' % len(commits))
        actions = ''
        for commit in commits:
            file_code = commit.stats.total
            action = {
                'commit_id': commit.hexsha,
                'created_at': str(commit.committed_datetime).replace(' ', 'T'),
                'author': commit.author.name,
                'email': commit.author.email,
                'title': commit.summary,
                'body': commit.message,
                'file_changed': file_code['files'],
                'add': file_code['insertions'],
                'remove': file_code['deletions'],
                'total': file_code['lines'],
                'branch': branch,
                'repo': repo_url,
                'owner': owner,
                'org': self.org,
                'platform': platform,
                'commit_url': repo_url + '/commit/' + commit.hexsha,
            }
            index_id = hashlib.md5(action['commit_url'].encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def gitee_repos(self, owner, token):
        client = GiteeClient(owner, None, token)
        repos = self.getGenerator(client.org())
        repos_names = []
        for repo in repos:
            repos_names.append(repo['path'] + '->')
        return repos_names

    def github_repos(self, owner, token):
        client = GithubClient(org=owner, repository=None, token=token)
        repos = client.get_repos(org=owner)
        repos_names = []
        for repo in repos:
            repos_names.append(repo['name'] + '->')
        return repos_names

    def getGenerator(self, response):
        data = []
        try:
            while 1:
                if isinstance(response, types.GeneratorType):
                    res_data = next(response)
                    if isinstance(res_data, str):
                        data += json.loads(res_data.encode('utf-8'))
                    else:
                        data += json.loads(res_data.decode('utf-8'))
                else:
                    data = json.loads(response)
                    break
        except StopIteration:
            return data
        except JSONDecodeError:
            print("Gitee get JSONDecodeError, error: ", response)
        except Exception as ex:
            print('*** getGenerator fail ***', ex)
            return data

        return data