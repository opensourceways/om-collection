import base64
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

GITEE_BASE = "gitee.com"
GITHUB_BASE = "github.com"


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
        self.gitee_repo_branch = config.get('gitee_repo_branch')
        self.github_repo_branch = config.get('github_repo_branch')
        self.username = config.get('username')
        self.password = config.get('password')
        self.esClient = ESClient(config)

    def run(self, from_time):
        print("Git commit log collect: start")
        # 配置默认获取最近 <before_days> 天的数据
        if self.start_date is None and self.before_days:
            self.start_date = datetime.date.today() + datetime.timedelta(days=-int(self.before_days))

        # 代码托管平台 gitee or github
        for items in self.platform_owner_token.split(';'):
            if not str(items).__contains__('->'):
                continue
            vs = items.split('->')
            platform = vs[0]
            owner = vs[1]
            token = None if vs[2] == '' else vs[2]

            # 指定了仓库则获取指定仓库数据，否则获取owner下的所有仓库
            repos = []
            if platform == 'gitee':
                if self.gitee_repo_branch:
                    repos = self.gitee_repo_branch.split(';')
                else:
                    repos = self.gitee_repos(owner=owner, token=token)
            elif platform == 'github':
                if self.github_repo_branch:
                    repos = self.github_repo_branch.split(';')
                else:
                    repos = self.github_repos(owner=owner, token=token)

            for repo in repos:
                if not str(repo).__contains__('->'):
                    continue
                rb = repo.split('->')
                self.getLog(platform, owner, repo_name=rb[0], branch_name=rb[1])

    def getLog(self, platform, owner, repo_name, branch_name):
        # 本地仓库目录
        owner_path = self.code_base_path + platform + '/' + owner + '/'
        if not os.path.exists(owner_path):
            os.makedirs(owner_path)
        code_path = owner_path + repo_name

        username = base64.b64decode(self.username).decode()
        passwd = base64.b64decode(self.password).decode()
        if platform == 'gitee':
            remote_repo = 'https://%s:%s@%s/%s/%s' % (username, passwd, GITEE_BASE, owner, repo_name)
        elif platform == 'github':
            remote_repo = 'https://%s:%s@%s/%s/%s' % (username, passwd, GITHUB_BASE, owner, repo_name)
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

        try:
            repo = git.Repo(code_path)
        except Exception:
            return
        if branch_name != '':
            # checkout到指定分支获取数据
            print('*** start %s repo: %s/%s; branch: %s ***' % (platform, owner, repo_name, branch_name))
            repo.git.checkout(branch_name)
            merge_commits = list(repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name, merges=True))
            self.parse_commits(merge_commits, platform, owner, branch_name, remote_repo, 1)
            no_merge_commits = list(repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name, no_merges=True))
            self.parse_commits(no_merge_commits, platform, owner, branch_name, remote_repo, 0)
        else:
            # 遍历所有分支，获取数据
            for branch in repo.git.branch('-r').split('\n'):
                if branch.startswith('  origin/HEAD ->'):
                    continue
                branch_name = branch.split('/')[1]
                print('*** start %s repo: %s/%s; branch: %s ***' % (platform, owner, repo_name, branch_name))
                repo.git.checkout(branch_name)
                merge_commits = list(repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name, merges=True))
                self.parse_commits(merge_commits, platform, owner, branch_name, remote_repo, 1)
                no_merge_commits = list(repo.iter_commits(since=self.start_date, until=self.end_date, author=self.user_commit_name, no_merges=True))
                self.parse_commits(no_merge_commits, platform, owner, branch_name, remote_repo, 0)

    def parse_commits(self, commits, platform, owner, branch, repo_url, is_merge):
        print(' -> is merge: %d, commit count: %d' % (is_merge, len(commits)))
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
                'is_merge': is_merge,
            }
            id_str = action['commit_url'] + '-' + branch
            index_id = hashlib.md5(id_str.encode('utf-8')).hexdigest()
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
