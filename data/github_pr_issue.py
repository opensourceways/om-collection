import json

from collect.github import GithubClient
from data.common import ESClient


class GitHubPrIssue(object):
    def __init__(self, config=None):
        self.config = config
        self.org = config.get('org')
        self.esClient = ESClient(config)
        self.github_token = config.get('github_token')
        self.index_name = config.get('index_name')
        self.is_set_repo = config.get('is_set_repo')
        self.is_set_pr = config.get('is_set_pr')
        self.is_set_issue = config.get('is_set_issue')
        self.repos = config.get('repos')
        self.repos_name = []

    def run(self, from_date):
        print('Start collection github pr/issue')
        if self.is_set_repo == 'true' and self.repos is None:
            self.write_repos()
        else:
            self.repos_name = str(self.repos).split(";")

        for repo in self.repos_name:
            client = GithubClient(org=self.org, repository=repo, token=self.github_token)
            if self.is_set_pr == 'true':
                self.write_pr(client=client, repo=repo)
            if self.is_set_issue == 'true':
                self.write_issue(client=client, repo=repo)

        print('Finish collection github pr/issue')

    def write_repos(self):
        print('****** Start collection repos of org ******')
        client = GithubClient(org=self.org, repository=None, token=self.github_token)
        repos = client.get_repos(org=self.org)
        actions = ''
        for repo in repos:
            self.repos_name.append(repo['name'])
            repo_data = {
                'repo_id': repo['id'],
                'name': repo['name'],
                'full_name': repo['full_name'],
                'github_repo': repo['full_name'],
                'user_id': repo['owner']['id'],
                'user_login': repo['owner']['login'],
                'private': repo['private'],
                'html_url': repo['html_url'],
                'description': repo['description'],
                'fork': repo['fork'],
                'language': repo['language'],
                'forks_count': repo['forks_count'],
                'stargazers_count': repo['stargazers_count'],
                'watchers_count': repo['watchers_count'],
                'created_at': self.format_time_z(repo['created_at']),
                'updated_at': self.format_time_z(repo['updated_at']),
                'is_github_repo': 1,
            }
            index_id = 'repo_' + str(repo['id'])
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection repos of org, count=%i ******' % len(repos))

    def write_pr(self, client, repo):
        print('****** Start collection pulls of repo, repo=%s ******' % repo)
        prs = client.get_pr(owner=self.org, repo=repo)
        actions = ''
        for pr in prs:
            pr_num = pr['number']
            # pr reviews
            print('****** Start collection pull_reviews, num=%i ******' % pr_num)
            reviews = client.get_pr_review(owner=self.org, repo=repo, pr_num=pr_num)
            review_actions = ''
            review_times = []
            for review in reviews:
                try:
                    submitted_at = review['submitted_at']
                except KeyError:
                    submitted_at = pr['created_at']
                review_times.append(self.format_time_z(submitted_at))
                reviews_data = {
                    'pr_id': pr['id'],
                    'pr_number': pr_num,
                    'pr_review_id': review['id'],
                    'user_id': review['user']['id'],
                    'user_login': review['user']['login'],
                    'html_url': review['html_url'],
                    'pr_review_body': review['body'],
                    'pr_review_state': review['state'],
                    'created_at': self.format_time_z(submitted_at),
                    'updated_at': self.format_time_z(submitted_at),
                    'submitted_at': self.format_time_z(submitted_at),
                    'github_repo': self.org + '/' + repo,
                    'is_github_pr_review': 1,
                }
                index_id = 'pr_review_' + str(pr_num) + '_' + str(review['id'])
                index_data = {"index": {"_index": self.index_name, "_id": index_id}}
                review_actions += json.dumps(index_data) + '\n'
                review_actions += json.dumps(reviews_data) + '\n'
            self.esClient.safe_put_bulk(review_actions)

            # pr comments
            print('****** Start collection pull_comments, num=%i ******' % pr_num)
            comments = client.get_pr_comment(owner=self.org, repo=repo, pr_num=pr_num)
            comment_actions = ''
            comment_times = []
            for comment in comments:
                comment_times.append(self.format_time_z(comment['created_at']))
                comments_data = {
                    'pr_id': pr['id'],
                    'pr_number': pr_num,
                    'id': comment['id'],
                    'user_id': comment['user']['id'],
                    'user_login': comment['user']['login'],
                    'html_url': comment['html_url'],
                    'pr_comment_body': comment['body'],
                    'created_at': self.format_time_z(comment['created_at']),
                    'updated_at': self.format_time_z(comment['updated_at']),
                    'github_repo': self.org + '/' + repo,
                    'is_github_pr_comment': 1,
                    'is_github_comment': 1,
                }
                index_id = 'pr_comment_' + str(pr_num) + '_' + str(comment['id'])
                index_data = {"index": {"_index": self.index_name, "_id": index_id}}
                comment_actions += json.dumps(index_data) + '\n'
                comment_actions += json.dumps(comments_data) + '\n'
            self.esClient.safe_put_bulk(comment_actions)

            # pr
            pr_data = {
                'pr_id': pr['id'],
                'html_url': pr['html_url'],
                'pr_number': pr['number'],
                'user_id': pr['user']['id'],
                'user_login': pr['user']['login'],
                'pr_state': pr['state'],
                'pr_title': pr['title'],
                'pr_body': pr['body'],
                'created_at': self.format_time_z(pr['created_at']),
                'updated_at': self.format_time_z(pr['updated_at']),
                'closed_at': self.format_time_z(pr['closed_at']),
                'merged_at': self.format_time_z(pr['merged_at']),
                'github_repo': self.org + '/' + repo,
                'is_github_pr': 1,
            }
            if reviews:
                pr_data['pr_review_count'] = len(reviews)
                pr_data['pr_review_first'] = min(review_times)
            if comments:
                pr_data['pr_comment_count'] = len(comments)
                pr_data['pr_comment_first'] = min(comment_times)
            index_id = 'pr_' + str(pr_num)
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(pr_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection pulls of repo, repo=%s, pr_count=%i ******' % (repo, len(prs)))

    def write_issue(self, client, repo):
        print('****** Start collection issues of repo, repo=%s ******' % repo)
        issues = client.get_issue(owner=self.org, repo=repo)
        actions = ''
        count = 0
        for issue in issues:
            issue_num = issue['number']
            if ('/issues/%i' % issue_num) not in issue['html_url']:
                continue
            count += 1
            # issue comments
            print('****** Start collection issue_comments, num=%i ******' % issue_num)
            comments = client.get_issue_comment(owner=self.org, repo=repo, issue_num=issue_num)
            comment_actions = ''
            comment_times = []
            for comment in comments:
                comment_times.append(self.format_time_z(comment['created_at']))
                comments_data = {
                    'issue_id': issue['id'],
                    'issue_number': issue_num,
                    'id': comment['id'],
                    'user_id': comment['user']['id'],
                    'user_login': comment['user']['login'],
                    'html_url': comment['html_url'],
                    'issue_comment_body': comment['body'],
                    'created_at': self.format_time_z(comment['created_at']),
                    'updated_at': self.format_time_z(comment['updated_at']),
                    'github_repo': self.org + '/' + repo,
                    'is_github_issue_comment': 1,
                    'is_github_comment': 1,
                }
                index_id = 'issue_comment_' + str(issue_num) + '_' + str(comment['id'])
                index_data = {"index": {"_index": self.index_name, "_id": index_id}}
                comment_actions += json.dumps(index_data) + '\n'
                comment_actions += json.dumps(comments_data) + '\n'
            self.esClient.safe_put_bulk(comment_actions)

            # issue
            issue_data = {
                'issue_id': issue['id'],
                'html_url': issue['html_url'],
                'issue_number': issue['number'],
                'user_id': issue['user']['id'],
                'user_login': issue['user']['login'],
                'issue_state': issue['state'],
                'issue_title': issue['title'],
                'issue_body': issue['body'],
                'created_at': self.format_time_z(issue['created_at']),
                'updated_at': self.format_time_z(issue['updated_at']),
                'closed_at': self.format_time_z(issue['closed_at']),
                'github_repo': self.org + '/' + repo,
                'is_github_issue': 1,
            }
            if comments:
                issue_data['issue_comment_count'] = len(comments)
                issue_data['issue_comment_first'] = min(comment_times)
            index_id = 'issue_' + str(issue_num)
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(issue_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection issues of repo, repo=%s, issue_count=%i ******' % (repo, count))

    @staticmethod
    def format_time_z(time_str):
        if time_str is None or time_str == '':
            return None
        return str(time_str).replace('Z', '+00:00')
