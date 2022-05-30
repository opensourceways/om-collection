#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2020-05
#

import json
import datetime
from collect.github import GithubClient
from data import common
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
        self.is_set_star = config.get('is_set_star')
        self.is_set_watch = config.get('is_set_watch')
        self.is_set_fork = config.get('is_set_fork')
        self.repos = config.get('repos')
        self.robot_user = config.get('robot_user')
        self.repos_name = []
        self.tokens = config.get('tokens')
        self.is_write_page = config.get('is_write_page')

    def run(self, from_date):
        print('Start collection github pr/issue/swf')
        if self.is_set_repo == 'true' and self.repos is None:
            self.write_repos()
        else:
            self.repos_name = str(self.repos).split(";")
        for repo in self.repos_name:
            client = GithubClient(org=self.org, repository=repo, token=self.github_token)
            client.tokens = self.tokens.split(',')
            client.used_tokens.append(self.github_token)
            if self.is_set_pr == 'true':
                if self.is_write_page == 'true':
                    self.write_pr_pre(client=client, repo=repo)
                else:
                    self.write_pr(client=client, repo=repo)
            if self.is_set_issue == 'true':
                if self.is_write_page == 'true':
                    self.write_issue_pre(client=client, repo=repo)
                else:
                    self.write_issue(client=client, repo=repo)
            if self.is_set_star == 'true':
                self.write_star(client=client, repo=repo)
            if self.is_set_watch == 'true':
                self.write_watch(client=client, repo=repo)
            if self.is_set_fork == 'true':
                self.write_forks(client=client, repo=repo)

        print('Finish collection github pr/issue/swf')

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
                'is_github_account': 1,
                'is_project_internal_user': 0,
            }
            index_id = 'repo_' + str(repo['id'])
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection repos of org, count=%i ******' % len(repos))

    def parse_pr(self, prs, client, repo):
        actions = ''
        for pr in prs:
            pr_num = pr.get('number')
            if pr_num is None:
                print('****** pr number is None, pr=%s ******' % pr)
                continue

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
                review_user_id = ''
                review_user_login = ''
                if review.get('user') is not None and review.get('user').get('login') is not None:
                    review_user_id = review['user']['id']
                    review_user_login = review['user']['login']
                if self.robot_user:
                    robot_users = self.robot_user.split(',')
                    if review.get('user') is not None and review.get('user').get('login') is not None and review.get('user').get('login') not in robot_users:
                        review_times.append(self.format_time_z(submitted_at))
                reviews_data = {
                    'pr_id': pr['id'],
                    'pr_number': pr_num,
                    'pr_title': pr['title'],
                    'pr_body': pr['body'],
                    'pr_review_id': review['id'],
                    'user_id': review_user_id,
                    'user_login': review_user_login,
                    'html_url': review['html_url'],
                    'pr_review_body': review['body'],
                    'pr_review_state': review['state'],
                    'created_at': self.format_time_z(submitted_at),
                    'updated_at': self.format_time_z(submitted_at),
                    'submitted_at': self.format_time_z(submitted_at),
                    'github_repo': self.org + '/' + repo,
                    'is_github_pr_review': 1,
                    'is_github_account': 1,
                    'is_project_internal_user': 0,
                }
                index_id = 'pr_review_' + repo + '_' + str(pr_num) + '_' + str(review['id'])
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
                comment_user_id = ''
                comment_user_login = ''
                if comment.get('user') is not None and comment.get('user').get('login') is not None:
                    comment_user_id = comment['user']['id']
                    comment_user_login = comment['user']['login']
                if self.robot_user:
                    robot_users = self.robot_user.split(',')
                    if comment.get('user') is not None and comment.get('user').get('login') is not None and comment.get('user').get('login') not in robot_users:
                        comment_times.append(self.format_time_z(comment['created_at']))
                comments_data = {
                    'pr_id': pr['id'],
                    'pr_number': pr_num,
                    'pr_title': pr['title'],
                    'pr_body': pr['body'],
                    'id': comment['id'],
                    'user_id': comment_user_id,
                    'user_login': comment_user_login,
                    'html_url': comment['html_url'],
                    'pr_comment_body': comment['body'],
                    'created_at': self.format_time_z(comment['created_at']),
                    'updated_at': self.format_time_z(comment['updated_at']),
                    'github_repo': self.org + '/' + repo,
                    'is_github_pr_comment': 1,
                    'is_github_comment': 1,
                    'is_github_account': 1,
                    'is_project_internal_user': 0,
                }
                index_id = 'pr_comment_' + repo + '_' + str(pr_num) + '_' + str(comment['id'])
                index_data = {"index": {"_index": self.index_name, "_id": index_id}}
                comment_actions += json.dumps(index_data) + '\n'
                comment_actions += json.dumps(comments_data) + '\n'
            self.esClient.safe_put_bulk(comment_actions)

            # pr_issue comments
            print('****** Start collection pull_issue_comments, num=%i ******' % pr_num)
            issue_comments = client.get_issue_comment(owner=self.org, repo=repo, issue_num=pr_num)
            issue_comment_actions = ''
            issue_comment_times = []
            for issue_comment in issue_comments:
                issue_comment_user_id = ''
                issue_comment_user_login = ''
                if issue_comment.get('user') is not None and issue_comment.get('user').get('login') is not None:
                    issue_comment_user_id = issue_comment['user']['id']
                    issue_comment_user_login = issue_comment['user']['login']
                if self.robot_user:
                    robot_users = self.robot_user.split(',')
                    if issue_comment.get('user') is not None and issue_comment.get('user').get('login') is not None and issue_comment.get('user').get('login') not in robot_users:
                        issue_comment_times.append(self.format_time_z(issue_comment['created_at']))
                issue_comments_data = {
                    'pr_id': pr['id'],
                    'pr_number': pr_num,
                    'pr_title': pr['title'],
                    'pr_body': pr['body'],
                    'id': issue_comment['id'],
                    'user_id': issue_comment_user_id,
                    'user_login': issue_comment_user_login,
                    'html_url': issue_comment['html_url'],
                    'pr_issue_comment_body': issue_comment['body'],
                    'created_at': self.format_time_z(issue_comment['created_at']),
                    'updated_at': self.format_time_z(issue_comment['updated_at']),
                    'github_repo': self.org + '/' + repo,
                    'is_github_pr_issue_comment': 1,
                    'is_github_comment': 1,
                    'is_github_account': 1,
                    'is_project_internal_user': 0,
                }
                index_id = 'pr_issue_comment_' + repo + '_' + str(pr_num) + '_' + str(issue_comment['id'])
                index_data = {"index": {"_index": self.index_name, "_id": index_id}}
                issue_comment_actions += json.dumps(index_data) + '\n'
                issue_comment_actions += json.dumps(issue_comments_data) + '\n'
            self.esClient.safe_put_bulk(issue_comment_actions)

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
                'time_to_close_days': common.get_time_diff_days(pr['created_at'], pr['closed_at']),
                'github_repo': self.org + '/' + repo,
                'is_github_pr': 1,
                'is_github_account': 1,
                'is_project_internal_user': 0,
            }
            if reviews and len(reviews) != 0:
                pr_data['pr_review_count'] = len(reviews)
                if len(review_times) != 0:
                    pr_data['pr_review_first'] = min(review_times)
                    pr_data['first_review_pr_time'] = (
                            datetime.datetime.strptime(pr_data['pr_review_first'], '%Y-%m-%dT%H:%M:%S+00:00')
                            - datetime.datetime.strptime(pr_data['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S+00:00')).total_seconds()
            if comments and len(comments) != 0:
                pr_data['pr_comment_count'] = len(comments)
                if len(comment_times) != 0:
                    pr_data['pr_comment_first'] = min(comment_times)
                    pr_data['first_comment_pr_time'] = (
                            datetime.datetime.strptime(pr_data['pr_comment_first'], '%Y-%m-%dT%H:%M:%S+00:00')
                            - datetime.datetime.strptime(pr_data['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S+00:00')).total_seconds()
            if issue_comments and len(issue_comments) != 0:
                pr_data['pr_issue_comment_count'] = len(issue_comments)
                if len(issue_comment_times) != 0:
                    pr_data['pr_issue_comment_first'] = min(issue_comment_times)
                    pr_data['first_comment_pr_issue_time'] = (
                            datetime.datetime.strptime(pr_data['pr_issue_comment_first'], '%Y-%m-%dT%H:%M:%S+00:00')
                            - datetime.datetime.strptime(pr_data['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S+00:00')).total_seconds()
            index_id = 'pr_' + repo + '_' + str(pr_num)
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(pr_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection pulls of repo, repo=%s, pr_count=%i ******' % (repo, len(prs)))

    # 每获取一页就入库
    def write_pr_pre(self, client, repo):
        print('****** Start collection pulls of repo, repo=%s ******' % repo)
        client.get_pr_pre(owner=self.org, repo=repo, func=self.parse_pr)

    def write_pr(self, client, repo):
        print('****** Start collection pulls of repo, repo=%s ******' % repo)
        prs = client.get_pr(owner=self.org, repo=repo)
        self.parse_pr(prs, client, repo)

    def parse_issue(self, issues, client, repo):
        actions = ''
        count = 0
        for issue in issues:
            issue_num = issue.get('number')
            if issue_num is not None and ('/issues/%i' % issue_num) not in issue['html_url']:
                continue
            count += 1
            # issue comments
            print('****** Start collection issue_comments, num=%i ******' % issue_num)
            comments = client.get_issue_comment(owner=self.org, repo=repo, issue_num=issue_num)
            comment_actions = ''
            comment_times = []
            for comment in comments:
                comment_user_id = ''
                comment_user_login = ''
                if comment.get('user') is not None and comment.get('user').get('login') is not None:
                    comment_user_id = comment['user']['id']
                    comment_user_login = comment['user']['login']
                if self.robot_user:
                    robot_users = self.robot_user.split(',')
                    if comment.get('user') is not None and comment.get('user').get('login') is not None and comment.get('user').get('login') not in robot_users:
                        comment_times.append(self.format_time_z(comment['created_at']))
                comments_data = {
                    'issue_id': issue['id'],
                    'issue_number': issue_num,
                    'id': comment['id'],
                    'user_id': comment_user_id,
                    'user_login': comment_user_login,
                    'html_url': comment['html_url'],
                    'issue_comment_body': comment['body'],
                    'created_at': self.format_time_z(comment['created_at']),
                    'updated_at': self.format_time_z(comment['updated_at']),
                    'github_repo': self.org + '/' + repo,
                    'is_github_issue_comment': 1,
                    'is_github_comment': 1,
                    'is_github_account': 1,
                    'is_project_internal_user': 0,
                }
                index_id = 'issue_comment_' + repo + '_' + str(issue_num) + '_' + str(comment['id'])
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
                'time_to_close_days': common.get_time_diff_days(issue['created_at'], issue['closed_at']),
                'github_repo': self.org + '/' + repo,
                'is_github_issue': 1,
                'is_github_account': 1,
                'is_project_internal_user': 0,
            }
            if comments and len(comments) != 0:
                issue_data['issue_comment_count'] = len(comments)
                if len(comment_times) != 0:
                    issue_data['issue_comment_first'] = min(comment_times)
                    issue_data['first_comment_issue_time'] = (
                            datetime.datetime.strptime(issue_data['issue_comment_first'], '%Y-%m-%dT%H:%M:%S+00:00')
                            - datetime.datetime.strptime(issue_data['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S+00:00')).total_seconds()
            index_id = 'issue_' + repo + '_' + str(issue_num)
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(issue_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection issues of repo, repo=%s, issue_count=%i ******' % (repo, count))

    # 每获取一页就入库
    def write_issue_pre(self, client, repo):
        print('****** Start collection issues of repo, repo=%s ******' % repo)
        client.get_issue_pre(owner=self.org, repo=repo, func=self.parse_issue)

    def write_issue(self, client, repo):
        print('****** Start collection issues of repo, repo=%s ******' % repo)
        issues = client.get_issue(owner=self.org, repo=repo)
        self.parse_issue(issues, client, repo)

    def write_star(self, client, repo):
        print('****** Start collection stars of repo, repo=%s ******' % repo)
        stars = client.get_swf(owner=self.org, repo=repo, item='stargazers')
        actions = ''
        for star in stars:
            user = star['user']
            star_data = {
                'created_at': self.format_time_z(star['starred_at']),
                'user_login': user['login'],
                'user_id': user['id'],
                'node_id': user['node_id'],
                'html_url': user['html_url'],
                'site_admin': user['site_admin'],
                'github_repo': self.org + '/' + repo,
                'is_github_star': 1,
                'is_github_account': 1,
                'is_project_internal_user': 0,
            }
            index_id = 'star_' + repo + '_' + str(user['id'])
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(star_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection stars of repo, repo=%s, star_count=%i ******' % (repo, len(stars)))

    # TODO no created_at
    def write_watch(self, client, repo):
        print('****** Start collection watches of repo, repo=%s ******' % repo)
        watches = client.get_swf(owner=self.org, repo=repo, item='subscribers')
        actions = ''
        for watch in watches:
            watch_data = {
                # 'created_at': self.format_time_z(star['starred_at']),
                # 'user_login': user['login'],
                # 'user_id': user['id'],
                # 'node_id': user['node_id'],
                # 'html_url': user['html_url'],
                # 'site_admin': user['site_admin'],
                # 'github_repo': self.org + '/' + repo,
                'is_github_star': 1,
                'is_github_account': 1,
                'is_project_internal_user': 0,
            }
            index_id = 'watch_' + repo + '_' + str("user['id']")
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(watch_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection watches of repo, repo=%s, star_count=%i ******' % (repo, len(watches)))

    def write_forks(self, client, repo):
        print('****** Start collection forks of repo, repo=%s ******' % repo)
        forks = client.get_swf(owner=self.org, repo=repo, item='forks')
        actions = ''
        for fork in forks:
            user = fork['owner']
            fork_data = {
                'created_at': self.format_time_z(fork['created_at']),
                'updated_at': self.format_time_z(fork['updated_at']),
                'html_url': fork['html_url'],
                'user_login': user['login'],
                'user_id': user['id'],
                'node_id': user['node_id'],
                'site_admin': user['site_admin'],
                'github_repo': self.org + '/' + repo,
                'is_github_fork': 1,
                'is_github_account': 1,
                'is_project_internal_user': 0,
            }
            index_id = 'fork_' + repo + '_' + str(user['id'])
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(fork_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection forks of repo, repo=%s, pr_count=%i ******' % (repo, len(forks)))

    @staticmethod
    def format_time_z(time_str):
        if time_str is None or time_str == '':
            return None
        return str(time_str).replace('Z', '+00:00')
