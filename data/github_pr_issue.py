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
        self.tokens = config.get('tokens').split(',') if config.get('tokens') else []
        self.is_write_page = config.get('is_write_page')
        self.from_page = config.get('from_page')
        self.end_page = config.get('end_page')
        self.label_removed_swf = config.get('label_removed_swf', 'false')
        self.index_name_org = config.get('index_name_org')
        self.user_org_dic = {}

    def run(self, from_date):
        dic = self.esClient.getOrgByGiteeID()
        self.user_org_dic = dic[0]
        print('Start collection github pr/issue/swf')
        if self.is_set_repo == 'true' and self.repos is None:
            self.write_repos()
        else:
            self.repos_name = str(self.repos).split(";")
        for repo in self.repos_name:
            client = GithubClient(org=self.org, repository=repo, token=self.github_token, tokens=self.tokens)
            if self.from_page:
                client.from_page = int(self.from_page)
            if self.end_page:
                client.end_page = int(self.end_page)
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
            if self.label_removed_swf == 'true':
                self.tag_removed_swf(client)
            if self.is_set_star == 'true':
                self.write_star(client=client, repo=repo)
            if self.is_set_watch == 'true':
                self.write_watch(client=client, repo=repo)
            if self.is_set_fork == 'true':
                self.write_forks(client=client, repo=repo)

        print('Finish collection github pr/issue/swf')

    def write_repos(self):
        print('****** Start collection repos of org ******')
        client = GithubClient(org=self.org, repository=None, token=self.github_token, tokens=self.tokens)
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
            repo_data.update(self.get_user_info(repo['owner']['login']))
            index_id = 'repo_' + str(repo['id'])
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection repos of org, count=%i ******' % len(repos))

    def parse_pr(self, prs, client, repo):
        # 获取PR信息
        for pr in prs:
            actions = ''
            pr_num = pr.get('number')
            print('****** Start collection pull num=%i ******' % pr_num)
            if pr_num is None:
                print('****** pr number is None, pr=%s ******' % pr)
                continue

            # 获取PR的代码review信息
            reviews = client.get_pr_review(owner=self.org, repo=repo, pr_num=pr_num)
            review_actions = ''
            review_times = []
            for review in reviews:
                print('****** Start collection pull_reviews, num=%i ******' % review.get('id'))
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
                    if review.get('user') is not None and review.get('user').get('login') is not None and \
                            review.get('user').get('login') not in robot_users and \
                            review.get('user').get('login') != pr['user']['login']:
                        review_times.append(self.format_time_z(submitted_at))
                else:
                    review_times.append(self.format_time_z(submitted_at))
                reviews_data = {
                    'pr_id': pr['id'],
                    'pr_number': pr_num,
                    'pr_title': pr['title'],
                    'pr_body': pr['body'],
                    'id': review['id'],
                    'pr_review_id': review['id'],
                    'user_id': review_user_id,
                    'user_login': review_user_login,
                    'html_url': review['html_url'],
                    'pr_comment_body': review['body'],
                    'pr_review_state': review['state'],
                    'created_at': self.format_time_z(submitted_at),
                    'updated_at': self.format_time_z(submitted_at),
                    'submitted_at': self.format_time_z(submitted_at),
                    'github_repo': self.org + '/' + repo,
                    'is_github_pr_review': 1,
                    'is_github_account': 1,
                    'is_project_internal_user': 0,
                }
                reviews_data.update(self.get_user_info(review_user_login))
                index_id = 'pr_review_' + repo + '_' + str(pr_num) + '_' + str(review['id'])
                index_data = {"index": {"_index": self.index_name, "_id": index_id}}
                review_actions += json.dumps(index_data) + '\n'
                review_actions += json.dumps(reviews_data) + '\n'
            actions += review_actions
            # self.esClient.safe_put_bulk(review_actions)

            # 获取PR的代码提交代码review时的comment信息
            comments = client.get_pr_comment(owner=self.org, repo=repo, pr_num=pr_num)
            comment_actions = ''
            comment_times = []
            for comment in comments:
                print('****** Start collection pull_comments, num=%i ******' % comment.get('id'))
                comment_user_id = ''
                comment_user_login = ''
                if comment.get('user') is not None and comment.get('user').get('login') is not None:
                    comment_user_id = comment['user']['id']
                    comment_user_login = comment['user']['login']
                if self.robot_user:
                    robot_users = self.robot_user.split(',')
                    if comment.get('user') is not None and comment.get('user').get('login') is not None and \
                            comment.get('user').get('login') not in robot_users and \
                            comment.get('user').get('login') != pr['user']['login']:
                        comment_times.append(self.format_time_z(comment['created_at']))
                else:
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
                comments_data.update(self.get_user_info(comment_user_login))
                index_id = 'pr_comment_' + repo + '_' + str(pr_num) + '_' + str(comment['id'])
                index_data = {"index": {"_index": self.index_name, "_id": index_id}}
                comment_actions += json.dumps(index_data) + '\n'
                comment_actions += json.dumps(comments_data) + '\n'
            actions += comment_actions
            # self.esClient.safe_put_bulk(comment_actions)

            # 获取直接回复PR的评论信息
            # 注:不是代码review，在pr下方评论框回复，由于pr源于issue，因此被叫做issue comment
            issue_comments = client.get_issue_comment(owner=self.org, repo=repo, issue_num=pr_num)
            issue_comment_actions = ''
            issue_comment_times = []
            for issue_comment in issue_comments:
                print('****** Start collection pull_issue_comments, num=%i ******' % issue_comment.get('id'))
                issue_comment_user_id = ''
                issue_comment_user_login = ''
                if issue_comment.get('user') is not None and issue_comment.get('user').get('login') is not None:
                    issue_comment_user_id = issue_comment['user']['id']
                    issue_comment_user_login = issue_comment['user']['login']
                if self.robot_user:
                    robot_users = self.robot_user.split(',')
                    if issue_comment.get('user') is not None and issue_comment.get('user').get('login') is not None \
                            and issue_comment.get('user').get('login') not in robot_users \
                            and issue_comment.get('user').get('login') != pr['user']['login']:
                        issue_comment_times.append(self.format_time_z(issue_comment['created_at']))
                else:
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
                    'pr_comment_body': issue_comment['body'],
                    'created_at': self.format_time_z(issue_comment['created_at']),
                    'updated_at': self.format_time_z(issue_comment['updated_at']),
                    'github_repo': self.org + '/' + repo,
                    'is_github_pr_issue_comment': 1,
                    'is_github_comment': 1,
                    'is_github_account': 1,
                    'is_project_internal_user': 0,
                }
                issue_comments_data.update(self.get_user_info(issue_comment_user_login))
                index_id = 'pr_issue_comment_' + repo + '_' + str(pr_num) + '_' + str(issue_comment['id'])
                index_data = {"index": {"_index": self.index_name, "_id": index_id}}
                issue_comment_actions += json.dumps(index_data) + '\n'
                issue_comment_actions += json.dumps(issue_comments_data) + '\n'
            actions += issue_comment_actions
            # self.esClient.safe_put_bulk(issue_comment_actions)

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
                'time_to_close_seconds': common.get_time_diff_seconds(pr['created_at'], pr['closed_at']),
                'github_repo': self.org + '/' + repo,
                'is_github_pr': 1,
                'is_github_account': 1,
                'is_project_internal_user': 0,
                'head_label': pr['head']['label'],
                'head_label_ref': pr['head']['ref'],
                'base_label': pr['base']['label'],
                'base_label_ref': pr['base']['ref'],
            }
            if pr['merged_at']:
                pull_info = client.get_pull_by_number(self.org, repo, pr_num)
                pr_data.update({
                    'additions': pull_info[0]['additions'],
                    'deletions': pull_info[0]['deletions'],
                    'changed_files': pull_info[0]['changed_files']
                })
            pr_data.update(self.get_user_info(pr['user']['login']))
            pr_first_reply_times = []
            if reviews and len(reviews) != 0:
                pr_data['pr_review_count'] = len(reviews)
                if len(review_times) != 0:
                    pr_data['pr_review_first'] = min(review_times)
                    first_review_pr_time = (
                                datetime.datetime.strptime(pr_data['pr_review_first'], '%Y-%m-%dT%H:%M:%S+00:00')
                                - datetime.datetime.strptime(pr_data['created_at'],
                                                             '%Y-%m-%dT%H:%M:%S+00:00')).total_seconds()
                    pr_data['first_review_pr_time'] = first_review_pr_time
                    pr_first_reply_times.append(first_review_pr_time)
            if comments and len(comments) != 0:
                pr_data['pr_comment_count'] = len(comments)
                if len(comment_times) != 0:
                    pr_data['pr_comment_first'] = min(comment_times)
                    first_comment_pr_time = (
                            datetime.datetime.strptime(pr_data['pr_comment_first'], '%Y-%m-%dT%H:%M:%S+00:00')
                            - datetime.datetime.strptime(pr_data['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S+00:00')).total_seconds()
                    pr_data['first_comment_pr_time'] = first_comment_pr_time
                    pr_first_reply_times.append(first_comment_pr_time)
            if issue_comments and len(issue_comments) != 0:
                pr_data['pr_issue_comment_count'] = len(issue_comments)
                if len(issue_comment_times) != 0:
                    pr_data['pr_issue_comment_first'] = min(issue_comment_times)
                    first_comment_pr_issue_time = (
                            datetime.datetime.strptime(pr_data['pr_issue_comment_first'], '%Y-%m-%dT%H:%M:%S+00:00')
                            - datetime.datetime.strptime(pr_data['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S+00:00')).total_seconds()
                    pr_data['first_comment_pr_issue_time'] = first_comment_pr_issue_time
                    pr_first_reply_times.append(first_comment_pr_issue_time)
            if len(pr_first_reply_times) == 0:
                pr_first_reply_time = -1
            else:
                pr_first_reply_time = min(pr_first_reply_times)
            pr_data['first_reply_time'] = pr_first_reply_time

            index_id = 'pr_' + repo + '_' + str(pr_num)
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(pr_data) + '\n'
            self.esClient.safe_put_bulk(actions)
        # self.esClient.safe_put_bulk(actions)
        print('****** Finish collection pulls of repo, repo=%s, pr_count=%i ******' % (repo, len(prs)))

    # 每获取一页的PR信息就写入数据库
    def write_pr_pre(self, client, repo):
        print('****** Start perpage collection pulls of repo, repo=%s ******' % repo)
        client.get_pr_pre(owner=self.org, repo=repo, func=self.parse_pr)

    # 获取完所有的PR信息再写入数据库
    def write_pr(self, client, repo):
        print('****** Start collection pulls of repo, repo=%s ******' % repo)
        prs = client.get_pr(owner=self.org, repo=repo)
        self.parse_pr(prs, client, repo)

    def parse_issue(self, issues, client, repo):
        count = 0
        for issue in issues:
            actions = ''
            issue_num = issue.get('number')
            print('****** Start collection issue, num=%i ******' % issue_num)
            if issue_num is not None and ('/issues/%i' % issue_num) not in issue['html_url']:
                continue
            count += 1
            # issue comments
            
            comments = client.get_issue_comment(owner=self.org, repo=repo, issue_num=issue_num)
            comment_actions = ''
            comment_times = []
            for comment in comments:
                comment_user_id = ''
                comment_user_login = ''

                print('****** Start collection issue_comments, num=%i ******' % comment.get('id'))
                if comment.get('user') is not None and comment.get('user').get('login') is not None:
                    comment_user_id = comment['user']['id']
                    comment_user_login = comment['user']['login']
                if self.robot_user:
                    robot_users = self.robot_user.split(',')
                    if comment.get('user') is not None and comment.get('user').get('login') is not None and \
                            comment.get('user').get('login') not in robot_users and \
                            comment.get('user').get('login') != issue['user']['login']:
                        comment_times.append(self.format_time_z(comment['created_at']))
                else:
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
                comments_data.update(self.get_user_info(comment_user_login))
                index_id = 'issue_comment_' + repo + '_' + str(issue_num) + '_' + str(comment['id'])
                index_data = {"index": {"_index": self.index_name, "_id": index_id}}
                comment_actions += json.dumps(index_data) + '\n'
                comment_actions += json.dumps(comments_data) + '\n'
            actions += comment_actions
            # self.esClient.safe_put_bulk(comment_actions)

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
                'time_to_close_seconds': common.get_time_diff_seconds(issue['created_at'], issue['closed_at']),
                'github_repo': self.org + '/' + repo,
                'is_github_issue': 1,
                'is_github_account': 1,
                'is_project_internal_user': 0,
            }
            issue_data.update(self.get_user_info(issue['user']['login']))
            if comments and len(comments) != 0:
                issue_data['issue_comment_count'] = len(comments)
                if len(comment_times) != 0:
                    issue_data['issue_comment_first'] = min(comment_times)
                    issue_data['first_comment_issue_time'] = (
                            datetime.datetime.strptime(issue_data['issue_comment_first'], '%Y-%m-%dT%H:%M:%S+00:00')
                            - datetime.datetime.strptime(issue_data['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S+00:00')).total_seconds()
                    issue_data['first_reply_time'] = issue_data['first_comment_issue_time']
                else:
                    issue_data['first_reply_time'] = -1
            else:
                issue_data['first_reply_time'] = -1
            index_id = 'issue_' + repo + '_' + str(issue_num)
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(issue_data) + '\n'
            self.esClient.safe_put_bulk(actions)
        # self.esClient.safe_put_bulk(actions)
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
            star_data.update(self.get_user_info(user['login']))
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
                'github_repo': self.org + '/' + repo,
                'user_id': watch['id'],
                'is_github_watch': 1,
                'is_github_account': 1,
                'is_project_internal_user': 0,
            }
            watch_data.update(self.get_user_info(watch['user']['login']))
            index_id = 'watch_' + repo + '_' + str(watch['id'])
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
                'fork_id': fork['id'],
                'node_id': user['node_id'],
                'site_admin': user['site_admin'],
                'github_repo': self.org + '/' + repo,
                'is_github_fork': 1,
                'is_github_account': 1,
                'is_project_internal_user': 0,
            }
            fork_data.update(self.get_user_info(user['login']))
            index_id = 'fork_' + repo + '_' + str(user['id'])
            index_data = {"index": {"_index": self.index_name, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(fork_data) + '\n'
        self.esClient.safe_put_bulk(actions)
        print('****** Finish collection forks of repo, repo=%s, pr_count=%i ******' % (repo, len(forks)))

    def updateRemovedData(self, newdata, type, matches, matchs_not=None, repo=None):
        original_ids = []
        if newdata is None or not isinstance(newdata, list):
            return original_ids
        data = self.esClient.getItemsByMatchs(matches, size=10000, matchs_not=matchs_not)
        newdataids = []
        if not data:
            return original_ids
        data_num = data['hits']['total']['value']
        original_datas = data['hits']['hits']
        for d in newdata:
            if type == 'star':  # star watch数据都只有用户id，只能使用type_仓库_用户id来进行区分
                user_id = d['user']['id']
                newdataids.append('_'.join([type, repo, str(user_id)]))
            elif type == 'watch':
                newdataids.append('_'.join([type, repo, str(d['id'])]))
            else:
                newdataids.append(d['id'])
        if data_num == len(newdata): # 旧数据条数等于新数据条数时认为没有变化
            return original_ids

        for ordata in original_datas:
            if type == "pr":
                db_id = ordata['_source']["pull_id"]
            elif type == 'star' or type == 'watch':
                db_id = ordata['_id']
            else:
                db_id_key = type + "_id"
                db_id = ordata['_source'][db_id_key]

            original_ids.append(db_id)
            if db_id not in newdataids:
                print("[update] set {}({}) is_removed to 1".format(type, db_id))
                if type == 'issue': # 标记issue comment
                    issue_id = ordata['_source']['issue_id']
                    query = '''{
                          "script": {
                            "source": "ctx._source['is_removed']=1"
                          },
                          "query": {
                            "term": {
                              "issue_id": %d
                            }
                          }
                        }''' % issue_id
                    self.esClient.updateByQuery(query=query)
                    print('*** tag removed issue: %s' % ordata['_id'])
                elif type == 'repo': # 标记仓库所有贡献
                    repo = ordata['_source']['github_repo']
                    query = '''{
                          "script": {
                            "source": "ctx._source['is_removed']=1"
                          },
                          "query": {
                            "term": {
                              "github_repo.keyword": "%s"
                            }
                          }
                        }''' % repo
                    self.esClient.updateByQuery(query=query)
                    print('*** tag removed repo: %s' % repo)
                self.esClient.updateToRemoved(ordata['_id'])

        return original_ids

    def tag_removed_swf(self, client):
        repo_data = client.repo()
        watch_num = repo_data['subscribers_count']
        fork_num = repo_data['forks_count']
        star_num = repo_data['stargazers_count']

        # label forks
        forks = client.get_swf(owner=self.org, repo=client.repository, item='forks')
        self.updateRemovedData(forks, 'fork', [
            {
                "name": "is_github_fork",
                "value": 1
            },
            {
                "name": "github_repo.keyword",
                "value": self.org + "/" + client.repository
            }], 
            matchs_not=[{"name": "is_removed", "value": 1}]
        )

        # label stars
        stars = client.get_swf(owner=self.org, repo=client.repository, item='stargazers')
        self.updateRemovedData(stars, 'star', [
            {
                "name": "is_github_star",
                "value": 1
            },
            {
                "name": "github_repo.keyword",
                "value": self.org + "/" + client.repository
            }], 
            matchs_not=[{"name": "is_removed", "value": 1}], repo=client.repository
        )

        # label watches
        watches = client.get_swf(owner=self.org, repo=client.repository, item='subscribers')
        self.updateRemovedData(watches, 'watch', [
            {
                "name": "is_github_watch",
                "value": 1
            },
            {
                "name": "github_repo.keyword",
                "value": self.org + "/" + client.repository
            }], 
            matchs_not=[{"name": "is_removed", "value": 1}], repo=client.repository
        )

    def get_user_info(self, user):
        is_project_internal_user = 0
        tag_user_company = self.user_org_dic.get(user, 'independent')
        if tag_user_company == 'MindSpore':
            is_project_internal_user = 1

        user_info = {
            'is_project_internal_user': is_project_internal_user,
            'tag_user_company': tag_user_company
        }
        return user_info

    def update_user_org(self):
        all_user = self.esClient.getTotalAuthorName(size=20000)
        user_map = {}
        for item in all_user:
            user_map.update({item['key']: 'independent'})

        for key, value in self.user_org_dic.items():
            user_map.update({key: value})

        for user, tag_user_company in user_map.items():
            print('*** update %s : %s' % (user, tag_user_company))
            is_project_internal_user = 0
            if tag_user_company == 'MindSpore':
                is_project_internal_user = 1
            query = '''{
                "script": {
                    "source": "ctx._source.tag_user_company = params.tag_user_company;ctx._source.is_project_internal_user = params.is_project_internal_user",
                    "params": {
                        "tag_user_company": "%s",
                        "is_project_internal_user": "%s"
                    },
                    "lang": "painless"
                },
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "user_login.keyword": "%s"
                                }
                            }
                        ]
                    }
                }
            }''' % (tag_user_company, is_project_internal_user, user)
            self.esClient.updateByQuery(query=query.encode('utf-8'))

    @staticmethod
    def format_time_z(time_str):
        if time_str is None or time_str == '':
            return None
        return str(time_str).replace('Z', '+00:00')
