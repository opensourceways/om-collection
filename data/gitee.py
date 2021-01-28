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


import os
import signal
import yaml

import json
import requests
import logging

import re
import types
import threading

import dateutil.parser
import dateutil.rrule
import dateutil.tz

import time
import datetime

from json import JSONDecodeError
from data import common
from data.common import ESClient
from collect.gitee import GiteeClient

logger = logging.getLogger(__name__)

ISSUE_TYPE = 'issue'
PULL_TYPE = 'pull_request'
COMMENT_TYPE = 'comment'
ISSUE_COMMENT_TYPE = 'issue_comment'
REVIEW_COMMENT_TYPE = 'review_comment'
REPOSITORY_TYPE = 'repository'


class Gitee(object):

    def __init__(self, config=None):
        self.config = config
        self.orgs = self.getOrgs(config.get('orgs'))
        self.index_name = config.get('index_name')
        self.gitee_token = config.get('gitee_token')
        self.skip_user = config.get('skip_user', "").split(',')
        self.esClient = ESClient(config)
        self.is_gitee_enterprise = config.get('is_gitee_enterprise')
        self.filters = config.get('gitee_repo_filter')
        self.is_update_repo_author = config.get('is_update_repo_author')
        self.is_set_itself_author = config.get('is_set_itself_author')
        self.is_set_pr_issue_repo_fork = config.get('is_set_pr_issue_repo_fork')
        self.is_set_first_contribute = config.get('is_set_first_contribute')
        self.is_set_star_watch = config.get('is_set_star_watch')
        self.is_set_sigs_star = config.get('is_set_sigs_star')
        self.internal_users = config.get('internal_users', 'users')
        self.collect_from_time = config.get('collect_from_time')
        self.is_set_collect = config.get('is_set_collect')
        self.yaml_url = config.get('yaml_url')
        self.yaml_path = config.get('yaml_path')
        self.maintainer_index = config.get('maintain_index')
        self.sig_index = config.get('sig_index')
        self.versiontimemapping = config.get('versiontimemapping')
        self.internal_company_name = config.get('internal_company_name', 'internal_company')
        self.internalUsers = []
        self.all_user = []
        self.all_user_info = []
        self.companyinfos = []
        self.enterpriseUsers = []
        self.index_name_all = None
        if 'index_name_all' in config:
            self.index_name_all = config.get('index_name_all').split(',')

    def run(self, from_time):
        # self.esClient.getUserInfoFromDataFile()
        print("Collect gitee data: staring")
        self.getEnterpriseUser()
        # return
        startTime = time.time()
        self.internalUsers = self.getItselfUsers(self.internal_users)

        if self.is_set_itself_author == 'true':
            self.tagUsers(tag_user_company=self.internal_company_name)
            # self.tagUsers()
        else:
            if self.is_set_pr_issue_repo_fork == 'true':
                self.writeData(self.writeContributeForSingleRepo, from_time)

            self.externalUpdateRepo()
            if self.is_set_first_contribute == 'true':
                self.updateIsFirstCountributeItem()
            if self.is_set_collect == 'true':
                self.collectTotal(self.collect_from_time)

            if self.is_set_star_watch == 'true':
                self.writeData(self.writeSWForSingleRepo, from_time)
            if self.is_set_sigs_star == 'true':
                self.getSartUsersList()

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect all gitee data finished after %s" % spent_time)

    def writeData(self, func, from_time):
        threads = []
        for org in self.orgs:
            repos = self.get_repos(org)
            for r in repos:
                t = threading.Thread(
                    target=func,
                    args=(org, r, from_time))
                threads.append(t)
                t.start()

                if len(threads) % 20 == 0:
                    for t in threads:
                        t.join()
                    threads = []

            for t in threads:
                t.join()
            threads = []

    def externalUpdateRepo(self):
        if self.is_update_repo_author == 'true':
            self.updateCreatedRepoAuthor()

    def getOrgs(self, orgsStr):
        orgs = []
        if orgsStr:
            orgs = orgsStr.split(",")
            print(orgs)
        else:
            print("The 'orgs' field must be set")
        return orgs

    def writeContributeForSingleRepo(self, org, repo, from_time=None):
        repo_name = repo['path']
        is_public = repo['public']
        self.writeRepoData(org, repo_name, from_time)
        self.writePullData(org, repo_name, is_public, from_time)
        self.writeIssueData(org, repo_name, is_public, from_time)
        self.writeForks(org, repo_name, from_time)

    def writeSWForSingleRepo(self, org, repo, from_time=None):
        repo_name = repo['path']
        self.writeStars(org, repo_name)
        self.writeWatchs(org, repo_name)

    def checkIsCollectRepo(self, path, is_public):
        filters = self.filters.split(',')
        for f in filters:
            if f in path:
                return False

            if is_public == False:
                return False
        return True

    def get_repos(self, org):
        client = GiteeClient(org, None, self.gitee_token)
        print(self.is_gitee_enterprise)
        if self.is_gitee_enterprise == "true":
            org_data = self.getGenerator(client.enterprises())
        else:
            org_data = self.getGenerator(client.org())

        if self.filters is None:
            for repo in org_data:
                print(repo['path'])
                # if repo['public'] == False:
                #     print("https://openeuler-ci-bot:edison12345@gitee.com/" + org + "/" + repo['path'] + ",")
                # else:
                #     print("https://gitee.com/" + org + "/" + repo['path'] + ",")
            return org_data

        repos = []
        for repo in org_data:
            path = repo['path']
            if self.checkIsCollectRepo(path, repo['public']) == True:
                print(repo['path'])
                repos.append(repo)

        return repos

    def updateRemovedForks(self, gitee_repo, forks):
        matchs = [{
            "name": "is_gitee_fork",
            "value": 1,
        },
            {
                "name": "gitee_repo.keyword",
                "value": gitee_repo,
            }
        ]
        data = self.esClient.getItemsByMatchs(matchs)

        fork_num = data['hits']['total']['value']
        original_forks = data['hits']['hits']
        print("%s original fork num is (%d), The current fork num is (%d)" % (gitee_repo, fork_num, len(forks)))
        if fork_num == len(forks):
            return
        for fork in original_forks:
            if fork['_source']['fork_id'] not in forks:
                print("[update] set fork(%s) is_removed to 1" % fork['_source']['fork_id'])
                self.esClient.updateForkToRemoved(fork['_id'])

    def writeForks(self, owner, repo, from_date):
        startTime = datetime.datetime.now()
        # from_date = self.getFromDate(from_date, [
        #     {"name": "is_gitee_fork", "value": 1}])
        # print("Start collect fork data from ", from_date)

        client = GiteeClient(owner, repo, self.gitee_token)
        fork_data = self.getGenerator(client.forks())
        actions = ""

        fork_ids = []
        for fork in fork_data:
            # if common.str_to_datetime(fork["updated_at"]) < from_date:
            #     continue

            action = {
                "fork_id": fork["id"],
                "created_at": fork["created_at"],
                "updated_at": fork["updated_at"],
                "author_name": fork['namespace']['name'],
                "user_login": fork['namespace']['path'],
                "repository": fork["full_name"],
                "org_name": owner,
                "gitee_repo": "https://gitee.com/" + owner + "/" + repo,
                "fork_gitee_repo": re.sub('.git$', '', fork['html_url']),
                "is_gitee_fork": 1,
            }
            userExtra = self.esClient.getUserInfo(action['user_login'])
            action.update(userExtra)

            indexData = {
                "index": {"_index": self.index_name, "_id": "fork_" + str(fork['id'])}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
            fork_ids.append(fork["id"])

        self.esClient.safe_put_bulk(actions)
        # self.updateRemovedForks("https://gitee.com/" + owner + "/" + repo, fork_ids)

        endTime = datetime.datetime.now()
        print("Collect repo(%s) fork request data finished, spend %s seconds"
              % (owner + "/" + repo, (endTime - startTime).seconds))

    def writeStars(self, owner, repo):
        client = GiteeClient(owner, repo, self.gitee_token)
        star_data = self.getGenerator(client.stars())
        actions = ""

        for star in star_data:
            star_id = owner + "/" + repo + "_" + "star" + str(star['id'])
            if self.esClient.checkFieldExist(filter=star_id) == True:
                continue
            # if star['login'] in self.skip_user:
            #     continue
            action = {
                "user_id": star["id"],
                "star_id": star_id,
                "created_at": star["star_at"],
                "updated_at": common.datetime_utcnow().strftime('%Y-%m-%d'),
                "user_login": star['login'],
                "author_name": star['name'],
                "gitee_repo": "https://gitee.com/" + owner + "/" + repo,
                "org_name": owner,
                "is_gitee_star": 1,
            }
            userExtra = self.esClient.getUserInfo(action['user_login'])
            action.update(userExtra)
            indexData = {
                "index": {"_index": self.index_name, "_id": star_id}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'
        self.esClient.safe_put_bulk(actions)

    def writeWatchs(self, owner, repo):
        client = GiteeClient(owner, repo, self.gitee_token)

        watch_data = self.getGenerator(client.watchs())
        actions = ""
        for watch in watch_data:
            # if watch['login'] in self.skip_user:
            #     continue

            watch_id = owner + "/" + repo + "_" + "watch" + str(watch['id'])
            if self.esClient.checkFieldExist(filter=[watch_id]) == True:
                continue
            action = {
                "user_id": watch["id"],
                "watch_id": watch_id,
                "updated_at": common.datetime_utcnow().strftime('%Y-%m-%d'),
                "created_at": watch["watch_at"],
                "user_login": watch['login'],
                "author_name": watch['name'],
                "gitee_repo": "https://gitee.com/" + owner + "/" + repo,
                "org_name": owner,
                "is_gitee_watch": 1,
            }
            userExtra = self.esClient.getUserInfo(action['user_login'])
            action.update(userExtra)

            indexData = {
                "index": {"_index": self.index_name, "_id": watch_id}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(action) + '\n'

        self.esClient.safe_put_bulk(actions)

    def writeRepoData(self, owner, repo, from_date=None):
        client = GiteeClient(owner, repo, self.gitee_token)
        repo_data = self.getGenerator(client.repo())
        actions = ""
        repo_detail = {
            "created_at": repo_data["created_at"],
            "updated_at": repo_data["updated_at"],
            "repository_forks_count": int(repo_data["forks_count"]),
            "repository_stargazers_count": int(repo_data["stargazers_count"]),
            "repository_watchers_count": int(repo_data["watchers_count"]),
            "org_name": repo_data['namespace']['path'],
            "author_name": repo_data['owner']['name'],
            "owner_name": repo_data['owner']['name'],
            "owner_login": repo_data['owner']['login'],
            "user_login": repo_data['owner']['login'],
            "repository": repo_data["full_name"],
            "public": repo_data["public"],
            "private": repo_data["private"],
            "gitee_repo": re.sub('.git$', '', repo_data['html_url']),
            "is_gitee_repo": 1,
        }
        userExtra = self.esClient.getUserInfo(repo_data['owner']['login'])
        repo_detail.update(userExtra)

        maintainerdata = self.esClient.getRepoMaintainer(self.maintainer_index, repo_data["full_name"])
        repo_detail.update(maintainerdata)
        sigcount = self.esClient.getRepoSigCount(self.sig_index, repo_data["full_name"])
        repo_detail.update(sigcount)
        signames = self.esClient.getRepoSigNames(self.sig_index, repo_data['full_name'])
        repo_detail.update(signames)
        branches = self.getGenerator(client.getSingleReopBranch())
        brinfo = self.getbranchinfo(branches, client, owner, repo, repo_data['path'], self.versiontimemapping)
        repo_detail["branches"] = brinfo

        indexData = {
            "index": {"_index": self.index_name,
                      "_id": "gitee_repo_" + re.sub('.git$', '', repo_data['html_url'])}}
        actions += json.dumps(indexData) + '\n'
        actions += json.dumps(repo_detail) + '\n'
        self.esClient.safe_put_bulk(actions)

    def getbranchinfo(self, branches, client, owner, repo, repopath, versiontimemapping_index):
        result = []
        version = None
        for br in branches:
            brresult = {}
            try:
                brresult["brname"] = br['name']
                spec = client.getspecFile(owner, repo, br['name'])
                version = spec.version
            except:
                print('reop:%s branch:%s has No version' % (repopath, br['name']))
            if version and self.versiontimemapping:
                if str(version).startswith('%{'):
                    index = str(version).find("}")
                    if index == -1:
                        index = len(version) - 1
                    version = version[2:index]
                    version = spec.macros.get(version)
                    brresult["version"] = version
                times = self.esClient.geTimeofVersion(version, repopath, versiontimemapping_index)
                interval = 0
                if times is not None:
                    interval = datetime.datetime.now() - datetime.datetime.strptime(times, '%Y-%m-%d %H:%M:%S')
                    print("releasetime:" + str(times))
                else:
                    times = ""
                    # 版本发布到目前时间
                brresult['releasetime2now'] = 0 if interval == 0 else interval.days
                # 版本号
                brresult['version'] = version
                # 版本发布时间
                brresult['releasetime'] = times
                print('reop:%s branch:%s has No version' % (repopath, br['name']))
                result.append(brresult)
        return result

    def getFromDate(self, from_date, filters):
        if from_date is None:
            from_date = self.esClient.get_from_date(filters)
        else:
            from_date = common.str_to_datetime(from_date)
        return from_date

    def writePullData(self, owner, repo, public, from_date=None):
        startTime = datetime.datetime.now()
        from_date = self.getFromDate(from_date, [
            {"name": "is_gitee_pull_request", "value": 1}])
        print("Start collect issue data from ", from_date)

        client = GiteeClient(owner, repo, self.gitee_token)

        if public == True:
            client = GiteeClient(owner, repo, self.gitee_token)
            print("repo is public")

        # collect pull request
        actions = ""
        pull_data = self.getGenerator(client.pulls())
        for x in pull_data:
            print(x['number'])
            if common.str_to_datetime(x['updated_at']) < from_date:
                continue
            if x['user']['login'] in self.skip_user:
                continue

            pr_number = x['number']

            pull_code_diff = self.getGenerator(client.pull_code_diff(pr_number))
            pull_action_logs = self.getGenerator(client.pull_action_logs(pr_number))
            pull_review_comments = self.getGenerator(client.pull_review_comments(pr_number))

            codediffadd = 0
            codediffdelete = 0
            for item in pull_code_diff:
                if isinstance(item, dict):
                    codediffadd = int(codediffadd) + int(item['additions'])
                    codediffdelete = int(codediffadd) + int(item['deletions'])
            merged_item = None
            if x['state'] == "closed":
                if isinstance(pull_action_logs, list):
                    merged_item = pull_action_logs[0]
                else:
                    merged_item = pull_action_logs
            x['codediffadd'] = codediffadd
            x['codediffdelete'] = codediffdelete
            eitem = self.__get_rich_pull(x, merged_item)

            ecomments = self.get_rich_pull_reviews(pull_review_comments, eitem, owner)
            firstreplyprtime = ""
            lastreplyprtime = ""
            for ec in ecomments:
                if not firstreplyprtime:
                    firstreplyprtime = str(ec['created_at'])
                    lastreplyprtime = str(ec['created_at'])
                else:
                    ectime = str(ec['created_at'])
                    if ectime < firstreplyprtime:
                        firstreplyprtime = ectime
                    if ectime > lastreplyprtime:
                        lastreplyprtime = ectime
                print(ec['pull_comment_id'])
                if ec['user_login'] in self.skip_user:
                    continue
                indexData = {
                    "index": {"_index": self.index_name, "_id": ec['pull_comment_id']}}
                actions += json.dumps(indexData) + '\n'
                actions += json.dumps(ec) + '\n'
            try:
                eitem['firstreplyprtime'] = (datetime.datetime.strptime(firstreplyprtime,
                                                                        '%Y-%m-%dT%H:%M:%S+08:00') - datetime.datetime.strptime(
                    eitem['created_at'], '%Y-%m-%dT%H:%M:%S+08:00')).days
                eitem['lastreplyprtime'] = (datetime.datetime.now() - (
                    datetime.datetime.strptime(lastreplyprtime, '%Y-%m-%dT%H:%M:%S+08:00'))).days
            except Exception as e:
                print(e)
                print(firstreplyprtime)
                print(eitem['created_at'])
            eitem['prcommentscount'] = len(ecomments)
            if self.sig_index:
                eitem['pulls_signames'] = self.esClient.getRepoSigNames(self.sig_index, owner + "/" + repo)
            indexData = {"index": {"_index": self.index_name, "_id": eitem['id']}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(eitem) + '\n'
            if len(actions) > 10000:
                self.esClient.safe_put_bulk(actions)
                actions = ""
        self.esClient.safe_put_bulk(actions)

        endTime = datetime.datetime.now()
        print("Collect pull request data finished, spend %s seconds" % (
                endTime - startTime).seconds)

    def writeIssueData(self, owner, repo, public, from_date=None):
        startTime = datetime.datetime.now()

        client = GiteeClient(owner, repo, self.gitee_token)
        if from_date is None:
            from_date = self.esClient.get_from_date(
                [{"name": "is_gitee_issue", "value": 1},
                 {"name": "gitee_repo.keyword",
                  "value": "https://gitee.com/" + owner + "/" + repo}])
        else:
            from_date = common.str_to_datetime(from_date)
        print("Start collect repo(%s/%s) issue data from %s" % (
            owner, repo, from_date))

        # common.
        if public == True:
            client = GiteeClient(owner, repo, self.gitee_token)
            print("repo is public")

        # collect issue
        actions = ""
        issue_data = self.getGenerator(client.issues(from_date))
        for i in issue_data:
            print(str(i['number']))
            issue_comments = self.getGenerator(client.issue_comments(i['number']))
            i['comments_data'] = issue_comments
            issue_item = self.get_rich_issue(i)
            firstreplyissuetime = ""
            lastreplyissuetime = ""
            issue_comments = self.get_rich_issue_comments(issue_comments, issue_item)
            for ic in issue_comments:
                if not firstreplyissuetime:
                    firstreplyissuetime = str(ic['created_at'])
                    lastreplyissuetime = str(ic['created_at'])
                else:
                    ictime = str(ic['created_at'])
                    if ictime < firstreplyissuetime:
                        firstreplyissuetime = ictime
                    if ictime > lastreplyissuetime:
                        lastreplyissuetime = ictime
                if ic['user_login'] in self.skip_user:
                    continue
                print(ic['issue_comment_id'])
                indexData = {
                    "index": {"_index": self.index_name,
                              "_id": ic['issue_comment_id']}}
                actions += json.dumps(indexData) + '\n'
                actions += json.dumps(ic) + '\n'
            try:
                issue_item['firstreplyissuetime'] = (datetime.datetime.strptime(firstreplyissuetime,
                                                                                '%Y-%m-%dT%H:%M:%S+08:00') - datetime.datetime.strptime(
                    issue_item['created_at'], '%Y-%m-%dT%H:%M:%S+08:00')).days
                issue_item['lastreplyissuetime'] = (datetime.datetime.now() - (
                    datetime.datetime.strptime(lastreplyissuetime, '%Y-%m-%dT%H:%M:%S+08:00'))).days
                indexData = {"index": {"_index": self.index_name, "_id": issue_item['id']}}
                actions += json.dumps(indexData) + '\n'
                actions += json.dumps(issue_item) + '\n'
            except Exception as e:
                print(e)

        self.esClient.safe_put_bulk(actions)
        endTime = datetime.datetime.now()
        print("Collect repo(%s/%s) issue data finished, spend %s seconds" % (
            owner, repo, (endTime - startTime).seconds))

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

        return data

    def get_rich_pull_reviews(self, comments, eitem, owner=None):
        ecomments = []

        for comment in comments:
            ecomment = {}
            # Copy data from the enriched pull
            ecomment['pull_labels'] = eitem['pull_labels']
            ecomment['pull_id'] = eitem['pull_id']
            ecomment['pull_id_in_repo'] = eitem['pull_id_in_repo']
            ecomment['issue_id_in_repo'] = eitem['issue_id_in_repo']
            ecomment['issue_title'] = eitem['issue_title']
            ecomment['issue_url'] = eitem['issue_url']
            ecomment['pull_url'] = eitem['pull_url']
            ecomment['pull_state'] = eitem['pull_state']
            ecomment['pull_created_at'] = eitem['pull_created_at']
            ecomment['pull_updated_at'] = eitem['pull_updated_at']

            ecomment['base_label'] = eitem['base_label']
            ecomment['base_label_ref'] = eitem['base_label_ref']
            ecomment['head_label'] = eitem['head_label']
            ecomment['head_label_ref'] = eitem['head_label_ref']

            if 'pull_merged_at' in eitem:
                ecomment['pull_merged_at'] = eitem['pull_merged_at']
            if 'pull_closed_at' in eitem:
                ecomment['pull_closed_at'] = eitem['pull_closed_at']
            # ecomment['pull_merged'] = eitem['pull_merged']
            ecomment['pull_state'] = eitem['pull_state']
            ecomment['gitee_repo'] = eitem['gitee_repo']
            ecomment['repository'] = eitem['repository']
            ecomment['item_type'] = COMMENT_TYPE
            ecomment['sub_type'] = REVIEW_COMMENT_TYPE
            ecomment['org_name'] = owner

            # Copy data from the raw comment
            ecomment['url'] = comment['html_url']
            ecomment['comment_url'] = comment['html_url']

            user = comment.get('user', None)
            if user is not None and user:
                ecomment['user_name'] = user['name']
                ecomment['author_name'] = user['name']
                ecomment['user_login'] = user['login']
                ecomment["user_domain"] = None
            # extract reactions and add it to enriched item
            # ecomment.update(self.__get_reactions(comment))

            ecomment['created_at'] = comment['created_at']
            ecomment['updated_at'] = comment['updated_at']
            ecomment['comment_updated_at'] = comment['updated_at']
            # ecomment['first_contribute_at'] = get_first_contribute_at(ecomment['author_name'], comment['created_at'])
            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['pull_comment_id'] = '{}_review_comment_{}'.format(eitem['id'], comment['id'])
            ecomment['id'] = comment['id']
            # ecomment.update(self.get_grimoire_fields(comment['updated_at'], REVIEW_COMMENT_TYPE))
            # due to backtrack compatibility, `is_gitee2_*` is replaced with `is_gitee_*`

            ecomment['is_gitee_{}'.format(REVIEW_COMMENT_TYPE)] = 1
            ecomment['is_gitee_comment'] = 1

            userExtra = self.esClient.getUserInfo(ecomment['user_login'])
            ecomment.update(userExtra)
            ecomments.append(ecomment)

        return ecomments

    def __get_rich_pull(self, item, merged_item):
        rich_pr = {}

        # The real data
        pull_request = item

        rich_pr['time_to_close_days'] = \
            common.get_time_diff_days(pull_request['created_at'], pull_request['closed_at'])

        if pull_request['state'] != 'closed':
            rich_pr['time_open_days'] = \
                common.get_time_diff_days(pull_request['created_at'], common.datetime_utcnow().replace(tzinfo=None))
        else:
            rich_pr['time_open_days'] = rich_pr['time_to_close_days']

        rich_pr['user_login'] = pull_request['user']['login']

        user = pull_request.get('user', None)
        if user is not None and user:
            rich_pr['user_name'] = user['name']
            rich_pr['author_name'] = user['name']
            # rich_pr["user_domain"] = self.get_email_domain(user['email']) if user['email'] else None
            rich_pr["user_domain"] = None
        else:
            rich_pr['user_name'] = None
            rich_pr["user_domain"] = None
            rich_pr['author_name'] = None

        if merged_item is not None and 'user' in merged_item:
            rich_pr['merge_author_login'] = merged_item['user']['login']
            rich_pr['merge_author_name'] = merged_item['user']['name']
            # rich_pr["merge_author_domain"] = self.get_email_domain(merged_by['email']) if merged_by['email'] else None
            rich_pr["merge_author_domain"] = None
        else:
            rich_pr['merge_author_name'] = None
            rich_pr['merge_author_login'] = None
            rich_pr["merge_author_domain"] = None

        rich_pr['id'] = pull_request['id']
        rich_pr['pull_id'] = pull_request['id']
        rich_pr['pull_id_in_repo'] = pull_request['html_url'].split("/")[-1]
        rich_pr['issue_id_in_repo'] = pull_request['html_url'].split("/")[-1]
        rich_pr['repository'] = pull_request['url']
        rich_pr['issue_title'] = pull_request['title']
        rich_pr['issue_title_analyzed'] = pull_request['title']
        rich_pr['pull_state'] = pull_request['state']
        if (rich_pr['pull_state'] == 'open'):
            rich_pr["is_pull_state_open"] = 1
        elif (rich_pr['pull_state'] == 'closed'):
            rich_pr["is_pull_state_closed"] = 1
        elif (rich_pr['pull_state'] == 'merged'):
            rich_pr["is_pull_state_merged"] = 1
        rich_pr['pull_created_at'] = pull_request['created_at']
        rich_pr['pull_updated_at'] = pull_request['updated_at']
        rich_pr['created_at'] = pull_request['created_at']
        rich_pr['updated_at'] = pull_request['updated_at']
        # rich_pr['first_contribute_at'] = get_first_contribute_at(rich_pr['user_name'], pull_request['created_at'])

        if pull_request['merged_at'] and pull_request['merged_at'] is not None:
            rich_pr['pull_merged_at'] = pull_request['merged_at']
        if pull_request['closed_at'] and pull_request['closed_at'] is not None:
            rich_pr['pull_closed_at'] = pull_request['closed_at']
        rich_pr['url'] = pull_request['html_url']
        rich_pr['pull_url'] = pull_request['html_url']
        rich_pr['issue_url'] = pull_request['html_url']

        labels = []
        [labels.append(label['name']) for label in pull_request['labels'] if 'labels' in pull_request]
        rich_pr['pull_labels'] = labels

        rich_pr['item_type'] = PULL_TYPE

        # rich_pr['gitee_repo'] = rich_pr['repository'].replace(gitee, '')
        rich_pr['gitee_repo'] = re.sub('.git$', '', pull_request['base']['repo']['html_url'])
        rich_pr['org_name'] = pull_request['base']['repo']['namespace']['path']

        rich_pr['base_label'] = pull_request['base']['label']
        rich_pr['base_label_ref'] = pull_request['base']['ref']
        rich_pr['head_label'] = pull_request['head']['label']
        rich_pr['head_label_ref'] = pull_request['head']['ref']
        # GMD code development metrics
        # rich_pr['forks'] = pull_request['base']['repo']['forks_count']
        rich_pr['code_merge_duration'] = common.get_time_diff_days(pull_request['created_at'],
                                                                   pull_request['merged_at'])
        # rich_pr['num_review_comments'] = pull_request['review_comments']

        # rich_pr['time_to_merge_request_response'] = None
        # if pull_request['review_comments'] != 0:
        #    min_review_date = self.get_time_to_merge_request_response(pull_request)
        #    rich_pr['time_to_merge_request_response'] = \
        #        get_time_diff_days(str_to_datetime(pull_request['created_at']), min_review_date)

        # if self.prjs_map:
        #    rich_pr.update(self.get_item_project(rich_pr))
        userExtra = self.esClient.getUserInfo(rich_pr['user_login'])
        rich_pr.update(userExtra)
        rich_pr['addcodenum'] = pull_request['codediffadd']
        rich_pr['deletecodenum'] = pull_request['codediffdelete']
        if 'project' in item:
            rich_pr['project'] = item['project']

        rich_pr['is_gitee_{}'.format(PULL_TYPE)] = 1

        return rich_pr

    def get_rich_issue(self, item):
        rich_issue = {}
        # The real data
        issue = item

        rich_issue['time_to_close_days'] = \
            common.get_time_diff_days(issue['created_at'], issue['finished_at'])

        if issue['state'] != 'closed':
            rich_issue['time_open_days'] = \
                common.get_time_diff_days(issue['created_at'], common.datetime_utcnow().replace(tzinfo=None))
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        # rich_issue['user_login'] = issue['user']['login']

        user = issue.get('user', None)
        if user is not None and user:
            rich_issue['user_id'] = user['id']
            rich_issue['user_name'] = user['name']
            rich_issue['author_name'] = user['name']
            rich_issue['user_login'] = user['login']
            # rich_issue["user_domain"] = self.get_email_domain(user['email']) if user['email'] else None
            # rich_issue['user_org'] = user['company']
            # rich_issue['user_location'] = user['location']
            # rich_issue['user_geolocation'] = None
        milestone = issue.get('milestone', None)
        if milestone is not None and milestone:
            rich_issue['milestone_url'] = milestone['url']
            rich_issue['milestone_html_url'] = milestone['html_url']
            rich_issue['milestone_id'] = milestone['id']
            rich_issue['milestone_number'] = milestone['number']
            rich_issue['milestone_repository_id'] = milestone['repository_id']
            rich_issue['milestone_state'] = milestone['state']
            rich_issue['milestone_title'] = milestone['title']
            rich_issue['milestone_description'] = milestone['description']
            rich_issue['milestone_updated_at'] = milestone['updated_at']
            rich_issue['milestone_created_at'] = milestone['created_at']
            rich_issue['milestone_open_issues'] = milestone['open_issues']
            rich_issue['milestone_closed_issues'] = milestone['closed_issues']
            rich_issue['milestone_due_on'] = milestone['due_on']

        assignee = issue.get('assignee', None)
        if assignee and assignee is not None:
            assignee = issue['assignee']
            rich_issue['assignee_login'] = assignee['login']
            rich_issue['assignee_name'] = assignee['name']
            # rich_issue["assignee_domain"] = self.get_email_domain(assignee['email']) if assignee['email'] else None
            # rich_issue['assignee_org'] = assignee['company']
            # rich_issue['assignee_location'] = assignee['location']
            # rich_issue['assignee_geolocation'] = None
        else:
            rich_issue['assignee_name'] = None
            rich_issue['assignee_login'] = None
            # rich_issue["assignee_domain"] = None
            # rich_issue['assignee_org'] = None
            # rich_issue['assignee_location'] = None
            # rich_issue['assignee_geolocation'] = None

        rich_issue['id'] = issue['id']
        # 获取issue严重级别
        rich_issue['priority'] = issue['priority']
        rich_issue['issue_id'] = issue['id']
        rich_issue['issue_number'] = issue['number']
        rich_issue['issue_id_in_repo'] = issue['html_url'].split("/")[-1]
        # rich_issue['repository'] = self.get_project_repository(rich_issue)
        rich_issue['repository'] = issue['repository']['full_name']
        rich_issue['repository_forks_count'] = int(issue['repository']['forks_count'])
        rich_issue['repository_stargazers_count'] = int(issue['repository']['stargazers_count'])
        rich_issue['repository_watchers_count'] = int(issue['repository']['watchers_count'])
        rich_issue['issue_title'] = issue['title']
        rich_issue['issue_title_analyzed'] = issue['title']
        rich_issue['issue_state'] = issue['state']
        rich_issue['issue_type'] = issue['issue_type']
        if (rich_issue['issue_state'] == 'progressing'):
            rich_issue['is_issue_state_progressing'] = 1
        elif (rich_issue['issue_state'] == 'open'):
            rich_issue['is_issue_state_open'] = 1
        elif (rich_issue['issue_state'] == 'closed'):
            rich_issue['is_issue_state_closed'] = 1
        elif (rich_issue['issue_state'] == 'rejected'):
            rich_issue['is_issue_state_rejected'] = 1
        rich_issue['issue_created_at'] = issue['created_at']
        rich_issue['issue_updated_at'] = issue['updated_at']
        rich_issue['issue_closed_at'] = issue['finished_at']
        rich_issue['created_at'] = issue['created_at']
        rich_issue['updated_at'] = issue['updated_at']
        rich_issue['closed_at'] = issue['finished_at']
        rich_issue['url'] = issue['html_url']
        rich_issue['issue_url'] = issue['html_url']

        # extract reactions and add it to enriched item
        # rich_issue.update(self.__get_reactions(issue))

        labels = []
        [labels.append(label['name']) for label in issue['labels'] if 'labels' in issue]
        rich_issue['issue_labels'] = labels

        rich_issue['item_type'] = ISSUE_TYPE
        rich_issue['issue_pull_request'] = True
        if 'head' not in issue.keys() and 'pull_request' not in issue.keys():
            rich_issue['issue_pull_request'] = False

        # rich_issue['gitee_repo'] = rich_issue['repository'].replace(gitee, '')
        rich_issue['gitee_repo'] = re.sub('.git$', '', issue['repository']['html_url'])
        rich_issue['org_name'] = issue['repository']['namespace']['path']
        # if self.prjs_map:
        #    rich_issue.update(self.get_item_project(rich_issue))

        if 'project' in issue:
            rich_issue['project'] = issue['project']

        rich_issue['time_to_first_attention'] = None
        if issue['comments'] != 0:
            rich_issue['time_to_first_attention'] = \
                common.get_time_diff_days(common.str_to_datetime(issue['created_at']),
                                          common.get_time_to_first_attention(issue))

        # rich_issue.update(self.get_grimoire_fields(issue['created_at'], ISSUE_TYPE))
        # due to backtrack compatibility, `is_gitee2_*` is replaced with `is_gitee_*`
        # rich_issue.pop('is_gitee2_{}'.format(ISSUE_TYPE))
        rich_issue['is_gitee_{}'.format(ISSUE_TYPE)] = 1

        userExtra = self.esClient.getUserInfo(rich_issue['user_login'])
        rich_issue.update(userExtra)
        return rich_issue

    def get_rich_issue_comments(self, comments, eitem):
        ecomments = []

        for comment in comments:
            ecomment = {}

            # Copy data from the enriched issue
            ecomment['issue_labels'] = eitem['issue_labels']
            ecomment['org_name'] = eitem['org_name']
            ecomment['issue_id'] = eitem['issue_id']
            ecomment['issue_number'] = eitem['issue_number']
            ecomment['issue_id_in_repo'] = eitem['issue_id_in_repo']
            ecomment['issue_url'] = eitem['issue_url']
            ecomment['issue_title'] = eitem['issue_title']
            ecomment['issue_state'] = eitem['issue_state']
            ecomment['issue_created_at'] = eitem['issue_created_at']
            ecomment['issue_updated_at'] = eitem['issue_updated_at']
            ecomment['issue_closed_at'] = eitem['issue_closed_at']
            # ecomment['created_at'] = eitem['issue_created_at']
            # ecomment['updated_at'] = eitem['issue_updated_at']
            ecomment['closed_at'] = eitem['issue_closed_at']
            ecomment['issue_pull_request'] = eitem['issue_pull_request']
            ecomment['gitee_repo'] = eitem['gitee_repo']
            ecomment['repository'] = eitem['repository']
            ecomment['repository_forks_count'] = eitem['repository_forks_count']
            ecomment['repository_stargazers_count'] = eitem['repository_stargazers_count']
            ecomment['repository_watchers_count'] = eitem['repository_watchers_count']
            ecomment['item_type'] = COMMENT_TYPE
            ecomment['sub_type'] = ISSUE_COMMENT_TYPE

            # Copy data from the raw comment
            # ecomment['url'] = comment['html_url']

            # extract reactions and add it to enriched item
            # ecomment.update(self.__get_reactions(comment))
            user = comment.get('user', None)
            if user is not None and user:
                ecomment['user_name'] = user['name']
                ecomment['author_name'] = user['name']
                ecomment['user_login'] = user['login']
                ecomment["user_domain"] = None

            ecomment['created_at'] = comment['created_at']
            ecomment['updated_at'] = comment['updated_at']
            ecomment['issue_comment_updated_at'] = comment['updated_at']
            ecomment['comment_updated_at'] = comment['updated_at']
            # ecomment['first_contribute_at'] = get_first_contribute_at(ecomment['author_name'], comment['created_at'])
            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['issue_comment_id'] = '{}_issue_comment_{}'.format(eitem['issue_id'], comment['id'])
            ecomment['id'] = comment['id']
            ecomment['is_gitee_{}'.format(ISSUE_COMMENT_TYPE)] = 1
            ecomment['is_gitee_comment'] = 1

            if 'project' in eitem:
                ecomment['project'] = eitem['project']
            userExtra = self.esClient.getUserInfo(ecomment['user_login'])
            ecomment.update(userExtra)
            # self.add_repository_labels(ecomment)
            # self.add_metadata_filter_raw(ecomment)
            # self.add_gelk_metadata(ecomment)

            ecomments.append(ecomment)

        return ecomments

    def getUserInfo(self, login):
        userExtra = {}
        if self.is_gitee_enterprise == 'true':
            if login in self.enterpriseUsers:
                userExtra["tag_user_company"] = self.internal_company_name
                userExtra["is_project_internal_user"] = 1
            else:
                userExtra["tag_user_company"] = "n/a"
                userExtra["is_project_internal_user"] = 0
        else:
            if login in self.internalUsers:
                userExtra["tag_user_company"] = "openeuler"
                userExtra["is_project_internal_user"] = 1
            else:
                userExtra["tag_user_company"] = "n/a"
                userExtra["is_project_internal_user"] = 0

        return userExtra

    def updateIsFirstCountributeItem(self):
        all_user = self.esClient.getTotalAuthorName(
            field="user_login.keyword")
        for user in all_user:
            # if user["key"] in self.all_user:
            #     continue
            user_name = user["key"]
            # self.all_user.append(user_name)
            print("start to update user:", user)
            self.esClient.setIsFirstCountributeItem(user_name)

    def updateCreatedRepoAuthor(self):
        client = GiteeClient("openeuler", "community", self.gitee_token)
        data = self.getGenerator(client.pulls(state='merged'))

        for d in data:
            print("updateCreatedRepoAuthor pull request number=%d" % d["number"])
            pull_files = self.getGenerator(client.pull_files(d["number"]))
            for p in pull_files:
                if p["filename"] != "repository/src-openeuler.yaml" \
                        and p["filename"] != "repository/openeuler.yaml":
                    continue

                lines = p["patch"]["diff"].split("\n")
                for line in lines:
                    if "+- name: " not in line:
                        continue
                    repo_name = line.split("+- name: ")[1]
                    print(repo_name)

                    author_name = d['base']['user']['name']
                    user_login = d['base']['user']['login']
                    print(author_name)
                    is_internal = False
                    if user_login in self.internalUsers:
                        is_internal = True
                    self.esClient.updateRepoCreatedName(
                        p["filename"].split('/')[1].split('.')[0], repo_name,
                        author_name, user_login, is_internal)

    def getAllIndex(self, user):
        matchs = [{
            "name": "user_login.keyword",
            "value": user,
        }]
        data = self.esClient.getItemsByMatchs(matchs, size=0)
        all_count = data["hits"]["total"]["value"]
        print("get %d items from user %s" % (all_count, user))
        if all_count < 1:
            return []

        aggs = '''"aggs": {
            "uniq_gender": {
                "terms": {
                    "field": "_id",
                    "size": %s
                }
            }
        }''' % all_count
        ids = self.esClient.getItemsByMatchs(matchs, size=0, aggs=aggs)

        print("update %d buckets for user %s" % (len(ids['aggregations']["uniq_gender"]["buckets"]), user))
        return ids['aggregations']["uniq_gender"]["buckets"]

    def getItselfUsers(self, filename="users"):
        f = open(filename, 'r')

        users = []
        for line in f.readlines():
            if line != "\n":
                users.append(line.split('\n')[0])
        print(users)
        print(len(users))
        return users

    def tagUsers(self, from_date=None, tag_user_company="openeuler"):
        # users = self.getItselfUsers()
        if self.is_gitee_enterprise == "true":
            users = self.enterpriseUsers
        else:
            users = self.internalUsers
        all_user = self.esClient.getTotalAuthorName(
            field="user_login.keyword")
        for user in all_user:
            u = user["key"]
            if u == "mindspore_ci":
                continue
            if u in users:
                update_data = {
                    "doc": {
                        "tag_user_company": tag_user_company,
                        "is_project_internal_user": 1,
                    }
                }
            else:
                update_data = {
                    "doc": {
                        "tag_user_company": "n/a",
                        "is_project_internal_user": 0,
                    }
                }
            if self.yaml_url:
                cmd = 'wget %s' % self.yaml_url
                os.popen(cmd)
                datas = yaml.load_all(open(self.yaml_path)).__next__()

                for data in datas:
                    if data['gitee_id'] == u:
                        if not data["companies"]['company_name'] and data['emails']:
                            for email in data['emails']:
                                if email.endswith('@huawei.com'):
                                    update_data["companies"]['company_name'] = 'Huawei'
                                    break
                        else:
                            if re.search(r'huawei', data["companies"]['company_name'], re.I):
                                update_data["companies"]['company_name'] = 'Huawei'
            # for u in users:
            actions = ""
            ids = self.getAllIndex(u)
            for id in ids:
                action = common.getSingleAction(
                    self.index_name, id["key"], update_data, act="update")
                actions += action

            self.esClient.safe_put_bulk(actions)

    def collectTotal(self, from_time):
        self.collectTotalByType(from_time, "is_gitee_pull_request")
        self.collectTotalByType(from_time, "is_gitee_issue")

    def collectTotalByType(self, from_time, type):
        matchs = [{"name": type, "value": 1}]
        from_date = datetime.datetime.strptime(from_time, "%Y%m%d")
        to_date = datetime.datetime.today()
        data = self.esClient.getCountByDateRange(matchs, from_date, to_date)
        print(data)
        actions = ""
        for d in data:
            print("date = %s, count = %s" % (
                d.get("to_as_string"), d.get("doc_count")))
            created_at = d.get("to_as_string")
            body = {
                "all_count": d.get("doc_count"),
                "created_at": created_at,
                "updated_at": created_at,
                type + "_total": 1
            }

            id = created_at + type + "_total"
            action = common.getSingleAction(self.index_name, id, body)
            actions += action
        self.esClient.safe_put_bulk(actions)

    def getEnterpriseUser(self):
        if self.is_gitee_enterprise != "true":
            return

        client = GiteeClient(self.orgs[0], "", self.gitee_token)

        data = self.getGenerator(client.enterprise_members())
        for d in data:
            user = d.get("user").get("login")
            print(user)
            self.enterpriseUsers.append(user)

    def getUserInfoFromDataFile(self):
        f = open('data.json')
        userInfos = json.load(f)
        alluserinfos = []
        for user in userInfos.get("users"):
            print(user.get('gitee_id'))
            print(user.get('github_id'))
            print(user.get('companies'))
            if user.get('gitee_id'):
                alluserinfos.append({user.get('gitee_id'): user})
        print(alluserinfos)
        self.all_user_info = alluserinfos

        for company in userInfos.get("companies"):
            for alia in company.get("aliases"):
                self.companyinfos[alia] = company.get("company_name")
            for domain in company.get("domains"):
                self.companyinfos[domain] = company.get("company_name")

    def getSartUsersList(self):
        if self.index_name_all:
            for index in range(len(self.orgs)):
                client = GiteeClient(self.orgs[index], "", self.gitee_token)
                respons = client.org_followers(self.orgs[index])

                for r in respons:
                    for user in json.loads(r):
                        id = self.orgs[index] + '_star_' + str(user['id'])
                        user['created_at'] = user['followed_at'].replace('Z', '+08:00')
                        user['is_set_sigs_star'] = 1
                        user['user_login'] = user['login']
                        user['user_name'] = user['name']
                        user['author_name'] = user['name']
                        userExtra = self.esClient.getUserInfo(user['login'])
                        user.update(userExtra)
                        action = common.getSingleAction(self.index_name_all[index], id, user)
                        self.esClient.safe_put_bulk(action)
