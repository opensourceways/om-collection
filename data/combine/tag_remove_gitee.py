import datetime
import json
import re
import threading
import types
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from json import JSONDecodeError

import requests
import yaml

from collect.gitee import GiteeClient
from data.common import ESClient


class TagRemovedGitee(object):

    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.tag_remove_yaml = config.get('tag_removed_yaml', 'tag_remove_gitee.yaml')
        self.scroll_duration = config.get('scroll_duration')
        self.thread_pool_max = int(config.get('thread_pool_max', 100))
        self.headers = {'Content-Type': 'application/json'}
        self.index_name = None
        self.gitee_token = None
        self.orgs = []
        self.old_repos = []

    def run(self, from_time):
        datas = yaml.safe_load(open(self.tag_remove_yaml, encoding='UTF-8'))
        for data in datas['data']:
            self.orgs = data['orgs']
            self.gitee_token = data['gitee_token']
            self.index_name = data['index_name']
            self.esClient.index_name = data['index_name']

            self.tag_removed_repo()
            self.tag_removed_issue()

            self.index_name = None
            self.gitee_token = None
            self.orgs = []
            self.old_repos = []

    # 标记已经删除的仓库，和跟仓库有关的贡献
    def tag_removed_repo(self):
        removed_repos = self.get_removed_repos()
        for repo in removed_repos:
            query = '''{
                          "script": {
                            "source": "ctx._source['is_removed']=1"
                          },
                          "query": {
                            "term": {
                              "gitee_repo.keyword": "%s"
                            }
                          }
                        }''' % repo
            self.esClient.updateByQuery(query=query)
            print('*** tag removed repo: %s' % repo)

    # 标记已经删除的issue和评论
    def tag_removed_issue(self):
        search = '''{
                          "size": 1000,
                          "_source": {
                            "includes": [
                              "url"
                            ]
                          },
                          "query": {
                            "bool": {
                              "filter": [
                                {
                                  "query_string": {
                                    "analyze_wildcard": true,
                                    "query": "is_gitee_issue:1 AND !is_removed:1"
                                  }
                                }
                              ]
                            }
                          }
                        }'''
        self.esClient.scrollSearch(index_name=self.index_name, search=search, scroll_duration=self.scroll_duration,
                                   func=self.tag_removed_issue_thread)

    def tag_removed_issue_thread(self, hits):
        with ThreadPoolExecutor(max_workers=self.thread_pool_max) as executor:
            executor.map(self.tag_removed_issue_func, hits)

            # tasks = [executor.submit(self.tag_removed_issue_func, hit) for hit in hits]
            # for task in as_completed(tasks, timeout=2):
            #     data = task.result()
            #     print(data)

    def tag_removed_issue_func(self, hit):
        _id = hit['_id']
        url = hit['_source']['url']
        res = requests.get(url=url, headers=self.headers, timeout=(6.05, 6.05))
        if res.status_code == 404:
            query = '''{
                          "script": {
                            "source": "ctx._source['is_removed']=1"
                          },
                          "query": {
                            "term": {
                              "issue_url.keyword": "%s"
                            }
                          }
                        }''' % url
            self.esClient.updateByQuery(query)
            print('tag removed issue: %s' % url)
            # return 'tag removed issue: %s' % url
        else:
            print('issue id: %s; is exist' % _id)
            # return 'issue id: %s; is exist' % _id

    def get_removed_repos(self):
        self.get_old_repos()  # 库中已有的repos

        gitee_repos = []  # gitee获取到的repos
        repo_count = 0
        for org in self.orgs:
            client = GiteeClient(org, None, self.gitee_token)
            repo_count += int(client.org_repos_count())

            repos = self.gitee_repos(org, self.gitee_token)
            gitee_repos.extend(repos)

        if len(gitee_repos) != repo_count:
            print('*** len(gitee_repos): %d; repo_count: %d', (len(gitee_repos), repo_count))
            return []

        removed_repos = set(self.old_repos).difference(set(gitee_repos))  # 已经删除的repos
        return removed_repos

    def get_old_repos(self):
        search = '''{
                      "size": 10000,
                      "_source": {
                        "includes": [
                          "gitee_repo"
                        ]
                      },
                      "query": {
                        "bool": {
                          "filter": [
                            {
                              "query_string": {
                                "analyze_wildcard": true,
                                "query": "is_gitee_repo:1"
                              }
                            }
                          ]
                        }
                      }
                    }'''
        self.esClient.scrollSearch(index_name=self.index_name, search=search, scroll_duration=self.scroll_duration,
                                   func=self.get_old_repos_func)

    def get_old_repos_func(self, hits):
        for hit in hits:
            repo_url = hit['_source']['gitee_repo']
            self.old_repos.append(repo_url)

    def gitee_repos(self, owner, token):
        client = GiteeClient(owner, None, token)
        repos = self.getGenerator(client.org())
        repos_urls = []
        for repo in repos:
            repos_urls.append(re.sub(r'.git$', '', repo['html_url']))
        return repos_urls

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
