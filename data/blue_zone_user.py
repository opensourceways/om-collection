import hashlib
import json
from datetime import datetime

import xlrd
import yaml

from data.common import ESClient


class BlueZoneUser(object):
    def __init__(self, config=None):
        self.config = config
        self.org = config.get('org')
        self.esClient = ESClient(config)
        self.user_index = config.get('user_index')
        self.gitee_pr_issue_index = config.get('gitee_pr_issue_indexs')
        self.gitee_commit_index = config.get('gitee_commit_indexs')
        self.github_pr_issue_index = config.get('github_pr_issue_indexs')
        self.github_commit_index = config.get('github_commit_indexs')
        self.target_index = config.get('target_index')
        self.users = []
        self.user = {}
        self.startTime = config.get('start_time')
        self.endTime = config.get('end_time')

    def run(self, from_date):
        print('Start collection BlueZoneUserContributes')
        current_date = datetime.today().strftime('%Y-%m-%d')
        if self.startTime:
            self.startTime = self.startTime + 'T00:00:00+08:00'
        else:
            self.startTime = current_date + 'T00:00:00+08:00'

        if self.endTime:
            self.endTime = self.endTime + 'T23:59:59+08:00'
        else:
            self.endTime = current_date + 'T23:59:59+08:00'

        self.get_blue_users()
        for user in self.users:
            print('******* %s *******' % user['name'])
            self.user = user
            self.user.__delitem__('created_at')
            if user['gitee_id'] is not None and user['gitee_id'] != '':
                self.get_pr_gitee()
                self.get_issue_gitee()
                self.get_pr_issue_comment_gitee()
                self.get_commit(indexs_str=self.gitee_commit_index)
            elif user['github_id'] is not None and user['github_id'] != '':
                self.get_pr_github()
                self.get_issue_github()
                self.get_pr_issue_comment_github()
                self.get_commit(indexs_str=self.github_commit_index)
            else:
                continue

    def userFromExcel(self):
        wb = xlrd.open_workbook("C:\\Users\\Administrator\\Desktop\\blue_zone_user.xls")
        sh = wb.sheet_by_name("Sheet2")

        cell_name_index_dict = {}
        for i in range(sh.ncols):
            cell_name = sh.cell_value(0, i)
            cell_name_index_dict.update({cell_name: i})

        actions = ''
        for r in range(1, sh.nrows):
            name = self.getCellValue(r, '姓名', sh, cell_name_index_dict)
            org = self.getCellValue(r, '项目群', sh, cell_name_index_dict)
            gitee_id = self.getCellValue(r, 'gitee_id', sh, cell_name_index_dict)
            github_id = self.getCellValue(r, 'github_id', sh, cell_name_index_dict)
            emails = self.getCellValue(r, '邮箱', sh, cell_name_index_dict)

            if gitee_id is not None and gitee_id != '':
                id = gitee_id
            else:
                id = github_id

            action = {'name': name,
                      'org': org,
                      'github_id': github_id,
                      'gitee_id': gitee_id,
                      'emails': emails,
                      'created_at': '2021-08-11T11:21:39+08:00'}

            index_data = {"index": {"_index": "blue_zone_users_test", "_id": id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(action) + '\n'

        self.esClient.safe_put_bulk(actions)

    def getCellValue(self, row_index, cell_name, sheet, cell_name_index_dict):
        if cell_name not in cell_name_index_dict:
            return ''
        cell_value = sheet.cell_value(row_index, cell_name_index_dict.get(cell_name))
        return cell_value

    def get_blue_users(self):
        users_search = '''{"size": 10000,"query": {"bool": {"must": [{"match_all": {}}]}}}'''
        self.esClient.scrollSearch(index_name=self.user_index, search=users_search, func=self.blue_users_func)

    def blue_users_func(self, hits):
        for hit in hits:
            self.users.append(hit['_source'])

    def get_pr_gitee(self):
        indexs = str(self.gitee_pr_issue_index).split(";")
        search = '''{
                      "size": 200,
                      "_source": {
                        "includes": [
                          "created_at",
                          "issue_title",
                          "pull_state",
                          "gitee_repo",
                          "pull_id",
                          "url",
                          "is_gitee_pull_request"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "user_login.keyword": "%s"
                              }
                            },
                            {
                              "term": {
                                "is_gitee_pull_request": "1"
                              }
                            },
                            {
                              "range": {
                                "updated_at": {
                                  "gte": "%s",
                                  "lte": "%s"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (self.user['gitee_id'].strip(), self.startTime, self.endTime)
        for index in indexs:
            self.esClient.scrollSearch(index_name=index, search=search, func=self.pr_func_gitee)

    def pr_func_gitee(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            repo_data = {
                'created_at': source['created_at'],
                'pr_title': source['issue_title'],
                'pr_state': source['pull_state'],
                'repo': source['gitee_repo'],
                'is_pr': source['is_gitee_pull_request'],
                'pull_id': source['pull_id'],
                'pr_body': '',
                'url': source['url'],
            }
            repo_data.update(self.user)
            id = str(source['gitee_repo']) + 'pr' + str(source['pull_id'])
            index_id = hashlib.md5(id.encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.target_index, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_issue_gitee(self):
        indexs = str(self.gitee_pr_issue_index).split(";")
        search = '''{
                      "size": 200,
                      "_source": {
                        "includes": [
                          "created_at",
                          "issue_title",
                          "body",
                          "issue_state",
                          "gitee_repo",
                          "issue_id",
                          "url",
                          "is_gitee_issue"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "user_login.keyword": "%s"
                              }
                            },
                            {
                              "term": {
                                "is_gitee_issue": "1"
                              }
                            },
                            {
                              "range": {
                                "updated_at": {
                                  "gte": "%s",
                                  "lte": "%s"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (self.user['gitee_id'].strip(), self.startTime, self.endTime)
        for index in indexs:
            self.esClient.scrollSearch(index_name=index, search=search, func=self.issue_func_gitee)

    def issue_func_gitee(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            body = source['body'] if 'body' in source else ''
            repo_data = {
                'created_at': source['created_at'],
                'issue_title': source['issue_title'],
                'issue_state': source['issue_state'],
                'repo': source['gitee_repo'],
                'is_issue': source['is_gitee_issue'],
                'issue_id': source['issue_id'],
                'issue_body': body,
                'url': source['url'],
            }
            repo_data.update(self.user)
            id = str(source['gitee_repo']) + 'issue' + str(source['issue_id'])
            index_id = hashlib.md5(id.encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.target_index, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_pr_issue_comment_gitee(self):
        indexs = str(self.gitee_pr_issue_index).split(";")
        search = '''{
                      "size": 200,
                      "_source": {
                        "includes": [
                          "created_at",
                          "issue_id",
                          "pull_id",
                          "id",
                          "gitee_repo",
                          "url",
                          "issue_url",
                          "pull_url",
                          "body",
                          "is_gitee_comment"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "user_login.keyword": "%s"
                              }
                            },
                            {
                              "term": {
                                "is_gitee_comment": "1"
                              }
                            },
                            {
                              "range": {
                                "updated_at": {
                                  "gte": "%s",
                                  "lte": "%s"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (self.user['gitee_id'].strip(), self.startTime, self.endTime)
        for index in indexs:
            self.esClient.scrollSearch(index_name=index, search=search, func=self.pr_issue_comment_func_gitee)

    def pr_issue_comment_func_gitee(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            if 'url' not in source:
                url = ''
                if 'issue_url' in source:
                    url = source['issue_url']
                if 'pull_url' in source:
                    url = source['pull_url']
            else:
                url = source['url']
            parent = ''
            if 'issue_id' in source:
                parent = 'issue_comment'
            if 'pull_id' in source:
                parent = 'pr_comment'
            body = source['body'] if 'body' in source else ''
            repo_data = {
                'created_at': source['created_at'],
                'id': source['id'],
                'repo': source['gitee_repo'],
                'is_comment': source['is_gitee_comment'],
                'comment_body': body,
                'url': url,
            }
            repo_data.update(self.user)
            id = str(source['gitee_repo']) + parent + str(source['id'])
            index_id = hashlib.md5(id.encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.target_index, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_commit(self, indexs_str):
        indexs = str(indexs_str).split(";")
        search = '''{
                      "size": 200,
                      "_source": {
                        "includes": [
                          "created_at",
                          "add",
                          "remove",
                          "repo",
                          "commit_id"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "email.keyword": "%s"
                              }
                            },
                            {
                              "term": {
                                "is_merge": 0
                              }
                            },
                            {
                              "range": {
                                "created_at": {
                                  "gte": "%s",
                                  "lte": "%s"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (self.user['emails'].strip(), self.startTime, self.endTime)
        for index in indexs:
            self.esClient.scrollSearch(index_name=index, search=search, func=self.commit_func)

    def commit_func(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            repo_data = {
                'created_at': source['created_at'],
                'commit_id': source['commit_id'],
                'repo': source['repo'],
                'line_add': source['add'],
                'line_remove': source['remove'],
                'is_commit': 1,
            }
            repo_data.update(self.user)
            id = str(source['repo']) + 'commit' + str(source['commit_id'])
            index_id = hashlib.md5(id.encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.target_index, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_pr_github(self):
        indexs = str(self.github_pr_issue_index).split(";")
        search = '''{
                      "size": 200,
                      "_source": {
                        "includes": [
                          "created_at",
                          "pr_title",
                          "pr_state",
                          "github_repo",
                          "pr_id",
                          "html_url",
                          "is_github_pr"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "user_login.keyword": "%s"
                              }
                            },
                            {
                              "term": {
                                "is_github_pr": "1"
                              }
                            },
                            {
                              "range": {
                                "updated_at": {
                                  "gte": "%s",
                                  "lte": "%s"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (self.user['github_id'].strip(), self.startTime, self.endTime)
        for index in indexs:
            self.esClient.scrollSearch(index_name=index, search=search, func=self.pr_func_github)

    def pr_func_github(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            repo_data = {
                'created_at': source['created_at'],
                'pr_title': source['pr_title'],
                'pr_state': source['pr_state'],
                'repo': 'https://github.com/' + source['github_repo'],
                'is_pr': source['is_github_pr'],
                'pull_id': source['pr_id'],
                'pr_body': '',
                'url': source['html_url'],
            }
            repo_data.update(self.user)
            id = str(source['github_repo']) + 'pr' + str(source['pr_id'])
            index_id = hashlib.md5(id.encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.target_index, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_issue_github(self):
        indexs = str(self.github_pr_issue_index).split(";")
        search = '''{
                      "size": 200,
                      "_source": {
                        "includes": [
                          "created_at",
                          "issue_title",
                          "issue_state",
                          "issue_body",
                          "github_repo",
                          "issue_id",
                          "html_url",
                          "is_github_issue"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "user_login.keyword": "%s"
                              }
                            },
                            {
                              "term": {
                                "is_github_issue": "1"
                              }
                            },
                            {
                              "range": {
                                "updated_at": {
                                  "gte": "%s",
                                  "lte": "%s"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (self.user['github_id'].strip(), self.startTime, self.endTime)
        for index in indexs:
            self.esClient.scrollSearch(index_name=index, search=search, func=self.issue_func_github)

    def issue_func_github(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            repo_data = {
                'created_at': source['created_at'],
                'issue_title': source['issue_title'],
                'issue_state': source['issue_state'],
                'repo': 'https://github.com/' + source['github_repo'],
                'is_issue': source['is_github_issue'],
                'issue_id': source['issue_id'],
                'issue_body': source['issue_body'],
                'url': source['html_url'],
            }
            repo_data.update(self.user)
            id = str(source['github_repo']) + 'issue' + str(source['issue_id'])
            index_id = hashlib.md5(id.encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.target_index, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)

    def get_pr_issue_comment_github(self):
        indexs = str(self.github_pr_issue_index).split(";")
        search = '''{
                      "size": 200,
                      "_source": {
                        "includes": [
                          "created_at",
                          "issue_id",
                          "pr_id",
                          "id",
                          "github_repo",
                          "html_url",
                          "pr_comment_body",
                          "issue_comment_body",
                          "is_github_comment"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "user_login.keyword": "%s"
                              }
                            },
                            {
                              "term": {
                                "is_github_comment": "1"
                              }
                            },
                            {
                              "range": {
                                "updated_at": {
                                  "gte": "%s",
                                  "lte": "%s"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (self.user['github_id'].strip(), self.startTime, self.endTime)
        for index in indexs:
            self.esClient.scrollSearch(index_name=index, search=search, func=self.pr_issue_comment_func_github)

    def pr_issue_comment_func_github(self, hits):
        actions = ''
        for hit in hits:
            source = hit['_source']
            parent = body = ''
            if 'issue_id' in source:
                parent = 'issue_comment'
                body = source['issue_comment_body']
            if 'pr_id' in source:
                parent = 'pr_comment'
                body = source['pr_comment_body']
            repo_data = {
                'created_at': source['created_at'],
                'id': source['id'],
                'repo': 'https://github.com/' + source['github_repo'],
                'is_comment': source['is_github_comment'],
                'comment_body': body,
                'url': source['html_url'],
            }
            repo_data.update(self.user)
            id = str(source['github_repo']) + parent + str(source['id'])
            index_id = hashlib.md5(id.encode('utf-8')).hexdigest()
            index_data = {"index": {"_index": self.target_index, "_id": index_id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(repo_data) + '\n'
        self.esClient.safe_put_bulk(actions)
