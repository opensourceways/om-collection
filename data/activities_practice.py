import calendar
import datetime
import json
import re
import types
from json import JSONDecodeError
from collect.gitee import GiteeClient
from data.common import ESClient


class ActivitiesPractice(object):
    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.gitee_index_name = config.get('gitee_index_name')
        self.scroll_duration = config.get('scroll_duration')
        self.gitee_token = config.get('gitee_token')
        self.success_assign = config.get('success_assign')
        self.model_keys = config.get('model_keys')
        self.model_dict = {}

    def run(self, from_time):
        print("activities practice collect: start")
        self.get_model_dict()
        self.intern_issue()

    def get_model_dict(self):
        for model_key in self.model_keys.split(','):
            self.model_dict.update({model_key: ''})

    def intern_issue(self):
        search = '''{
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "term": {
                                "is_gitee_issue": "1"
                              }
                            },
                            {
                              "term": {
                                "issue_labels.keyword": "intern"
                              }
                            }
                          ],
                          "must_not": [],
                          "should": []
                        }
                      },
                      "size": 100
                    }'''
        self.esClient.scrollSearch(index_name=self.gitee_index_name, search=search,
                                   scroll_duration=self.scroll_duration, func=self.intern_issue_func)

    def intern_issue_func(self, hits):
        actions = ''
        count1 = []
        count2 = 0
        print('********* hits: %d ************' % len(hits))
        for hit in hits:
            count2 += 1
            try:
                res = {}
                source = hit['_source']
                if source['issue_title'].startswith('test') or source['issue_title'].startswith('测试'):
                    count1.append(source['issue_title'])
                    continue
                res['created_at'] = source['created_at']
                res['updated_at'] = source['updated_at']
                res['issue_title'] = source['issue_title']
                res['issue_type'] = source['issue_type']
                res['user_login'] = source['user_login']
                res['repository'] = source['repository']
                res['issue_state'] = source['issue_state']
                res['issue_url'] = source['issue_url']
                res['is_removed'] = 0

                ownerRepo = str(source['repository']).split('/')
                client = GiteeClient(ownerRepo[0], ownerRepo[1], self.gitee_token)

                if client.is_exists_issue(source['issue_url']) is False:
                    res['is_removed'] = 1

                comments = self.get_data(client.issue_comments(source['issue_number']))
                current_assign_user = ''
                before_assign_users = []
                for comment in comments:
                    body = comment['body']
                    if body is None or self.success_assign not in body:
                        continue
                    assign_user = re.findall('^@(.*) , ', body)[0]
                    before_assign_users.append(assign_user)
                    current_assign_user = assign_user
                    res['assign_time'] = comment['created_at']
                res['current_assign_user'] = current_assign_user
                res['before_assign_users'] = before_assign_users

                parse_dict = self.parse_issue_body(body=source['body'])
                res['score'] = re.match(r'\d+', parse_dict['任务分值']).group(0)
                res['background_desc'] = parse_dict['背景描述']
                res['requirement'] = parse_dict['需求描述']
                res['env_require'] = parse_dict['环境要求']
                res['output'] = parse_dict['产出标准']
                res['pr_commit_repo'] = parse_dict['PR提交地址']
                res['expected_completion_date'] = self.expected_completion_date(date_str=parse_dict['期望完成时间'])
                res['develop_guidance'] = parse_dict['开发指导']
                tutor = re.split(r'[ ,，\n]+', parse_dict['导师及邮箱'])
                if len(tutor) < 2:
                    res['tutor_login'] = re.sub(r'[^a-zA-Z0-9_]', '', tutor[0])
                else:
                    res['tutor_login'] = re.sub(r'[^a-zA-Z0-9_]', '', tutor[0])
                    res['tutor_email'] = re.sub(r'[^a-zA-Z0-9_@]', '', tutor[1])
                res['remark'] = parse_dict['备注'].strip()

                _id = source['repository'] + '_' + source['issue_number']
                indexData = {"index": {"_index": self.index_name, "_id": _id}}
                actions += json.dumps(indexData) + '\n'
                actions += json.dumps(res) + '\n'
                print('*********** index: %d, issue: %s' % (count2, res['issue_url']))
            except:
                continue
        print('count1: %s' % count1)
        self.esClient.safe_put_bulk(actions)

    def parse_issue_body(self, body):
        if body is None:
            return self.model_dict
        parse_dict = self.model_dict
        items = re.split(r'\n【', body)
        for item in items:
            if item is None or item == '':
                continue
            i = re.split(r'】', item, maxsplit=1)
            key = i[0].replace('【', '')
            if len(i) != 2 or key not in parse_dict:
                continue
            parse_dict.update({i[0].replace('【', ''): i[1].strip()})
        return parse_dict

    def expected_completion_date(self, date_str):
        ymd = re.findall(r'\d+', date_str)
        if len(ymd) < 2:
            return None

        year = int(ymd[0])
        month = int(ymd[1])
        if month < 0 or month > 12:
            return None

        try:
            return datetime.datetime(year, month, int(ymd[2])).strftime('%Y-%m-%d')
        except ValueError or TypeError:
            monthCountDay = calendar.monthrange(year, month)[1]
            return datetime.date(year, month, day=monthCountDay).strftime('%Y-%m-%d')

    def get_data(self, response):
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
                    # if isinstance(data, dict):
                    #     data = []
                    break
        except StopIteration:
            return data
        except JSONDecodeError:
            print("Gitee get JSONDecodeError, error: ", response)
        except Exception as ex:
            print('*** getGenerator fail ***', ex)
            return data

        return data
