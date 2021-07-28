import json
import time
import types
from json import JSONDecodeError

from obs import ObsClient

from collect.gitee import GiteeClient
from collect.github import GithubClient
from data.common import ESClient


class TryMe(object):
    def __init__(self, config=None):
        self.esClient = ESClient(config)
        self.eamil_gitee_index = config.get('eamil_gitee_index')
        self.index_name = config.get('index_name')
        self.ak = config.get("access_key_id")
        self.sk = config.get("secret_access_key")
        self.server = config.get("server")
        self.bucket_name = config.get("bucket_name")
        self.object_key = config.get("object_key")
        self.gitee_token = config.get("gitee_token")
        self.github_token = config.get("github_token")
        self.obs_client = ObsClient(access_key_id=self.ak, secret_access_key=self.sk, server=self.server)
        self.gitee_client = GiteeClient(owner=None, repository=None, token=self.gitee_token)
        self.giteehub_client = GithubClient(org=None, repository=None, token=self.github_token)

    def run(self, startTime):
        email_gitee = self.getEmailGiteeDict()
        self.userLogin(email_gitee)

    def userLogin(self, email_gitee):
        resp = self.obs_client.getObject(bucketName=self.bucket_name, objectKey=self.object_key)
        if resp.status > 300:
            return
        chunk = resp.body.response.read(resp.body.contentLength)
        if not chunk:
            return
        json_node = json.loads(chunk)
        actions = ''
        now = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())
        for _, user_info in json_node['userInfoMap'].items():
            res = user_info
            id = user_info['id']
            name = user_info['name']
            email = user_info['email']

            if str(id).startswith('gitee'):
                user = self.getGenerator(self.gitee_client.user(name))
            elif str(id).startswith('github'):
                user = self.giteehub_client.getUserByName(name)
            else:
                continue

            if 'login' in user:
                ctime = user['created_at']
                if ctime.__contains__('Z'):
                    ctime = ctime.replace('Z', '+08:00')
                utime = user['updated_at']
                if utime.__contains__('Z'):
                    utime = utime.replace('Z', '+08:00')
                res['user_id'] = user['id']
                res['user_login'] = user['login']
                res['user_name'] = user['name']
                res['user_html_url'] = user['html_url']
                res['created_at'] = ctime
                res['updated_at'] = utime
            elif email is not None and email in email_gitee:
                res['user_login'] = email_gitee.get(email)
                res['created_at'] = now
                res['updated_at'] = now
                print('get user_login by email success. email=%s, id=%s, name=%s' % (email, id, name))
            else:
                print('Not Found login by name=%s, id=%s, email=%s' % (name, id, email))

            index_data = {"index": {"_index": self.index_name, "_id": id}}
            actions += json.dumps(index_data) + '\n'
            actions += json.dumps(res) + '\n'
        self.esClient.safe_put_bulk(actions)

    def getGenerator(self, response):
        data = ''
        try:
            if isinstance(response, types.GeneratorType):
                res_data = next(response)
                data = json.loads(res_data.encode('utf-8'))
            else:
                data = json.loads(response)
        except JSONDecodeError:
            return data
        return data

    def getEmailGiteeDict(self):
        search = '"must": [{"match_all": {}}]'
        hits = self.esClient.searchEsList(index_name=self.eamil_gitee_index, search=search)
        data = {}
        if hits is not None and len(hits) > 0:
            for hit in hits:
                source = hit['_source']
                data.update({source['email']: source['gitee_id']})
        return data
