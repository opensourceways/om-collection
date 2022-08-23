import datetime
import hashlib
import json
import os
import xml.etree.ElementTree as ET
from collections import Counter
from dateutil import relativedelta
from data.common import ESClient


class RpmRepoDownload(object):
    def __init__(self, config=None):
        self.config = config
        self.org = config.get('org')
        self.esClient = ESClient(config)
        self.start_date = config.get('start_date')
        self.index_name = config.get('index_name')
        self.repo_index = config.get('repo_index')
        self.download_index_pre = config.get('download_index_pre')
        self.primary_xml_base = config.get('primary_xml_base')
        self.rsync = config.get('rsync')
        self.rsync_local_path = config.get('rsync_local_path')
        self.is_sync_update = config.get('is_sync_update')
        self.rpm_primary_source = {}
        self.source_repo = {'java-1.8.0-openjdk': 'openjdk-1.8.0',
                            'java-11-openjdk': 'openjdk-11',
                            'java-latest-openjdk': 'openjdk-latest'}

    def run(self, from_time):
        print("*** rpm repo download start ***")
        self.repos()

        # 二进制包对应的仓库
        self.getAllPrimaryXml()

        # 按月分割下载数据后的index
        download_indexes = self.getDownloadIndexes()

        # 按月统计每个仓库的下载量
        self.repoDownloadStatics(indexes=download_indexes)

        # 全量获取所有PrimaryXml（第一次执行），后续仅需要获取更新过的PrimaryXml
        self.is_sync_update = 'true'

        print("*** rpm repo download finish ***")

    def repoDownloadStatics(self, indexes):
        repo_rpm = {}
        for i, v in self.source_repo.items():
            repo_rpm[v] = [i] if v not in repo_rpm.keys() else repo_rpm[v] + [i]

        for index in indexes:
            date_index = index.split('_')[-1]
            date_str = str(date_index[0:4]) + '-' + str(date_index[4:]) + '-' + '01'
            for repo, rpms in repo_rpm.items():
                print('*** statics repo: %s, date: %s ***' % (repo, date_str))
                counter = Counter({})
                for rpm in rpms:
                    counter = self.statics(index=index, rpm=rpm, counter=counter)
                actions = ''
                for key, value in counter.items():
                    action = {
                        'repo': repo,
                        'package_version': key,
                        'download_count': value,
                        'created_at': date_str
                    }
                    id = repo + key + date_str
                    index_id = hashlib.md5(id.encode('utf-8')).hexdigest()
                    indexData = {"index": {"_index": self.index_name, "_id": index_id}}
                    actions += json.dumps(indexData) + '\n'
                    actions += json.dumps(action) + '\n'
                self.esClient.safe_put_bulk(actions)

    def statics(self, index, rpm, counter):
        search = '''{
                  "size":0,
                  "query": {
                    "bool": {
                      "must": [
                        {
                          "term": {
                            "is_rpm_download": "1"
                          }
                        },
                        {
                          "match_phrase": {
                            "path": "%s"
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "versions": {
                      "terms": {
                        "field": "package_version.keyword",
                        "size": 10000,
                        "min_doc_count": 1
                      }
                    }
                  }
                }''' % rpm
        data = self.esClient.esSearch(index_name=index, search=search)
        for bucket in data['aggregations']['versions']['buckets']:
            counter = counter + Counter({bucket['key']: bucket['doc_count']})
        return counter

    def getDownloadIndexes(self):
        yesterday = datetime.date.today() + datetime.timedelta(days=-1)
        end = datetime.date(yesterday.year, yesterday.month, 1)
        download_indexes = []
        if self.start_date:
            sd_sp = self.start_date.split('-')
            begin = datetime.date(int(sd_sp[0]), int(sd_sp[1]), 1)
            while begin <= end:
                sp = begin.strftime("%Y%m")
                download_indexes.append(self.download_index_pre + '_' + sp)
                begin = begin + relativedelta.relativedelta(months=1)
        else:
            download_indexes.append(self.download_index_pre + '_' + end.strftime("%Y%m"))
        return download_indexes

    def getAllPrimaryXml(self):
        cmd_rsync = '''rsync -a -v -r --include={'*primary.xml.gz','*/'} --exclude=* --partial --progress --delete %s %s''' % (
            self.rsync, self.rsync_local_path)
        if self.is_sync_update == 'true':
            files_pop = os.popen(cmd_rsync)
        else:
            print('*** cmd_sync: %s' % cmd_rsync)
            os.system(cmd_rsync)
            cmd_find = 'find %s -name *-primary.xml.gz' % self.rsync_local_path
            print('*** cmd_find: %s' % cmd_find)
            files_pop = os.popen(cmd_find)

        files = files_pop.readlines()
        for file in files:
            file = file.replace('\n', '')
            print(file)
            if file.endswith('-primary.xml.gz') is False:
                continue
            cmd_gzip = 'gzip -d %s -k' % file
            os.system(cmd_gzip)

            de_file = file.replace('.gz', '')
            self.parsePrimaryXml(de_file)

            cmd_rm = 'rm -r %s' % de_file
            os.system(cmd_rm)
            print('*** parse file: %s' % de_file)

    def parsePrimaryXml(self, file):
        tree = ET.parse(file)
        # 根节点
        root = tree.getroot()
        # 标签名
        root_tag = root.tag
        # 命名空间
        sp = root_tag.split('}')
        if len(sp) == 2:
            ns = sp[0] + '}'
        else:
            ns = ''
        format_ele_ns = ''
        c = 0

        packages = root.findall(ns + 'package')
        for package in packages:
            try:
                name = package.find(ns + 'name').text
                ver_ele = package.find(ns + 'version')
                version = ver_ele.attrib['ver']
                rel = ver_ele.attrib['rel']

                format_ele = package.find(ns + 'format')
                if c == 0:
                    format_ele_tag = format_ele[0].tag
                    format_ele_sp = format_ele_tag.split('}')
                    if len(format_ele_sp) == 2:
                        format_ele_ns = format_ele_sp[0] + '}'

                source_rpm = format_ele.find(format_ele_ns + 'sourcerpm').text

                rpm_primary_name = name + '-' + version
                rpm_source_name = source_rpm.split(('-%s' % rel))[0]
                self.rpm_primary_source[rpm_primary_name] = rpm_source_name
                if rpm_source_name in self.source_repo:
                    self.source_repo[rpm_primary_name] = self.source_repo[rpm_source_name]
                else:
                    print('%s    %s  not in repo_source' % (rpm_primary_name, rpm_source_name))
                c += 1
            except Exception as e:
                c += 1
                continue

    def repos(self):
        query = '''{
                  "size":1000,
                  "query": {
                    "bool": {
                      "must": [
                        {
                          "term": {
                            "is_gitee_repo": "1"
                          }
                        },
                        {
                          "match": {
                            "org_name.keyword": "src-openeuler"
                          }
                        },
                        {
                          "exists": {
                            "field": "branch_detail"
                          }
                        }
                      ],
                      "must_not": [
                        {
                          "term": {
                            "is_removed": "1"
                          }
                        }
                      ]
                    }
                  }
                }'''
        self.esClient.scrollSearch(index_name=self.repo_index, search=query, scroll_duration='1m',
                                   func=self.repoDataFunc)

    def repoDataFunc(self, hits):
        for hit in hits:
            source = hit['_source']
            repo_name = source['repository'].replace('src-openeuler/', '')
            branch_detail = source['branch_detail']
            for branch in branch_detail:
                try:
                    package_name = branch['package_name']
                    version = branch['version']
                    package_source = '%s-%s' % (package_name, version)
                    package_source1 = '%s-%s' % (repo_name, version)
                    self.source_repo[package_source] = repo_name
                    self.source_repo[package_source1] = repo_name
                except Exception as e:
                    print('*** repo has no branch_detail: %s ***' % repo_name)
                    continue
