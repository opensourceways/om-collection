import yaml
from data import common
from data.common import ESClient
from data.tag_labels.text_process import TextProcess


class IssueRuleLabel(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.orgs = config.get('orgs')
        self.index_name = config.get('index_name')
        self.rule_yaml = config.get('rule_yaml', 'issue_rule_label.yaml')
        self.stopwords_file = config.get('stopwords_file', 'cn_stopwords.txt')
        self.text_process = TextProcess(text=None)
        self.countlist = []
        self.label_rules = {}
        self.cn_stopwords = []

    def run(self, from_time):
        # 自定义停用词词典
        self.get_stopwords()

        # 自定义规则标签字典
        self.get_label_rule()

        # 获取issue数据，使用自定义规则标签
        search = '''{
                      "size": 10,
                      "_source": {
                        "includes": [
                          "issue_title",
                          "body"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "term": {
                                "is_gitee_issue": 1
                              }
                            },
                            {
                              "exists": {
                                "field": "body"
                              }
                            }
                          ]
                        }
                      }
                    }'''
        self.esClient.scrollSearch(index_name=self.index_name, search=search, scroll_duration='1m',
                                   func=self.rule_label_func)

    # 自定义停用词词典
    def get_stopwords(self):
        f = open("data/tag_labels/cn_stopwords.txt", "r", encoding='utf8')
        for word in f.readlines():
            self.cn_stopwords.append(word.strip('\n'))

    # 自定义规则标签字典
    def get_label_rule(self):
        datas = yaml.load_all(open(self.rule_yaml, 'r', encoding='utf8')).__next__()
        for data in datas['label_rules']:
            self.label_rules.update({data['label']: data['aliases']})

    # 基于匹配的标签
    def rule_label_func(self, hits):
        actions = ''
        for hit in hits:
            hit_id = hit['_id']
            source = hit['_source']
            title = source['issue_title']
            body = source['body'] if 'body' in source else ''
            text = title + ',' + body

            # 数据去噪
            word_list = self.text_process.body_clean(text=text)

            # 分词
            tokens = self.text_process.hanlp_text_spilt_noun(text_list=word_list, self_stopwords=self.cn_stopwords)

            # 匹配
            labels = []
            for token in tokens:
                for label, rule in self.label_rules.items():
                    if token in rule:
                        labels.append(label)

            if len(labels) == 0:
                continue
            update_data = {
                "doc": {
                    "rule_labes": labels,
                }
            }
            action = common.getSingleAction(self.index_name, hit_id, update_data, act="update")
            actions += action

        self.esClient.safe_put_bulk(actions)

    # 所有ISSUE，生成TF-IDF语料库
    def tf_idf_word_count_func(self, hits):
        for hit in hits:
            source = hit['_source']
            title = source['issue_title']
            body = source['body'] if 'body' in source else ''
            text = title + ',' + body

            # 数据去噪
            word_list = self.text_process.body_clean(text=text)

            # 分词
            tokens = self.text_process.hanlp_text_spilt_noun(text_list=word_list, self_stopwords=self.cn_stopwords)

            # 词频语料库
            self.countlist.append(self.text_process.count_term(tokens))

    # 基于关键词提取的标签
    def tf_idf_label_func(self, hits):
        actions = ''
        for hit in hits:
            hit_id = hit['_id']
            source = hit['_source']
            title = source['issue_title']
            body = source['body'] if 'body' in source else ''
            text = title + ',' + body

            # 数据去噪
            word_list = self.text_process.body_clean(text=text)

            # 分词
            tokens = self.text_process.hanlp_text_spilt_noun(text_list=word_list, self_stopwords=self.cn_stopwords)

            # TF-IDF关键词提取
            labels = self.text_process.tf_idf(countlist=tokens)

            if len(labels) == 0:
                continue
            update_data = {
                "doc": {
                    "rule_labes": labels,
                }
            }
            action = common.getSingleAction(self.index_name, hit_id, update_data, act="update")
            actions += action

        self.esClient.safe_put_bulk(actions)
