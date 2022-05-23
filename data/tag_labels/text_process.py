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
import math
import re
import hanlp
from collections import Counter
# from nltk.corpus import stopwords


class TextProcess(object):
    def __init__(self, text):
        self.text = text
        self.HanLP = hanlp.load(hanlp.pretrained.mtl.CLOSE_TOK_POS_NER_SRL_DEP_SDP_CON_ELECTRA_SMALL_ZH)

    def body_clean(self, text):
        text = re.sub(r'\@(.*?)[^A-Za-z0-9\-_]', '', text)  # 用户名去除
        text = re.sub(r'\> \@(.*?)[^A-Za-z0-9\-_]', '', text)  # 用户名去除
        text = re.sub(r'\> (.*?)\n', '', text)  # 引用的评论进行去除

        pattern = re.compile(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')  # http去除
        text = re.sub(pattern, '', text)  # http网址去除
        text = re.sub(r'\s', ' ', text)  # 回车去除
        text = re.sub(r'```(.*?)```', '', text)  # ```代码去除
        text = re.sub(r'`(.*?)`', '', text)  # '  '文本去除
        text = re.sub(r'\#(.*?)\s', '', text)  # #37099 去除
        text = re.sub(r'!\[输入图片说明\]\((.*?)\"\)', '', text)  # #37099 去除
        text = re.sub(r'<!-- #请根据issue的类型在标题右侧下拉框中选择对应的选项（需求、缺陷或CVE等）-->', '', text)  # #37099 去除
        text = re.sub(r'<!-- #请根据issue相关的版本在里程碑中选择对应的节点，若是与版本无关，请选择“不关联里程碑”-->', '', text)  # #37099 去除

        return text

    def hanlp_text_spilt_noun(self, text_list, self_stopwords):
        pattern = re.compile(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')  # http去除
        text = re.sub(pattern, '', text_list)  # http网址去除
        lower = text.lower()  # 全部小写
        punctuation_set = "!#$%&'*+,/:;<.=>?@【】[\]^`{|}~"
        remove_punctuation_map = dict(
            (ord(char), None) for char in punctuation_set)  # 去除标点符号 !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
        no_punctuation = lower.translate(remove_punctuation_map)
        simple_punctuation = '[’!"#$%&\'*+,/:;.<=>?@[\\]^`{|}~，。,]'
        no_punctuation = re.sub(simple_punctuation, '', no_punctuation)

        if no_punctuation == '' or len(no_punctuation) == 1:
            return no_punctuation
        else:
            res = self.HanLP(text_list, tasks=['tok/fine', 'pos/pku'])
            tokens = res['tok/fine']
            stopwords_list = self_stopwords  # stopwords.words('english') + self_stopwords  # 中英文分词
            postag = res['pos/pku']  # 词性识别
            index = 0
            tokens_new = []
            for pos in postag:
                if 'n' in pos and 'nr' not in pos or 'm' in pos:  # 只选取n词
                    tokens_new.append(tokens[index])
                index += 1
            tokens = [w for w in tokens_new if not w in stopwords_list]
            return tokens

    def count_term(self, tokens):
        count = Counter(tokens)
        return count

    def tf_idf(self, countlist):
        title_tfidf = []
        index = 0
        for i, count in enumerate(countlist):
            scores = {word: self.tfidf(word, count, countlist) for word in count}
            sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            title_tfidf.append(sorted_words)
            index += 1
        return title_tfidf

    # 一段文本中出现的频率较高的词
    def tf(self, word, count):
        return count[word] / sum(count.values())

    def n_containing(self, word, count_list):
        return sum(1 for count in count_list if word in count)

    # 逆文档频率
    def idf(self, word, count_list):
        return math.log(len(count_list)) / (1 + self.n_containing(word, count_list))

    # TF-IDF的值
    def tfidf(self, word, count, count_list):
        return self.tf(word, count) * self.idf(word, count_list)
