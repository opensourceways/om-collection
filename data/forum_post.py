#  Copyright (c) 2023.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import json
import time

from data.common import ESClient

FORUMDOMAIM = 'https://forum.openeuler.org'


class ForumPost(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)

    def run(self, start):
        self.get_topics()

    def get_topics(self):
        url = FORUMDOMAIM + '/latest.json'
        page = 0
        while True:
            page += 1
            print('start page: ', page)
            params = {
                'no_definitions': 'true',
                'page': page
            }
            res = self.esClient.request_get(url=url, params=params)
            if res.status_code != 200:
                print('get page failed...')
                continue
            page_topics = res.json().get('topic_list').get('topics')
            if len(page_topics) <= 0:
                break
            actions = self.get_post(page_topics)
            self.esClient.safe_put_bulk(actions)

    def get_post(self, topics):
        actions = ''
        for topic in topics:
            print('start collecting topic: ', topic.get('title'))
            url = FORUMDOMAIM + '/t/%s/%s.json'
            url = url % (topic.get('slug'), str(topic.get('id')))
            page = 0
            while True:
                page += 1
                params = {
                    'track_visit': 'true',
                    'forceLoad': 'true',
                    'page': page
                }
                res = self.esClient.request_get(url=url, params=params)
                time.sleep(3)
                if res.status_code == 404:
                    print('collect topic over..')
                    break
                if res.status_code != 200:
                    print(topic.get('slug'), topic.get('id'), 'failed')
                    continue
                posts = res.json().get('post_stream').get('posts')
                if len(posts) <= 0:
                    print('collect topic over..')
                    break
                title = res.json().get('title')
                for post in posts:
                    post.update({'title': title})
                    id_str = str(post.get('id')) + '_' + str(topic.get('id'))
                    indexData = {"index": {"_index": self.index_name, "_id": id_str}}
                    actions += json.dumps(indexData) + '\n'
                    actions += json.dumps(post) + '\n'
        return actions

