#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2024/12/15

import json
import time
import ssl
import urllib.parse
import pytz
import logging

from data.common import ESClient
from pymongo import MongoClient
from data import common
from datetime import datetime

logger = logging.getLogger(__name__)

class XiheUserProfile(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.index_name = config.get('index_name')
        self.index_name_user_info = config.get('index_name_user_info')
        self.mongodb_url = config.get('mongodb_url')
        self.mongodb_tlscafile = config.get('mongodb_tlscafile')

    def run(self, from_time):
        logger.info("Collect xihe user profile data: staring")
        start_time = time.time()
        self.get_all_data()
        end_time = time.time()
        spent_time = time.strftime("%H:%M:%S", time.gmtime(end_time - start_time))
        logger.info("Collect xihe user profile data finished after %s" % spent_time)

    def get_all_data(self):
        with MongoClient(self.mongodb_url, ssl=True, tlscafile=self.mongodb_tlscafile, tlsallowinvalidhostnames=True) as client:
            db = client['xihe-new']
            self.collect_project(db)
            self.collect_model(db)
            self.collect_dataset(db)
            self.collect_course(db)
            self.collect_competition(db)
            self.collect_promotion(db)
            self.collect_user_info(db)

    def collect_project(self, db):
        collection = db['project']
        documents = collection.find()
        if not documents:
            logger.warning("table project null")
            return
        type = 'project'
        actions = ''
        for doc in documents:
            owner = doc.get("owner")
            items = doc.get("items")
            if items:
                for item in items:
                    name = item.get("name")
                    if not name:
                        continue
                    id = item.get("id")
                    like_count = int(item.get("like_count", 0))
                    fork_count = int(item.get("fork_count", 0))
                    download_count = int(item.get("download_count", 0))
                    tz = pytz.timezone('Asia/Shanghai')
                    dt_created_at = datetime.utcfromtimestamp(item.get("created_at"))
                    created_at = pytz.utc.localize(dt_created_at).astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S')
                    dt_updated_at = datetime.utcfromtimestamp(item.get("updated_at"))
                    updated_at = pytz.utc.localize(dt_updated_at).astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S')
                    es_id = f'{type}_{owner}_{id}'
                    content_body = {
                        'id': id,
                        'username': owner,
                        'name': name,
                        'like_count': like_count,
                        'fork_count': fork_count,
                        'download_count': download_count,
                        'timestamp': created_at,
                        'created_at': created_at,
                        'updated_at': updated_at,
                        'type': type}
                    action = common.getSingleAction(self.index_name, es_id, content_body)
                    actions += action
        self.esClient.safe_put_bulk(actions)
        logger.info("Collect project success")

    def collect_model(self, db):
        collection = db['model']
        documents = collection.find()
        if not documents:
            logger.warning("table model null")
            return
        type = 'model'
        actions = ''
        for doc in documents:
            owner = doc.get("owner")
            items = doc.get("items")
            if items:
                for item in items:
                    name = item.get("name")
                    if not name:
                        continue
                    id = item.get("id")
                    like_count = int(item.get("like_count", 0))
                    download_count = int(item.get("download_count", 0))
                    tz = pytz.timezone('Asia/Shanghai')
                    dt_created_at = datetime.utcfromtimestamp(item.get("created_at"))
                    created_at = pytz.utc.localize(dt_created_at).astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S')
                    dt_updated_at = datetime.utcfromtimestamp(item.get("updated_at"))
                    updated_at = pytz.utc.localize(dt_updated_at).astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S')
                    es_id = f'{type}_{owner}_{id}'
                    content_body = {
                        'id': id,
                        'username': owner,
                        'name': name,
                        'like_count': like_count,
                        'download_count': download_count,
                        'timestamp': created_at,
                        'created_at': created_at,
                        'updated_at': updated_at,
                        'type': type}
                    action = common.getSingleAction(self.index_name, es_id, content_body)
                    actions += action
        self.esClient.safe_put_bulk(actions)
        logger.info("Collect model success")

    def collect_dataset(self, db):
        collection = db['dataset']
        documents = collection.find()
        if not documents:
            logger.warning("table dataset null")
            return
        type = 'dataset'
        actions = ''
        for doc in documents:
            owner = doc.get("owner")
            items = doc.get("items")
            if items:
                for item in items:
                    name = item.get("name")
                    if not name:
                        continue
                    id = item.get("id")
                    like_count = int(item.get("like_count", 0))
                    download_count = int(item.get("download_count", 0))
                    tz = pytz.timezone('Asia/Shanghai')
                    dt_created_at = datetime.utcfromtimestamp(item.get("created_at"))
                    created_at = pytz.utc.localize(dt_created_at).astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S')
                    dt_updated_at = datetime.utcfromtimestamp(item.get("updated_at"))
                    updated_at = pytz.utc.localize(dt_updated_at).astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S')
                    es_id = f'{type}_{owner}_{id}'
                    content_body = {
                        'id': id,
                        'username': owner,
                        'name': name,
                        'like_count': like_count,
                        'download_count': download_count,
                        'timestamp': created_at,
                        'created_at': created_at,
                        'updated_at': updated_at,
                        'type': type}
                    action = common.getSingleAction(self.index_name, es_id, content_body)
                    actions += action
        self.esClient.safe_put_bulk(actions)
        logger.info("Collect dataset success")

    def collect_course(self, db):
        course_map = {}
        course_collection = db['course']
        course_documents = course_collection.find()
        if not course_documents:
            logger.warning("table course null")
            return
        for doc in course_documents:
            id = doc.get("id")
            name = doc.get("name")
            duration = doc.get("duration")
            if id:
                start_date, end_date = duration.split('-')
                course_map[id] = {
                    'name': name,
                    'start_date': start_date,
                    'end_date': end_date
                }
        record_collection = db['course_record']
        record_documents = record_collection.find()
        if not record_documents:
            logger.warning("table course_record null")
            return
        type = 'course'
        actions = ''
        for doc in record_documents:
            owner = doc.get("account")
            course_id = doc.get("course_id")
            if course_id in course_map:
                course_data = course_map.get(course_id)
                start_date = course_data.get("start_date")
                end_date = course_data.get("end_date")
                name = course_data.get("name")
                finish_count = int(doc.get("finish_count", 0))
                play_count = int(doc.get("play_count", 0))
                if start_date.count('.') == 1:
                    timestamp = start_date.replace('.', '-') + '-01T08:00:00'
                else:
                    timestamp = start_date.replace('.', '-') + 'T08:00:00'
                es_id = f'{type}_{owner}_{course_id}'
                content_body = {
                    'id': course_id,
                    'username': owner,
                    'name': name,
                    'finish_count': finish_count,
                    'play_count': play_count,
                    'timestamp': timestamp,
                    'start_at': start_date,
                    'end_at': end_date,
                    'type': type}
                action = common.getSingleAction(self.index_name, es_id, content_body)
                actions += action
        self.esClient.safe_put_bulk(actions)
        logger.info("Collect course success")

    def collect_competition(self, db):
        competition_map = {}
        competition_collection = db['competition']
        competition_documents = competition_collection.find()
        if not competition_documents:
            logger.warning("table competition null")
            return
        for doc in competition_documents:
            id = doc.get("id")
            name = doc.get("name")
            duration = doc.get("duration")
            if id:
                start_date, end_date = duration.split('-')
                competition_map[id] = {
                    'name': name,
                    'start_date': start_date,
                    'end_date': end_date
                }
        record_collection = db['competition_player']
        record_documents = record_collection.find()
        type = 'competition'
        actions = ''
        if not record_documents:
            logger.warning("table competition_player null")
            return
        for doc in record_documents:
            leader = doc.get("leader")
            cid = doc.get("cid")
            competitors = doc.get('competitors');
            if cid in competition_map and competitors:
                for item in competitors:
                    course_data = competition_map.get(cid)
                    start_date = course_data.get("start_date")
                    end_date = course_data.get("end_date")
                    name = course_data.get("name")
                    account = item.get("account")
                    if leader == account:
                        is_leader = True
                    else:
                        is_leader = False
                    if start_date.count('.') == 1:
                        timestamp = start_date.replace('.', '-') + '-01T08:00:00'
                    else:
                        timestamp = start_date.replace('.', '-') + 'T08:00:00'
                    es_id = f'{type}_{account}_{cid}'
                    content_body = {
                        'id': cid,
                        'username': account,
                        'name': name,
                        'is_leader': is_leader,
                        'timestamp': timestamp,
                        'start_at': start_date,
                        'end_at': end_date,
                        'type': type}
                    action = common.getSingleAction(self.index_name, es_id, content_body)
                    actions += action
        self.esClient.safe_put_bulk(actions)
        logger.info("Collect competition success")

    def collect_promotion(self, db):
        collection = db['promotion']
        documents = collection.find()
        type = 'promotion'
        actions = ''
        if not documents:
            logger.warning("table promotion null")
            return
        for doc in documents:
            id = doc.get("id")
            name = doc.get("name")
            start_time = doc.get("start_time")
            end_time = doc.get("end_time")
            reg_users = doc.get("reg_users")
            if reg_users:
                for item in reg_users:
                    owner = item.get("user")
                    if not owner:
                        continue
                    tz = pytz.timezone('Asia/Shanghai')
                    dt_created_at = datetime.utcfromtimestamp(item.get("created_at"))
                    timestamp = pytz.utc.localize(dt_created_at).astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S')
                    dt_start_at = datetime.utcfromtimestamp(start_time)
                    start_at = pytz.utc.localize(dt_start_at).astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S')
                    dt_end_time = datetime.utcfromtimestamp(end_time)
                    end_at = pytz.utc.localize(dt_end_time).astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S')
                    es_id = f'{type}_{owner}_{id}'
                    content_body = {
                        'id': id,
                        'username': owner,
                        'name': name,
                        'timestamp': timestamp,
                        'start_at': start_at,
                        'end_at': end_at,
                        'type': type}
                    action = common.getSingleAction(self.index_name, es_id, content_body)
                    actions += action
        self.esClient.safe_put_bulk(actions)
        logger.info("Collect promotion success")

    def collect_user_info(self, db):
        collection = db['registration']
        documents = collection.find()
        if not documents:
            logger.warning("table registration null")
            return
        type = 'userinfo'
        actions = ''
        for doc in documents:
            account = doc.get("account")
            detail = doc.get("detail")
            if detail:
                detail1 = detail.get("detail1", "None")
                detail2 = detail.get("detail2", "None")
            city = doc.get("city")
            email = doc.get("email")
            identity = doc.get("identity", "other")
            if identity == '':
                identity = "other"
            name = doc.get("name")
            phone = doc.get("phone")
            province = doc.get("province")
            timestamp = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())
            es_id = f'{type}_{account}'
            content_body = {
                'username': account,
                'name': name,
                'province': province,
                'city': city,
                'email': email,
                'phone': phone,
                'identity': identity,
                'company': detail1,
                'profession': detail2,
                'timestamp': timestamp}
            action = common.getSingleAction(self.index_name_user_info, es_id, content_body)
            actions += action
        self.esClient.safe_put_bulk(actions)
        logger.info("Collect user info success")