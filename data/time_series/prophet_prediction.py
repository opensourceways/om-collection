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
import datetime
import json
import time
from data.common import ESClient
from prophet import Prophet
import pandas as pd


class ProphetPrediction(object):
    def __init__(self, config=None):
        self.config = config
        self.esClient = ESClient(config)
        self.orgs = config.get('orgs')
        self.index_name = config.get('index_name')
        self.sig_index = config.get('sig_index')
        self.skip_users = config.get('skip_users')
        self.is_user_prediction = config.get('is_user_prediction')
        self.is_sig_prediction = config.get('is_sig_prediction')
        self.user_prediction_index = config.get('user_prediction_index')
        self.sig_prediction_index = config.get('sig_prediction_index')
        self.start_date = config.get('start_date', '2019-01-01')
        self.end_date = config.get('end_date', self.get_last_day_of_last_month())
        self.prediction_periods = int(config.get('prediction_periods', '3'))
        self.event_weight = config.get('event_weight')
        self.events_weight = {}
        self.single_user_events = []
        self.repo_sigs = {}
        self.sigs_time_series = []

    def run(self, from_time):
        if self.event_weight is None:
            print('*** must set event_weight ***')
            return
        self.get_event_weight()

        if self.is_user_prediction == 'true':
            all_user_buckets = self.esClient.getTotalAuthorName(field="actor.login.keyword", size=10000)
            print('all_user_buckets: %d' % len(all_user_buckets))
            self.user_activity_predict(all_user_buckets)
        if self.is_sig_prediction == 'true':
            self.repo_sigs = self.esClient.getRepoSigs()
            self.sig_activity_predict()

    def user_activity_predict(self, all_user_buckets):
        for user_bucket in all_user_buckets:
            user = user_bucket['key']
            if self.skip_users and user in str(self.skip_users).split(','):
                continue
            user_time_series = []
            search = '''{
                                  "size": 10000,
                                  "_source": {
                                    "includes": [
                                      "type",
                                      "created_at",
                                      "actor.login"
                                    ]
                                  },
                                  "query": {
                                    "bool": {
                                      "must": [
                                        {
                                          "match": {
                                            "actor.login.keyword": "%s"
                                          }
                                        },
                                        {
                                          "range": {
                                            "created_at": {
                                                "gte": "%sT00:00:00+08:00",
                                                "lte": "%sT23:59:59+08:00"
                                            }
                                          }
                                        }
                                      ]
                                    }
                                  }
                                }''' % (user, self.start_date, self.end_date)
            self.esClient.scrollSearch(index_name=self.index_name, search=search, scroll_duration='1m',
                                       func=self.single_user_event_func)
            for event in self.single_user_events:
                if event['event_type'] not in self.events_weight:
                    user_activity = 0
                else:
                    user_activity = self.events_weight[event['event_type']]
                date_month = event['created_at'][0:7]
                # 用户时间序列活跃度数据
                user_action = {
                    'actor_login': event['user_login'],
                    'ds': date_month,
                    'y': user_activity,
                    'predict_label': 0
                }
                user_time_series.append(user_action)
            if len(user_time_series) == 0:
                continue

            # 用户时间序列活跃度数据 -> DataFrame
            user_df = pd.DataFrame(user_time_series).groupby(['actor_login', 'ds']).sum('popularity').sort_values(
                'ds').reset_index()

            # 数据量小于2不能做预测
            if len(user_df) < 2:
                print('%s: Dataframe has less than 2 non-NaN rows' % user)
                self.single_user_events = []
                continue

            # 用户活跃度预测
            predict_result = self.time_series_predict(time_series_data=user_df, item='actor_login', item_value=user,
                                                      activity_cap=300)
            # 将活跃度映射到[active, potential, churned]
            result = self.churn_state(predict_result=predict_result)

            # 数据写入ES
            actions = ''
            for index, data in result.iterrows():
                created_at = time.strftime("%Y-%m-%d", time.strptime(data['ds'], "%Y-%m"))
                action = {
                    'user_login': data['actor_login'],
                    'date_month': data['ds'],
                    'activity_metric': data['y'],
                    'predict_label': data['predict_label'],
                    'churn_label': data['churn_label'],
                    'created_at': created_at,
                }
                index_id = data['actor_login'] + '_' + data['ds']
                index_data = {"index": {"_index": self.user_prediction_index, "_id": index_id}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'
            self.esClient.safe_put_bulk(actions)

            self.single_user_events = []

    def single_user_event_func(self, hits):
        for hit in hits:
            source = hit['_source']
            event = {
                'user_login': source['actor']['login'],
                'event_type': source['type'],
                'created_at': source['created_at'],
            }
            self.single_user_events.append(event)

    def sig_activity_predict(self):
        # 所有的sig的活跃度数据
        search = '''{
                      "size": 10000,
                      "_source": {
                        "includes": [
                          "type",
                          "created_at",
                          "repo.full_name"
                        ]
                      },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "range": {
                                "created_at": {
                                  "gte": "%sT00:00:00+08:00",
                                  "lte": "%sT23:59:59+08:00"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (self.start_date, self.end_date)
        self.esClient.scrollSearch(index_name=self.index_name, search=search, scroll_duration='1m',
                                   func=self.sig_activity_func)
        sigs_df = pd.DataFrame(self.sigs_time_series).groupby(['sig', 'ds']).sum('popularity').sort_values(
            'ds').reset_index()
        sigs = set(sigs_df['sig'].values)

        for sig in sigs:
            sig_df = sigs_df[sigs_df['sig'] == sig]
            # 数据量小于2不能做预测
            if len(sig_df) < 2:
                print('%s: Dataframe has less than 2 non-NaN rows' % sig)
                continue

            # sig活跃度预测
            predict_result = self.time_series_predict(time_series_data=sig_df, item='sig', item_value=sig,
                                                      activity_cap=500)

            # 将活跃度映射到[active, potential, churned]
            result = self.churn_state(predict_result=predict_result)

            # 数据写入ES
            actions = ''
            for index, data in result.iterrows():
                created_at = time.strftime("%Y-%m-%d", time.strptime(data['ds'], "%Y-%m"))
                action = {
                    'sig_name': data['sig'],
                    'date_month': data['ds'],
                    'activity_metric': data['y'],
                    'predict_label': data['predict_label'],
                    'churn_label': data['churn_label'],
                    'created_at': created_at,
                }
                index_id = data['sig'] + '_' + data['ds']
                index_data = {"index": {"_index": self.sig_prediction_index, "_id": index_id}}
                actions += json.dumps(index_data) + '\n'
                actions += json.dumps(action) + '\n'
            self.esClient.safe_put_bulk(actions)

    def sig_activity_func(self, hits):
        for hit in hits:
            source = hit['_source']
            repo = source['repo']['full_name']
            if repo not in self.repo_sigs:
                continue
            sigs = self.repo_sigs[repo]

            if source['type'] not in self.events_weight:
                sig_activity = 0
            else:
                sig_activity = self.events_weight[source['type']]

            # sig时间序列活跃度数据
            for sig in sigs:
                sig_action = {
                    'sig': sig,
                    'ds': source['created_at'][0:7],
                    'y': sig_activity,
                    'predict_label': 0
                }
                self.sigs_time_series.append(sig_action)

    def time_series_predict(self, time_series_data, item, item_value, activity_cap=500):
        # 活跃度预测值限制为[activity_floor, activity_cap]
        # time_series_data['floor'] = 0
        # time_series_data['cap'] = activity_cap

        model = Prophet(seasonality_mode='additive', changepoint_prior_scale=0.5).fit(time_series_data)
        future = model.make_future_dataframe(periods=self.prediction_periods, freq='MS')
        # future['floor'] = 0
        # future['cap'] = activity_cap
        future_time_series_data = model.predict(future)

        # print("****** 原始数据 ******")
        # print(time_series_data)
        # print("****** 预测数据 ******")
        # print(future_time_series_data)

        future_time_series_data = future_time_series_data.tail(3)[['ds', 'yhat']]
        future_time_series_data.rename(columns={'yhat': 'y'}, inplace=True)
        future_time_series_data['ds'] = future_time_series_data['ds'].map(lambda x: x.strftime('%Y-%m'))
        future_time_series_data[item] = item_value
        future_time_series_data['predict_label'] = 1

        user_time_series_df = pd.DataFrame(future_time_series_data)
        user_time_series_df['y'][user_time_series_df['y'] < 0] = 0
        return pd.concat([time_series_data, user_time_series_df]).reset_index().drop(columns=['index'])

    def get_event_weight(self):
        events_weight = str(self.event_weight).split(',')
        for item in events_weight:
            event_weight = str(item).split(':')
            self.events_weight.update({event_weight[0]: float(event_weight[1])})

    def churn_state(self, predict_result):
        predict_result['churn_label'] = ''
        for index, result in predict_result.iterrows():
            if result['y'] > predict_result['y'].quantile(0.3):
                predict_result.at[index, 'churn_label'] = 'active'
            elif predict_result['y'].quantile(0.1) < result['y'] <= predict_result['y'].quantile(0.3):
                predict_result.at[index, 'churn_label'] = 'potential'
            else:
                predict_result.at[index, 'churn_label'] = 'churned'
        return predict_result

    def get_last_day_of_last_month(self):
        today = datetime.date.today()
        end_date = datetime.date(today.year, today.month, 1) - datetime.timedelta(days=1)
        return str(end_date)