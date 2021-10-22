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
        self.start_date = config.get('start_date', '2019-01-01')
        self.end_date = config.get('end_date', 'now')
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
            all_user_buckets = self.esClient.getTotalAuthorName(field="actor.login.keyword", size=3)
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
                                              "gte": "%s",
                                              "lte": "%s"
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
                    'y': user_activity
                }
                user_time_series.append(user_action)

            user_df = pd.DataFrame(user_time_series) \
                .groupby(['actor_login', 'ds']) \
                .sum('popularity') \
                .sort_values('ds') \
                .reset_index()
            print(user_df)

            # 用户活跃度预测
            # result = self.time_series_predict(time_series_data=user_df)

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
                                  "gte": "%s",
                                  "lte": "%s"
                                }
                              }
                            }
                          ]
                        }
                      }
                    }''' % (self.start_date, self.end_date)
        self.esClient.scrollSearch(index_name=self.index_name, search=search, scroll_duration='1m',
                                   func=self.sig_activity_func)
        sigs_df = pd.DataFrame(self.sigs_time_series) \
            .groupby(['sig', 'ds']) \
            .sum('popularity') \
            .sort_values('ds') \
            .reset_index()
        sigs = set(sigs_df['sig'].values)
        for sig in sigs:
            sig_df = sigs_df[sigs_df['sig'] == sig]
            print(sig_df)
            print('sig活跃度预测')

            # sig活跃度预测
            # result = self.time_series_predict(time_series_data=sig_df)
            # print(result)

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
                    'y': sig_activity
                }
                self.sigs_time_series.append(sig_action)

    # def time_series_predict(self, time_series_data):
    #     time_series = Prophet(seasonality_mode='additive').fit(time_series_data)
    #     future = time_series.make_future_dataframe(periods=self.prediction_periods, freq='M')
    #     future_time_series_data = time_series.predict(future)
    #
    #     # time_series.plot_components(future_time_series_data)
    #
    #     return future_time_series_data

    def get_event_weight(self):
        events_weight = str(self.event_weight).split(',')
        for item in events_weight:
            event_weight = str(item).split(':')
            self.events_weight.update({event_weight[0]: float(event_weight[1])})
