import datetime
import json
import requests
import time
from dateutil.relativedelta import relativedelta
from data.common import ESClient
import math


class SigScores(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.index_name_gitee = config.get('index_name_gitee')
        self.index_name_maintainer = config.get('index_name_maintainer')
        self.index_name_meeting = config.get('index_name_meeting')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.esClient = ESClient(config)
        self.from_date = config.get('from_date')
        self.is_radar = config.get('is_radar')
        if self.is_radar == 'true':
            self.params = json.loads((config.get('radar_params')))
            self.metrics = config.get('radar_metrics').split(';')
            self.products_query = config.get('products_query').split(';')
            self.product_quality_query = config.get('product_quality_query').split(';')
            self.process_quality_query = config.get('process_quality_query').split(';')
            self.org_robustness_query = config.get('org_robustness_query').split(';')
            self.influence_query = config.get('influence_query').split(';')
        else:
            self.params = json.loads((config.get('params')))
            self.metrics = config.get('metrics').split(';')
            self.query_strs = config.get('query').split(';')
            self.sig_contributes_query = config.get('sig_contributes_query')
            self.sig_meetings_query = config.get('sig_meetings_query')
            self.sig_maintainers_query = config.get('sig_maintainers_query')

    def run(self, time=None):
        self.get_sig_score_by_days()

    def get_query_data(self, index, query, from_time, end_time):
        query = query % (from_time, end_time)
        url = self.url + '/' + index + '/_search'
        res = requests.post(url, headers=self.esClient.default_headers, verify=False, data=query.encode('utf-8'))
        data = res.json()['aggregations']['group_filed']['buckets']
        return data

    def get_sig_score_by_days(self):
        if self.from_date is None:
            from_date = datetime.date.today()
        else:
            from_date = datetime.datetime.strptime(self.from_date, "%Y%m%d")
        now_date = datetime.date.today().strftime("%Y%m%d")

        actions = ""
        count = 0
        while from_date.strftime("%Y%m%d") <= now_date:
            actions += self.get_sig_score(from_date)
            from_date += relativedelta(days=1)
            count += 1
            if count > 10:
                self.esClient.safe_put_bulk(actions)
                actions = ""
                count = 0
        self.esClient.safe_put_bulk(actions)

    def get_sig_score(self, date):
        date_ago = date - relativedelta(years=1)
        from_time = time.mktime(date_ago.timetuple()) * 1000
        end_time = time.mktime(date.timetuple()) * 1000
        print("Compute sig score: ", date)
        created_at = date.strftime("%Y-%m-%dT00:00:01+08:00")
        if self.is_radar == 'true':
            res = self.compute_sig_radar_scores(from_time, end_time)
        else:
            res = self.compute_sig_scores(from_time, end_time)

        actions = ''
        for data in res:
            data.update({"created_at": created_at})
            indexData = {"index": {"_index": self.index_name, "_id": data.get('sig_names') + "_sig_score_" + str(date)}}
            actions += json.dumps(indexData) + '\n'
            actions += json.dumps(data) + '\n'
        return actions

    def compute_sig_scores(self, from_time, end_time):
        all_sigs = []
        metrics_data = self.get_sig_metrics(from_time, end_time)
        for key in metrics_data:
            sig_score = 0
            data = metrics_data.get(key)
            metric_value = {}
            for m in self.metrics:
                params = self.params.get(m)
                value = data.get(m) if m in data else 0
                if m == 'PR_Review':
                    value = value / data.get('PR_Merged') if 'PR_Merged' in data and data.get('PR_Merged') != 0 else 0
                if m == 'Issue_Comment':
                    value = value / data.get('Issue_Closed') if 'Issue_Closed' in data and data.get(
                        'Issue_Closed') != 0 else 0
                score = math.log(1 + value) / math.log(1 + max(value, params[1])) * params[0]
                sig_score += score
                metric_value.update({m: value})
            action = {
                'sig_names': key,
                'score': sig_score,
                'value': metric_value
            }
            all_sigs.append(action)
        all_sigs.sort(key=lambda x: (x['score']), reverse=True)
        for rank in range(len(all_sigs)):
            all_sigs[rank].update({'rank': rank + 1})
            rank += 1
        return all_sigs

    def get_sig_metrics(self, from_time, end_time):
        res_data = {}
        metrics_index = 0
        for query in self.query_strs:
            gitee_data = self.get_query_data(self.index_name_gitee, query, from_time, end_time)
            res_data = self.get_metrics_value(res_data, gitee_data, metrics_index)
            metrics_index += 1

        meeting_data = self.get_query_data(self.index_name_meeting, self.sig_meetings_query, from_time, end_time)
        res_data = self.get_metrics_meeting(res_data, meeting_data, metrics_index)

        metrics_index += 2
        maintainer_data = self.get_query_data(self.index_name_maintainer, self.sig_maintainers_query, 0,
                                              end_time)
        res_data = self.get_metrics_value(res_data, maintainer_data, metrics_index)

        return res_data

    def get_metrics_meeting(self, res_data, datas, index):
        res = res_data
        sigs = res.keys()
        if len(datas) != 0:
            for data in datas:
                key = data.get('key')
                count = data.get('doc_count')
                value = data.get('count').get('value')
                if key not in sigs:
                    res.update({key: {self.metrics[index]: count}})
                    res.update({key: {self.metrics[index + 1]: value}})
                else:
                    res.get(key).update({self.metrics[index]: count})
                    res.get(key).update({self.metrics[index + 1]: value})
        return res

    def get_metrics_value(self, res_data, datas, index):
        res = res_data
        sigs = res.keys()
        if len(datas) != 0:
            for data in datas:
                key = data.get('key')
                if 'count' not in data:
                    value = data.get('doc_count')
                else:
                    value = data.get('count').get('value')
                if key not in sigs:
                    res.update({key: {self.metrics[index]: value}})
                else:
                    res.get(key).update({self.metrics[index]: value})
        return res

    def compute_sig_radar_scores(self, from_time, end_time):
        res_data = {}
        for radar_metrics in self.metrics:
            if radar_metrics == 'products':
                res_data = self.get_sig_products_scores(res_data, radar_metrics, from_time, end_time)
            if radar_metrics == 'product_quality':
                res_data = self.get_sig_product_quality_scores(res_data, radar_metrics, from_time, end_time)
            if radar_metrics == 'process_quality':
                res_data = self.get_sig_process_quality_scores(res_data, radar_metrics, from_time, end_time)
            if radar_metrics == 'org_robustness':
                res_data = self.get_sig_org_robustness_scores(res_data, radar_metrics, from_time, end_time)
            if radar_metrics == 'influence':
                res_data = self.get_sig_influence_scores(res_data, radar_metrics, from_time, end_time)

        return self.get_sig_radar_rank(res_data)

    def get_sig_products_scores(self, res_data, radar_metrics, from_time, end_time):
        res = res_data
        params_dict = self.params.get(radar_metrics)
        metrics_keys = list(params_dict.keys())
        metrics_i = 0
        for query in self.products_query:
            metric = metrics_keys[metrics_i]
            datas = self.get_query_data(self.index_name_gitee, query, from_time, end_time)
            params = params_dict.get(metrics_keys[metrics_i])
            res = self.get_radar_score_common(res_data, datas, radar_metrics, metric, params)
            metrics_i += 1
        return res

    def get_sig_product_quality_scores(self, res_data, radar_metrics, from_time, end_time):
        res = res_data
        params_dict = self.params.get(radar_metrics)
        metrics_keys = list(params_dict.keys())
        metrics_i = 0
        for query in self.product_quality_query:
            metric = metrics_keys[metrics_i]
            if metric == 'comment':
                datas = self.get_query_data(self.index_name_gitee, query, from_time, end_time)
            else:
                datas = []
            params = params_dict.get(metrics_keys[metrics_i])
            res = self.get_radar_score_common(res_data, datas, radar_metrics, metric, params)
            metrics_i += 1
        return res

    def get_sig_process_quality_scores(self, res_data, radar_metrics, from_time, end_time):
        res = res_data
        params_dict = self.params.get(radar_metrics)
        metrics_keys = list(params_dict.keys())
        metrics_i = 0
        for query in self.process_quality_query:
            metric = metrics_keys[metrics_i]
            datas = self.get_query_data(self.index_name_gitee, query, from_time, end_time)
            params = params_dict.get(metrics_keys[metrics_i])
            res = self.get_radar_score_common(res_data, datas, radar_metrics, metric, params)
            metrics_i += 1
        return res

    def get_sig_org_robustness_scores(self, res_data, radar_metrics, from_time, end_time):
        res = res_data
        params_dict = self.params.get(radar_metrics)
        metrics_keys = list(params_dict.keys())
        metrics_i = 0
        for query in self.org_robustness_query:
            metric = metrics_keys[metrics_i]
            # if metric == 'Maintainer':
            #     datas = self.get_query_data(self.index_name_maintainer, query, 0, end_time)
            # else:
            #     datas = self.get_query_data(self.index_name_gitee, query, from_time, end_time)
            datas = self.get_query_data(self.index_name_gitee, query, from_time, end_time)

            params = params_dict.get(metrics_keys[metrics_i])
            res = self.get_radar_score_common(res_data, datas, radar_metrics, metric, params)
            metrics_i += 1
        return res

    def get_sig_influence_scores(self, res_data, radar_metrics, from_time, end_time):
        res = res_data
        params_dict = self.params.get(radar_metrics)
        metrics_keys = list(params_dict.keys())
        metrics_i = 0
        for query in self.influence_query:
            metric = metrics_keys[metrics_i]
            if metric == 'Download':
                datas = self.get_query_data(self.index_name_maintainer, query, from_time, end_time)
            else:
                datas = self.get_query_data(self.index_name_gitee, query, from_time, end_time)

            params = params_dict.get(metrics_keys[metrics_i])
            res = self.get_radar_score_common(res_data, datas, radar_metrics, metric, params)
            metrics_i += 1
        return res

    def get_radar_score_common(self, res_data, datas, radar_metrics, metric, params):
        res = res_data
        sigs = res.keys()
        if len(datas) != 0:
            sum_metric = 0
            for data in datas:
                key = data.get('key')
                value = data.get('count').get('value')
                value = value if value is not None else 0
                score = math.log(1 + value) / math.log(1 + max(value, params[1])) * params[2]  # * params[0]
                if key not in sigs:
                    res.update({key: {radar_metrics: {metric: score}}})
                    res.get(key).get(radar_metrics).update({"metrics": {metric: value}})
                elif radar_metrics not in res.get(key):
                    res.get(key).update({radar_metrics: {metric: score}})
                    res.get(key).get(radar_metrics).update({"metrics": {metric: value}})
                else:
                    res.get(key).get(radar_metrics).update({metric: score})
                    res.get(key).get(radar_metrics).get("metrics").update({metric: value})
                sum_metric += value
            for sig in res.keys():
                if res.get(sig).get(radar_metrics):
                    res.get(sig).get(radar_metrics).get('metrics').update({metric + '_mean': sum_metric / len(datas)})
        return res

    def get_sig_radar_rank(self, res):
        all_sigs = []
        for sig in res:
            action = {'sig_names': sig}
            radar_value = res.get(sig)
            for radar_metrics in self.metrics:
                weight = [-1, 1] if radar_metrics == 'process_quality' else [1, 0]
                metric_value = radar_value.get(radar_metrics) if radar_value.get(radar_metrics) else {}
                score = 0
                normalized_score = 0
                for metric in metric_value:
                    if metric != 'metrics':
                        w = self.params.get(radar_metrics).get(metric)[2]
                        m_value = metric_value.get(metric) * weight[0] + weight[1] * w
                        score += m_value
                        normalized_score += m_value * w
                action.update({radar_metrics: {'score': score}})
                action.get(radar_metrics).update({"normalized_score": normalized_score})
                if metric_value.get("metrics"):
                    action.get(radar_metrics).update({"metrics": metric_value.get("metrics")})
            all_sigs.append(action)

        for radar_metrics in self.metrics:
            all_sigs.sort(key=lambda x: (x[radar_metrics]['score']), reverse=True)
            for rank in range(len(all_sigs)):
                all_sigs[rank].get(radar_metrics).update({'rank': rank + 1})
                rank += 1

        for radar_metrics in self.metrics:
            sum_score = 0
            for i in range(len(all_sigs)):
                sum_score += all_sigs[i].get(radar_metrics).get('score')
            mean_score = sum_score / len(all_sigs)
            for i in range(len(all_sigs)):
                all_sigs[i].get(radar_metrics).update({'mean': mean_score})
        return all_sigs
