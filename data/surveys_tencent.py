from collect.surveys_tencent import SurveysTencentApi
from data.common import ESClient


class SurveysTencent(object):
    def __init__(self, config=None):
        self.config = config
        self.index_name = config.get('index_name')
        self.user_id = config.get('user_id')
        self.app_id = config.get('app_id')
        self.secret = config.get('secret')
        self.per_page = config.get('per_page')
        self.esClient = ESClient(config)
        self.surveys_tencent_api = SurveysTencentApi(self.app_id, self.secret)
        self.token = self.surveys_tencent_api.get_token()

    def run(self, from_time):
        print("Collect tencent surveys data: start")

    def collect_survey_details(self):
        # 获取问卷列表（注意翻页）
        current_page = 1
        surveys = []
        surveys_req = self.surveys_tencent_api.get_surveys(access_token=self.token, user_id=self.user_id,
                                                           current_page=current_page,
                                                           per_page=self.per_page)
        for survey in surveys:
            # 根据survey_id获取问卷详情
            survey_id = survey['id']
            survey_req = self.surveys_tencent_api.get_survey_legacy(access_token=self.token, survey_id=survey_id)

            # 根据survey_id获取回答列表
            # 根据survey_id和answer_id获取回答详情
            # 数据解析
            # 数据入库
        print("Collect tencent surveys data: end")
