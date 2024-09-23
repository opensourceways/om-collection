import numpy as np


class NDCG:
    def calc_dcg(self, rel, top_n):
        """
        忽略相关性权重的计算方式
        :param rel: 列表格式，最大支持100
        :param top_n: 截取前多少位
        :return: dcg
        """
        top = 100 if top_n > 100 else top_n
        log2_table = np.log2(np.arange(2, 102))
        rel = np.asarray(rel)[:top]
        dcg = np.sum(np.divide(rel, log2_table[:rel.shape[0]]))
        return dcg

    def calc_dcg_new(self, rel, top_n):
        """
        为相关性加重权限的计算方式
        :param rel: 列表格式，最大支持100
        :param top_n: 截取前多少位
        :return: dcg
        """
        top = 100 if top_n > 100 else top_n
        log2_table = np.log2(np.arange(2, 102))
        rel = np.asarray(rel)[:top]
        dcg = np.sum(np.divide(np.power(2, rel) - 1, log2_table[:rel.shape[0]]))
        return dcg

    def calc_ndcg(self, rel, top_n, use_rel_weight=False):
        """
        ndcg = dcg / idcg
        :param rel: 表格式，最大支持100
        :param top_n: 截取前多少位
        :param use_rel_weight: 是否使用相关性加权算法，默认不使用
        :return: ndcg
        """
        dcg = self.calc_dcg(rel, top_n)
        idcg = self.calc_dcg(sorted(rel, reverse=True), top_n)
        if use_rel_weight:
            dcg = self.calc_dcg_new(rel, top_n)
            idcg = self.calc_dcg_new(sorted(rel, reverse=True), top_n)

        ndcg = 0 if idcg == 0 else dcg / idcg
        print('dcg: %f, idcg: %f, ndcg: %f' % (dcg, idcg, ndcg))
        return ndcg

    def calc_dcg_dict(self, rel_dic):
        """
        忽略相关性权重的计算方式。相关性为0的项可忽略，不影响计算结果
        :param rel_dic: 字典格式 {"位置":"相关性"}
        :return: dcg
        """
        sum = 0
        for key, value in rel_dic.items():
            s = value / np.log2(key + 1)
            sum += s
        return sum

    def calc_dcg_dict_new(self, rel_dic):
        """
        为相关性加重权限的计算方式。相关性为0的项可忽略，不影响计算结果
        :param rel_dic: 字典格式 {"位置":"相关性"}
        :return: dcg
        """
        sum = 0
        for key, value in rel_dic.items():
            s = (np.power(2, value) - 1) / np.log2(key + 1)
            sum += s
        return sum

    def calc_ndcg_dict(self, rel_dic, use_rel_weight=False):
        """
        ndcg = dcg / idcg
        :param rel_dic: 字典格式 {"位置":"相关性"}
        :param use_rel_weight: 是否使用相关性加权算法，默认不使用
        :return:
        """
        dcg = self.calc_dcg_dict(rel_dic)
        idcg = self.calc_dcg(sorted(list(rel_dic.values()), reverse=True), 30)
        if use_rel_weight:
            dcg = self.calc_dcg_dict_new(rel_dic)
            idcg = self.calc_dcg_new(sorted(list(rel_dic.values()), reverse=True), 30)

        ndcg = 0 if idcg == 0 else dcg / idcg
        print('dcg: %f, idcg: %f, ndcg: %f' % (dcg, idcg, ndcg))
        return ndcg


if __name__ == '__main__':
    rels = [6.09861228866811, 5, 10]
    n = 5
    NDCG().calc_ndcg(rels, 30, use_rel_weight=True)

    rel_dict = {1: 6.09861228866811, 2: 5.0, 3: 10.0}
    NDCG().calc_ndcg_dict(rel_dict, use_rel_weight=True)
