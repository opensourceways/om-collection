import unittest

from algorithm import ndcg


class TestNdcg(unittest.TestCase):
    def setUP(self):
        pass

    def tearDown(self):
        pass

    def test_calc_dcg(self):
        rels = [3, 2, 3, 0, 1, 2, 3, 0]
        top_n = 30
        res = ndcg.NDCG().calc_dcg(rels, top_n)
        self.assertEqual(res, 7.861126688593501)

    def test_calc_dcg_new(self):
        rels = [3, 2, 3, 0, 1, 2, 3, 0]
        top_n = 30
        res = ndcg.NDCG().calc_dcg_new(rels, top_n)
        print(res)
        self.assertEqual(res, 16.181596962606314)

    def test_calc_ndcg(self):
        rels = [3, 2, 3, 0, 1, 2, 3, 0]
        top_n = 30
        res1 = ndcg.NDCG().calc_ndcg(rels, top_n)
        res2 = ndcg.NDCG().calc_ndcg(rels, top_n, use_rel_weight=True)
        self.assertEqual(res1, 0.9376282146628032)
        self.assertEqual(res2, 0.9129094409936804)

    def test_calc_dcg_dict(self):
        rel_dict = {1: 3, 2: 2, 3: 3, 4: 0, 5: 1, 6: 2, 7: 3, 8: 0}
        res = ndcg.NDCG().calc_dcg_dict(rel_dict)
        self.assertEqual(res, 7.861126688593502)

    def test_calc_dcg_dict_new(self):
        rel_dict = {1: 3, 2: 2, 3: 3, 4: 0, 5: 1, 6: 2, 7: 3, 8: 0}
        res = ndcg.NDCG().calc_dcg_dict_new(rel_dict)
        print(res)
        self.assertEqual(res, 16.181596962606314)

    def test_calc_ndcg_dict(self):
        rel_dict = {1: 3, 2: 2, 3: 3, 4: 0, 5: 1, 6: 2, 7: 3, 8: 0}
        res1 = ndcg.NDCG().calc_ndcg_dict(rel_dict)
        res2 = ndcg.NDCG().calc_ndcg_dict(rel_dict, use_rel_weight=True)
        self.assertEqual(res1, 0.9376282146628033)
        self.assertEqual(res2, 0.9129094409936804)


if __name__ == '__main__':
    unittest.main()
