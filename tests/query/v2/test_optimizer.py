# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


class TestOptimizer(NebulaTestSuite):

    @classmethod
    def prepare(cls):
        cls.use_nba()

    def test_PushFilterDownGetNbrsRule(self):
        resp = self.execute_query('''
            GO 1 STEPS FROM "Boris Diaw" OVER serve
            WHERE $^.player.age > 18 YIELD serve.start_year as start_year
        ''')
        expected_plan = [
            ["Project", [1]],
            ["GetNeighbors", [2], ['($^.player.age>18)']],
            ["Start", []]
        ]
        expected_data = [[2003], [2005], [2008], [2012], [2016]]
        self.check_exec_plan(resp, expected_plan)
        self.check_out_of_order_result(resp, expected_data)

        resp = self.execute_query('''
            GO 1 STEPS FROM "James Harden" OVER like REVERSELY
            WHERE $^.player.age > 18 YIELD like.likeness as likeness
        ''')
        expected_plan = [
            ["Project", [1]],
            ["GetNeighbors", [2], ['($^.player.age>18)']],
            ["Start", []]
        ]
        expected_data = [[90], [80], [99]]
        self.check_exec_plan(resp, expected_plan)
        self.check_out_of_order_result(resp, expected_data)

        resp = self.execute_query('''
            GO 1 STEPS FROM "Boris Diaw" OVER serve
            WHERE serve.start_year > 2005 YIELD serve.start_year as start_year
        ''')
        expected_plan = [
            ["Project", [1]],
            ["GetNeighbors", [2], ['(serve.start_year>2005)']],
            ["Start", []]
        ]
        expected_data = [[2008], [2012], [2016]]
        self.check_exec_plan(resp, expected_plan)
        self.check_out_of_order_result(resp, expected_data)

        resp = self.execute_query('''
            GO 1 STEPS FROM "Lakers" OVER serve REVERSELY
            WHERE serve.start_year < 2017 YIELD serve.start_year as start_year
        ''')
        expected_plan = [
            ["Project", [1]],
            ["GetNeighbors", [2], ['(serve.start_year<2017)']],
            ["Start", []]
        ]
        expected_data = [[2012], [1996], [2008], [1996], [2012]]
        self.check_exec_plan(resp, expected_plan)
        self.check_out_of_order_result(resp, expected_data)

    @pytest.mark.skip(reason="Depends on other opt rules to eliminate duplicate project nodes")
    def test_PushFilterDownGetNbrsRule_Failed(self):
        resp = self.execute_query('''
            GO 1 STEPS FROM "Boris Diaw" OVER serve
            WHERE $^.player.age > 18 AND $$.team.name == "Lakers"
            YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["Filter", [2], ['($$.team.name=="Lakers")']],
            ["GetNeighbors", [3], ['($^.player.age>18)']],
            ["Start", []]
        ]
        expected_data = [['Boris Diaw']]
        self.check_exec_plan(resp, expected_plan)
        self.check_out_of_order_result(resp, expected_data)

        resp = self.execute_query('''
            GO 1 STEPS FROM "Boris Diaw" OVER serve
            WHERE $^.player.age > 18 OR $$.team.name == "Lakers"
            YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["Filter", [2], ['($^.player.age>18) OR ($$.team.name=="Lakers")']]
            ["GetNeighbors", [3]],
            ["Start", []]
        ]
        expected_data = [['Boris Diaw']]
        self.check_exec_plan(resp, expected_plan)
        self.check_out_of_order_result(resp, expected_data)

        # fail to optimize cases
        resp = self.execute_query('''
            GO 1 STEPS FROM "Boris Diaw" OVER serve \
            WHERE $$.team.name == "Lakers" YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["Filter", [2]],
            ["GetNeighbors", [3]],
            ["Start", []]
        ]
        expected_data = [['Boris Diaw']]
        self.check_exec_plan(resp, expected_plan)
        self.check_out_of_order_result(resp, expected_data)

    def test_TopNRule(self):
        resp = self.execute_query('''
            GO 1 STEPS FROM "Marco Belinelli" OVER like
            YIELD like.likeness AS likeness
             | ORDER BY likeness
             | LIMIT 2
        ''')
        expected_plan = [
            ["DataCollect", [1]],
            ["TopN", [2]],
            ["Project", [3]],
            ["GetNeighbors", [4]],
            ["Start", []]
        ]
        expected_data = [[50], [55]]
        self.check_exec_plan(resp, expected_plan)
        self.check_result(resp, expected_data)

        resp = self.execute_query('''
            GO 1 STEPS FROM "Marco Belinelli" OVER like REVERSELY
            YIELD like.likeness AS likeness |
            ORDER BY likeness |
            LIMIT 1
        ''')
        expected_plan = [
            ["DataCollect", [1]],
            ["TopN", [2]],
            ["Project", [3]],
            ["GetNeighbors", [4]],
            ["Start", []]
        ]
        expected_data = [[83]]
        self.check_exec_plan(resp, expected_plan)
        self.check_result(resp, expected_data)

    def test_TopNRule_Failed(self):
        resp = self.execute_query('''
            GO 1 STEPS FROM "Marco Belinelli" OVER like
            YIELD like.likeness as likeness
             | ORDER BY likeness
             | LIMIT 2, 3
        ''')
        expected_plan = [
            ["DataCollect", [1]],
            ["Limit", [2]],
            ["Sort", [3]],
            ["Project", [4]],
            ["GetNeighbors", [5]],
            ["Start", []]
        ]
        expected_data = [[60]]
        self.check_exec_plan(resp, expected_plan)
        self.check_result(resp, expected_data)

        resp = self.execute_query('''
            GO 1 STEPS FROM "Marco Belinelli" OVER like
            YIELD like.likeness AS likeness
             | ORDER BY likeness
        ''')
        expected_plan = [
            ["DataCollect", [1]],
            ["Sort", [2]],
            ["Project", [3]],
            ["GetNeighbors", [4]],
            ["Start", []]
        ]
        expected_data = [[50], [55], [60]]
        self.check_exec_plan(resp, expected_plan)
        self.check_result(resp, expected_data)
