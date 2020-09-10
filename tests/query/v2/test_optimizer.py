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
            GO 1 STEPS FROM "Kobe Bryant" OVER serve
            WHERE $^.player.age > 18 YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["GetNeighbors", [2], ['($^.player.age>18)']],
            ["Start", []]
        ]
        self.check_exec_plan(resp, expected_plan)

        resp = self.execute_query('''
            GO 1 STEPS FROM "Kobe Bryant" OVER like REVERSELY
            WHERE $^.player.age > 18 YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["GetNeighbors", [2], ['($^.player.age>18)']],
            ["Start", []]
        ]
        self.check_exec_plan(resp, expected_plan)

        resp = self.execute_query('''
            GO 1 STEPS FROM "Kobe Bryant" OVER serve
            WHERE serve.start_year > 2002 YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["GetNeighbors", [2], ['(serve.start_year>2002)']],
            ["Start", []]
        ]
        self.check_exec_plan(resp, expected_plan)

        resp = self.execute_query('''
            GO 1 STEPS FROM "Lakerys" OVER serve REVERSELY
            WHERE serve.start_year > 2002 YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["GetNeighbors", [2], ['(serve.start_year>2002)']],
            ["Start", []]
        ]
        self.check_exec_plan(resp, expected_plan)

    @pytest.mark.skip(reason="Depends on other opt rules to eliminate duplicate project nodes")
    def test_PushFilterDownGetNbrsRule_Failed(self):
        resp = self.execute_query('''
            GO 1 STEPS FROM "Kobe Bryant" OVER serve
            WHERE $^.player.age > 18 AND $$.team.name == "Lakers"
            YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["Filter", [2], ['($$.team.name=="Lakers")']],
            ["GetNeighbors", [3], ['($^.player.age>18)']],
            ["Start", []]
        ]
        self.check_exec_plan(resp, expected_plan)

        resp = self.execute_query('''
            GO 1 STEPS FROM "Kobe Bryant" OVER serve
            WHERE $^.player.age > 18 OR $$.team.name == "Lakers"
            YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["Filter", [2], ['($^.player.age>18) OR ($$.team.name=="Lakers")']]
            ["GetNeighbors", [3]],
            ["Start", []]
        ]
        self.check_exec_plan(resp, expected_plan)

        # fail to optimize cases
        resp = self.execute_query('''
            GO 1 STEPS FROM "Kobe Bryant" OVER serve \
            WHERE $$.team.name == "Lakers" YIELD $^.player.name AS name
        ''')
        expected_plan = [
            ["Project", [1]],
            ["Filter", [2]],
            ["GetNeighbors", [3]],
            ["Start", []]
        ]
        self.check_exec_plan(resp, expected_plan)
