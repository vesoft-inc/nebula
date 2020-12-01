# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_NULL, T_EMPTY
import pytest


class TestCaseExpression(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def cleanup():
        pass

    def test_generic_case_expression(self):
        stmt = 'YIELD CASE 2 + 3 WHEN 4 THEN 0 WHEN 5 THEN 1 ELSE 2 END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[1]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE true WHEN false THEN 0 END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[T_NULL]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'GO FROM "Jonathon Simmons" OVER serve YIELD $$.team.name as name, \
            CASE serve.end_year > 2017 WHEN true THEN "ok" ELSE "no" END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [['Spurs', 'no'], ['Magic', 'ok'], ['76ers', 'ok']]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''GO FROM "Boris Diaw" OVER serve YIELD \
            $^.player.name, serve.start_year, serve.end_year, \
            CASE serve.start_year > 2006 WHEN true THEN "new" ELSE "old" END, $$.team.name'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [
            ["Boris Diaw", 2003, 2005, "old", "Hawks"],
            ["Boris Diaw", 2005, 2008, "old", "Suns"],
            ["Boris Diaw", 2008, 2012, "new", "Hornets"],
            ["Boris Diaw", 2012, 2016, "new", "Spurs"],
            ["Boris Diaw", 2016, 2017, "new", "Jazz"]
        ]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''GO FROM "Rajon Rondo" OVER serve WHERE \
            CASE serve.start_year WHEN 2016 THEN true ELSE false END YIELD \
            $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [
            ["Rajon Rondo", 2016, 2017, "Bulls"],
        ]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE WHEN 4 > 5 THEN 0 WHEN 3+4==7 THEN 1 ELSE 2 END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[1]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE WHEN false THEN 0 ELSE 1 END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[1]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'GO FROM "Tim Duncan" OVER serve YIELD $$.team.name as name, \
            CASE WHEN serve.start_year < 1998 THEN "old" ELSE "young" END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [['Spurs', 'old']]
        self.check_out_of_order_result(resp, expected_data)

        # we are not able to deduce the return type of case expression in where_clause
        stmt = '''GO FROM "Rajon Rondo" OVER serve WHERE \
            CASE WHEN serve.start_year > 2016 THEN true ELSE false END YIELD \
            $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [
            ["Rajon Rondo", 2018, 2019, "Lakers"],
            ["Rajon Rondo", 2017, 2018, "Pelicans"]
        ]
        self.check_out_of_order_result(resp, expected_data)

    def test_conditional_case_expression(self):
        stmt = 'YIELD 3 > 5 ? 0 : 1'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[1]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD true ? "yes" : "no"'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [["yes"]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'GO FROM "Tim Duncan" OVER serve YIELD $$.team.name as name, \
            serve.start_year < 1998 ? "old" : "young"'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [['Spurs', 'old']]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''GO FROM "Rajon Rondo" OVER serve WHERE \
            serve.start_year > 2016 ? true : false YIELD \
            $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [
            ["Rajon Rondo", 2018, 2019, "Lakers"],
            ["Rajon Rondo", 2017, 2018, "Pelicans"]
        ]
        self.check_out_of_order_result(resp, expected_data)

    def test_generic_with_conditional_case_expression(self):
        stmt = '''YIELD CASE 2 + 3 WHEN CASE 1 WHEN 1 \
            THEN 5 ELSE 4 END THEN 0 WHEN 5 THEN 1 ELSE 2 END'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[0]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE 2 + 3 WHEN 5 THEN CASE 1 WHEN 1 THEN 7 ELSE 4 END ELSE 2 END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[7]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE 2 + 3 WHEN 3 THEN 7 ELSE CASE 9 WHEN 8 THEN 10 ELSE 11 END END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[11]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE 3 > 2 ? 1 : 0 WHEN 1 THEN 5 ELSE 4 END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[5]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE 1 WHEN true ? 1 : 0 THEN 5 ELSE 4 END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[5]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE 1 WHEN 1 THEN 7 > 0 ? 6 : 9 ELSE 4 END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[6]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE 1 WHEN 2 THEN 6 ELSE false ? 4 : 9 END'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[9]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''YIELD CASE WHEN 2 > 7 THEN false ? 3 : 8 \
            ELSE CASE true WHEN false THEN 9 ELSE 11 END END'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[11]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''YIELD CASE 3 WHEN 4 THEN 5 ELSE 6 END \
            > 11 ? 7 : CASE WHEN true THEN 8 ELSE 9 END'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[8]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''YIELD 8 > 11 ? CASE WHEN true THEN 8 ELSE 9 END : \
            CASE 14 WHEN 8+6 THEN 0 ELSE 1 END'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[0]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''YIELD CASE 3 WHEN 4 THEN 5 ELSE 6 END > (3 > 2 ? 8 : 9) ? \
            CASE WHEN true THEN 8 ELSE 9 END : \
            CASE 14 WHEN 8+6 THEN 0 ELSE 1 END'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[0]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''GO FROM "Jonathon Simmons" OVER serve YIELD $$.team.name as name, \
            CASE serve.end_year > 2017 WHEN true THEN 2017 < 2020 ? "ok" : "no" \
            ELSE CASE WHEN false THEN "good" ELSE "bad" END END'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [['Spurs', 'bad'], ['Magic', 'ok'], ['76ers', 'ok']]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''GO FROM "Boris Diaw" OVER serve YIELD \
            $^.player.name, serve.start_year, serve.end_year, \
            CASE serve.start_year > 2006 ? false : true \
            WHEN true THEN "new" ELSE CASE WHEN serve.start_year != 2012 THEN "old" \
            WHEN serve.start_year > 2009 THEN "bad" ELSE "good" END END, $$.team.name'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [
            ["Boris Diaw", 2003, 2005, "new", "Hawks"],
            ["Boris Diaw", 2005, 2008, "new", "Suns"],
            ["Boris Diaw", 2008, 2012, "old", "Hornets"],
            ["Boris Diaw", 2012, 2016, "bad", "Spurs"],
            ["Boris Diaw", 2016, 2017, "old", "Jazz"]
        ]
        self.check_out_of_order_result(resp, expected_data)
