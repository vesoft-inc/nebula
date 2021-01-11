# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest
from nebula2.graph import ttypes

from tests.common.nebula_test_suite import NebulaTestSuite


class TestYield(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_base(self):
        query = 'YIELD 1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, ['1'])
        self.check_result(resp, [[1]])

        query = "YIELD 1+1, '1+1', (int)3.14, (string)(1+1), (string)true"
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = [
            "(1+1)", "1+1", "(INT)3.14", "(STRING)(1+1)", "(STRING)true"
        ]
        self.check_column_names(resp, columns)
        expect_result = [[2, "1+1", 3, "2", "true"]]
        self.check_result(resp, expect_result)

        query = 'YIELD "Hello", hash("Hello")'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["Hello", "hash(Hello)"]
        self.check_column_names(resp, columns)
        expect_result = [['Hello', 2275118702903107253]]
        self.check_result(resp, expect_result)

    def test_hash_call(self):
        query = 'YIELD hash("Boris")'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["hash(Boris)"]
        self.check_column_names(resp, columns)
        expect_result = [[9126854228122744212]]
        self.check_result(resp, expect_result)

        query = 'YIELD hash(123)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["hash(123)"]
        self.check_column_names(resp, columns)
        expect_result = [[123]]
        self.check_result(resp, expect_result)

        query = 'YIELD hash(123 + 456)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["hash((123+456))"]
        self.check_column_names(resp, columns)
        expect_result = [[579]]
        self.check_result(resp, expect_result)

        query = 'YIELD hash(123.0)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["hash(123.0)"]
        # self.check_column_names(resp, columns)
        expect_result = [[-2256853663865737834]]
        self.check_result(resp, expect_result)

        # query = 'YIELD hash(!0)'
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)
        # columns = ["hash(!(0))"]
        # self.check_column_names(resp, columns)
        # expect_result = [[1]]
        # self.check_result(resp, expect_result)

    def test_logic(self):
        query = 'YIELD NOT FALSE OR FALSE AND FALSE XOR FALSE'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["((!(false) OR (false AND false)) XOR false)"]
        self.check_column_names(resp, columns)
        expect_result = [[True]]
        self.check_result(resp, expect_result)

        query = 'YIELD !false OR false AND false XOR true'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["((!(false) OR (false AND false)) XOR true)"]
        self.check_column_names(resp, columns)
        expect_result = [[False]]
        self.check_result(resp, expect_result)

        query = 'YIELD (NOT false OR false) AND false XOR true'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["(((!(false) OR false) AND false) XOR true)"]
        self.check_column_names(resp, columns)
        expect_result = [[True]]
        self.check_result(resp, expect_result)

    @pytest.mark.skip(reason="")
    def test_logic_2(self):
        query = 'YIELD NOT 0 OR 0 AND 0 XOR 0'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["((!(0) OR (0 AND 0)) XOR 0)"]
        self.check_column_names(resp, columns)
        expect_result = [[True]]
        self.check_result(resp, expect_result)

        query = 'YIELD !0 OR 0 AND 0 XOR 1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["((!(0) OR (0 AND 0)) XOR 1)"]
        self.check_column_names(resp, columns)
        expect_result = [[False]]
        self.check_result(resp, expect_result)

        query = 'YIELD (NOT 0 OR 0) AND 0 XOR 1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["(((!(0) OR 0) AND 0) XOR 1)"]
        self.check_column_names(resp, columns)
        expect_result = [[True]]
        self.check_result(resp, expect_result)

        query = 'YIELD 2.5 % 1.2 XOR 1.6'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["(2.500000000000000%(1.200000000000000 XOR 1.600000000000000))"]
        self.check_column_names(resp, columns)
        expect_result = [[2.5]]
        self.check_result(resp, expect_result)

        query = 'YIELD (5 % 3) XOR 1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["((5%3) XOR 1)"]
        self.check_column_names(resp, columns)
        expect_result = [[3]]
        self.check_result(resp, expect_result)

    def test_in_call(self):
        query = 'YIELD 1 IN [0,1,2], 123'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["(1 IN [0,1,2])", "123"]
        self.check_column_names(resp, columns)
        expect_result = [[True, 123]]
        self.check_result(resp, expect_result)

    def test_yield_pipe(self):
        query = '''GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team \
        | YIELD $-.team'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$-.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Suns"], ["Hawks"], ["Spurs"], ["Hornets"], ["Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team \
        | YIELD $-.team WHERE 1 == 1'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$-.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Suns"], ["Hawks"], ["Spurs"], ["Hornets"], ["Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team \
        | YIELD $-.team WHERE $-.start > 2005'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$-.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Spurs"], ["Hornets"], ["Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team \
        | YIELD $-.*'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$-.name", "$-.start", "$-.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Boris Diaw", 2005, "Suns"],
                         ["Boris Diaw", 2003, "Hawks"],
                         ["Boris Diaw", 2012, "Spurs"],
                         ["Boris Diaw", 2008, "Hornets"],
                         ["Boris Diaw", 2016, "Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team \
        | YIELD $-.* WHERE $-.start > 2005'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$-.name", "$-.start", "$-.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Boris Diaw", 2012, "Spurs"],
                         ["Boris Diaw", 2008, "Hornets"],
                         ["Boris Diaw", 2016, "Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team \
        | YIELD $-.*,hash(123) as hash WHERE $-.start > 2005'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$-.name", "$-.start", "$-.team", "hash"]
        self.check_column_names(resp, columns)
        expect_result = [["Boris Diaw", 2012, "Spurs", 123],
                         ["Boris Diaw", 2008, "Hornets", 123],
                         ["Boris Diaw", 2016, "Jazz", 123]]
        self.check_out_of_order_result(resp, expect_result)

    def test_yield_var(self):
        query = '''$var = GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team; \
        YIELD $var.team'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$var.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Suns"], ["Hawks"], ["Spurs"], ["Hornets"], ["Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''$var = GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team; \
        YIELD $var.team WHERE 1 == 1'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$var.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Suns"], ["Hawks"], ["Spurs"], ["Hornets"], ["Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''$var = GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team; \
        YIELD $var.team WHERE $var.start > 2005'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$var.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Spurs"], ["Hornets"], ["Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''$var = GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team; \
        YIELD $var.*'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$var.name", "$var.start", "$var.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Boris Diaw", 2005, "Suns"],
                         ["Boris Diaw", 2003, "Hawks"],
                         ["Boris Diaw", 2012, "Spurs"],
                         ["Boris Diaw", 2008, "Hornets"],
                         ["Boris Diaw", 2016, "Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''$var = GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team; \
        YIELD $var.* WHERE $var.start > 2005'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$var.name", "$var.start", "$var.team"]
        self.check_column_names(resp, columns)
        expect_result = [["Boris Diaw", 2012, "Spurs"],
                         ["Boris Diaw", 2008, "Hornets"],
                         ["Boris Diaw", 2016, "Jazz"]]
        self.check_out_of_order_result(resp, expect_result)

        query = '''$var = GO FROM "Boris Diaw" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team; \
        YIELD $var.*, hash(123) as hash WHERE $var.start > 2005'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$var.name", "$var.start", "$var.team", "hash"]
        self.check_column_names(resp, columns)
        expect_result = [["Boris Diaw", 2012, "Spurs", 123],
                         ["Boris Diaw", 2008, "Hornets", 123],
                         ["Boris Diaw", 2016, "Jazz", 123]]
        self.check_out_of_order_result(resp, expect_result)

    def test_error(self):
        query = 'YIELD $-'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)

        query = '''$var = GO FROM "Boris Diaw" OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team; \
        YIELD $var.team WHERE $-.start > 2005'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

        query = '''$var = GO FROM "Boris Diaw" OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team; \
        YIELD $var.team WHERE $var1.start > 2005'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

        query = '''$var = GO FROM "Boris Diaw" OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team; \
        YIELD $var.abc'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

        query = '''$var = GO FROM "Boris Diaw" OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team; \
        YIELD $$.a.team'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''$var = GO FROM "Boris Diaw" OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team; \
        YIELD $^.a.team'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''$var = GO FROM "Boris Diaw" OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team; \
        YIELD a.team'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

        query = '''GO FROM "Boris Diaw" OVER like | YIELD $-.abc;'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

    @pytest.mark.skip(reason="")
    def test_calculate_overflow(self):
        query = '''YIELD 9223372036854775807+1'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD -9223372036854775807-2'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD -9223372036854775807+-2'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD 1-(-9223372036854775807)'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD 9223372036854775807*2'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD -9223372036854775807*-2'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD 9223372036854775807*-2'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD -9223372036854775807*2'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD 1/0'''
        resp = self.execute(query)
        print(resp)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD 2%0'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = '''YIELD 9223372036854775807'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # Negation of -9223372036854775808 incurs a runtime error under UBSan
        query = '''YIELD -9223372036854775808'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = '''YIELD -2*4611686018427387904'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = '''YIELD -9223372036854775808*1'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = '''YIELD -9223372036854775809'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

    def test_agg_call(self):
        query = '''YIELD COUNT(1), $-.name'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

        query = '''YIELD 1+COUNT(*), 1+1'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["(1+COUNT(*))", "(1+1)"]
        self.check_column_names(resp, columns)
        expect_result = [[2, 2]]
        self.check_result(resp, expect_result)

        query = '''YIELD COUNT(*), 1+1'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["COUNT(*)", "(1+1)"]
        self.check_column_names(resp, columns)
        expect_result = [[1, 2]]
        self.check_result(resp, expect_result)

        query = '''GO FROM "Carmelo Anthony" OVER like YIELD $$.player.age AS age, like.likeness AS like \
        | YIELD COUNT(*), $-.age'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # Test input
        query = '''GO FROM "Carmelo Anthony" OVER like YIELD $$.player.age AS age, like.likeness AS like \
        | YIELD AVG($-.age), SUM($-.like), COUNT(*), 1+1'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["AVG($-.age)", "SUM($-.like)", "COUNT(*)", "(1+1)"]
        self.check_column_names(resp, columns)
        expect_result = [[34.666666666666664, 270, 3, 2]]
        self.check_result(resp, expect_result)

        # Yield field has not input
        query = '''GO FROM "Carmelo Anthony" OVER like | YIELD COUNT(*)'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["COUNT(*)"]
        self.check_column_names(resp, columns)
        expect_result = [[3]]
        self.check_result(resp, expect_result)

        query = '''GO FROM "Carmelo Anthony" OVER like | YIELD 1'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["1"]
        self.check_column_names(resp, columns)
        expect_result = [[1], [1], [1]]
        self.check_result(resp, expect_result)

        # input is empty
        query = '''GO FROM "Nobody" OVER like | YIELD 1'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["1"]
        self.check_column_names(resp, columns)

        # Test var
        query = '''$var = GO FROM "Carmelo Anthony" OVER like \
        YIELD $$.player.age AS age, like.likeness AS like; \
        YIELD AVG($var.age), SUM($var.like), COUNT(*)'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["AVG($var.age)", "SUM($var.like)", "COUNT(*)"]
        self.check_column_names(resp, columns)
        expect_result = [[34.666666666666664, 270, 3]]
        self.check_result(resp, expect_result)

    def test_empty_input(self):
        query = '''GO FROM "NON_EXIST_VERTEX_ID" OVER serve \
        YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team \
        | YIELD $-.team'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$-.team"]
        self.check_column_names(resp, columns)
        expect_result = []
        self.check_result(resp, expect_result)

        query = '''$var = GO FROM "NON_EXIST_VERTEX_ID" OVER serve YIELD \
        $^.player.name as name, serve.start_year as start, $$.team.name as team; \
        YIELD $var.team'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["$var.team"]
        self.check_column_names(resp, columns)
        expect_result = []
        self.check_result(resp, expect_result)

        query = '''GO FROM "Marco Belinelli" OVER serve \
        YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team \
        | YIELD $-.name as name WHERE $-.start > 20000 \
        | YIELD $-.name AS name'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["name"]
        self.check_column_names(resp, columns)
        expect_result = []
        self.check_result(resp, expect_result)

    def test_duplicate_columns(self):
        query = 'YIELD 1, 1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["1", "1"]
        self.check_column_names(resp, columns)
        expect_result = [[1, 1]]
        self.check_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" OVER serve YIELD \
        $^.player.name as team, serve.start_year as start, $$.team.name as team \
        | YIELD $-.team'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

        query = '''$var=GO FROM "Boris Diaw" OVER serve YIELD \
        $^.player.name as team, serve.start_year as start, $$.team.name as team; \
        YIELD $var.team'''
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

    def test_pipe_yield_go(self):
        query = '''GO FROM "Tim Duncan" OVER serve YIELD serve._src AS id | \
        YIELD $-.id AS id | \
        GO FROM $-.id OVER serve YIELD $$.team.name AS name'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["name"]
        self.check_column_names(resp, columns)
        expect_result = [["Spurs"]]
        self.check_result(resp, expect_result)

        query = '''$var = GO FROM "Tim Duncan" OVER serve YIELD serve._src AS id | YIELD $-.id AS id; \
        GO FROM $var.id OVER serve YIELD $$.team.name AS name'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["name"]
        self.check_column_names(resp, columns)
        expect_result = [["Spurs"]]
        self.check_result(resp, expect_result)

        query = '''$var = GO FROM "Tim Duncan" OVER serve YIELD serve._src AS id; \
        $var2 = YIELD $var.id AS id; \
        GO FROM $var2.id OVER serve YIELD $$.team.name AS name'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ["name"]
        self.check_column_names(resp, columns)
        expect_result = [["Spurs"]]
        self.check_result(resp, expect_result)

    @pytest.mark.skip(reason="")
    def test_with_comment(self):
        query = 'YIELD 1--1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        expect_result = [[2]]
        self.check_result(resp, expect_result)

        query = 'YIELD 1-- 1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        expect_result = [[1]]
        self.check_result(resp, expect_result)

        query = 'YIELD 1 --1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        expect_result = [[1]]
        self.check_result(resp, expect_result)

    def test_map(self):
        query = 'yield {key1: true, key2: "hello"}["key2"]'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_result(resp, [['hello']])

    def test_constant_expression_with_where(self):
        query = 'YIELD 1+1, "Hello world!" WHERE true;'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_result(resp, [[2, 'Hello world!']])

        query = 'YIELD 1+1, "Hello world!" WHERE NULL;'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        query = '''GO FROM "Tim Duncan", "Tim Duncan" OVER serve YIELD $$.team.name AS team
                   | YIELD 1+1, "Hello world!" WHERE $-.team == "Spurs"'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ['(1+1)', 'Hello world!']
        self.check_column_names(resp, columns)
        expect_result = [[2, 'Hello world!'], [2, 'Hello world!']]
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Tim Duncan", "Tim Duncan" OVER serve YIELD $$.team.name AS team
                   | YIELD 1+1, "Hello world!" WHERE $-.team != "Spurs"'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        columns = ['(1+1)', 'Hello world!']
        self.check_column_names(resp, columns)
        self.check_empty_result(resp)
