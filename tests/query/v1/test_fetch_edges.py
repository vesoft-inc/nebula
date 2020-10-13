# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from nebula2.graph import ttypes
from tests.common.nebula_test_suite import NebulaTestSuite, T_NULL, T_EMPTY

class TestFetchEdges(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    @classmethod
    def cleanup(cls):
        pass

    def test_fetch_edges_base(self):
        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks" YIELD serve.start_year, serve.end_year'
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve.start_year',
                               'serve.end_year']
        expect_result = [['Boris Diaw', 'Hawks', 0, 2003, 2005]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks" YIELD serve.start_year > 2001, serve.end_year'
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               '(serve.start_year>2001)',
                               'serve.end_year']
        expect_result = [['Boris Diaw', 'Hawks', 0, True, 2005]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks"@0 YIELD serve.start_year, serve.end_year'
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve.start_year',
                               'serve.end_year']
        expect_result = [['Boris Diaw', 'Hawks', 0, 2003, 2005]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks", "Boris Diaw"->"Suns" YIELD serve.start_year, serve.end_year'
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve.start_year',
                               'serve.end_year']
        expect_result = [['Boris Diaw', 'Hawks', 0, 2003, 2005], ['Boris Diaw', 'Suns', 0, 2005, 2008]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" OVER serve YIELD serve._src AS src, serve._dst AS dst \
                   | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year'''
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve.start_year',
                               'serve.end_year']
        expect_result = [["Boris Diaw", "Hawks", 0, 2003, 2005],
                         ["Boris Diaw", "Suns", 0, 2005, 2008],
                         ["Boris Diaw", "Hornets", 0, 2008, 2012],
                         ["Boris Diaw", "Spurs", 0, 2012, 2016],
                         ["Boris Diaw", "Jazz", 0, 2016, 2017]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = '''$a = GO FROM "Boris Diaw" OVER serve YIELD serve._src AS src, serve._dst AS dst; \
                   FETCH PROP ON serve $a.src->$a.dst YIELD serve.start_year, serve.end_year'''
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        # TODO hash key
        # query = 'FETCH PROP ON serve hash("Boris Diaw")->hash("Hawks") YIELD serve.start_year, serve.end_year'
        # resp = self.execute_query(query)
        # expect_result = [[2003, 2005]]
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

        # TODO uuid key
        # query = 'FETCH PROP ON serve uuid("Boris Diaw")->uuid("Hawks") YIELD serve.start_year, serve.end_year'
        # resp = self.execute_query(query)
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

    def test_fetch_edges_no_yield(self):
        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks"'
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve.start_year',
                               'serve.end_year']
        expect_result = [['Boris Diaw', 'Hawks', 0, 2003, 2005]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks"@0'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        # TODO hash key
        # query = 'FETCH PROP ON serve hash("Boris Diaw")->hash("Hawks")'
        # resp = self.execute_query(query)
        # expect_result = [['Boris Diaw', 'Hawks', 0, 2003, 2005]]
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

        # TODO uuid key
        # query = 'FETCH PROP ON serve uuid("Boris Diaw")->uuid("Hawks")'
        # resp = self.execute_query(query)
        # expect_result = [['Boris Diaw', 'Hawks', 0, 2003, 2005]]
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

    def test_fetch_edges_distinct(self):
        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks", "Boris Diaw"->"Hawks" YIELD DISTINCT serve.start_year, serve.end_year'
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve.start_year',
                               'serve.end_year']
        expect_result = [["Boris Diaw", "Hawks", 0, 2003, 2005]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw", "Boris Diaw" OVER serve YIELD serve._src AS src, serve._dst AS dst \
                   | FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve.start_year, serve.end_year'''
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve.start_year',
                               'serve.end_year']
        expect_result = [["Boris Diaw", "Hawks", 0, 2003, 2005],
                         ["Boris Diaw", "Suns", 0, 2005, 2008],
                         ["Boris Diaw", "Hornets", 0, 2008, 2012],
                         ["Boris Diaw", "Spurs", 0, 2012, 2016],
                         ["Boris Diaw", "Jazz", 0, 2016, 2017]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = '''$a = GO FROM "Boris Diaw", "Boris Diaw" OVER serve YIELD serve._src AS src, serve._dst AS dst; \
                   FETCH PROP ON serve $a.src->$a.dst YIELD DISTINCT serve.start_year, serve.end_year'''
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Tim Duncan", "Tony Parker" OVER serve YIELD serve._src AS src, serve._dst AS dst \
                   | FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve._dst as dst'''
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'dst']
        expect_result = [["Tim Duncan", "Spurs", 0, "Spurs"],
                         ["Tony Parker", "Spurs", 0, "Spurs"],
                         ["Tony Parker", "Hornets", 0, "Hornets"]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

    def test_fetch_edges_empty_input(self):
        query = '''GO FROM "not_exist_vertex" OVER serve YIELD serve._src AS src, serve._dst AS dst \
                   | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year'''
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve.start_year',
                               'serve.end_year']
        expect_result = []
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Marco Belinelli" OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start \
                   | YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000 \
                   | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year'''
        resp = self.execute_query(query)
        expect_result = []
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)


    def test_fetch_edges_syntax_error(self):
        # TODO(shylock) hash key
        # query = 'FETCH PROP ON serve hash(\"Boris Diaw\")->hash(\"Spurs\") YIELD $^.serve.start_year'
        # resp = self.execute(query)
        # self.check_resp_failed(resp);

        # query = 'FETCH PROP ON serve hash(\"Boris Diaw\")->hash(\"Spurs\") YIELD $$.serve.start_year'
        # resp = self.execute(query)
        # self.check_resp_failed(resp);

        # query = 'FETCH PROP ON serve hash(\"Boris Diaw\")->hash(\"Spurs\") YIELD abc.start_year'
        # resp = self.execute(query)
        # self.check_resp_failed(resp);

        query = 'FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD $^.serve.start_year'
        resp = self.execute(query)
        self.check_resp_failed(resp);

        query = 'FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD $$.serve.start_year'
        resp = self.execute(query)
        self.check_resp_failed(resp);

        query = 'FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD abc.start_year'
        resp = self.execute(query)
        self.check_resp_failed(resp);

    def test_fetch_edges_not_exist_edge(self):
        # TODO(shylock) hash key
        # query = 'FETCH PROP ON serve hash(\"Zion Williamson\")->hash(\"Spurs\") YIELD serve.start_year'
        # resp = self.execute_query(query)
        # expect_result = []
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

        # query = 'FETCH PROP ON serve uuid(\"Zion Williamson\")->uuid(\"Spurs\") YIELD serve.start_year'
        # resp = self.execute_query(query)
        # expect_result = []
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON serve "Zion Williamson"->"Spurs" YIELD serve.start_year'
        resp = self.execute_query(query)
        # TODO:: here must return empty
        expect_result = [[T_EMPTY, T_EMPTY, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

    def test_fetch_edges_not_duplicate_column(self):
        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks" YIELD serve.start_year, serve.start_year'
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve.start_year',
                               'serve.start_year']
        expect_result = [["Boris Diaw", "Hawks", 0, 2003, 2003]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks" YIELD serve._src, serve._dst, serve._rank'
        resp = self.execute_query(query)
        expect_columns_name = ['serve._src',
                               'serve._dst',
                               'serve._rank',
                               'serve._src',
                               'serve._dst',
                               'serve._rank']
        expect_result = [["Boris Diaw", "Hawks", 0, "Boris Diaw", "Hawks", 0]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_columns_name)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" OVER serve YIELD serve._src AS src, serve._dst AS src \
                   | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year'''
        resp = self.execute(query)
        self.check_resp_failed(resp);

    def test_fetch_edges_not_exist_prop(self):
        query = 'FETCH PROP ON serve "Boris Diaw"->"Hawks" YIELD serve.not_exist_prop'
        resp = self.execute(query)
        self.check_resp_failed(resp);
