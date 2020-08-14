# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from nebula2.graph import ttypes
from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_NULL

# using _vid instead VertexID for vertex id column name not compatible to 1.0
class TestFetchQuery(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    @classmethod
    def cleanup(cls):
        pass

    def test_fetch_vertex_base(self):
        query = 'FETCH PROP ON player "Boris Diaw" YIELD player.name, player.age'
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON player "Boris Diaw" YIELD player.name, player.age, player.age > 30'
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age', '(player.age>30)']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36, True]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON player $-.id YIELD player.name, player.age'''
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age']
        expect_result = [
            ['Tony Parker', 'Tony Parker', 36],
            ['Tim Duncan', 'Tim Duncan', 42]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''$var = GO FROM "Boris Diaw" over like YIELD like._dst as id;
            FETCH PROP ON player $var.id YIELD player.name, player.age'''
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''$var = GO FROM "Boris Diaw" over like YIELD like._dst as id;
            FETCH PROP ON player $var.id YIELD player.name as name, player.age
            | ORDER BY name'''
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'name', 'player.age']
        expect_result = [
            ['Tim Duncan', 'Tim Duncan', 42],
            ['Tony Parker', 'Tony Parker', 36],
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_result(resp, expect_result)

        # TODO insert hash key
        #query = 'FETCH PROP ON player hash("Boris Diaw") YIELD player.name, player.age';
        #resp = self.execute_query(query)
        #expect_result = [['Boris Diaw', 36]]
        #self.check_resp_succeeded(resp)
        #self.check_out_of_order_result(resp, expect_result)

        # TODO insert uuid key
        #query = 'FETCH PROP ON player uuid("Boris Diaw") YIELD player.name, player.age';
        #resp = self.execute_query(query)
        #expect_result = [['Boris Diaw', 36]]
        #self.check_resp_succeeded(resp)
        #self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_no_yield(self):
        query = 'FETCH PROP ON player "Boris Diaw"'
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # TODO insert hash key
        # query = 'FETCH PROP ON player hash("Boris Diaw")'
        # resp = self.execute_query(query)
        # expect_result = [[TODO, 'Boris Diaw', 36]]
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

        # TODO insert uuid key
        # query = 'FETCH PROP ON player uuid("Boris Diaw")'
        # resp = self.execute_query(query)
        # expect_result = [[TODO, 'Boris Diaw', 36]]
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_disctinct(self):
        query = 'FETCH PROP ON player "Boris Diaw", "Boris Diaw" YIELD DISTINCT player.name, player.age'
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON player "Boris Diaw", "Tony Parker" YIELD DISTINCT player.name, player.age'
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36], ['Tony Parker', 'Tony Parker', 36]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_syntax_error(self):
        query = 'FETCH PROP ON player "Boris Diaw" YIELD $^.player.name, player.age'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        query = 'FETCH PROP ON player "Boris Diaw" YIELD $$.player.name, player.age'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        query = 'FETCH PROP ON player "Boris Diaw" YIELD abc.name, player.age'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        query = '''GO FROM "11" over like YIELD like._dst as id \
                   | FETCH PROP ON player "11" YIELD $-.id'''
        resp = self.execute(query)
        self.check_resp_failed(resp)
    
    def test_fetch_vertex_not_exist_tag(self):
        query = 'FETCH PROP ON not_exist_tag "Boris Diaw"';
        resp = self.execute(query)
        self.check_resp_failed(resp)

    def test_fetch_vertex_not_exist_vertex(self):
        query = 'FETCH PROP ON player "not_exist_vertex"'
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age']
        expected = []
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expected)

        # It's not compatible to 1.0, now not support $- again
        # query = 'GO FROM "not_exist_vertex" OVER serve | FETCH PROP ON team $-'
        # resp = self.execute_query(query)
        # expect_column_names = ['_vid', 'team.name']
        # expected = []
        # self.check_resp_succeeded(resp)
        # self.check_column_names(resp, expect_column_names)
        # self.check_out_of_order_result(resp, expected)

        query = 'FETCH PROP ON * "not_exist_vertex"'
        resp = self.execute_query(query)
        expected = []
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected)

    # not compatible to 1.0 * extend to all tag in space
    def test_fetch_vertex_get_all(self):
        query = 'FETCH PROP ON * "Boris Diaw"'
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36, T_NULL, T_NULL, T_NULL]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON bachelor "Tim Duncan" '
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'bachelor.name', 'bachelor.speciality']
        expect_result = [["Tim Duncan", "Tim Duncan", "psychology"]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON * "Tim Duncan" '
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Tim Duncan', 'Tim Duncan', 42, T_NULL, "Tim Duncan", "psychology"]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_duplicate_column_names(self):
        query = 'FETCH PROP ON player "Boris Diaw" YIELD player.name, player.name'
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.name']
        expect_result = [['Boris Diaw', 'Boris Diaw', 'Boris Diaw']]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # TODO(shylock) GO validator error ?
        # query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id, like._dst as id
            # | FETCH PROP ON player $-.id YIELD player.name, player.age'''
        # resp = self.execute(query)
        # self.check_resp_failed(resp)


    def test_fetch_vertex_not_exist_prop(self):
        query = 'FETCH PROP ON player "Boris Diaw" YIELD player.name1'
        resp = self.execute(query)
        self.check_resp_failed(resp)

    def test_fetch_vertex_empty_input(self):
        query = '''GO FROM "not_exist_vertex" over like YIELD like._dst as id
            | FETCH PROP ON player $-.id'''
        resp = self.execute_query(query)
        expect_column_names = ['_vid', 'player.name', 'player.age']
        expect_result = []
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Marco Belinelli" over serve YIELD serve._dst as id, serve.start_year as start
            | YIELD $-.id as id WHERE $-.start > 20000
            | FETCH PROP ON player $-.id'''
        resp = self.execute_query(query)
        expect_result = []
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)
