# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY


class TestFetchQuery(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_fetch_vertex_base(self):
        query = 'FETCH PROP ON player "Boris Diaw" YIELD player.name, player.age'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON player "Boris Diaw" YIELD player.name, player.age, player.age > 30'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', '(player.age>30)']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36, True]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON player $-.id YIELD player.name, player.age'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age']
        expect_result = [
            ['Tony Parker', 'Tony Parker', 36],
            ['Tim Duncan', 'Tim Duncan', 42]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''$var = GO FROM "Boris Diaw" over like YIELD like._dst as id;
            FETCH PROP ON player $var.id YIELD player.name, player.age'''
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''$var = GO FROM "Boris Diaw" over like YIELD like._dst as id;
            FETCH PROP ON player $var.id YIELD player.name as name, player.age
            | ORDER BY name'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'name', 'player.age']
        expect_result = [
            ['Tim Duncan', 'Tim Duncan', 42],
            ['Tony Parker', 'Tony Parker', 36],
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_result(resp, expect_result)

        # TODO insert hash key
        #query = 'FETCH PROP ON player hash("Boris Diaw") YIELD player.name, player.age';
        #resp = self.execute(query)
        #expect_result = [['Boris Diaw', 36]]
        #self.check_resp_succeeded(resp)
        #self.check_out_of_order_result(resp, expect_result)

        # TODO insert uuid key
        #query = 'FETCH PROP ON player uuid("Boris Diaw") YIELD player.name, player.age';
        #resp = self.execute(query)
        #expect_result = [['Boris Diaw', 36]]
        #self.check_resp_succeeded(resp)
        #self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_no_yield(self):
        query = 'FETCH PROP ON player "Boris Diaw"'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # TODO insert hash key
        # query = 'FETCH PROP ON player hash("Boris Diaw")'
        # resp = self.execute(query)
        # expect_result = [[TODO, 'Boris Diaw', 36]]
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

        # TODO insert uuid key
        # query = 'FETCH PROP ON player uuid("Boris Diaw")'
        # resp = self.execute(query)
        # expect_result = [[TODO, 'Boris Diaw', 36]]
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_disctinct(self):
        query = 'FETCH PROP ON player "Boris Diaw", "Boris Diaw" YIELD DISTINCT player.name, player.age'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON player "Boris Diaw", "Tony Parker" YIELD DISTINCT player.name, player.age'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age']
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
        # not exist
        query = 'FETCH PROP ON player "not_exist_vertex"'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age']
        expected = []
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expected)

        # not exist and exist
        query = 'FETCH PROP ON player "not_exist_vertex", "Boris Diaw"'
        resp = self.execute(query)
        expected = [['Boris Diaw', 'Boris Diaw', 36]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expected)

        # It's not compatible to 1.0, now not support $- again
        # query = 'GO FROM "not_exist_vertex" OVER serve | FETCH PROP ON team $-'
        # resp = self.execute(query)
        # expect_column_names = ['VertexID', 'team.name']
        # expected = []
        # self.check_resp_succeeded(resp)
        # self.check_column_names(resp, expect_column_names)
        # self.check_out_of_order_result(resp, expected)

        # not exist
        query = 'FETCH PROP ON * "not_exist_vertex"'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expected = []
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expected)

        # not exist and exist
        query = 'FETCH PROP ON * "Boris Diaw", "not_exist_vertex"'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expected = [['Boris Diaw', 'Boris Diaw', 36, T_EMPTY, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expected)


    # not compatible to 1.0 * extend to all tag in space
    def test_fetch_vertex_get_all(self):
        query = 'FETCH PROP ON * "Boris Diaw"'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36, T_EMPTY, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON bachelor "Tim Duncan" '
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'bachelor.name', 'bachelor.speciality']
        expect_result = [["Tim Duncan", "Tim Duncan", "psychology"]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON * "Tim Duncan" '
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # multi vertices
        query = 'FETCH PROP ON * "Tim Duncan", "Boris Diaw"'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"],
                         ['Boris Diaw', 'Boris Diaw', 36, T_EMPTY, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # from input
        query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON * $-.id'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 'Tony Parker', 36, T_EMPTY, T_EMPTY, T_EMPTY],
            ['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # from var
        query = '''$a = GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON * $a.id'''
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_get_all_with_yield(self):
        query = 'FETCH PROP ON * "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36, T_EMPTY, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON * "Boris Diaw" YIELD player.age, team.name, bachelor.speciality'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.age', 'team.name', 'bachelor.speciality']
        expect_result = [['Boris Diaw', 36, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON * "Tim Duncan" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON * "Tim Duncan" YIELD player.name, team.name, bachelor.name'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'team.name', 'bachelor.name']
        expect_result = [['Tim Duncan', 'Tim Duncan', T_EMPTY, "Tim Duncan"]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # multi vertices
        query = 'FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"],
                         ['Boris Diaw', 'Boris Diaw', 36, T_EMPTY, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD player.age, team.name, bachelor.name'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.age', 'team.name', 'bachelor.name']
        expect_result = [['Tim Duncan', 42, T_EMPTY, "Tim Duncan"],
                         ['Boris Diaw', 36, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # from input
        query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON * $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 'Tony Parker', 36, T_EMPTY, T_EMPTY, T_EMPTY],
            ['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON * $-.id YIELD player.age, team.name, bachelor.speciality'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.age', 'team.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 36, T_EMPTY, T_EMPTY],
            ['Tim Duncan',  42, T_EMPTY, "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # from var
        query = '''$a = GO FROM "Boris Diaw" over like YIELD like._dst as id;
            FETCH PROP ON * $a.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 'Tony Parker', 36, T_EMPTY, T_EMPTY, T_EMPTY],
            ['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''$a = GO FROM "Boris Diaw" over like YIELD like._dst as id;
            FETCH PROP ON * $a.id YIELD player.age, team.name, bachelor.speciality'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.age', 'team.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 36, T_EMPTY, T_EMPTY],
            ['Tim Duncan',  42, T_EMPTY, "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_get_all_with_yield_invalid(self):
        # not exist tag
        query = 'FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD not_exist_tag.name'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        # not exist property
        query = 'FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD player.not_exist_prop'
        resp = self.execute(query)
        self.check_resp_failed(resp)

    def test_fetch_vertex_on_multi_tags(self):
        query = 'FETCH PROP ON player, team "Boris Diaw"'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # multi vertices
        query = 'FETCH PROP ON player, bachelor "Tim Duncan", "Boris Diaw"'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Tim Duncan', 'Tim Duncan', 42, "Tim Duncan", "psychology"],
                         ['Boris Diaw', 'Boris Diaw', 36, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # from input
        query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON player, bachelor $-.id'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'bachelor.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 'Tony Parker', 36, T_EMPTY, T_EMPTY],
            ['Tim Duncan', 'Tim Duncan', 42, "Tim Duncan", "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # from var
        query = '''$a = GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON palyer, bachelor * $a.id'''
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_on_multi_tags_with_yield(self):
        query = 'FETCH PROP ON team, player, bachelor "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Boris Diaw', 'Boris Diaw', 36, T_EMPTY, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON player, team, bachelor "Boris Diaw" YIELD player.age, team.name, bachelor.speciality'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.age', 'team.name', 'bachelor.speciality']
        expect_result = [['Boris Diaw', 36, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON team, player, bachelor "Tim Duncan" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON player, team, bachelor "Tim Duncan" YIELD player.name, team.name, bachelor.name'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'team.name', 'bachelor.name']
        expect_result = [['Tim Duncan', 'Tim Duncan', T_EMPTY, "Tim Duncan"]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # multi vertices
        query = 'FETCH PROP ON bachelor, team, player "Tim Duncan", "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"],
                         ['Boris Diaw', 'Boris Diaw', 36, T_EMPTY, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = 'FETCH PROP ON player, team, bachelor "Tim Duncan", "Boris Diaw" YIELD player.age, team.name, bachelor.name'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.age', 'team.name', 'bachelor.name']
        expect_result = [['Tim Duncan', 42, T_EMPTY, "Tim Duncan"],
                         ['Boris Diaw', 36, T_EMPTY, T_EMPTY]]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # from input
        query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON player, team, bachelor $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 'Tony Parker', 36, T_EMPTY, T_EMPTY, T_EMPTY],
            ['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id
            | FETCH PROP ON player, team, bachelor $-.id YIELD player.age, team.name, bachelor.speciality'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.age', 'team.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 36, T_EMPTY, T_EMPTY],
            ['Tim Duncan',  42, T_EMPTY, "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        # from var
        query = '''$a = GO FROM "Boris Diaw" over like YIELD like._dst as id;
            FETCH PROP ON player, team, bachelor $a.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age', 'team.name', 'bachelor.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 'Tony Parker', 36, T_EMPTY, T_EMPTY, T_EMPTY],
            ['Tim Duncan', 'Tim Duncan', 42, T_EMPTY, "Tim Duncan", "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''$a = GO FROM "Boris Diaw" over like YIELD like._dst as id;
            FETCH PROP ON player, team, bachelor $a.id YIELD player.age, team.name, bachelor.speciality'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.age', 'team.name', 'bachelor.speciality']
        expect_result = [
            ['Tony Parker', 36, T_EMPTY, T_EMPTY],
            ['Tim Duncan',  42, T_EMPTY, "psychology"]
        ]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

    def test_fetch_vertex_duplicate_column_names(self):
        query = 'FETCH PROP ON player "Boris Diaw" YIELD player.name, player.name'
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.name']
        expect_result = [['Boris Diaw', 'Boris Diaw', 'Boris Diaw']]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Boris Diaw" over like YIELD like._dst as id, like._dst as id
            | FETCH PROP ON player $-.id YIELD player.name, player.age'''
        resp = self.execute(query)
        self.check_resp_failed(resp)


    def test_fetch_vertex_not_exist_prop(self):
        query = 'FETCH PROP ON player "Boris Diaw" YIELD player.name1'
        resp = self.execute(query)
        self.check_resp_failed(resp)

    def test_fetch_vertex_empty_input(self):
        query = '''GO FROM "not_exist_vertex" over like YIELD like._dst as id
            | FETCH PROP ON player $-.id'''
        resp = self.execute(query)
        expect_column_names = ['VertexID', 'player.name', 'player.age']
        expect_result = []
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)

        query = '''GO FROM "Marco Belinelli" over serve YIELD serve._dst as id, serve.start_year as start
            | YIELD $-.id as id WHERE $-.start > 20000
            | FETCH PROP ON player $-.id'''
        resp = self.execute(query)
        expect_result = []
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expect_column_names)
        self.check_out_of_order_result(resp, expect_result)
