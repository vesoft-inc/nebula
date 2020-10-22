# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import pytest

from tests.common.nebula_test_suite import NebulaTestSuite, T_NULL, T_EMPTY


class TestDeleteVertices(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.load_data()

    @classmethod
    def cleanup(self):
        query = 'DROP SPACE IF EXISTS deletenoedges_space'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_delete_one_vertex(self):
        resp = self.execute_query('GO FROM "Boris Diaw" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["Tony Parker"], ["Tim Duncan"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Tony Parker" OVER like REVERSELY')
        self.check_resp_succeeded(resp)
        expect_result = [["LaMarcus Aldridge"],
                         ["Boris Diaw"],
                         ["Dejounte Murray"],
                         ["Marco Belinelli"],
                         ["Tim Duncan"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query(' FETCH PROP ON player "Tony Parker" YIELD player.name, player.age')
        self.check_resp_succeeded(resp)
        expect_result = [["Tony Parker", "Tony Parker", 36]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('FETCH PROP ON serve "Tony Parker"->"Spurs" '
                                  'YIELD serve.start_year, serve.end_year')
        self.check_resp_succeeded(resp)
        expect_result = [['Tony Parker', 'Spurs', 0, 1999, 2018]]
        self.check_out_of_order_result(resp, expect_result)

        # delete vertex
        resp = self.execute('DELETE VERTEX "Tony Parker"')
        self.check_resp_succeeded(resp)

        # check
        resp = self.execute_query('FETCH PROP ON player "Tony Parker" YIELD player.name, player.age')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('FETCH PROP ON serve "Tony Parker"->"Spurs" '
                                  'YIELD serve.start_year, serve.end_year')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Boris Diaw" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["Tim Duncan"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Tony Parker" OVER like REVERSELY')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

    def test_delete_multi_vertices(self):
        resp = self.execute_query('GO FROM "Chris Paul" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["LeBron James"], ["Dwyane Wade"], ["Carmelo Anthony"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute('DELETE VERTEX "LeBron James", "Dwyane Wade", "Carmelo Anthony"')
        self.check_resp_succeeded(resp)

        # check result
        resp = self.execute_query('GO FROM "Chris Paul" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

    @pytest.mark.skip(reason="does not support int vid")
    def test_delete_with_hash(self):
        resp = self.execute_query('GO FROM hash("Tracy McGrady") OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [[6293765385213992205], [-2308681984240312228], [-3212290852619976819]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM hash("Grant Hill") OVER like REVERSELY')
        self.check_resp_succeeded(resp)
        expect_result = [[4823234394086728974]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('FETCH PROP ON player hash("Grant Hill") YIELD player.name, player.age')
        self.check_resp_succeeded(resp)
        expect_result = [[6293765385213992205, "Grant Hill", 46]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('FETCH PROP ON serve hash("Grant Hill")->hash("Pistons") '
                                  'YIELD serve.start_year, serve.end_year')
        self.check_resp_succeeded(resp)
        expect_result = [6293765385213992205, -2742277443392542725, 0, 1994, 2000]
        self.check_out_of_order_result(resp, expect_result)

        # delete vertex
        resp = self.execute('DELETE VERTEX hash("Grant Hill")')
        self.check_resp_succeeded(resp)

        # check again
        resp = self.execute_query('GO FROM hash("Tracy McGrady") OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["LKobe Bryant"], ["Rudy Gay"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM hash("Grant Hill") OVER like REVERSELY')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('FETCH PROP ON player hash("Grant Hill") YIELD player.name, player.age')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

        # delete not exist vertex
        resp = self.execute('DELETE VERTEX hash("Non-existing Vertex")')
        self.check_resp_succeeded(resp)

        # insert a vertex without edges
        resp = self.execute('INSERT VERTEX player(name, age) VALUES hash("A Loner"): ("A Loner", 0)')
        self.check_resp_succeeded(resp)

        # delete a vertex without edges
        resp = self.execute('DELETE VERTEX hash("A Loner")')
        self.check_resp_succeeded(resp)

        resp = self.execute_query('FETCH PROP ON player hash("A Loner") YIELD player.name, player.age')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

    @pytest.mark.skip(reason="does not support uuid")
    def test_delete_with_uuid(self):
        resp = self.execute_query('FETCH PROP ON player UUID("Grant Hill") YIELD player.name, player.age')
        self.check_resp_succeeded(resp)
        expect_result = [["Grant Hill", 46]]
        self.check_result(resp, expect_result, {0})

        resp = self.execute_query('FETCH PROP ON serve UUID("Grant Hill")->UUID("Pistons") '
                                  'YIELD serve.start_year, serve.end_year')
        self.check_resp_succeeded(resp)
        expect_result = [1994, 2000]
        self.check_out_of_order_result(resp, expect_result)

        # delete vertex
        resp = self.execute('DELETE VERTEX UUID("Grant Hill")')
        self.check_resp_succeeded(resp)

        resp = self.execute_query('FETCH PROP ON player UUID("Grant Hill") YIELD player.name, player.age')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_result(resp, expect_result, {0})

        resp = self.execute_query('FETCH PROP ON serve UUID("Grant Hill")->UUID("Pistons") '
                                  'YIELD serve.start_year, serve.end_year')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

        # delete not exist vertex
        resp = self.execute('DELETE VERTEX UUID("Non-existing Vertex")')
        self.check_resp_succeeded(resp)

        # insert a vertex without edges
        resp = self.execute('INSERT VERTEX player(name, age) VALUES UUID("A Loner"): ("A Loner", 0)')
        self.check_resp_succeeded(resp)

        # delete a vertex without edges
        resp = self.execute('DELETE VERTEX UUID("A Loner")')
        self.check_resp_succeeded(resp)

        resp = self.execute_query('FETCH PROP ON player UUID("A Loner") YIELD player.name, player.age')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

    def test_delete_with_no_edge(self):
        resp = self.execute('create space deletenoedges_space;'
                            'use deletenoedges_space;'
                            'CREATE TAG person(name string, age int);')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp = self.execute('INSERT VERTEX person(name, age) VALUES "101":("Tony Parker", 36)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DELETE VERTEX "101"')
        self.check_resp_succeeded(resp)

        resp = self.execute_query('FETCH PROP ON person "101" yield person.name, person.age')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)
