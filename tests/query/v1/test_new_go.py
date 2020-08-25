# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY, T_NULL
import pytest

class TestGoQuery(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def cleanup():
        pass

    def test_one_step(self):
        stmt = 'GO FROM "Tim Duncan" OVER serve'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO FROM "Tim Duncan", "Tim Duncan" OVER serve'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"],
                ["Spurs"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'YIELD "Tim Duncan" as vid | GO FROM $-.vid OVER serve'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])


        stmt = '''GO FROM "Boris Diaw" OVER serve YIELD \
            $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            "rows" : [
                ["Boris Diaw", 2003, 2005, "Hawks"],
                ["Boris Diaw", 2005, 2008, "Suns"],
                ["Boris Diaw", 2008, 2012, "Hornets"],
                ["Boris Diaw", 2012, 2016, "Spurs"],
                ["Boris Diaw", 2016, 2017, "Jazz"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM "Rajon Rondo" OVER serve WHERE \
            serve.start_year >= 2013 && serve.end_year <= 2018 YIELD \
            $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            "rows" : [
                ["Rajon Rondo", 2014, 2015, "Mavericks"],
                ["Rajon Rondo", 2015, 2016, "Kings"],
                ["Rajon Rondo", 2016, 2017, "Bulls"],
                ["Rajon Rondo", 2017, 2018, "Pelicans"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM "Boris Diaw" OVER like YIELD like._dst as id \
            | GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Hornets"],
                ["Trail Blazers"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Thunders' OVER serve REVERSELY"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Russell Westbrook"],
                ["Kevin Durant"],
                ["James Harden"],
                ["Carmelo Anthony"],
                ["Paul George"],
                ["Ray Allen"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM "Boris Diaw" OVER like YIELD like._dst as id \
            | ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Hornets"],
                ["Trail Blazers"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM "NON EXIST VERTEX ID" OVER like YIELD like._dst as id \
            | (GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve)'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = '''GO FROM "Boris Diaw" OVER like, serve YIELD like._dst as id \
            | ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Hornets"],
                ["Trail Blazers"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_assignment_simple(self):
        stmt = '''$var = GO FROM "Tracy McGrady" OVER like YIELD like._dst as id; \
            GO FROM $var.id OVER like'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tracy McGrady"],
                ["LaMarcus Aldridge"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_assignment_pipe(self):
        stmt = '''$var = (GO FROM "Tracy McGrady" OVER like YIELD like._dst as id \
            | GO FROM $-.id OVER like YIELD like._dst as id); \
            GO FROM $var.id OVER like'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Kobe Bryant"],
                ["Grant Hill"],
                ["Rudy Gay"],
                ["Tony Parker"],
                ["Tim Duncan"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_assignment_empty_result(self):
        stmt = '''$var = GO FROM "-1" OVER like YIELD like._dst as id; \
            GO FROM $var.id OVER like'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)


    def test_variable_undefined(self):
        stmt = "GO FROM $var OVER like"
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    def test_distinct(self):
        stmt = '''GO FROM "Nobody" OVER serve \
            YIELD DISTINCT $^.player.name as name, $$.team.name as name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        stmt = '''GO FROM "Boris Diaw" OVER like YIELD like._dst as id \
            | GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve \
            YIELD DISTINCT serve._dst, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst", "$$.team.name"],
            "rows" : [
                ["Spurs", "Spurs"],
                ["Hornets", "Hornets"],
                ["Trail Blazers", "Trail Blazers"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO 2 STEPS FROM "Tony Parker" OVER like YIELD DISTINCT like._dst'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tim Duncan"],
                ["Tony Parker"],
                ["Manu Ginobili"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_vertex_noexist(self):
        stmt = 'GO FROM "NON EXIST VERTEX ID" OVER serve'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = '''GO FROM "NON EXIST VERTEX ID" OVER serve YIELD \
            $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = '''GO FROM "NON EXIST VERTEX ID" OVER serve YIELD DISTINCT \
            $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

    def test_empty_inputs(self):
        stmt = '''GO FROM "NON EXIST VERTEX ID" OVER like YIELD like._dst as id \
            | GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = '''GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve._dst as id, serve.start_year as start \
            | YIELD $-.id as id WHERE $-.start > 20000 | Go FROM $-.id over serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

    def test_multi_edges_over_all(self):
        stmt = 'GO FROM "Russell Westbrook" OVER * REVERSELY YIELD serve._dst, like._dst'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL, "James Harden"],
                [T_NULL, "Dejounte Murray"],
                [T_NULL, "Paul George"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO FROM "Russell Westbrook" OVER * REVERSELY YIELD serve._src, like._src'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL, "Russell Westbrook"],
                [T_NULL, "Russell Westbrook"],
                [T_NULL, "Russell Westbrook"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO FROM "Russell Westbrook" OVER * REVERSELY'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["James Harden", T_NULL, T_NULL],
                ["Dejounte Murray", T_NULL, T_NULL],
                ["Paul George", T_NULL, T_NULL]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO FROM "Dirk Nowitzki" OVER * YIELD serve._dst, like._dst'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Mavericks", T_NULL],
                [T_NULL, "Steve Nash"],
                [T_NULL, "Jason Kidd"],
                [T_NULL, "Dwyane Wade"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO FROM "Paul Gasol" OVER *'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL, "Grizzlies", T_NULL],
                [T_NULL, "Lakers", T_NULL],
                [T_NULL, "Bulls", T_NULL],
                [T_NULL, "Spurs", T_NULL],
                [T_NULL, "Bucks", T_NULL],
                ["Kobe Bryant", T_NULL, T_NULL],
                ["Marc Gasol", T_NULL, T_NULL]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM "Manu Ginobili" OVER * REVERSELY YIELD like.likeness, \
            teammate.start_year, serve.start_year, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [95, T_NULL, T_NULL, "Tim Duncan"],
                [95, T_NULL, T_NULL, "Tony Parker"],
                [90, T_NULL, T_NULL, "Tiago Splitter"],
                [99, T_NULL, T_NULL, "Dejounte Murray"],
                [T_NULL, 2002, T_NULL, "Tim Duncan"],
                [T_NULL, 2002, T_NULL, "Tony Parker"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO FROM "LaMarcus Aldridge" OVER * YIELD $$.team.name, $$.player.name'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Trail Blazers", T_NULL],
                [T_NULL, "Tim Duncan"],
                [T_NULL, "Tony Parker"],
                ["Spurs", T_NULL]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM "Boris Diaw" OVER * YIELD like._dst as id \
            | ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Spurs"],
                ["Hornets"],
                ["Trail Blazers"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    @pytest.mark.skip(reason = 'return diffrent type when edge type wanted.')
    def test_edge_type(self):
        stmt = '''GO FROM "Russell Westbrook" OVER serve, like \
            YIELD serve.start_year, like.likeness, serve._type, like._type'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [2008, T_NULL, 6, T_NULL],
                [T_NULL, 90, T_NULL, 5],
                [T_NULL, 90, T_NULL, 5]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM "Russell Westbrook" OVER serve, like REVERSELY \
            YIELD serve._dst, like._dst, serve._type, like._type'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL, "James Harden", T_NULL, -5],
                [T_NULL, "Dejounte Murray", T_NULL, -5],
                [T_NULL, "Paul George", T_NULL, -5],
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_multi_edges(self):
        stmt = '''GO FROM "Russell Westbrook" OVER serve, like \
            YIELD serve.start_year, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [2008, T_NULL],
                [T_NULL, 90],
                [T_NULL, 90]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO FROM "Shaquile O\'Neal" OVER serve, like'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Magic", T_NULL],
                ["Lakers",T_NULL],
                ["Heat", T_NULL],
                ["Suns", T_NULL],
                ["Cavaliers", T_NULL],
                ["Celtics", T_NULL],
                [T_NULL, "JaVale McGee"],
                [T_NULL, "Tim Duncan"],
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Russell Westbrook' OVER serve, like"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Thunders", T_NULL],
                [T_NULL, "Paul George"],
                [T_NULL, "James Harden"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM "Russell Westbrook" OVER serve, like REVERSELY \
            YIELD serve._dst, like._dst, serve.start_year, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL, "James Harden", T_NULL, 80],
                [T_NULL, "Dejounte Murray", T_NULL, 99],
                [T_NULL, "Paul George", T_NULL, 95],
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO FROM "Russell Westbrook" OVER serve, like REVERSELY YIELD serve._src, like._src'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL, "Russell Westbrook"],
                [T_NULL, "Russell Westbrook"],
                [T_NULL, "Russell Westbrook"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GO FROM "Russell Westbrook" OVER serve, like REVERSELY'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL, "James Harden"],
                [T_NULL, "Dejounte Murray"],
                [T_NULL, "Paul George"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM "Manu Ginobili" OVER like, teammate REVERSELY YIELD like.likeness, \
            teammate.start_year, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [95, T_NULL, "Tim Duncan"],
                [95, T_NULL, "Tony Parker"],
                [90, T_NULL, "Tiago Splitter"],
                [99, T_NULL, "Dejounte Murray"],
                [T_NULL, 2002, "Tim Duncan"],
                [T_NULL, 2002, "Tony Parker"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_reference_pipe_in_yieldandwhere(self):
        stmt = '''GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id \
            | GO FROM $-.id OVER like \
            YIELD $-.name, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$-.name", "$^.player.name", "$$.player.name"],
            "rows" : [
                ["Tim Duncan", "Manu Ginobili", "Tim Duncan"],
                ["Tim Duncan", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili"],
                ["Tim Duncan", "Tony Parker", "Tim Duncan"],
                ["Chris Paul", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "Chris Paul"],
                ["Chris Paul", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "Chris Paul"],
                ["Chris Paul", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id \
            | GO FROM $-.id OVER like \
            WHERE $-.name != $$.player.name \
            YIELD $-.name, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$-.name", "$^.player.name", "$$.player.name"],
            "rows" : [
                ["Tim Duncan", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili"],
                ["Chris Paul", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    @pytest.mark.skip(reason = '$-.* over * $var.* not implement')
    def test_all_prop(self):
        stmt = '''GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id \
            | GO FROM $-.id OVER like \
            YIELD $-.*, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$-.name", "$^.player.name", "$$.player.name"],
            "rows" : [
                ["Tim Duncan", "Manu Ginobili", "Manu Ginobili", "Tim Duncan"],
                ["Tim Duncan", "Tony Parker", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Tony Parker", "Manu Ginobili"],
                ["Tim Duncan", "Tony Parker", "Tony Parker", "Tim Duncan"],
                ["Chris Paul", "LeBron James", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "Carmelo Anthony", "Chris Paul"],
                ["Chris Paul", "Carmelo Anthony", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "Dwyane Wade", "Chris Paul"],
                ["Chris Paul", "Dwyane Wade", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id; \
            GO FROM $var.id OVER like \
            YIELD $var.*, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Tim Duncan", "Manu Ginobili", "Manu Ginobili", "Tim Duncan"],
                ["Tim Duncan", "Tony Parker", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Tony Parker", "Manu Ginobili"],
                ["Tim Duncan", "Tony Parker", "Tony Parker", "Tim Duncan"],
                ["Chris Paul", "LeBron James", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "Carmelo Anthony", "Chris Paul"],
                ["Chris Paul", "Carmelo Anthony", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "Dwyane Wade", "Chris Paul"],
                ["Chris Paul", "Dwyane Wade", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])


    def test_reference_variable_in_yieldandwhere(self):
        stmt = '''$var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id; \
            GO FROM $var.id OVER like \
            YIELD $var.name, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$var.name", "$^.player.name", "$$.player.name"],
            "rows" : [
                ["Tim Duncan", "Manu Ginobili", "Tim Duncan"],
                ["Tim Duncan", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili"],
                ["Tim Duncan", "Tony Parker", "Tim Duncan"],
                ["Chris Paul", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "Chris Paul"],
                ["Chris Paul", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "Chris Paul"],
                ["Chris Paul", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id; \
            GO FROM $var.id OVER like \
            WHERE $var.name != $$.player.name \
            YIELD $var.name, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$var.name", "$^.player.name", "$$.player.name"],
            "rows" : [
                ["Tim Duncan", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili"],
                ["Chris Paul", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_no_existent_prop(self):
        stmt = "GO FROM 'Tim Duncan' OVER serve YIELD $^.player.test"
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = "GO FROM 'Tim Duncan' OVER serve yield $^.player.test"
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = "GO FROM 'Tim Duncan' OVER serve YIELD serve.test"
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    @pytest.mark.skip(reason="not support udf_is_in now!")
    def test_is_incall(self):
        stmt = '''GO FROM 'Boris Diaw' OVER serve \
            WHERE udf_is_in($$.team.name, 'Hawks', 'Suns') \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            "rows" : [
                ["Boris Diaw", 2003, 2005, "Hawks"],
                ["Boris Diaw", 2005, 2008, "Suns"],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._dst AS id \
            | GO FROM  $-.id OVER serve WHERE udf_is_in($-.id, 'Tony Parker', 123)'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"],
                ["Hornets"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._dst AS id \
            | GO FROM  $-.id OVER serve WHERE udf_is_in($-.id, 'Tony Parker', 123) && 1 == 1'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"],
                ["Hornets"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    @pytest.mark.skip(reason = 'return not implement')
    def test_return_test(self):
        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE $A.dst == 123; \
            RETURN $rA IF $rA IS NOT NULL; \
            GO FROM $A.dst OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"],
                ["Spurs"],
                ["Hornets"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE 1 == 1; \
            RETURN $rA IF $rA IS NOT NULL; \
            GO FROM $A.dst OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$A.dst"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dstA; \
            $rA = YIELD $A.* WHERE $A.dstA == 123; \
            RETURN $rA IF $rA IS NOT NULL; \
            $B = GO FROM $A.dstA OVER like YIELD like._dst AS dstB; \
            $rB = YIELD $B.* WHERE $B.dstB == 456; \
            RETURN $rB IF $rB IS NOT NULL; \
            GO FROM $B.dstB OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"],
                ["Spurs"],
                ["Trail Blazers"],
                ["Spurs"],
                ["Spurs"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE $A.dst == 123; \
            RETURN $rA IF $rA IS NOT NULL'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE 1 == 1; \
            RETURN $rA IF $rA IS NOT NULL'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$A.dst"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE 1 == 1; \
            RETURN $B IF $B IS NOT NULL'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE 1 == 1; \
            RETURN $B IF $A IS NOT NULL'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # will return error
        stmt = '''RETURN $rA IF $rA IS NOT NULL;'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    def test_reversely_one_step(self):
        stmt = "GO FROM 'Tim Duncan' OVER like REVERSELY YIELD like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Boris Diaw"],
                ["Tiago Splitter"],
                ["Dejounte Murray"],
                ["Shaquile O'Neal"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' OVER like REVERSELY YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$$.player.name"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Boris Diaw"],
                ["Tiago Splitter"],
                ["Dejounte Murray"],
                ["Shaquile O'Neal"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Tim Duncan' OVER like REVERSELY \
            WHERE $$.player.age < 35 \
            YIELD $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$$.player.name"],
            "rows" : [
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Tiago Splitter"],
                ["Dejounte Murray"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' OVER * REVERSELY YIELD like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Boris Diaw"],
                ["Tiago Splitter"],
                ["Dejounte Murray"],
                ["Shaquile O'Neal"],
                [T_NULL],
                [T_NULL]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_only_id_n_steps(self):
        stmt = "GO 2 STEPS FROM 'Tony Parker' OVER like YIELD like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tim Duncan"],
                ["Tim Duncan"],
                ["Tony Parker"],
                ["Tony Parker"],
                ["Manu Ginobili"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 3 STEPS FROM 'Tony Parker' OVER like YIELD like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Manu Ginobili"],
                ["Tim Duncan"],
                ["Tim Duncan"],
                ["LaMarcus Aldridge"],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 1 STEPS FROM 'Tony Parker' OVER like YIELD like._dst AS id"\
               " | GO 2 STEPS FROM $-.id OVER like YIELD like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Manu Ginobili"],
                ["Tim Duncan"],
                ["Tim Duncan"],
                ["LaMarcus Aldridge"],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_reversely_two_steps(self):
        stmt = "GO 2 STEPS FROM 'Kobe Bryant' OVER like REVERSELY YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$$.player.name"],
            "rows" : [
                ["Marc Gasol"],
                ["Vince Carter"],
                ["Yao Ming"],
                ["Grant Hill"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 2 STEPS FROM 'Kobe Bryant' OVER * REVERSELY YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$$.player.name"],
            "rows" : [
                ["Marc Gasol"],
                ["Vince Carter"],
                ["Yao Ming"],
                ["Grant Hill"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Manu Ginobili' OVER * REVERSELY YIELD like._dst AS id \
            | GO FROM $-.id OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Spurs"],
                ["Spurs"],
                ["Hornets"],
                ["Spurs"],
                ["Hawks"],
                ["76ers"],
                ["Spurs"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_reversely_with_pipe(self):
        stmt = '''GO FROM 'LeBron James' OVER serve YIELD serve._dst AS id \
            | GO FROM $-.id OVER serve REVERSELY YIELD $^.team.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.team.name", "$$.player.name"],
            "rows" : [
                ["Cavaliers", "Kyrie Irving"],
                ["Cavaliers", "Dwyane Wade"],
                ["Cavaliers", "Shaquile O'Neal"],
                ["Cavaliers", "Danny Green"],
                ["Cavaliers", "LeBron James"],
                ["Heat", "Dwyane Wade"],
                ["Heat", "LeBron James"],
                ["Heat", "Ray Allen"],
                ["Heat", "Shaquile O'Neal"],
                ["Heat", "Amar'e Stoudemire"],
                ["Lakers", "Kobe Bryant"],
                ["Lakers", "LeBron James"],
                ["Lakers", "Rajon Rondo"],
                ["Lakers", "Steve Nash"],
                ["Lakers", "Paul Gasol"],
                ["Lakers", "Shaquile O'Neal"],
                ["Lakers", "JaVale McGee"],
                ["Lakers", "Dwight Howard"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'LeBron James' OVER serve YIELD serve._dst AS id \
            | GO FROM $-.id OVER serve REVERSELY WHERE $$.player.name != 'LeBron James' \
            YIELD $^.team.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.team.name", "$$.player.name"],
            "rows" : [
                ["Cavaliers", "Kyrie Irving"],
                ["Cavaliers", "Dwyane Wade"],
                ["Cavaliers", "Shaquile O'Neal"],
                ["Cavaliers", "Danny Green"],
                ["Heat", "Dwyane Wade"],
                ["Heat", "Ray Allen"],
                ["Heat", "Shaquile O'Neal"],
                ["Heat", "Amar'e Stoudemire"],
                ["Lakers", "Kobe Bryant"],
                ["Lakers", "Rajon Rondo"],
                ["Lakers", "Steve Nash"],
                ["Lakers", "Paul Gasol"],
                ["Lakers", "Shaquile O'Neal"],
                ["Lakers", "JaVale McGee"],
                ["Lakers", "Dwight Howard"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Manu Ginobili' OVER like REVERSELY YIELD like._dst AS id \
            | GO FROM $-.id OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Spurs"],
                ["Spurs"],
                ["Hornets"],
                ["Spurs"],
                ["Hawks"],
                ["76ers"],
                ["Spurs"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_bidirect(self):
        stmt = "GO FROM 'Tim Duncan' OVER serve bidirect"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Spurs"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' OVER like bidirect"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Boris Diaw"],
                ["Tiago Splitter"],
                ["Dejounte Murray"],
                ["Shaquile O'Neal"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' OVER serve, like bidirect"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst", "like._dst"],
            "rows" : [
                ["Spurs", T_NULL],
                [T_NULL, "Tony Parker"],
                [T_NULL, "Manu Ginobili"],
                [T_NULL, "Tony Parker"],
                [T_NULL, "Manu Ginobili"],
                [T_NULL, "LaMarcus Aldridge"],
                [T_NULL, "Marco Belinelli"],
                [T_NULL, "Danny Green"],
                [T_NULL, "Aron Baynes"],
                [T_NULL, "Boris Diaw"],
                [T_NULL, "Tiago Splitter"],
                [T_NULL, "Dejounte Murray"],
                [T_NULL, "Shaquile O'Neal"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' OVER serve bidirect YIELD $$.team.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$$.team.name"],
            "rows" : [
                ["Spurs"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' OVER like bidirect YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$$.player.name"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Boris Diaw"],
                ["Tiago Splitter"],
                ["Dejounte Murray"],
                ["Shaquile O'Neal"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Tim Duncan' OVER like bidirect WHERE like.likeness > 90 \
            YIELD $^.player.name, like._dst, $$.player.name, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.player.name", "like._dst", "$$.player.name", "like.likeness"],
            "rows" : [
                ["Tim Duncan", "Tony Parker", "Tony Parker", 95],
                ["Tim Duncan", "Manu Ginobili", "Manu Ginobili", 95],
                ["Tim Duncan", "Tony Parker", "Tony Parker", 95],
                ["Tim Duncan", "Dejounte Murray", "Dejounte Murray", 99]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_bidirect_over_all(self):
        stmt = '''GO FROM 'Tim Duncan' OVER * bidirect \
            YIELD $^.player.name, serve._dst, $$.team.name, like._dst, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.player.name", "serve._dst", "$$.team.name", "like._dst", "$$.player.name"],
            "rows" : [
                ["Tim Duncan", "Spurs", "Spurs", T_NULL, T_NULL],
                ["Tim Duncan", T_NULL, T_NULL, "Tony Parker", "Tony Parker"],
                ["Tim Duncan", T_NULL, T_NULL, "Manu Ginobili", "Manu Ginobili"],
                ["Tim Duncan", T_NULL, T_NULL, "Tony Parker", "Tony Parker"],
                ["Tim Duncan", T_NULL, T_NULL, "Manu Ginobili", "Manu Ginobili"],
                ["Tim Duncan", T_NULL, T_NULL, "LaMarcus Aldridge", "LaMarcus Aldridge"],
                ["Tim Duncan", T_NULL, T_NULL, "Marco Belinelli", "Marco Belinelli"],
                ["Tim Duncan", T_NULL, T_NULL, "Danny Green", "Danny Green"],
                ["Tim Duncan", T_NULL, T_NULL, "Aron Baynes", "Aron Baynes"],
                ["Tim Duncan", T_NULL, T_NULL, "Boris Diaw", "Boris Diaw"],
                ["Tim Duncan", T_NULL, T_NULL, "Tiago Splitter", "Tiago Splitter"],
                ["Tim Duncan", T_NULL, T_NULL, "Dejounte Murray", "Dejounte Murray"],
                ["Tim Duncan", T_NULL, T_NULL, "Shaquile O'Neal", "Shaquile O'Neal"],
                ["Tim Duncan", T_NULL, T_NULL, T_NULL, "Tony Parker"],
                ["Tim Duncan", T_NULL, T_NULL, T_NULL, "Manu Ginobili"],
                ["Tim Duncan", T_NULL, T_NULL, T_NULL, "Danny Green"],
                ["Tim Duncan", T_NULL, T_NULL, T_NULL, "LaMarcus Aldridge"],
                ["Tim Duncan", T_NULL, T_NULL, T_NULL, "Tony Parker"],
                ["Tim Duncan", T_NULL, T_NULL, T_NULL, "Manu Ginobili"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' OVER * bidirect"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst", "serve._dst", "teammate._dst"],
            "rows" : [
                [T_NULL, "Spurs", T_NULL],
                ["Tony Parker", T_NULL, T_NULL],
                ["Manu Ginobili", T_NULL, T_NULL],
                ["Tony Parker", T_NULL, T_NULL],
                ["Manu Ginobili", T_NULL, T_NULL],
                ["LaMarcus Aldridge", T_NULL, T_NULL],
                ["Marco Belinelli", T_NULL, T_NULL],
                ["Danny Green", T_NULL, T_NULL],
                ["Aron Baynes", T_NULL, T_NULL],
                ["Boris Diaw", T_NULL, T_NULL],
                ["Tiago Splitter", T_NULL, T_NULL],
                ["Dejounte Murray", T_NULL, T_NULL],
                ["Shaquile O'Neal", T_NULL, T_NULL],
                [T_NULL, T_NULL, "Tony Parker"],
                [T_NULL, T_NULL, "Manu Ginobili"],
                [T_NULL, T_NULL, "LaMarcus Aldridge"],
                [T_NULL, T_NULL, "Danny Green"],
                [T_NULL, T_NULL, "Tony Parker"],
                [T_NULL, T_NULL, "Manu Ginobili"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    @pytest.mark.skip(reason = 'not check duplicate column yet')
    def test_duplicate_column_name(self):
        stmt = "GO FROM 'Tim Duncan' OVER serve YIELD serve._dst, serve._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst", "serve._dst"],
            "rows" : [
                ["Spurs", "Spurs"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._dst AS id, like.likeness AS id \
            | GO FROM $-.id OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    def test_contains(self):
        """ the name_label is not a string any more, will be deprecated such a way of writing.
        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE $$.team.name CONTAINS Haw\
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            "rows" : [
                ["Boris Diaw", 2003, 2005, "Hawks"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])
        """

        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE $$.team.name CONTAINS \"Haw\"\
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            "rows" : [
                ["Boris Diaw", 2003, 2005, "Hawks"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])


        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE (string)serve.start_year CONTAINS "05" \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            "rows" : [
                ["Boris Diaw", 2005, 2008, "Suns"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE $^.player.name CONTAINS "Boris" \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            "rows" : [
                ["Boris Diaw", 2003, 2005, "Hawks"],
                ["Boris Diaw", 2005, 2008, "Suns"],
                ["Boris Diaw", 2008, 2012, "Hornets"],
                ["Boris Diaw", 2012, 2016, "Spurs"],
                ["Boris Diaw", 2016, 2017, "Jazz"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE !($^.player.name CONTAINS "Boris") \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE "Leo" CONTAINS "Boris" \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

    def test_with_intermediate_data(self):
        # zero to zero
        """
        stmt = "GO 0 TO 0 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)
        """

        # simple
        stmt = "GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Tim Duncan"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Tim Duncan"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like \
            YIELD DISTINCT like._dst, like.likeness, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst", "like.likeness", "$$.player.name"],
            "rows" : [
                ["Manu Ginobili", 95, "Manu Ginobili"],
                ["LaMarcus Aldridge", 90, "LaMarcus Aldridge"],
                ["Tim Duncan", 95, "Tim Duncan"],
                ["Tony Parker", 95, "Tony Parker"],
                ["Tony Parker", 75, "Tony Parker"],
                ["Tim Duncan", 75, "Tim Duncan"],
                ["Tim Duncan", 90, "Tim Duncan"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like \
            YIELD DISTINCT like._dst, like.likeness, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst", "like.likeness", "$$.player.name"],
            "rows" : [
                ["Manu Ginobili", 95, "Manu Ginobili"],
                ["LaMarcus Aldridge", 90, "LaMarcus Aldridge"],
                ["Tim Duncan", 95, "Tim Duncan"],
                ["Tony Parker", 95, "Tony Parker"],
                ["Tony Parker", 75, "Tony Parker"],
                ["Tim Duncan", 75, "Tim Duncan"],
                ["Tim Duncan", 90, "Tim Duncan"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 1 TO 3 STEPS FROM 'Tim Duncan' OVER serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Spurs"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 0 TO 3 STEPS FROM 'Tim Duncan' OVER serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                ["Spurs"]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 2 TO 3 STEPS FROM 'Tim Duncan' OVER serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        #reversely
        stmt = "GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like REVERSELY YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tim Duncan"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Boris Diaw"],
                ["Dejounte Murray"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Tiago Splitter"],
                ["Shaquile O'Neal"],
                ["Rudy Gay"],
                ["Damian Lillard"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like REVERSELY YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tim Duncan"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Boris Diaw"],
                ["Dejounte Murray"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Tiago Splitter"],
                ["Shaquile O'Neal"],
                ["Rudy Gay"],
                ["Damian Lillard"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 2 TO 2 STEPS FROM 'Tony Parker' OVER like REVERSELY YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Boris Diaw"],
                ["Dejounte Murray"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Tiago Splitter"],
                ["Shaquile O'Neal"],
                ["Rudy Gay"],
                ["Damian Lillard"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # empty starts before last step
        stmt = "GO 1 TO 3 STEPS FROM 'Spurs' OVER serve REVERSELY"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Tim Duncan"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Rudy Gay"],
                ["Marco Belinelli"],
                ["Danny Green"],
                ["Kyle Anderson"],
                ["Aron Baynes"],
                ["Boris Diaw"],
                ["Tiago Splitter"],
                ["Cory Joseph"],
                ["David West"],
                ["Jonathon Simmons"],
                ["Dejounte Murray"],
                ["Tracy McGrady"],
                ["Paul Gasol"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 0 TO 3 STEPS FROM 'Spurs' OVER serve REVERSELY"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst"],
            "rows" : [
                ["Tim Duncan"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Rudy Gay"],
                ["Marco Belinelli"],
                ["Danny Green"],
                ["Kyle Anderson"],
                ["Aron Baynes"],
                ["Boris Diaw"],
                ["Tiago Splitter"],
                ["Cory Joseph"],
                ["David West"],
                ["Jonathon Simmons"],
                ["Dejounte Murray"],
                ["Tracy McGrady"],
                ["Paul Gasol"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # bidirectionally
        stmt = "GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like BIDIRECT YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tim Duncan"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Boris Diaw"],
                ["Dejounte Murray"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Tiago Splitter"],
                ["Shaquile O'Neal"],
                ["Rudy Gay"],
                ["Damian Lillard"],
                ["LeBron James"],
                ["Russell Westbrook"],
                ["Chris Paul"],
                ["Kyle Anderson"],
                ["Kevin Durant"],
                ["James Harden"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])


        stmt = "GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like BIDIRECT YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["like._dst"],
            "rows" : [
                ["Tim Duncan"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Boris Diaw"],
                ["Dejounte Murray"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Tiago Splitter"],
                ["Shaquile O'Neal"],
                ["Rudy Gay"],
                ["Damian Lillard"],
                ["LeBron James"],
                ["Russell Westbrook"],
                ["Chris Paul"],
                ["Kyle Anderson"],
                ["Kevin Durant"],
                ["James Harden"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # over *
        stmt = "GO 1 TO 2 STEPS FROM 'Russell Westbrook' OVER * YIELD serve._dst, like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst", "like._dst"],
            "rows" : [
                ["Thunders", T_NULL],
                [T_NULL, "Paul George"],
                [T_NULL, "James Harden"],
                ["Pacers", T_NULL],
                ["Thunders", T_NULL],
                [T_NULL, "Russell Westbrook"],
                ["Thunders", T_NULL],
                ["Rockets", T_NULL],
                [T_NULL, "Russell Westbrook"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO 0 TO 2 STEPS FROM 'Russell Westbrook' OVER * YIELD serve._dst, like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst", "like._dst"],
            "rows" : [
                ["Thunders", T_NULL],
                [T_NULL, "Paul George"],
                [T_NULL, "James Harden"],
                ["Pacers", T_NULL],
                ["Thunders", T_NULL],
                [T_NULL, "Russell Westbrook"],
                ["Thunders", T_NULL],
                ["Rockets", T_NULL],
                [T_NULL, "Russell Westbrook"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # with properties
        stmt = '''GO 1 TO 2 STEPS FROM 'Russell Westbrook' OVER * \
            YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst", "like._dst", "serve.start_year", "like.likeness", "$$.player.name"],
            "rows" : [
                ["Thunders", T_NULL, 2008, T_NULL, T_NULL],
                [T_NULL, "Paul George", T_NULL, 90, "Paul George"],
                [T_NULL, "James Harden", T_NULL, 90, "James Harden"],
                ["Pacers", T_NULL, 2010, T_NULL, T_NULL],
                ["Thunders", T_NULL, 2017, T_NULL, T_NULL],
                [T_NULL, "Russell Westbrook", T_NULL, 95, "Russell Westbrook"],
                ["Thunders", T_NULL, 2009, T_NULL, T_NULL],
                ["Rockets", T_NULL, 2012, T_NULL, T_NULL],
                [T_NULL, "Russell Westbrook", T_NULL, 80, "Russell Westbrook"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO 0 TO 2 STEPS FROM 'Russell Westbrook' OVER * \
            YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst", "like._dst", "serve.start_year", "like.likeness", "$$.player.name"],
            "rows" : [
                ["Thunders", T_NULL, 2008, T_NULL, T_NULL],
                [T_NULL, "Paul George", T_NULL, 90, "Paul George"],
                [T_NULL, "James Harden", T_NULL, 90, "James Harden"],
                ["Pacers", T_NULL, 2010, T_NULL, T_NULL],
                ["Thunders", T_NULL, 2017, T_NULL, T_NULL],
                [T_NULL, "Russell Westbrook", T_NULL, 95, "Russell Westbrook"],
                ["Thunders", T_NULL, 2009, T_NULL, T_NULL],
                ["Rockets", T_NULL, 2012, T_NULL, T_NULL],
                [T_NULL, "Russell Westbrook", T_NULL, 80, "Russell Westbrook"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO 1 TO 2 STEPS FROM 'Russell Westbrook' OVER * \
            REVERSELY YIELD serve._dst, like._dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst", "like._dst"],
            "rows" : [
                [T_NULL, "Dejounte Murray"],
                [T_NULL, "James Harden"],
                [T_NULL, "Paul George"],
                [T_NULL, "Dejounte Murray"],
                [T_NULL, "Russell Westbrook"],
                [T_NULL, "Luka Doncic"],
                [T_NULL, "Russell Westbrook"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO 0 TO 2 STEPS FROM 'Russell Westbrook' OVER * \
            REVERSELY YIELD serve._dst, like._dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["serve._dst", "like._dst"],
            "rows" : [
                [T_NULL, "Dejounte Murray"],
                [T_NULL, "James Harden"],
                [T_NULL, "Paul George"],
                [T_NULL, "Dejounte Murray"],
                [T_NULL, "Russell Westbrook"],
                [T_NULL, "Luka Doncic"],
                [T_NULL, "Russell Westbrook"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_error_massage(self):
        stmt = "GO FROM 'Tim Duncan' OVER serve YIELD $$.player.name as name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    @pytest.mark.skip(reason = 'not support zero step now')
    def test_zero_step(self):
        stmt = "GO 0 STEPS FROM 'Tim Duncan' OVER serve BIDIRECT"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = "GO 0 STEPS FROM 'Tim Duncan' OVER serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

    def test_go_cover_input(self):
        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst \
            | GO FROM $-.src OVER like \
            YIELD $-.src as src, like._dst as dst, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["src", "dst", "$^.player.name", "$$.player.name"],
            "rows" : [
                ["Tim Duncan", "Tony Parker", "Tim Duncan", "Tony Parker"],
                ["Tim Duncan", "Manu Ginobili", "Tim Duncan", "Manu Ginobili"],
                ["Tim Duncan", "Tony Parker", "Tim Duncan", "Tony Parker"],
                ["Tim Duncan", "Manu Ginobili", "Tim Duncan", "Manu Ginobili"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$a = GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst; \
            GO FROM $a.src OVER like YIELD $a.src as src, like._dst as dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["src", "dst"],
            "rows" : [
                ["Tim Duncan", "Tony Parker"],
                ["Tim Duncan", "Manu Ginobili"],
                ["Tim Duncan", "Tony Parker"],
                ["Tim Duncan", "Manu Ginobili"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # with intermidate data pipe
        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst \
            | GO 1 TO 2 STEPS FROM $-.src OVER like YIELD $-.src as src, like._dst as dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["src", "dst"],
            "rows" : [
                ["Tim Duncan", "Tony Parker"],
                ["Tim Duncan", "Manu Ginobili"],
                ["Tim Duncan", "Tony Parker"],
                ["Tim Duncan", "Manu Ginobili"],

                ["Tim Duncan", "Tim Duncan"],
                ["Tim Duncan", "Tim Duncan"],
                ["Tim Duncan", "Manu Ginobili"],
                ["Tim Duncan", "Manu Ginobili"],
                ["Tim Duncan", "LaMarcus Aldridge"],
                ["Tim Duncan", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tim Duncan"],
                ["Tim Duncan", "Tim Duncan"],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # var with properties
        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst \
            | GO 1 TO 2 STEPS FROM $-.src OVER like \
            YIELD $-.src as src, $-.dst, like._dst as dst, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["src", "$-.dst", "dst", "like.likeness"],
            "rows" : [
                ["Tim Duncan", "Tony Parker", "Tony Parker", 95],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili", 95],
                ["Tim Duncan", "Manu Ginobili", "Tony Parker", 95],
                ["Tim Duncan", "Manu Ginobili", "Manu Ginobili", 95],

                ["Tim Duncan", "Tony Parker", "Tim Duncan", 95],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili", 95],
                ["Tim Duncan", "Tony Parker", "LaMarcus Aldridge", 90],
                ["Tim Duncan", "Tony Parker", "Tim Duncan", 90],
                ["Tim Duncan", "Manu Ginobili", "Tim Duncan", 95],
                ["Tim Duncan", "Manu Ginobili", "Manu Ginobili", 95],
                ["Tim Duncan", "Manu Ginobili", "LaMarcus Aldridge", 90],
                ["Tim Duncan", "Manu Ginobili", "Tim Duncan", 90],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # partial neighbors input
        stmt = '''GO FROM 'Danny Green' OVER like YIELD like._src AS src, like._dst AS dst \
            | GO FROM $-.dst OVER teammate YIELD $-.src AS src, $-.dst, teammate._dst AS dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["src", "$-.dst", "dst"],
            "rows" : [
                ["Danny Green", "Tim Duncan", "Tony Parker"],
                ["Danny Green", "Tim Duncan", "Manu Ginobili"],
                ["Danny Green", "Tim Duncan", "LaMarcus Aldridge"],
                ["Danny Green", "Tim Duncan", "Danny Green"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # var
        stmt = '''$a = GO FROM 'Danny Green' OVER like YIELD like._src AS src, like._dst AS dst; \
            GO FROM $a.dst OVER teammate YIELD $a.src AS src, $a.dst, teammate._dst AS dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["src", "$a.dst", "dst"],
            "rows" : [
                ["Danny Green", "Tim Duncan", "Tony Parker"],
                ["Danny Green", "Tim Duncan", "Manu Ginobili"],
                ["Danny Green", "Tim Duncan", "LaMarcus Aldridge"],
                ["Danny Green", "Tim Duncan", "Danny Green"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_backtrack_overlap(self):
        stmt = '''GO FROM 'Tony Parker' OVER like YIELD like._src as src, like._dst as dst \
            | GO 2 STEPS FROM $-.src OVER like YIELD $-.src, $-.dst, like._src, like._dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$-.src", "$-.dst", "like._src", "like._dst"],
            "rows" : [
                ["Tony Parker", "Tim Duncan", "Tim Duncan", "Manu Ginobili"],
                ["Tony Parker", "Tim Duncan", "Tim Duncan", "Tony Parker"],
                ["Tony Parker", "Tim Duncan", "LaMarcus Aldridge", "Tony Parker"],
                ["Tony Parker", "Tim Duncan", "Manu Ginobili", "Tim Duncan"],
                ["Tony Parker", "Tim Duncan", "LaMarcus Aldridge", "Tim Duncan"],
                ["Tony Parker", "Manu Ginobili", "Tim Duncan", "Manu Ginobili"],
                ["Tony Parker", "Manu Ginobili", "Tim Duncan", "Tony Parker"],
                ["Tony Parker", "Manu Ginobili", "LaMarcus Aldridge", "Tony Parker"],
                ["Tony Parker", "Manu Ginobili", "Manu Ginobili", "Tim Duncan"],
                ["Tony Parker", "Manu Ginobili", "LaMarcus Aldridge", "Tim Duncan"],
                ["Tony Parker", "LaMarcus Aldridge", "Tim Duncan", "Manu Ginobili"],
                ["Tony Parker", "LaMarcus Aldridge", "Tim Duncan", "Tony Parker"],
                ["Tony Parker", "LaMarcus Aldridge", "LaMarcus Aldridge", "Tony Parker"],
                ["Tony Parker", "LaMarcus Aldridge", "Manu Ginobili", "Tim Duncan"],
                ["Tony Parker", "LaMarcus Aldridge", "LaMarcus Aldridge", "Tim Duncan"],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$a = GO FROM 'Tony Parker' OVER like YIELD like._src as src, like._dst as dst; \
            GO 2 STEPS FROM $a.src OVER like YIELD $a.src, $a.dst, like._src, like._dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["$a.src", "$a.dst", "like._src", "like._dst"],
            "rows" : [
                ["Tony Parker", "Tim Duncan", "Tim Duncan", "Manu Ginobili"],
                ["Tony Parker", "Tim Duncan", "Tim Duncan", "Tony Parker"],
                ["Tony Parker", "Tim Duncan", "LaMarcus Aldridge", "Tony Parker"],
                ["Tony Parker", "Tim Duncan", "Manu Ginobili", "Tim Duncan"],
                ["Tony Parker", "Tim Duncan", "LaMarcus Aldridge", "Tim Duncan"],
                ["Tony Parker", "Manu Ginobili", "Tim Duncan", "Manu Ginobili"],
                ["Tony Parker", "Manu Ginobili", "Tim Duncan", "Tony Parker"],
                ["Tony Parker", "Manu Ginobili", "LaMarcus Aldridge", "Tony Parker"],
                ["Tony Parker", "Manu Ginobili", "Manu Ginobili", "Tim Duncan"],
                ["Tony Parker", "Manu Ginobili", "LaMarcus Aldridge", "Tim Duncan"],
                ["Tony Parker", "LaMarcus Aldridge", "Tim Duncan", "Manu Ginobili"],
                ["Tony Parker", "LaMarcus Aldridge", "Tim Duncan", "Tony Parker"],
                ["Tony Parker", "LaMarcus Aldridge", "LaMarcus Aldridge", "Tony Parker"],
                ["Tony Parker", "LaMarcus Aldridge", "Manu Ginobili", "Tim Duncan"],
                ["Tony Parker", "LaMarcus Aldridge", "LaMarcus Aldridge", "Tim Duncan"],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

