# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest
from nebula2.graph import ttypes

from nebula_test_common.nebula_test_suite import NebulaTestSuite


class TestSetQuery(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.load_data()

    @classmethod
    def cleanup(cls):
        pass

    @pytest.mark.skip(reason="")
    def test_union_all(self):
        stmt = '''GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
         UNION ALL GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"],
        self.check_column_names(resp, column_names)
        expected_data = [["Tim Duncan", 1997, "Spurs"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"]]
        self.check_result(resp, expected_data)

        stmt = '''GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
         UNION ALL GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
         UNION ALL GO FROM "Manu Ginobili" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        colums = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, colums)
        expected_data = [["Tim Duncan", 1997, "Spurs"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"],
                         ["Manu Ginobili", 2002, "Spurs"]]
        self.check_result(resp, expected_data)

        stmt = '''(GO FROM "Tim Duncan" OVER like YIELD like._dst AS id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name) \
         UNION ALL GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Manu Ginobili", 2002, "Spurs"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"]]
        self.check_result(resp, expected_data)

        stmt = '''GO FROM "Tim Duncan" OVER like YIELD like._dst AS id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
         UNION ALL GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Manu Ginobili", 2002, "Spurs"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"]]
        self.check_result(resp, expected_data)

        stmt = '''GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
         UNION ALL (GO FROM "Tony Parker" OVER like YIELD like._dst AS id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Tim Duncan", 1997, "Spurs"],
                         ["LaMarcus Aldridge", 2015, "Spurs"],
                         ["LaMarcus Aldridge", 2006, "Trail Blazers"],
                         ["Manu Ginobili", 2002, "Spurs"],
                         ["Tim Duncan", 1997, "Spurs"]]
        self.check_result(resp, expected_data)

        stmt = '''GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
         UNION ALL GO FROM "Tony Parker" OVER like YIELD like._dst AS id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Tim Duncan", 1997, "Spurs"],
                         ["LaMarcus Aldridge", 2015, "Spurs"],
                         ["LaMarcus Aldridge", 2006, "Trail Blazers"],
                         ["Manu Ginobili", 2002, "Spurs"],
                         ["Tim Duncan", 1997, "Spurs"]]
        self.check_result(resp, expected_data)

        stmt = '''(GO FROM "Tim Duncan" OVER like YIELD like._dst AS id \
         UNION ALL GO FROM "Tony Parker" OVER like YIELD like._dst AS id) \
         | GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Manu Ginobili", 2002, "Spurs"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"],
                         ["LaMarcus Aldridge", 2015, "Spurs"],
                         ["LaMarcus Aldridge", 2006, "Trail Blazers"],
                         ["Manu Ginobili", 2002, "Spurs"],
                         ["Tim Duncan", 1997, "Spurs"]]
        self.check_result(resp, expected_data)

        stmt = '''GO FROM "Tim Duncan" OVER serve YIELD $^.player.name as name, $$.team.name as player \
         UNION ALL \
         GO FROM "Tony Parker" OVER serve \
         YIELD $^.player.name as name, serve.start_year'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Tim Duncan", "Spurs"], ["Tony Parker", "1999"],
                         ["Tony Parker", "2018"]]
        self.check_result(resp, expected_data)

        stmt = '''GO FROM "Nobody" OVER serve YIELD $^.player.name AS player, serve.start_year AS start \
         UNION ALL \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Tony Parker", 1999], ["Tony Parker", 2018]]
        self.check_result(resp, expected_data)

    @pytest.mark.skip(reason="")
    def test_union_distinct(self):
        stmt = '''(GO FROM "Tim Duncan" OVER like YIELD like._dst as id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name) \
          UNION \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
          UNION \
         GO FROM "Manu Ginobili" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Manu Ginobili", 2002, "Spurs"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"]]
        self.check_result(resp, expected_data)

        stmt = '''(GO FROM "Tim Duncan" OVER like YIELD like._dst as id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name) \
          UNION DISTINCT \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Manu Ginobili", 2002, "Spurs"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"]]
        self.check_result(resp, expected_data)

    @pytest.mark.skip(reason="")
    def test_minus(self):
        stmt = '''(GO FROM "Tim Duncan" OVER like YIELD like._dst as id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name) \
          MINUS \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Manu Ginobili", 2002, "Spurs"]]
        self.check_result(resp, expected_data)

    @pytest.mark.skip(reason="")
    def test_intersect(self):
        stmt = '''(GO FROM "Tim Duncan" OVER like YIELD like._dst as id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name) \
          INTERSECT \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"]]
        self.check_result(resp, expected_data)

    @pytest.mark.skip(reason="")
    def test_mix(self):
        stmt = '''(GO FROM "Tim Duncan" OVER like YIELD like._dst as id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name) \
          MINUS \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
          UNION \
         GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
          INTERSECT \
         GO FROM "Manu Ginobili" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Manu Ginobili", 2002, "Spurs"]]
        self.check_result(resp, expected_data)

    @pytest.mark.skip(reason="")
    def test_assign(self):
        stmt = '''$var = GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
          UNION ALL \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name; \
         YIELD $var.*'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Tim Duncan", 1997, "Spurs"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"]]
        self.check_result(resp, expected_data)

        stmt = '''$var = (GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
          UNION ALL \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name); \
         YIELD $var.*'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Tim Duncan", 1997, "Spurs"],
                         ["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"]]
        self.check_result(resp, expected_data)

        stmt = '''$var = (GO FROM "Tim Duncan" OVER like YIELD like._dst as id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name) \
          MINUS \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name; \
         YIELD $var.*'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Manu Ginobili", 2002, "Spurs"]]
        self.check_result(resp, expected_data)

        stmt = '''$var = (GO FROM "Tim Duncan" OVER like YIELD like._dst as id | \
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name) \
          INTERSECT \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name; \
         YIELD $var.*'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$^.player.name", "serve.start_year", "$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = [["Tony Parker", 1999, "Spurs"],
                         ["Tony Parker", 2018, "Hornets"]]
        self.check_result(resp, expected_data)

    @pytest.mark.skip(reason="")
    def test_empty_input(self):
        stmt = '''GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name \
          UNION \
         GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name \
          MINUS \
         GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name \
          INTERSECT \
         GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["serve.start_year", "$$.team.name"],
        self.check_column_names(resp, column_names)
        expected_data = []
        self.check_result(resp, expected_data)

        stmt = '''$var = GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name \
          UNION \
         GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name \
          MINUS \
         GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name \
          INTERSECT \
         GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name; \
         YIELD $var.*'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        column_names = ["$var.serve.start_year", "$var.$$.team.name"]
        self.check_column_names(resp, column_names)
        expected_data = []
        self.check_result(resp, expected_data)

    @pytest.mark.skip(reason="")
    def test_syntax_error(self):
        stmt = '''GO FROM "123" OVER like \
          YIELD like._src as src, like._dst as dst \
          | (GO FROM $-.src OVER serve \
          UNION GO FROM $-.dst OVER serve)'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)

    @pytest.mark.skip(reason="")
    def test_execution_error(self):
        stmt = '''GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name \
          UNION \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name1, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        stmt = '''GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year \
          UNION \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        stmt = '''GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year \
          UNION \
         GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)
