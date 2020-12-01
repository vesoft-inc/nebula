# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import pytest

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_NULL_BAD_TYPE


class TestStartsWithAndEndsWith(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_starts_with(self):
        stmt = "YIELD 'apple' STARTS WITH 'app'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['(apple STARTS WITH app)'],
            "rows": [
                [True]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' STARTS WITH 'a'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['(apple STARTS WITH a)'],
            "rows": [
                [True]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' STARTS WITH 'A'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['(apple STARTS WITH A)'],
            "rows": [
                [False]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' STARTS WITH 'b'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['(apple STARTS WITH b)'],
            "rows": [
                [False]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD '123' STARTS WITH '1'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        # expected column names should contain "" or '' if the query contains a string
        # to fix this issue, update toString() method in modules/common/src/common/datatypes/Value.cpp
        # the fix could causes past tests fail
        expected_data = {
            "column_names": ['(123 STARTS WITH 1)'],
            "rows": [
                [True]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 123 STARTS WITH 1"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['(123 STARTS WITH 1)'],
            "rows": [
                [T_NULL_BAD_TYPE]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_not_starts_with(self):
        stmt = "YIELD 'apple' NOT STARTS WITH 'app'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' NOT STARTS WITH 'a'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' NOT STARTS WITH 'A'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' NOT STARTS WITH 'b'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD '123' NOT STARTS WITH '1'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 123 NOT STARTS WITH 1"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [T_NULL_BAD_TYPE]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_starts_with_GO(self):
        stmt = '''GO FROM 'Tony Parker' OVER like WHERE like._dst STARTS WITH 'LaMarcus' YIELD $^.player.name'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['$^.player.name'],
            "rows": [
                ['Tony Parker']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])


        stmt = '''GO FROM 'Tony Parker' OVER like WHERE like._dst STARTS WITH 'Obama' YIELD $^.player.name'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['$^.player.name'],
            "rows": [
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_not_starts_with_GO(self):
        stmt = '''GO FROM 'Tony Parker' OVER like WHERE like._dst NOT STARTS WITH 'T' YIELD $^.player.name'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['$^.player.name'],
            "rows": [
                ['Tony Parker'],
                ['Tony Parker'],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
                  GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
                  AND $$.player.name NOT STARTS WITH 'Tony' YIELD $^.player.name, $$.player.name, like.likeness'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['$^.player.name', '$$.player.name', 'like.likeness'],
            "rows": [
                ['Manu Ginobili', 'Tim Duncan', 90],
                ['LaMarcus Aldridge', 'Tim Duncan', 75],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
                  GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
                  AND $^.player.name NOT STARTS WITH 'LaMarcus' YIELD $^.player.name, $$.player.name, like.likeness'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['$^.player.name', '$$.player.name', 'like.likeness'],
            "rows": [
                ['Manu Ginobili', 'Tim Duncan', 90],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
        GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
        AND $$.player.name NOT STARTS WITH 'Tony' YIELD $^.player.name, $$.player.name, like.likeness'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['$^.player.name', '$$.player.name', 'like.likeness'],
            "rows": [
                ['Manu Ginobili', 'Tim Duncan', 90],
                ['LaMarcus Aldridge', 'Tim Duncan', 75],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_ends_with(self):
        stmt = "YIELD 'apple' ENDS WITH 'le'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' ENDS WITH 'app'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' ENDS WITH 'a'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' ENDS WITH 'e'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' ENDS WITH 'E'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' ENDS WITH 'b'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD '123' ENDS WITH '3'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 123 ENDS WITH 3"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [T_NULL_BAD_TYPE]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_not_ends_with(self):
        stmt = "YIELD 'apple' NOT ENDS WITH 'le'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' NOT ENDS WITH 'app'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' NOT ENDS WITH 'a'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' NOT ENDS WITH 'e'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' NOT ENDS WITH 'E'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' NOT ENDS WITH 'b'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD '123' NOT ENDS WITH '3'"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 123 NOT ENDS WITH 3"
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": [],
            "rows": [
                [T_NULL_BAD_TYPE]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_ends_with_GO(self):
        stmt = '''GO FROM 'Tony Parker' OVER like WHERE like._dst ENDS WITH 'Ginobili' YIELD $^.player.name '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        # print(resp)
        expected_data = {
            "column_names": ['$^.player.name'],
            "rows": [
                ['Tony Parker'],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_not_ends_with_GO(self):
        stmt = '''GO FROM 'Tony Parker' OVER like WHERE like._dst NOT ENDS WITH 'Ginobili' YIELD $^.player.name '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names": ['$^.player.name'],
            "rows": [
                ['Tony Parker'],
                ['Tony Parker'],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])
