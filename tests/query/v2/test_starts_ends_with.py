# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_NULL, T_EMPTY, T_NULL_BAD_TYPE
import pytest

class TestStartsWithAndEndsWith(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def cleanup():
        pass

    def test_starts_with(self):
        stmt = "YIELD 'apple' STARTS WITH 'app'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' STARTS WITH 'a'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' STARTS WITH 'A'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' STARTS WITH 'b'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])      

        stmt = "YIELD '123' STARTS WITH '1'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 123 STARTS WITH 1"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL_BAD_TYPE]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])      

    def test_starts_with_GO(self):
        stmt = '''GO FROM 'Tony Parker' OVER like WHERE like.likeness IN [95,56,21]
                AND $$.player.name starts with 'Tim' YIELD $$.player.name '''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['Tim Duncan'],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Tony Parker' OVER like WHERE like._dst IN ['Tim Duncan', 'Danny Green'] 
                AND $$.player.name STARTS WITH 'Adam' YIELD $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
                  GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
                  AND $$.player.name STARTS WITH 'TONY' YIELD $^.player.name, $$.player.name, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$^.player.name', '$$.player.name', 'like.likeness'],
            "rows" : [
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
                  GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
                  AND $$.player.name STARTS WITH 'Tony' YIELD $^.player.name, $$.player.name, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$^.player.name', '$$.player.name', 'like.likeness'],
            "rows" : [
                ['LaMarcus Aldridge', 'Tony Parker', 75],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])
    
    def test_ends_with(self):
        stmt = "YIELD 'apple' ENDS WITH 'le'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' ENDS WITH 'app'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' ENDS WITH 'a'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])
     
        stmt = "YIELD 'apple' ENDS WITH 'e'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])
        
        stmt = "YIELD 'apple' ENDS WITH 'E'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'apple' ENDS WITH 'b'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])      

        stmt = "YIELD '123' ENDS WITH '3'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])  

        stmt = "YIELD 123 ENDS WITH 3"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [T_NULL_BAD_TYPE]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])  

    def test_ends_with_GO(self):
        stmt = '''GO FROM 'Tony Parker' OVER like WHERE like.likeness IN [95,56,21]
                AND $$.player.name ENDS WITH 'can' YIELD $$.player.name '''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['Tim Duncan'],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''GO FROM 'Tony Parker' OVER like WHERE like._dst IN ['Tim Duncan', 'Danny Green'] 
                AND $$.player.name ENDS WITH 'Smith' YIELD $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
                  GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
                  AND $$.player.name ENDS WITH 'PARKER' YIELD $^.player.name, $$.player.name, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$^.player.name', '$$.player.name', 'like.likeness'],
            "rows" : [
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
                  GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
                  AND $$.player.name ENDS WITH 'Parker' YIELD $^.player.name, $$.player.name, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$^.player.name', '$$.player.name', 'like.likeness'],
            "rows" : [
                ['LaMarcus Aldridge', 'Tony Parker', 75],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])
