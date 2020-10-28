# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


@pytest.mark.usefixtures('set_vertices_and_edges')
class TestMatch(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_single_node(self):
        VERTICES = self.VERTEXS
        stmt = 'MATCH (v:player {name: "Yao Ming"}) RETURN v'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['v'],
            "rows": [
                [VERTICES['Yao Ming']]
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = 'MATCH (v:player) WHERE v.name == "Yao Ming" RETURN v.age AS Age'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['Age'],
            "rows": [
                [38]
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = 'MATCH (v:player {age: 29}) return v.name AS Name'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['Name'],
            "rows": [
                ['James Harden'],
                ['Jonathon Simmons'],
                ['Klay Thompson'],
                ['Dejounte Murray']
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = 'MATCH (v:player {age: 29}) WHERE v.name STARTS WITH "J" return v.name AS Name'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['Name'],
            "rows": [
                ['James Harden'],
                ['Jonathon Simmons'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = 'MATCH (v:player) WHERE v.age >= 38 AND v.age < 45 return v.name AS Name, v.age AS Age'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['Name', 'Age'],
            "rows": [
                ['Paul Gasol', 38],
                ['Kobe Bryant', 40],
                ['Vince Carter', 42],
                ['Tim Duncan', 42],
                ['Yao Ming', 38],
                ['Dirk Nowitzki', 40],
                ['Manu Ginobili', 41],
                ['Ray Allen', 43],
                ['David West', 38],
                ['Tracy McGrady', 39],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

    def test_one_step(self):
        stmt = 'MATCH (v1:player{name: "LeBron James"}) -[r]-> (v2) RETURN type(r) AS Type, v2.name AS Name'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Type', 'Name'],
            'rows': [
                ['like', 'Ray Allen'],
                ['serve', 'Cavaliers'],
                ['serve', 'Heat'],
                ['serve', 'Cavaliers'],
                ['serve', 'Lakers'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2)
                  RETURN type(r) AS Type, v2.name AS Name
               '''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Type', 'Name'],
            'rows': [
                ['serve', 'Cavaliers'],
                ['serve', 'Heat'],
                ['serve', 'Cavaliers'],
                ['serve', 'Lakers'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2 {name: "Cavaliers"})
                  RETURN type(r) AS Type, v2.name AS Name
               '''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Type', 'Name'],
            'rows': [
                ['serve', 'Cavaliers'],
                ['serve', 'Cavaliers'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2 {name: "Cavaliers"})
                  WHERE r.start_year <= 2005 AND r.end_year >= 2005
                  RETURN r.start_year AS Start, r.end_year AS End
               '''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Start', 'End'],
            'rows': [
                [2003, 2010],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

    def test_two_steps(self):
        stmt = '''
                  MATCH (v1:player{age: 28}) -[:like]-> (v2) -[:like]-> (v3)
                  RETURN v1.name AS Player, v2.name AS Friend, v3.name AS FoF
               '''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Player', 'Friend', 'FoF'],
            'rows': [
                ['Paul George', 'Russell Westbrook', 'James Harden'],
                ['Paul George', 'Russell Westbrook', 'Paul George'],
                ['Damian Lillard', 'LaMarcus Aldridge', 'Tim Duncan'],
                ['Damian Lillard', 'LaMarcus Aldridge', 'Tony Parker']
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

    def test_failures(self):
        # No RETURN
        stmt = 'MATCH (v:player{name:"abc")'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    def test_unimplemented_features(self):
        # No label
        stmt = 'MATCH (v) return v'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # Scan by label
        stmt = 'MATCH (v:player) return v'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # Scan by label
        stmt = 'MATCH (v:player:person) return v'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # start from edge
        stmt = 'MATCH () -[r:serve]-> () return *'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # start from tail
        stmt = 'MATCH () -[]-> (v) return *'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # start from middle
        stmt = 'MATCH () --> (v) --> () return *'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # in bound
        stmt = 'MATCH (v:player) -[]-> () return *'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # bidirectly
        stmt = 'MATCH (v:player) -[]- () return *'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # multiple steps
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*2]-> () return *'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # variable steps
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*1..3]-> () return *'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*..3]-> () return *'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*1..]-> () return *'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)
