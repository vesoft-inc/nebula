# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


@pytest.mark.usefixtures('set_vertices_and_edges')
class TestVariableLengthRelationshipMatch(NebulaTestSuite):
    @classmethod
    def prepare(cls):
        cls.use_nba()

    @pytest.mark.skip
    def test_to_be_deleted(self):
        # variable steps
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*1..3]-> () return *'
        self.fail_query(stmt)
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*..3]-> () return *'
        self.fail_query(stmt)
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*1..]-> () return *'
        self.fail_query(stmt)

    @pytest.mark.skip
    def test_hops_0_to_1(self, like, serve):
        VERTICES, EDGES = self.VERTEXS, self.EDGS

        def like_row(dst: str):
            return [[like('Tracy McGrady', dst)], VERTICES[dst]]

        def serve_row(dst):
            return [[serve('Tracy McGrady', dst)], VERTICES[dst]]

        # single both direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve*0..1{start_year: 2000}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                serve_row("Magic")
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0..1{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                like_row("Vince Carter"),
                like_row("Yao Ming"),
                like_row("Grant Hill"),  # like each other
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*1{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                like_row("Vince Carter"),
                like_row("Yao Ming"),
                like_row("Grant Hill"),  # like each other
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # single direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0..1{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*1{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # single both direction edge without properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve*0..1]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                serve_row("Raptors"),
                serve_row("Magic"),
                serve_row("Spurs"),
                serve_row("Rockets"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0..1]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                like_row("Vince Carter"),
                like_row("Yao Ming"),
                like_row("Grant Hill"),  # like each other
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # multiple both direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1{start_year: 2000}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                serve_row("Magic"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # multiple single direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1{start_year: 2000}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                serve_row("Magic"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # multiple both direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                like_row("Vince Carter"),
                like_row("Yao Ming"),
                like_row("Grant Hill"),
                serve_row("Raptors"),
                serve_row("Magic"),
                serve_row("Spurs"),
                serve_row("Rockets"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # multiple single direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                serve_row("Raptors"),
                serve_row("Magic"),
                serve_row("Spurs"),
                serve_row("Rockets"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

    @pytest.mark.skip
    def test_mix_hops(self):
        stmt = '''
        MATCH (:player{name: "Tim Duncan"})-[e1:like]->()-[e2:serve*0..3]->()<-[e3:serve]-(v)
        RETURN e1, e2, e3, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": []
        }
        self.check_rows_with_header(stmt, expected)

    def test_more_cases(self, like, serve, like_2hop):
        # stmt = '''
        # MATCH (v:player{name: 'Tim Duncan'})-[e:like*0]-()
        # RETURN e
        # '''

        # stmt = '''
        # MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..0]-()
        # RETURN e
        # '''

        # stmt = '''
        # MATCH (v:player{name: 'Tim Duncan'})-[e:like*]-()
        # RETURN e
        # '''

        # stmt = '''
        # MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..0]-()-[e2:like*0..0]-()
        # RETURN e, e2
        # '''

        stmt='''
        MATCH p=(v:player{name: 'Tim Duncan'})-[:like|:serve*1..3]->(v1)
        WHERE e[0].likeness>90
        RETURN p
        '''
        resp = self.execute(stmt)
        self.check_resp_failed(resp)
        self.check_error_msg(resp, "SemanticError: Alias used but not defined: `e'")

        stmt='''
        MATCH p=(v:player{name: 'Tim Duncan'})-[:like|:serve*1..3]->(v1)
        RETURN e
        '''
        resp = self.execute(stmt)
        self.check_resp_failed(resp)
        self.check_error_msg(resp, "SemanticError: Alias used but not defined: `e'")

        stmt='''
        MATCH p=(v:player{name: 'Tim Duncan'})-[:like|:serve*1..3]->(v1)
        WHERE e[0].likeness+e[1].likeness>90
        RETURN p
        '''
        resp = self.execute(stmt)
        self.check_resp_failed(resp)
        self.check_error_msg(resp, "SemanticError: Alias used but not defined: `e'")
