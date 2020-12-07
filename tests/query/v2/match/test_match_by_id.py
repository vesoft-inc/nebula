# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import functools
import pytest

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.path_value import PathVal


@pytest.mark.usefixtures('set_vertices_and_edges')
class TestMatchById(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_signle_node(self):
        # single node
        stmt = '''
                    MATCH (n) WHERE id(n) == 'James Harden' RETURN n
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['n']
        self.check_column_names(resp, columns_name)
        result = [[self.VERTEXS['James Harden']]]
        self.check_out_of_order_result(resp, result)

        stmt = '''
                    MATCH (n) WHERE id(n) == 'not_exist_vertex' RETURN n
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, columns_name)
        result = []
        self.check_out_of_order_result(resp, result)

        # with expr
        stmt = '''
                    MATCH (n) WHERE id(n) == 'not_exist_vertex' RETURN id(n)
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['id(n)']
        self.check_column_names(resp, columns_name)
        result = []
        self.check_out_of_order_result(resp, result)

        stmt = '''
                    MATCH (n) WHERE id(n) == 'Tony Parker' RETURN id(n), labels(n)
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['id(n)', 'labels(n)']
        self.check_column_names(resp, columns_name)
        result = [['Tony Parker', ['player']]]
        self.check_out_of_order_result(resp, result)

        stmt = '''
                    MATCH (n) WHERE id(n) == 'not_exist_vertex' RETURN labels(n)
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['labels(n)']
        self.check_column_names(resp, columns_name)
        result = []
        self.check_out_of_order_result(resp, result)

        # multi nodes
        stmt = '''
                    MATCH (n) WHERE id(n) IN ['not_exist_vertex']
                    RETURN n
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['n']
        self.check_column_names(resp, columns_name)
        result = []
        self.check_out_of_order_result(resp, result)

        stmt = '''
                    MATCH (n) WHERE id(n) IN ['LaMarcus Aldridge', 'Tony Parker']
                    RETURN n
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, columns_name)
        result = [[self.VERTEXS['LaMarcus Aldridge']],
                  [self.VERTEXS['Tony Parker']]]
        self.check_out_of_order_result(resp, result)

        stmt = '''
                    MATCH (n) WHERE id(n) IN ['LaMarcus Aldridge', 'Tony Parker', 'not_exist_vertex']
                    RETURN n
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, columns_name)
        result = [[self.VERTEXS['LaMarcus Aldridge']],
                  [self.VERTEXS['Tony Parker']]]
        self.check_out_of_order_result(resp, result)

        # with expr
        stmt = '''
                    MATCH (n) WHERE id(n) IN ['LaMarcus Aldridge', 'Tony Parker', 'not_exist_vertex']
                    RETURN id(n)
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['id(n)']
        self.check_column_names(resp, columns_name)
        result = [['LaMarcus Aldridge'],
                  ['Tony Parker']]
        self.check_out_of_order_result(resp, result)

        stmt = '''
                    MATCH (n) WHERE id(n) IN ['LaMarcus Aldridge', 'Tony Parker', 'not_exist_vertex']
                    RETURN id(n), `tags`(n)
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['id(n)', 'tags(n)']
        self.check_column_names(resp, columns_name)
        result = [['LaMarcus Aldridge', ['player']],
                  ['Tony Parker', ['player']]]
        self.check_out_of_order_result(resp, result)

        stmt = 'MATCH (start)-[e]-(end) WHERE id(start) == "Paul George" RETURN *'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

        stmt = 'MATCH (start)-[e]-(end) WHERE id(start) IN ["Paul George", "not_exist_vertex"] RETURN *'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)

    def test_one_step(self):
        stmt = '''
                MATCH (v1) -[r]-> (v2)
                WHERE id(v1) == "LeBron James"
                RETURN type(r) AS Type, v2.name AS Name
               '''
        resp = self.execute(stmt)
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
                MATCH (v1) -[r:serve|:like]-> (v2)
                WHERE id(v1) == "LeBron James"
                RETURN type(r) AS Type, v2.name AS Name
               '''
        resp = self.execute(stmt)
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
                  MATCH (v1) -[r:serve]-> (v2)
                  WHERE id(v1) == "LeBron James"
                  RETURN type(r) AS Type, v2.name AS Name
               '''
        resp = self.execute(stmt)
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
                  MATCH (v1) -[r:serve]-> (v2 {name: "Cavaliers"})
                  WHERE id(v1) == "LeBron James"
                  RETURN type(r) AS Type, v2.name AS Name
               '''
        resp = self.execute(stmt)
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
                  MATCH (v1) -[:like]-> (v2)
                  WHERE id(v1) == "Danny Green"
                  RETURN v1.name AS Name, v2.name AS Friend
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Friend'],
            'rows': [
                ['Danny Green', 'LeBron James'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'Tim Duncan'],
            ]
        }

        stmt = '''
                  MATCH (v1) <-[:like]- (v2)
                  WHERE id(v1) == "Danny Green"
                  RETURN v1.name AS Name, v2.name AS Friend
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Friend'],
            'rows': [
                ['Danny Green', 'Dejounte Murray'],
                ['Danny Green', 'Marco Belinelli'],
            ]
        }

        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1) <-[:like]-> (v2)
                  WHERE id(v1) == "Danny Green"
                  RETURN v1.name AS Name, v2.name AS Friend
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Friend'],
            'rows': [
                ['Danny Green', 'Dejounte Murray'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'LeBron James'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'Tim Duncan'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1) -[:like]- (v2)
                  WHERE id(v1) == "Danny Green"
                  RETURN v1.name AS Name, v2.name AS Friend
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Friend'],
            'rows': [
                ['Danny Green', 'Dejounte Murray'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'LeBron James'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'Tim Duncan'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

    def test_two_steps(self):
        stmt = '''
                MATCH (v1) -[:like]-> (v2) -[:like]-> (v3)
                WHERE id(v1) == "Tim Duncan"
                RETURN v1.name AS Player, v2.name AS Friend, v3.name AS FoF
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Player', 'Friend', 'FoF'],
            'rows': [
                ['Tim Duncan', 'Manu Ginobili', 'Tim Duncan'],
                ['Tim Duncan', 'Tony Parker', 'LaMarcus Aldridge'],
                ['Tim Duncan', 'Tony Parker', 'Manu Ginobili'],
                ['Tim Duncan', 'Tony Parker', 'Tim Duncan'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

    def test_distinct(self):
        stmt = '''
                  MATCH (v1) -[:like]-> () -[:like]-> (v3)
                  WHERE id(v1) == 'Dwyane Wade'
                  RETURN v3.name AS Name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name'],
            'rows': [
                ['Carmelo Anthony'],
                ['Dwyane Wade'],
                ['Dwyane Wade'],
                ['LeBron James'],
                ['LeBron James'],
                ['Chris Paul'],
                ['Ray Allen'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1) -[:like]-> () -[:like]-> (v3)
                  WHERE id(v1) == 'Dwyane Wade'
                  RETURN DISTINCT v3.name AS Name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name'],
            'rows': [
                ['Carmelo Anthony'],
                ['Dwyane Wade'],
                ['LeBron James'],
                ['Chris Paul'],
                ['Ray Allen'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])


    def test_order_skip_limit(self):
        # ORDER BY
        stmt = '''
                  MATCH (v1) -[:like]-> (v)
                  WHERE id(v1) == 'Dejounte Murray'
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': [
                ['Tim Duncan', 42],
                ['Manu Ginobili', 41],
                ['Tony Parker', 36],
                ['LeBron James', 34],
                ['Chris Paul', 33],
                ['Marco Belinelli', 32],
                ['Danny Green', 31],
                ['Kevin Durant', 30],
                ['Russell Westbrook', 30],
                ['James Harden', 29],
                ['Kyle Anderson', 25],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # ORDER BY LIMIT
        stmt = '''
                  MATCH (v1) -[:like]-> (v)
                  WHERE id(v1) == 'Dejounte Murray'
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  LIMIT 3
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': [
                ['Tim Duncan', 42],
                ['Manu Ginobili', 41],
                ['Tony Parker', 36],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # ORDER BY SKIP
        stmt = '''
                  MATCH (v1) -[:like]-> (v)
                  WHERE id(v1) == 'Dejounte Murray'
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  SKIP 3
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': [
                ['LeBron James', 34],
                ['Chris Paul', 33],
                ['Marco Belinelli', 32],
                ['Danny Green', 31],
                ['Kevin Durant', 30],
                ['Russell Westbrook', 30],
                ['James Harden', 29],
                ['Kyle Anderson', 25],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # ORDER BY SKIP LIMIT
        stmt = '''
                  MATCH (v1) -[:like]-> (v)
                  WHERE id(v1) == 'Dejounte Murray'
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  SKIP 3
                  LIMIT 3
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': [
                ['LeBron James', 34],
                ['Chris Paul', 33],
                ['Marco Belinelli', 32],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])
        # SKIP all rows
        stmt = '''
                  MATCH (v1) -[:like]-> (v)
                  WHERE id(v1) == 'Dejounte Murray'
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  SKIP 11
                  LIMIT 3
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': []
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # LIMIT 0
        stmt = '''
                  MATCH (v1) -[:like]-> (v)
                  WHERE id(v1) == 'Dejounte Murray'
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  LIMIT 0
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': []
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # ORDER BY expr
        stmt = '''
                  MATCH (v1) -[:like]-> (v)
                  WHERE id(v1) == 'Dejounte Murray'
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY v.age DESC, v.name ASC
               '''
        resp = self.execute(stmt)
        self.check_resp_failed(resp)

    def test_return_path(self):
        VERTICES, EDGES = self.VERTEXS, self.EDGES

        stmt = '''
                MATCH p = (n)
                WHERE id(n) == "Tony Parker"
                RETURN p,n
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp);
        columns_name = ['p', 'n']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["Tony Parker"]]), VERTICES["Tony Parker"]]
            ]
        self.check_out_of_order_result(resp, result)

        stmt = '''
                MATCH p = (n)-[:like]->(m)
                WHERE id(n) == "LeBron James"
                RETURN p, n.name, m.name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['p', 'n.name', 'm.name']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["LeBron James"+"Ray Allen"+"like"+"0"], 1),
                    VERTICES["Ray Allen"]]),
                "LeBron James", "Ray Allen"]
            ]
        self.check_out_of_order_result(resp, result)

        stmt = '''
                MATCH p = (n)<-[:like]-(m)
                WHERE id(n) == "LeBron James"
                RETURN p, n.name, m.name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['p', 'n.name', 'm.name']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Dejounte Murray"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Dejounte Murray"]]),
                "LeBron James", "Dejounte Murray"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Carmelo Anthony"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Carmelo Anthony"]]),
                "LeBron James", "Carmelo Anthony"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Kyrie Irving"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Kyrie Irving"]]),
                "LeBron James", "Kyrie Irving"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Dwyane Wade"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Dwyane Wade"]]),
                "LeBron James", "Dwyane Wade"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Danny Green"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Danny Green"]]),
                "LeBron James", "Danny Green"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Chris Paul"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Chris Paul"]]),
                "LeBron James", "Chris Paul"],
            ]
        self.check_out_of_order_result(resp, result)

        stmt = '''
                MATCH p = (n)-[:like]-(m)
                WHERE id(n) == "LeBron James"
                RETURN p, n.name, m.name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['p', 'n.name', 'm.name']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["LeBron James"+"Ray Allen"+"like"+"0"], 1),
                    VERTICES["Ray Allen"]]),
                "LeBron James", "Ray Allen"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Dejounte Murray"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Dejounte Murray"]]),
                "LeBron James", "Dejounte Murray"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Carmelo Anthony"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Carmelo Anthony"]]),
                "LeBron James", "Carmelo Anthony"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Kyrie Irving"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Kyrie Irving"]]),
                "LeBron James", "Kyrie Irving"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Dwyane Wade"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Dwyane Wade"]]),
                "LeBron James", "Dwyane Wade"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Danny Green"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Danny Green"]]),
                "LeBron James", "Danny Green"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Chris Paul"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Chris Paul"]]),
                "LeBron James", "Chris Paul"],
            ]
        self.check_out_of_order_result(resp, result)

        stmt = '''
                MATCH p = (n)-[:like]->(m)-[:like]->(k)
                WHERE id(n) == "LeBron James"
                RETURN p, n.name, m.name, k.name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['p', 'n.name', 'm.name', 'k.name']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["LeBron James"+"Ray Allen"+"like"+"0"], 1),
                    VERTICES["Ray Allen"],
                    (EDGES["Ray Allen"+"Rajon Rondo"+"like"+"0"], 1),
                    VERTICES["Rajon Rondo"]]),
                "LeBron James", "Ray Allen", "Rajon Rondo"]
            ]
        self.check_out_of_order_result(resp, result)

        stmt = '''
                MATCH p=(n)-[:like]->()-[:like]->()
                WHERE id(n) == "LeBron James"
                RETURN *
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['n', 'p']
        self.check_column_names(resp, columns_name)
        result = [
            [VERTICES["LeBron James"], PathVal([VERTICES["LeBron James"],
                    (EDGES["LeBron James"+"Ray Allen"+"like"+"0"], 1),
                    VERTICES["Ray Allen"],
                    (EDGES["Ray Allen"+"Rajon Rondo"+"like"+"0"], 1),
                    VERTICES["Rajon Rondo"]])]
            ]
        self.check_out_of_order_result(resp, result)

    def test_hops_m_to_n(self,
                         like,
                         serve,
                         serve_2hop,
                         serve_3hop,
                         like_2hop,
                         like_3hop):
        VERTICES = self.VERTEXS

        def like_row_2hop(dst1: str, dst2: str):
            return [like_2hop('Tim Duncan', dst1, dst2), VERTICES[dst2]]

        def like_row_3hop(dst1: str, dst2: str, dst3: str):
            return [like_3hop('Tim Duncan', dst1, dst2, dst3), VERTICES[dst3]]

        def serve_row_2hop(team, player, r1=0, r2=0):
            return [[serve('Tim Duncan', team, r1), serve(player, team, r2)], VERTICES[player]]

        def serve_row_3hop(team1, player, team2, r1=0, r2=0, r3=0):
            return [
                [serve('Tim Duncan', team1, r1), serve(player, team1, r2), serve(player, team2, r3)],
                VERTICES[team2]
            ]

        # single both direction edge with properties
        stmt = '''
        MATCH (n)-[e:serve*2..3{start_year: 2000}]-(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (n)-[e:like*2..3{likeness: 90}]-(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [like_2hop("Tiago Splitter", "Manu Ginobili", "Tim Duncan"), VERTICES["Tiago Splitter"]],
            ],
        }
        self.check_rows_with_header(stmt, expected)

        # single direction edge with properties
        stmt = '''
        MATCH (n)-[e:serve*2..3{start_year: 2000}]->(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (n)-[e:like*2..3{likeness: 90}]->(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (n)<-[e:like*2..3{likeness: 90}]-(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [like_2hop("Tiago Splitter", "Manu Ginobili", "Tim Duncan"), VERTICES["Tiago Splitter"]],
            ],
        }
        self.check_rows_with_header(stmt, expected)

        # single both direction edge without properties
        stmt = '''
        MATCH (n)-[e:serve*2..3]-(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                serve_row_2hop("Spurs", "Dejounte Murray"),
                serve_row_2hop("Spurs", "Marco Belinelli"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Bulls"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hornets"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hawks"),
                serve_row_3hop("Spurs", "Marco Belinelli", "76ers"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Spurs", 0, 0, 1),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hornets", 0, 0, 1),
                serve_row_3hop("Spurs", "Marco Belinelli", "Raptors"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Warriors"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Kings"),
                serve_row_2hop("Spurs", "Danny Green"),
                serve_row_3hop("Spurs", "Danny Green", "Cavaliers"),
                serve_row_3hop("Spurs", "Danny Green", "Raptors"),
                serve_row_2hop("Spurs", "Aron Baynes"),
                serve_row_3hop("Spurs", "Aron Baynes", "Pistons"),
                serve_row_3hop("Spurs", "Aron Baynes", "Celtics"),
                serve_row_2hop("Spurs", "Jonathon Simmons"),
                serve_row_3hop("Spurs", "Jonathon Simmons", "76ers"),
                serve_row_3hop("Spurs", "Jonathon Simmons", "Magic"),
                serve_row_2hop("Spurs", "Rudy Gay"),
                serve_row_3hop("Spurs", "Rudy Gay", "Raptors"),
                serve_row_3hop("Spurs", "Rudy Gay", "Kings"),
                serve_row_3hop("Spurs", "Rudy Gay", "Grizzlies"),
                serve_row_2hop("Spurs", "Tony Parker"),
                serve_row_3hop("Spurs", "Tony Parker", "Hornets"),
                serve_row_2hop("Spurs", "Manu Ginobili"),
                serve_row_2hop("Spurs", "David West"),
                serve_row_3hop("Spurs", "David West", "Pacers"),
                serve_row_3hop("Spurs", "David West", "Warriors"),
                serve_row_3hop("Spurs", "David West", "Hornets"),
                serve_row_2hop("Spurs", "Tracy McGrady"),
                serve_row_3hop("Spurs", "Tracy McGrady", "Raptors"),
                serve_row_3hop("Spurs", "Tracy McGrady", "Magic"),
                serve_row_3hop("Spurs", "Tracy McGrady", "Rockets"),
                serve_row_2hop("Spurs", "Marco Belinelli", 0, 1),
                serve_row_3hop("Spurs", "Marco Belinelli", "Bulls", 0, 1, 0),
                serve_row_3hop("Spurs", "Marco Belinelli", "Spurs", 0, 1, 0),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hornets", 0, 1, 0),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hawks", 0, 1, 0),
                serve_row_3hop("Spurs", "Marco Belinelli", "76ers", 0, 1, 0),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hornets", 0, 1, 1),
                serve_row_3hop("Spurs", "Marco Belinelli", "Raptors", 0, 1, 0),
                serve_row_3hop("Spurs", "Marco Belinelli", "Warriors", 0, 1, 0),
                serve_row_3hop("Spurs", "Marco Belinelli", "Kings", 0, 1, 0),
                serve_row_2hop("Spurs", "Paul Gasol"),
                serve_row_3hop("Spurs", "Paul Gasol", "Lakers"),
                serve_row_3hop("Spurs", "Paul Gasol", "Bulls"),
                serve_row_3hop("Spurs", "Paul Gasol", "Grizzlies"),
                serve_row_3hop("Spurs", "Paul Gasol", "Bucks"),
                serve_row_2hop("Spurs", "LaMarcus Aldridge"),
                serve_row_3hop("Spurs", "LaMarcus Aldridge", "Trail Blazers"),
                serve_row_2hop("Spurs", "Tiago Splitter"),
                serve_row_3hop("Spurs", "Tiago Splitter", "Hawks"),
                serve_row_3hop("Spurs", "Tiago Splitter", "76ers"),
                serve_row_2hop("Spurs", "Cory Joseph"),
                serve_row_3hop("Spurs", "Cory Joseph", "Pacers"),
                serve_row_3hop("Spurs", "Cory Joseph", "Raptors"),
                serve_row_2hop("Spurs", "Kyle Anderson"),
                serve_row_3hop("Spurs", "Kyle Anderson", "Grizzlies"),
                serve_row_2hop("Spurs", "Boris Diaw"),
                serve_row_3hop("Spurs", "Boris Diaw", "Suns"),
                serve_row_3hop("Spurs", "Boris Diaw", "Jazz"),
                serve_row_3hop("Spurs", "Boris Diaw", "Hawks"),
                serve_row_3hop("Spurs", "Boris Diaw", "Hornets"),
            ],
        }
        self.check_rows_with_header(stmt, expected)

        # stmt = '''
        # MATCH (:player{name: "Tim Duncan"})-[e:like*2..3]-(v)
        # RETURN count(*)
        # '''
        # expected = {
        #     "column_names": ['count(*)'],
        #     "rows": [292],
        # }
        # self.check_rows_with_header(stmt, expected)

        # single direction edge without properties
        stmt = '''
        MATCH (n)-[e:serve*2..3]->(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (n)-[e:like*2..3]->(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                like_row_2hop("Tony Parker", "Tim Duncan"),
                like_row_3hop("Tony Parker", "Tim Duncan", "Manu Ginobili"),
                like_row_2hop("Tony Parker", "Manu Ginobili"),
                like_row_3hop("Tony Parker", "Manu Ginobili", "Tim Duncan"),
                like_row_2hop("Tony Parker", "LaMarcus Aldridge"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tony Parker"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tim Duncan"),
                like_row_2hop("Manu Ginobili", "Tim Duncan"),
                like_row_3hop("Manu Ginobili", "Tim Duncan", "Tony Parker"),
            ],
        }
        self.check_rows_with_header(stmt, expected)

        # multiple both direction edge with properties
        stmt = '''
        MATCH (n)-[e:serve|like*2..3{likeness: 90}]-(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [like_2hop("Tiago Splitter", "Manu Ginobili", "Tim Duncan"), VERTICES["Tiago Splitter"]]
            ],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (n)-[e:serve|like*2..3{start_year: 2000}]-(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        # multiple direction edge with properties
        stmt = '''
        MATCH (n)-[e:serve|like*2..3{likeness: 90}]->(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (n)<-[e:serve|like*2..3{likeness: 90}]-(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [like_2hop("Tiago Splitter", "Manu Ginobili", "Tim Duncan"), VERTICES['Tiago Splitter']]
            ],
        }
        self.check_rows_with_header(stmt, expected)

        # multiple both direction edge without properties
        # stmt = '''
        # MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3]-(v)
        # RETURN count(*)
        # '''
        # expected = {
        #     "column_names": ['COUNT(*)'],
        #     "rows": [927],
        # }
        # self.check_rows_with_header(stmt, expected)

        # multiple direction edge without properties
        stmt = '''
        MATCH (n)-[e:serve|like*2..3]->(v)
        WHERE id(n) == "Tim Duncan"
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                like_row_2hop("Tony Parker", "Tim Duncan"),
                like_row_3hop("Tony Parker", "Tim Duncan", "Manu Ginobili"),
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        like("Tony Parker", "Tim Duncan"),
                        serve("Tim Duncan", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                like_row_2hop("Tony Parker", "Manu Ginobili"),
                like_row_3hop("Tony Parker", "Manu Ginobili", "Tim Duncan"),
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        like("Tony Parker", "Manu Ginobili"),
                        serve("Manu Ginobili", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                like_row_2hop("Tony Parker", "LaMarcus Aldridge"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tony Parker"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tim Duncan"),
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        like("Tony Parker", "LaMarcus Aldridge"),
                        serve("LaMarcus Aldridge", "Trail Blazers"),
                    ],
                    VERTICES['Trail Blazers'],
                ],
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        like("Tony Parker", "LaMarcus Aldridge"),
                        serve("LaMarcus Aldridge", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        serve("Tony Parker", "Hornets"),
                    ],
                    VERTICES['Hornets'],
                ],
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        serve("Tony Parker", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                like_row_2hop("Manu Ginobili", "Tim Duncan"),
                like_row_3hop("Manu Ginobili", "Tim Duncan", "Tony Parker"),
                [
                    [
                        like("Tim Duncan", "Manu Ginobili"),
                        like("Manu Ginobili", "Tim Duncan"),
                        serve("Tim Duncan", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                [
                    [
                        like("Tim Duncan", "Manu Ginobili"),
                        serve("Manu Ginobili", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
            ],
        }
        self.check_rows_with_header(stmt, expected)


