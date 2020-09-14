# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_NULL, T_EMPTY
import pytest

class TestSubGraph(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()
        self.load_vertex_edge()

    def cleanup():
        pass

    def test_invalid_input(self):
        stmt = 'GET SUBGRAPH FROM $-.id'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = 'GET SUBGRAPH FROM $a.id'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = 'GO FROM "Tim Duncan" OVER like YIELD $$.player.age AS id | GET SUBGRAPH FROM $-.id'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = '$a = GO FROM "Tim Duncan" OVER like YIELD $$.player.age AS ID; GET SUBGRAPH FROM $a.ID'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = '$a = GO FROM "Tim Duncan" OVER like YIELD like._src AS src; GET SUBGRAPH FROM $b.src'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # stmt = 'GO FROM "Tim Duncan" OVER like YIELD $$._dst AS id, $^._src AS id | GET SUBGRAPH FROM $-.id'
        # resp = self.execute_query(stmt)
        # self.check_resp_failed(resp)

        # stmt = '$a = GO FROM "Tim Duncan" OVER like YIELD $$._dst AS id, $^._src AS id; GET SUBGRAPH FROM $a.id'
        # resp = self.execute_query(stmt)
        # self.check_resp_failed(resp)

    def test_zero_step(self):
        VERTEXS = self.VERTEXS
        EDGES = self.EDGES

        stmt = 'GET SUBGRAPH 0 STEPS FROM "Tim Duncan"'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        vertices = [
                    VERTEXS['Tim Duncan']
                   ]

        expected_data = {
            "column_names" : ['_vertices'],
            "rows" : [
                [vertices]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GET SUBGRAPH 0 STEPS FROM "Tim Duncan", "Spurs"'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        vertices = [
                    VERTEXS['Spurs'],
                    VERTEXS['Tim Duncan']
                   ]
        expected_data = {
            "column_names" : ['_vertices'],
            "rows" : [
                [vertices]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = 'GET SUBGRAPH 0 STEPS FROM "Tim Duncan", "Tony Parker", "Spurs"'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        vertices = [
                    VERTEXS['Spurs'],
                    VERTEXS['Tim Duncan'],
                    VERTEXS['Tony Parker'],
                   ]
        expected_data = {
            "column_names" : ['_vertices'],
            "rows" : [
                [vertices]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' over serve YIELD serve._dst AS id | GET SUBGRAPH 0 STEPS FROM $-.id"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        vertices = [
                    VERTEXS['Spurs']
                   ]

        expected_data = {
            "column_names" : ['_vertices'],
            "rows" : [
                [vertices]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' over like YIELD like._dst AS id | GET SUBGRAPH 0 STEPS FROM $-.id"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        vertices = [
                    VERTEXS['Tony Parker'],
                    VERTEXS['Manu Ginobili'],
                   ]

        expected_data = {
            "column_names" : ['_vertices'],
            "rows" : [
                [vertices]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$a = GO FROM 'Tim Duncan' over serve YIELD serve._dst AS id;
                  GET SUBGRAPH 0 STEPS FROM $a.id'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        vertices = [
                    VERTEXS['Spurs']
                   ]

        expected_data = {
            "column_names" : ['_vertices'],
            "rows" : [
                [vertices]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$a = GO FROM 'Tim Duncan' over like YIELD like._dst AS id;
                  GET SUBGRAPH 0 STEPS FROM $a.id'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        vertices = [
                    VERTEXS['Tony Parker'],
                    VERTEXS['Manu Ginobili'],
                   ]

        expected_data = {
            "column_names" : ['_vertices'],
            "rows" : [
                [vertices]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_subgraph(self):
        VERTEXS = self.VERTEXS
        EDGES = self.EDGES

        stmt = "GET SUBGRAPH FROM 'Tim Duncan'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS['Tim Duncan']]

        edge1 = [
                    EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Aron Baynes'+'Tim Duncan'+'like'+'0'],
                    EDGES['Boris Diaw'+'Tim Duncan'+'like'+'0'],
                    EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tim Duncan'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                    EDGES['Shaquile O\'Neal'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                    EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                    EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0']
                ]

        vertex2 = [
                    VERTEXS['Danny Green'],
                    VERTEXS['Manu Ginobili'],
                    VERTEXS['Aron Baynes'],
                    VERTEXS['Boris Diaw'],
                    VERTEXS['Shaquile O\'Neal'],
                    VERTEXS['Tony Parker'],
                    VERTEXS['Spurs'],
                    VERTEXS['Dejounte Murray'],
                    VERTEXS['LaMarcus Aldridge'],
                    VERTEXS['Marco Belinelli'],
                    VERTEXS['Tiago Splitter']
                  ]

        edge2 = [
                    EDGES['Dejounte Murray'+'Danny Green'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],
                    EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Danny Green'+'Spurs'+'serve'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Dejounte Murray'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                    EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Aron Baynes'+'Spurs'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Tony Parker'+'like'+'0'],
                    EDGES['Boris Diaw'+'Spurs'+'serve'+'0'],
                    EDGES['Dejounte Murray'+'Tony Parker'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Dejounte Murray'+'Spurs'+'serve'+'0'],
                    EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                    EDGES['Tiago Splitter'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'1'],
                    EDGES['Dejounte Murray'+'Marco Belinelli'+'like'+'0']
                ]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS["Tim Duncan"]]

        edge1 = [
                    EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Aron Baynes'+'Tim Duncan'+'like'+'0'],
                    EDGES['Boris Diaw'+'Tim Duncan'+'like'+'0'],
                    EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tim Duncan'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                    EDGES['Shaquile O\'Neal'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                    EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                    EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0']
                ]


        vertex2 = [
                    VERTEXS['Danny Green'],
                    VERTEXS['Manu Ginobili'],
                    VERTEXS['Aron Baynes'],
                    VERTEXS['Boris Diaw'],
                    VERTEXS['Shaquile O\'Neal'],
                    VERTEXS['Tony Parker'],
                    VERTEXS['Spurs'],
                    VERTEXS['Dejounte Murray'],
                    VERTEXS['LaMarcus Aldridge'],
                    VERTEXS['Marco Belinelli'],
                    VERTEXS['Tiago Splitter']
                  ]

        edge2 = [
                    EDGES['Dejounte Murray'+'Danny Green'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],
                    EDGES['Danny Green'+'LeBron James'+'like'+'0'],
                    EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Danny Green'+'Cavaliers'+'serve'+'0'],
                    EDGES['Danny Green'+'Raptors'+'serve'+'0'],
                    EDGES['Danny Green'+'Spurs'+'serve'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Dejounte Murray'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                    EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Aron Baynes'+'Celtics'+'serve'+'0'],
                    EDGES['Aron Baynes'+'Pistons'+'serve'+'0'],
                    EDGES['Aron Baynes'+'Spurs'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Tony Parker'+'like'+'0'],
                    EDGES['Boris Diaw'+'Hawks'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Hornets'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Jazz'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Spurs'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Suns'+'serve'+'0'],

                    EDGES['Yao Ming'+'Shaquile O\'Neal'+'like'+'0'],
                    EDGES['Shaquile O\'Neal'+'JaVale McGee'+'like'+'0'],
                    EDGES['Shaquile O\'Neal'+'Cavaliers'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Celtics'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Heat'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Lakers'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Magic'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Suns'+'serve'+'0'],

                    EDGES['Dejounte Murray'+'Tony Parker'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Tony Parker'+'Hornets'+'serve'+'0'],
                    EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                    EDGES['Tony Parker'+'Kyle Anderson'+'teammate'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],

                    EDGES['Cory Joseph'+'Spurs'+'serve'+'0'],
                    EDGES['David West'+'Spurs'+'serve'+'0'],
                    EDGES['Dejounte Murray'+'Spurs'+'serve'+'0'],
                    EDGES['Jonathon Simmons'+'Spurs'+'serve'+'0'],
                    EDGES['Kyle Anderson'+'Spurs'+'serve'+'0'],
                    EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                    EDGES['Paul Gasol'+'Spurs'+'serve'+'0'],
                    EDGES['Rudy Gay'+'Spurs'+'serve'+'0'],
                    EDGES['Tiago Splitter'+'Spurs'+'serve'+'0'],
                    EDGES['Tracy McGrady'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'1'],

                    EDGES['Dejounte Murray'+'Chris Paul'+'like'+'0'],
                    EDGES['Dejounte Murray'+'James Harden'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Kevin Durant'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Kyle Anderson'+'like'+'0'],
                    EDGES['Dejounte Murray'+'LeBron James'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Russell Westbrook'+'like'+'0'],

                    EDGES['Damian Lillard'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Rudy Gay'+'LaMarcus Aldridge'+'like'+'0'],

                    EDGES['LaMarcus Aldridge'+'Trail Blazers'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'76ers'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Bulls'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Hawks'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Hornets'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Kings'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Raptors'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Warriors'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Hornets'+'serve'+'1'],

                    EDGES['Tiago Splitter'+'76ers'+'serve'+'0'],
                    EDGES['Tiago Splitter'+'Hawks'+'serve'+'0']
                ]

        vertex3 = [
                    VERTEXS['Cavaliers'],
                    VERTEXS['Pistons'],
                    VERTEXS['Damian Lillard'],
                    VERTEXS['Kings'],
                    VERTEXS['Raptors'],
                    VERTEXS['Jazz'],
                    VERTEXS['LeBron James'],
                    VERTEXS['Paul Gasol'],
                    VERTEXS['Kyle Anderson'],
                    VERTEXS['Rudy Gay'],
                    VERTEXS['Kevin Durant'],
                    VERTEXS['Yao Ming'],
                    VERTEXS['James Harden'],
                    VERTEXS['Hornets'],
                    VERTEXS['David West'],
                    VERTEXS['Chris Paul'],
                    VERTEXS['Celtics'],
                    VERTEXS['Jonathon Simmons'],
                    VERTEXS['Hawks'],
                    VERTEXS['Heat'],
                    VERTEXS['Lakers'],
                    VERTEXS['Suns'],
                    VERTEXS['Magic'],
                    VERTEXS['Trail Blazers'],
                    VERTEXS['76ers'],
                    VERTEXS['JaVale McGee'],
                    VERTEXS['Cory Joseph'],
                    VERTEXS['Tracy McGrady'],
                    VERTEXS['Russell Westbrook'],
                    VERTEXS['Bulls'],
                    VERTEXS['Warriors']
                  ]

        edge3 = [
                    EDGES['LeBron James'+'Cavaliers'+'serve'+'0'],
                    EDGES['LeBron James'+'Cavaliers'+'serve'+'1'],
                    EDGES['Damian Lillard'+'Trail Blazers'+'serve'+'0'],

                    EDGES['Rudy Gay'+'Kings'+'serve'+'0'],
                    EDGES['Cory Joseph'+'Raptors'+'serve'+'0'],
                    EDGES['Rudy Gay'+'Raptors'+'serve'+'0'],
                    EDGES['Tracy McGrady'+'Raptors'+'serve'+'0'],

                    EDGES['Chris Paul'+'LeBron James'+'like'+'0'],
                    EDGES['LeBron James'+'Heat'+'serve'+'0'],
                    EDGES['LeBron James'+'Lakers'+'serve'+'0'],

                    EDGES['Paul Gasol'+'Bulls'+'serve'+'0'],
                    EDGES['Paul Gasol'+'Lakers'+'serve'+'0'],

                    EDGES['Tracy McGrady'+'Rudy Gay'+'like'+'0'],
                    EDGES['Kevin Durant'+'Warriors'+'serve'+'0'],
                    EDGES['Yao Ming'+'Tracy McGrady'+'like'+'0'],
                    EDGES['Russell Westbrook'+'James Harden'+'like'+'0'],
                    EDGES['James Harden'+'Russell Westbrook'+'like'+'0'],

                    EDGES['Chris Paul'+'Hornets'+'serve'+'0'],
                    EDGES['David West'+'Hornets'+'serve'+'0'],
                    EDGES['David West'+'Warriors'+'serve'+'0'],

                    EDGES['Jonathon Simmons'+'76ers'+'serve'+'0'],
                    EDGES['Jonathon Simmons'+'Magic'+'serve'+'0'],
                    EDGES['JaVale McGee'+'Lakers'+'serve'+'0'],
                    EDGES['Tracy McGrady'+'Magic'+'serve'+'0'],

                    EDGES['JaVale McGee'+'Warriors'+'serve'+'0']
                ]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan' IN like, serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        vertex1 = [VERTEXS["Tim Duncan"]]

        edge1 = [
                    EDGES['Aron Baynes'+'Tim Duncan'+'like'+'0'],
                    EDGES['Boris Diaw'+'Tim Duncan'+'like'+'0'],
                    EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tim Duncan'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                    EDGES['Shaquile O\'Neal'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                ]

        vertex2 = [
                    VERTEXS['Manu Ginobili'],
                    VERTEXS['Shaquile O\'Neal'],
                    VERTEXS['LaMarcus Aldridge'],
                    VERTEXS['Marco Belinelli'],
                    VERTEXS['Danny Green'],
                    VERTEXS['Tony Parker']
                  ]

        edge2 = [
                    EDGES['Dejounte Murray'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Yao Ming'+'Shaquile O\'Neal'+'like'+'0'],
                    EDGES['Damian Lillard'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Rudy Gay'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Danny Green'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],

                    EDGES['Boris Diaw'+'Tony Parker'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tony Parker'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'like'+'0']
                ]

        vertex3 = [
                    VERTEXS['Damian Lillard'],
                    VERTEXS['Rudy Gay'],
                    VERTEXS['Dejounte Murray'],
                    VERTEXS['Yao Ming'],
                    VERTEXS['Tiago Splitter'],
                    VERTEXS['Boris Diaw']
                  ]

        edge3 = [
                    EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0']
                ]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan' IN like OUT serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS["Tim Duncan"]]

        edge1 = [
                    EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                    EDGES['Aron Baynes'+'Tim Duncan'+'like'+'0'],
                    EDGES['Boris Diaw'+'Tim Duncan'+'like'+'0'],
                    EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tim Duncan'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                    EDGES['Shaquile O\'Neal'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'like'+'0']
                ]

        vertex2 = [
                    VERTEXS['Manu Ginobili'],
                    VERTEXS['Danny Green'],
                    VERTEXS['Tony Parker'],
                    VERTEXS['Aron Baynes'],
                    VERTEXS['Boris Diaw'],
                    VERTEXS['Shaquile O\'Neal'],
                    VERTEXS['Dejounte Murray'],
                    VERTEXS['LaMarcus Aldridge'],
                    VERTEXS['Marco Belinelli'],
                    VERTEXS['Tiago Splitter']
                  ]

        edge2 = [
                    EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                    EDGES['Dejounte Murray'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],

                    EDGES['Danny Green'+'Cavaliers'+'serve'+'0'],
                    EDGES['Danny Green'+'Raptors'+'serve'+'0'],
                    EDGES['Danny Green'+'Spurs'+'serve'+'0'],

                    EDGES['Dejounte Murray'+'Danny Green'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],

                    EDGES['Tony Parker'+'Hornets'+'serve'+'0'],
                    EDGES['Tony Parker'+'Spurs'+'serve'+'0'],

                    EDGES['Boris Diaw'+'Tony Parker'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tony Parker'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],


                    EDGES['Aron Baynes'+'Celtics'+'serve'+'0'],
                    EDGES['Aron Baynes'+'Pistons'+'serve'+'0'],
                    EDGES['Aron Baynes'+'Spurs'+'serve'+'0'],

                    EDGES['Boris Diaw'+'Hawks'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Hornets'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Jazz'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Spurs'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Suns'+'serve'+'0'],

                    EDGES['Shaquile O\'Neal'+'Cavaliers'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Celtics'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Heat'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Lakers'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Magic'+'serve'+'0'],
                    EDGES['Shaquile O\'Neal'+'Suns'+'serve'+'0'],

                    EDGES['Yao Ming'+'Shaquile O\'Neal'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Spurs'+'serve'+'0'],
                    EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0'],
                    EDGES['LaMarcus Aldridge'+'Trail Blazers'+'serve'+'0'],

                    EDGES['Damian Lillard'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Rudy Gay'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],

                    EDGES['Marco Belinelli'+'76ers'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Bulls'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Hawks'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Hornets'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Kings'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Raptors'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Warriors'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Hornets'+'serve'+'1'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'1'],

                    EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Marco Belinelli'+'like'+'0'],

                    EDGES['Tiago Splitter'+'76ers'+'serve'+'0'],
                    EDGES['Tiago Splitter'+'Hawks'+'serve'+'0'],
                    EDGES['Tiago Splitter'+'Spurs'+'serve'+'0']
                ]

        vertex3 = [
                    VERTEXS['Raptors'],
                    VERTEXS['Jazz'],
                    VERTEXS['Cavaliers'],
                    VERTEXS['Pistons'],
                    VERTEXS['Damian Lillard'],
                    VERTEXS['Kings'],
                    VERTEXS['Hornets'],
                    VERTEXS['Spurs'],
                    VERTEXS['Rudy Gay'],
                    VERTEXS['Yao Ming'],
                    VERTEXS['Hawks'],
                    VERTEXS['Heat'],
                    VERTEXS['Lakers'],
                    VERTEXS['Celtics'],
                    VERTEXS['Suns'],
                    VERTEXS['Magic'],
                    VERTEXS['Trail Blazers'],
                    VERTEXS['76ers'],
                    VERTEXS['Bulls'],
                    VERTEXS['Warriors']
                  ]

        edge3 = [
                    EDGES['Rudy Gay'+'Raptors'+'serve'+'0'],
                    EDGES['Damian Lillard'+'Trail Blazers'+'serve'+'0'],
                    EDGES['Rudy Gay'+'Kings'+'serve'+'0'],
                    EDGES['Rudy Gay'+'Spurs'+'serve'+'0'],

                    EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0']
                ]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan', 'James Harden' IN teammate OUT serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [
                    VERTEXS["Tim Duncan"],
                    VERTEXS["James Harden"]
                  ]

        edge1 = [
                    EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['James Harden'+'Rockets'+'serve'+'0'],
                    EDGES['James Harden'+'Thunders'+'serve'+'0'],
                ]
        vertex2 = [
                    VERTEXS['Manu Ginobili'],
                    VERTEXS['Tony Parker'],
                  ]

        edge2 = [
                    EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Hornets'+'serve'+'0'],
                    EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                    EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0']
                ]

        vertex3 = [
                    VERTEXS['Hornets'],
                    VERTEXS['Spurs']
                  ]

        edge3 = [
                    EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0']
                ]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 3 STEPS FROM 'Paul George' OUT serve BOTH like"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS["Paul George"]]

        edge1 = [
                    EDGES['Russell Westbrook'+'Paul George'+'like'+'0'],
                    EDGES['Paul George'+'Pacers'+'serve'+'0'],
                    EDGES['Paul George'+'Thunders'+'serve'+'0'],
                    EDGES['Paul George'+'Russell Westbrook'+'like'+'0']
                ]
        vertex2 = [
                    VERTEXS["Russell Westbrook"],
                  ]
        edge2 = [
                    EDGES['Dejounte Murray'+'Russell Westbrook'+'like'+'0'],
                    EDGES['James Harden'+'Russell Westbrook'+'like'+'0'],
                    EDGES['Russell Westbrook'+'Thunders'+'serve'+'0'],
                    EDGES['Russell Westbrook'+'James Harden'+'like'+'0']
                ]
        vertex3 = [
                    VERTEXS["Dejounte Murray"],
                    VERTEXS["James Harden"]
                  ]
        edge3 = [
                    EDGES['Dejounte Murray'+'Spurs'+'serve'+'0'],
                    EDGES['Dejounte Murray'+'Chris Paul'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Danny Green'+'like'+'0'],
                    EDGES['Dejounte Murray'+'James Harden'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Kevin Durant'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Kyle Anderson'+'like'+'0'],
                    EDGES['Dejounte Murray'+'LeBron James'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tim Duncan'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tony Parker'+'like'+'0'],

                    EDGES['Luka Doncic'+'James Harden'+'like'+'0'],
                    EDGES['James Harden'+'Rockets'+'serve'+'0'],
                    EDGES['James Harden'+'Thunders'+'serve'+'0']
                ]
        vertex4 = [
                    VERTEXS["LeBron James"],
                    VERTEXS["Marco Belinelli"],
                    VERTEXS["Danny Green"],
                    VERTEXS["Rockets"],
                    VERTEXS["Spurs"],
                    VERTEXS["Kevin Durant"],
                    VERTEXS["Kyle Anderson"],
                    VERTEXS["Tim Duncan"],
                    VERTEXS["Tony Parker"],
                    VERTEXS["Chris Paul"],
                    VERTEXS["Luka Doncic"],
                    VERTEXS["Manu Ginobili"],
                    VERTEXS["Pacers"],
                    VERTEXS["Thunders"]
                  ]
        edge4 = [
                    EDGES['Chris Paul'+'LeBron James'+'like'+'0'],
                    EDGES['Danny Green'+'LeBron James'+'like'+'0'],
                    EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'1'],

                    EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                    EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                    EDGES['Danny Green'+'Spurs'+'serve'+'0'],
                    EDGES['Chris Paul'+'Rockets'+'serve'+'0'],
                    EDGES['Kyle Anderson'+'Spurs'+'serve'+'0'],
                    EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                    EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                    EDGES['Tony Parker'+'Spurs'+'serve'+'0'],

                    EDGES['Kevin Durant'+'Thunders'+'serve'+'0'],
                    EDGES['Tony Parker'+'Kyle Anderson'+'teammate'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],

                    EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0']
                ]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3],
                [vertex4, edge4],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH FROM 'Tony Parker' BOTH like"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS["Tony Parker"]]

        edge1 = [
                    EDGES['Boris Diaw'+'Tony Parker'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tony Parker'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'like'+'0']
                ]
        vertex2 = [
                    VERTEXS["Dejounte Murray"],
                    VERTEXS["LaMarcus Aldridge"],
                    VERTEXS["Marco Belinelli"],
                    VERTEXS["Boris Diaw"],
                    VERTEXS["Tim Duncan"],
                    VERTEXS["Manu Ginobili"]
                  ]
        edge2 = [
                    EDGES['Dejounte Murray'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tim Duncan'+'like'+'0'],

                    EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],

                    EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                    EDGES['Boris Diaw'+'Tim Duncan'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0'],

                    EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0']
                ]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tim Duncan' over serve YIELD serve._src AS id | GET SUBGRAPH FROM $-.id"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS['Tim Duncan']]

        edge1 = [
                    EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Aron Baynes'+'Tim Duncan'+'like'+'0'],
                    EDGES['Boris Diaw'+'Tim Duncan'+'like'+'0'],
                    EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tim Duncan'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                    EDGES['Shaquile O\'Neal'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                    EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                    EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0']
                ]

        vertex2 = [
                    VERTEXS['Danny Green'],
                    VERTEXS['Manu Ginobili'],
                    VERTEXS['Aron Baynes'],
                    VERTEXS['Boris Diaw'],
                    VERTEXS['Shaquile O\'Neal'],
                    VERTEXS['Tony Parker'],
                    VERTEXS['Spurs'],
                    VERTEXS['Dejounte Murray'],
                    VERTEXS['LaMarcus Aldridge'],
                    VERTEXS['Marco Belinelli'],
                    VERTEXS['Tiago Splitter']
                  ]

        edge2 = [
                    EDGES['Dejounte Murray'+'Danny Green'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],
                    EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Danny Green'+'Spurs'+'serve'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Dejounte Murray'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                    EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Aron Baynes'+'Spurs'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Tony Parker'+'like'+'0'],
                    EDGES['Boris Diaw'+'Spurs'+'serve'+'0'],
                    EDGES['Dejounte Murray'+'Tony Parker'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Dejounte Murray'+'Spurs'+'serve'+'0'],
                    EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                    EDGES['Tiago Splitter'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'1'],
                    EDGES['Dejounte Murray'+'Marco Belinelli'+'like'+'0']
                ]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2]
            ]
        }

        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = '''$a = GO FROM 'Tim Duncan' over serve YIELD serve._src AS id;
                  GET SUBGRAPH FROM $a.id'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS['Tim Duncan']]

        edge1 = [
                    EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                    EDGES['Aron Baynes'+'Tim Duncan'+'like'+'0'],
                    EDGES['Boris Diaw'+'Tim Duncan'+'like'+'0'],
                    EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                    EDGES['Dejounte Murray'+'Tim Duncan'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                    EDGES['Shaquile O\'Neal'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                    EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                    EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0']
                ]

        vertex2 = [
                    VERTEXS['Danny Green'],
                    VERTEXS['Manu Ginobili'],
                    VERTEXS['Aron Baynes'],
                    VERTEXS['Boris Diaw'],
                    VERTEXS['Shaquile O\'Neal'],
                    VERTEXS['Tony Parker'],
                    VERTEXS['Spurs'],
                    VERTEXS['Dejounte Murray'],
                    VERTEXS['LaMarcus Aldridge'],
                    VERTEXS['Marco Belinelli'],
                    VERTEXS['Tiago Splitter']
                  ]

        edge2 = [
                    EDGES['Dejounte Murray'+'Danny Green'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],
                    EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                    EDGES['Danny Green'+'Spurs'+'serve'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                    EDGES['Dejounte Murray'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tiago Splitter'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                    EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                    EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],
                    EDGES['Aron Baynes'+'Spurs'+'serve'+'0'],
                    EDGES['Boris Diaw'+'Tony Parker'+'like'+'0'],
                    EDGES['Boris Diaw'+'Spurs'+'serve'+'0'],
                    EDGES['Dejounte Murray'+'Tony Parker'+'like'+'0'],
                    EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                    EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                    EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                    EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                    EDGES['Dejounte Murray'+'Spurs'+'serve'+'0'],
                    EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                    EDGES['Tiago Splitter'+'Spurs'+'serve'+'0'],
                    EDGES['Marco Belinelli'+'Spurs'+'serve'+'1'],
                    EDGES['Dejounte Murray'+'Marco Belinelli'+'like'+'0']
                ]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2]
            ]
        }

        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])