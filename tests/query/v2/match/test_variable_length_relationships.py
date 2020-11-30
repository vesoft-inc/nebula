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
        MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3{start_year: 2000}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:like*2..3{likeness: 90}]-(v)
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
        MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3{start_year: 2000}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:like*2..3{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})<-[e:like*2..3{likeness: 90}]-(v)
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
        MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3]-(v)
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
        MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name: "Tim Duncan"})-[e:like*2..3]->(v)
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
        MATCH (:player{name: "Tim Duncan"})-[e:serve|like*2..3{likeness: 90}]-(v)
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
        MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3{start_year: 2000}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        # multiple direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})<-[e:serve|like*2..3{likeness: 90}]-(v)
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
        MATCH (:player{name: "Tim Duncan"})-[e:serve|like*2..3]->(v)
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

    def test_return_all(self, like_2hop_start_with, like_3hop_start_with):
        like_row_2hop = like_2hop_start_with('Tim Duncan')
        like_row_3hop = like_3hop_start_with('Tim Duncan')
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:like*2..3]->()
        RETURN *
        '''
        expected = {
            "column_names": ['e'],
            "rows": [
                [like_row_2hop("Tony Parker", "Tim Duncan")],
                [like_row_3hop("Tony Parker", "Tim Duncan", "Manu Ginobili")],
                [like_row_2hop("Tony Parker", "Manu Ginobili")],
                [like_row_3hop("Tony Parker", "Manu Ginobili", "Tim Duncan")],
                [like_row_2hop("Tony Parker", "LaMarcus Aldridge")],
                [like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tony Parker")],
                [like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tim Duncan")],
                [like_row_2hop("Manu Ginobili", "Tim Duncan")],
                [like_row_3hop("Manu Ginobili", "Tim Duncan", "Tony Parker")],
            ],
        }
        self.check_rows_with_header(stmt, expected)

    def test_more_cases(self, like, serve, like_2hop):
        # stmt = '''
        # MATCH (v:player{name: 'Tim Duncan'})-[e:like*0]-()
        # RETURN e
        # '''

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*1]-()
        RETURN e
        '''
        expected = {
            "column_names": ['e'],
            "rows": [
                [[like('Tim Duncan', 'Manu Ginobili')]],
                [[like('Tim Duncan', 'Tony Parker')]],
                [[like('Dejounte Murray', 'Tim Duncan')]],
                [[like('Shaquile O\'Neal', 'Tim Duncan')]],
                [[like('Marco Belinelli', 'Tim Duncan')]],
                [[like('Boris Diaw', 'Tim Duncan')]],
                [[like('Manu Ginobili', 'Tim Duncan')]],
                [[like('Danny Green', 'Tim Duncan')]],
                [[like('Tiago Splitter', 'Tim Duncan')]],
                [[like('Aron Baynes', 'Tim Duncan')]],
                [[like('Tony Parker', 'Tim Duncan')]],
                [[like('LaMarcus Aldridge', 'Tim Duncan')]],
            ],
        }
        self.check_rows_with_header(stmt, expected)

        # stmt = '''
        # MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..0]-()
        # RETURN e
        # '''

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*1..1]-()
        RETURN e
        '''
        expected = {
            "column_names": ['e'],
            "rows": [
                [[like('Tim Duncan', 'Manu Ginobili')]],
                [[like('Tim Duncan', 'Tony Parker')]],
                [[like('Dejounte Murray', 'Tim Duncan')]],
                [[like('Shaquile O\'Neal', 'Tim Duncan')]],
                [[like('Marco Belinelli', 'Tim Duncan')]],
                [[like('Boris Diaw', 'Tim Duncan')]],
                [[like('Manu Ginobili', 'Tim Duncan')]],
                [[like('Danny Green', 'Tim Duncan')]],
                [[like('Tiago Splitter', 'Tim Duncan')]],
                [[like('Aron Baynes', 'Tim Duncan')]],
                [[like('Tony Parker', 'Tim Duncan')]],
                [[like('LaMarcus Aldridge', 'Tim Duncan')]],
            ],
        }
        self.check_rows_with_header(stmt, expected)

        # stmt = '''
        # MATCH (v:player{name: 'Tim Duncan'})-[e:like*]-()
        # RETURN e
        # '''

        # stmt = '''
        # MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..0]-()-[e2:like*0..0]-()
        # RETURN e, e2
        # '''

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*2..3]-()
        WHERE e[1].likeness>95 AND e[2].likeness==100
        RETURN e
        '''
        expected = {
            "column_names": ['e'],
            "rows": [[[
                like('Dejounte Murray', 'Tim Duncan'),
                like('Dejounte Murray', 'LeBron James'),
                like('LeBron James', 'Ray Allen'),
            ]]],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e1:like*1..2]-(v2{name: 'Tony Parker'})-[e2:serve]-(v3{name: 'Spurs'})
        RETURN e1, e2
        '''
        expected = {
            "column_names": ['e1', 'e2'],
            "rows": [
                [[like('Dejounte Murray', 'Tim Duncan'), like('Dejounte Murray', 'Tony Parker')], serve('Tony Parker', 'Spurs')],
                [[like('Tony Parker', 'Manu Ginobili'), like('Tim Duncan', 'Manu Ginobili')], serve('Tony Parker', 'Spurs')],
                [[like('Marco Belinelli', 'Tim Duncan'), like('Marco Belinelli', 'Tony Parker')], serve('Tony Parker', 'Spurs')],
                [[like('Boris Diaw', 'Tim Duncan'), like('Boris Diaw', 'Tony Parker')], serve('Tony Parker', 'Spurs')],
                [like_2hop('Tony Parker', 'Manu Ginobili', 'Tim Duncan'), serve('Tony Parker', 'Spurs')],
                [[like('LaMarcus Aldridge', 'Tim Duncan'), like('LaMarcus Aldridge', 'Tony Parker')], serve('Tony Parker', 'Spurs')],
                [[like('LaMarcus Aldridge', 'Tim Duncan'), like('Tony Parker', 'LaMarcus Aldridge')], serve('Tony Parker', 'Spurs')],
                [[like('Tim Duncan', 'Tony Parker')], serve('Tony Parker', 'Spurs')],
                [[like('Tony Parker', 'Tim Duncan')], serve('Tony Parker', 'Spurs')],
            ],
        }
        self.check_rows_with_header(stmt, expected)

        stmt='''
        MATCH p=(v:player{name: 'Tim Duncan'})-[:like|:serve*1..3]->(v1)
        WHERE e[0].likeness>90
        RETURN p
        '''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)
        self.check_error_msg(resp, "SemanticError: Alias used but not defined: `e'")

        stmt='''
        MATCH p=(v:player{name: 'Tim Duncan'})-[:like|:serve*1..3]->(v1)
        RETURN e
        '''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)
        self.check_error_msg(resp, "SemanticError: Alias used but not defined: `e'")

        stmt='''
        MATCH p=(v:player{name: 'Tim Duncan'})-[:like|:serve*1..3]->(v1)
        WHERE e[0].likeness+e[1].likeness>90
        RETURN p
        '''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)
        self.check_error_msg(resp, "SemanticError: Alias used but not defined: `e'")
