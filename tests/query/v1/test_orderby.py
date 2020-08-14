# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestOrderBy(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_syntax_error(self):
        resp = self.execute_query('ORDER BY')
        self.check_resp_failed(resp)

        resp = self.execute('GO FROM %ld OVER serve YIELD ' 
                            '$^.player.name as name, serve.start_year as start, $$.team.name' 
                            '| ORDER BY $-.$$.team.name')
        self.check_resp_failed(resp)

    def test_empty_input(self):
        # 1.0 will return empty, but 2.0 will return SemanticError, it makes sense
        resp = self.execute_query('ORDER BY $-.xx')
        self.check_resp_failed(resp)

        resp = self.execute_query('GO FROM "NON EXIST VERTEX ID" OVER serve YIELD '
                                  '$^.player.name as name, serve.start_year as start, $$.team.name as team'
                                  '| ORDER BY $-.name')
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, ['name', 'start', 'team'])
        self.check_result(resp, [])

        resp = self.execute_query('GO FROM "Marco Belinelli" OVER serve '
                                  'YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team '
                                  '| YIELD $-.name as name WHERE $-.start > 20000'
                                  '| ORDER BY $-.name')
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, ['name'])
        self.check_result(resp, [])

    def test_wrong_factor(self):
        resp = self.execute_query('GO FROM %ld OVER serve '
                                  'YIELD $^.player.name as name, '
                                  'serve.start_year as start, '
                                  '$$.team.name as team'
                                  '| ORDER BY $-.abc')
        self.check_resp_failed(resp)

    def test_single_factor(self):
        resp = self.execute_query('GO FROM "Boris Diaw" OVER serve '
                                  'YIELD  $^.player.name as name, '
                                  'serve.start_year as start, '
                                  '$$.team.name as team '
                                  '| ORDER BY $-.team')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ['name', 'start', 'team'])
        expected_result = [["Boris Diaw", 2003, "Hawks"],
                           ["Boris Diaw", 2008, "Hornets"],
                           ["Boris Diaw", 2016, "Jazz"],
                           ["Boris Diaw", 2012, "Spurs"],
                           ["Boris Diaw", 2005, "Suns"]]
        self.check_result(resp, expected_result)

        resp = self.execute_query('GO FROM "Boris Diaw" OVER serve '
                                  'YIELD $^.player.name as name, '
                                  'serve.start_year as start, '
                                  '$$.team.name as team '
                                  '| ORDER BY $-.team ASC')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ['name', 'start', 'team'])
        expected_result = [["Boris Diaw", 2003, "Hawks"],
                           ["Boris Diaw", 2008, "Hornets"],
                           ["Boris Diaw", 2016, "Jazz"],
                           ["Boris Diaw", 2012, "Spurs"],
                           ["Boris Diaw", 2005, "Suns"]]
        self.check_result(resp, expected_result)

        resp = self.execute_query('GO FROM "Boris Diaw" OVER serve '
                                  'YIELD $^.player.name as name, '
                                  'serve.start_year as start, '
                                  '$$.team.name as team '
                                  '| ORDER BY $-.team DESC')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ['name', 'start', 'team'])
        expected_result = [["Boris Diaw", 2005, "Suns"],
                           ["Boris Diaw", 2012, "Spurs"],
                           ["Boris Diaw", 2016, "Jazz"],
                           ["Boris Diaw", 2008, "Hornets"],
                           ["Boris Diaw", 2003, "Hawks"]]
        self.check_result(resp, expected_result)

    def test_multi_factors(self):
        resp = self.execute_query('GO FROM "Boris Diaw", "LaMarcus Aldridge" OVER serve '
                                  'WHERE serve.start_year >= 2012  '
                                  'YIELD $$.team.name as team, '
                                  '$^.player.name as player,  '
                                  '$^.player.age as age, '
                                  'serve.start_year as start '
                                  '| ORDER BY $-.team, $-.age')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ["team", "player", "age", "start"])
        expected_result = [["Jazz", "Boris Diaw", 36, 2016],
                           ["Spurs", "LaMarcus Aldridge", 33, 2015],
                           ["Spurs", "Boris Diaw", 36, 2012]]
        self.check_result(resp, expected_result)

        resp = self.execute_query('GO FROM "Boris Diaw", "LaMarcus Aldridge" OVER serve '
                                  'WHERE serve.start_year >= 2012  '
                                  'YIELD $$.team.name as team, '
                                  '$^.player.name as player,  '
                                  '$^.player.age as age, '
                                  'serve.start_year as start '
                                  '| ORDER BY $-.team ASC, $-.age ASC')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ["team", "player", "age", "start"])
        expected_result = [["Jazz", "Boris Diaw", 36, 2016],
                           ["Spurs", "LaMarcus Aldridge", 33, 2015],
                           ["Spurs", "Boris Diaw", 36, 2012]]
        self.check_result(resp, expected_result)

        resp = self.execute_query('GO FROM "Boris Diaw", "LaMarcus Aldridge" OVER serve '
                                  'WHERE serve.start_year >= 2012  '
                                  'YIELD $$.team.name as team, '
                                  '$^.player.name as player,  '
                                  '$^.player.age as age, '
                                  'serve.start_year as start '
                                  '| ORDER BY $-.team ASC, $-.age DESC')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ["team", "player", "age", "start"])
        expected_result = [["Jazz", "Boris Diaw", 36, 2016],
                           ["Spurs", "Boris Diaw", 36, 2012],
                           ["Spurs", "LaMarcus Aldridge", 33, 2015]]
        self.check_result(resp, expected_result)

        resp = self.execute_query('GO FROM "Boris Diaw", "LaMarcus Aldridge" OVER serve '
                                  'WHERE serve.start_year >= 2012  '
                                  'YIELD $$.team.name as team, '
                                  '$^.player.name as player,  '
                                  '$^.player.age as age, '
                                  'serve.start_year as start '
                                  '| ORDER BY $-.team DESC, $-.age ASC')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ["team", "player", "age", "start"])
        expected_result = [["Spurs", "LaMarcus Aldridge", 33, 2015],
                           ["Spurs", "Boris Diaw", 36, 2012],
                           ["Jazz", "Boris Diaw", 36, 2016]]
        self.check_result(resp, expected_result)

        resp = self.execute_query('GO FROM "Boris Diaw", "LaMarcus Aldridge" OVER serve '
                                  'WHERE serve.start_year >= 2012  '
                                  'YIELD $$.team.name as team, '
                                  '$^.player.name as player,  '
                                  '$^.player.age as age, '
                                  'serve.start_year as start '
                                  '| ORDER BY $-.team DESC, $-.age DESC')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ["team", "player", "age", "start"])
        expected_result = [["Spurs", "Boris Diaw", 36, 2012],
                           ["Spurs", "LaMarcus Aldridge", 33, 2015],
                           ["Jazz", "Boris Diaw", 36, 2016]]
        self.check_result(resp, expected_result)

    def test_output(self):
        resp = self.execute_query('GO FROM "Boris Diaw" OVER like '
                                  'YIELD like._dst as id '
                                  '| ORDER BY $-.id '
                                  '| GO FROM $-.id over serve')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ["serve._dst"])
        expected_result = [["Spurs"], ["Hornets"], ["Spurs"]]
        self.check_result(resp, expected_result)

    def test_duplicate_column(self):
        resp = self.execute_query('GO FROM "Boris Diaw" OVER serve '
                                  'YIELD $^.player.name as team, '
                                  'serve.start_year as start, '
                                  '$$.team.name as team '
                                  '| ORDER BY $-.team')
        self.check_resp_failed(resp)
