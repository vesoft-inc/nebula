# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestIndex(NebulaTestSuite):

    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS nbaLookup(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)
        resp = self.execute('USE nbaLookup')
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE like(likeness int)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE serve(start_year int, end_year int)")
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute("CREATE EDGE INDEX serve_index_1 on serve(start_year)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE INDEX serve_index_2 on serve(end_year)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE INDEX serve_index_3 on serve(start_year,end_year)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE INDEX like_index_1 on like(likeness)")
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('INSERT EDGE like(likeness) VALUES "100" -> "101":(95)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES "101" -> "102":(95)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES "102" -> "104":(85)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES "102" -> "103":(85)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES "105" -> "106":(90)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES "106" -> "100":(75)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES "100" -> "200":(1997, 2016)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES "101" -> "201":(1999, 2018)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES "102" -> "202":(1997, 2016)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES "103" -> "203":(1999, 2018)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES "105" -> "204":(1997, 2016)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES "121" -> "201":(1999, 2018)')
        self.check_resp_succeeded(resp)

        resp = self.execute("CREATE TAG player (name FIXED_STRING(30), age INT)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE TAG team (name FIXED_STRING(30))")
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute("CREATE TAG INDEX player_index_1 on player(name)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE TAG INDEX player_index_2 on player(age)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE TAG INDEX player_index_3 on player(name,age)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE TAG INDEX team_index_1 on team(name)")
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES "100":("Tim Duncan", 42)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES "101":("Tony Parker", 36)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES "102":("LaMarcus Aldridge", 33)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES "103":("xxx", 35)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES "104":("yyy", 28)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES "105":("zzz", 21)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES "106":("kkk", 21)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES "121":("Useless", 60)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES "121":("Useless", 20)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES "200":("Warriors")')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES "201":("Nuggets")')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES "202":("oopp")')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES "203":("iiiooo")')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES "204":("opl")')
        self.check_resp_succeeded(resp)

    def test_edge_index(self):
        resp = self.execute('LOOKUP ON serve where serve.start_year > 0')
        self.check_resp_succeeded(resp)
        col_names = ['SrcVID', 'DstVID', 'Ranking']
        self.check_column_names(resp, col_names)
        expected_result = [['100', '200', 0],
                           ['101', '201', 0],
                           ['102', '202', 0],
                           ['103', '203', 0],
                           ['105', '204', 0],
                           ['121', '201', 0]]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.execute('LOOKUP ON serve where serve.start_year > 1997 and serve.end_year < 2020')
        self.check_resp_succeeded(resp)
        col_names = ['SrcVID', 'DstVID', 'Ranking']
        self.check_column_names(resp, col_names)
        expected_result = [['101', '201', 0],
                           ['103', '203', 0],
                           ['121', '201', 0]]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.execute('LOOKUP ON serve where serve.start_year > 2000 and serve.end_year < 2020')
        self.check_resp_succeeded(resp)
        col_names = ['SrcVID', 'DstVID', 'Ranking']
        self.check_column_names(resp, col_names)
        self.check_empty_result(resp)

        resp = self.execute('LOOKUP ON like where like.likeness > 89')
        self.check_resp_succeeded(resp)
        col_names = ['SrcVID', 'DstVID', 'Ranking']
        self.check_column_names(resp, col_names)
        expected_result = [['100', '101', 0],
                           ['101', '102', 0],
                           ['105', '106', 0]]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.execute('LOOKUP ON like where like.likeness < 39')
        self.check_resp_succeeded(resp)
        col_names = ['SrcVID', 'DstVID', 'Ranking']
        self.check_column_names(resp, col_names)
        self.check_empty_result(resp)

    def test_tag_index(self):
        resp = self.execute('LOOKUP ON player where player.age == 35')
        self.check_resp_succeeded(resp)
        col_names = ['VertexID']
        self.check_column_names(resp, col_names)
        expected_result = [['103']]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.execute('LOOKUP ON player where player.age > 0')
        self.check_resp_succeeded(resp)
        col_names = ['VertexID']
        self.check_column_names(resp, col_names)
        expected_result = [['100'],
                           ['101'],
                           ['102'],
                           ['103'],
                           ['104'],
                           ['105'],
                           ['106'],
                           ['121']]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.execute('LOOKUP ON player where player.age < 100')
        self.check_resp_succeeded(resp)
        col_names = ['VertexID']
        self.check_column_names(resp, col_names)
        expected_result = [['100'],
                           ['101'],
                           ['102'],
                           ['103'],
                           ['104'],
                           ['105'],
                           ['106'],
                           ['121']]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.execute('LOOKUP ON player where player.name == "Useless"')
        self.check_resp_succeeded(resp)
        col_names = ['VertexID']
        self.check_column_names(resp, col_names)
        expected_result = [['121']]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.execute('LOOKUP ON player where player.name == "Useless" and player.age < 30')
        self.check_resp_succeeded(resp)
        col_names = ['VertexID']
        self.check_column_names(resp, col_names)
        expected_result = [['121']]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.execute('LOOKUP ON team where team.name == "Warriors"')
        self.check_resp_succeeded(resp)
        col_names = ['VertexID']
        self.check_column_names(resp, col_names)
        expected_result = [['200']]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.execute('LOOKUP ON team where team.name == "oopp"')
        self.check_resp_succeeded(resp)
        col_names = ['VertexID']
        self.check_column_names(resp, col_names)
        expected_result = [['202']]
        self.check_out_of_order_result(resp, expected_result)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space nbaLookup')
        self.check_resp_succeeded(resp)
