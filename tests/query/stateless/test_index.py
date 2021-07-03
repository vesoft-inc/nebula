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
            'CREATE SPACE IF NOT EXISTS nbaINDEX(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)

        resp = self.execute('USE nbaINDEX')
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE TAG player (name string, age int)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE TAG team (name string)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE like(likeness int)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE serve(start_year int, end_year int)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE TAG INDEX player_index_1 on player(name);")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE TAG INDEX team_index_1 on team(name)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE INDEX serve_index_1 on serve(start_year)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE INDEX like_index_1 on like(likeness)")
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp = self.execute('INSERT VERTEX player(name, age) VALUES 100:("Tim Duncan", 42)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES 101:("Tony Parker", 36);')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES 102:("LaMarcus Aldridge", 33);')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES 103:("姚明", 35);')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES 104:("徐", 28);')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES 105:("大美女", 21);')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES 106:("大帅哥", 21);')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX player(name, age) VALUES 121:("Useless", 60);')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES 200:("Warriors");')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES 201:("Nuggets")')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES 202:("宇宙第一")')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES 203:("世界第一")')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT VERTEX team(name) VALUES 204:("nebula第一")')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE like(likeness) VALUES 100 -> 101:(95)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES 101 -> 102:(95)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES 102 -> 104:(85)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES 102 -> 103:(85)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES 105 -> 106:(90)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(likeness) VALUES 106 -> 100:(75)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES 100 -> 200:(1997, 2016)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES 101 -> 201:(1999, 2018)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES 102 -> 202:(1997, 2016)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES 103 -> 203:(1999, 2018)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES 105 -> 204:(1997, 2016)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE serve(start_year, end_year) VALUES 121 -> 201:(1999, 2018)')
        self.check_resp_succeeded(resp)

    def test_index(self):
        resp = self.execute('FETCH PROP ON player 100;')
        self.check_resp_succeeded(resp)
        resp = self.execute('SHOW TAG INDEXES')
        self.check_resp_succeeded(resp)
        resp = self.execute('SHOW TAG INDEXES BY player')
        self.check_resp_succeeded(resp)
        resp = self.execute('SHOW EDGE INDEXES')
        self.check_resp_succeeded(resp)
        resp = self.execute('SHOW EDGE INDEXES BY serve')
        self.check_resp_succeeded(resp)
        resp = self.execute('DESCRIBE TAG INDEX player_index_1')
        self.check_resp_succeeded(resp)
        expect_result = [["name", "string"]]
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('DESCRIBE TAG INDEX team_index_1')
        expect_result = [["name", "string"]]
        self.check_out_of_order_result(resp.rows, expect_result)
        self.check_resp_succeeded(resp)
        resp = self.execute('DESCRIBE EDGE INDEX serve_index_1')
        self.check_resp_succeeded(resp)
        expect_result = [["start_year", "int"]]
        self.check_out_of_order_result(resp.rows, expect_result)
        resp = self.execute('DESCRIBE EDGE INDEX like_index_1')
        self.check_resp_succeeded(resp)
        expect_result = [["likeness", "int"]]
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('go from 101 over like')
        self.check_resp_succeeded(resp)
        expect_result = [[102]]
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('GO FROM 101 OVER serve')
        self.check_resp_succeeded(resp)
        expect_result = [[201]]
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('GO FROM 101 OVER serve,like')
        self.check_resp_succeeded(resp)
        expect_result = [[0, 102],[201, 0]]
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('GO FROM 102 OVER like yield $$.player.name')
        self.check_resp_succeeded(resp)
        expect_result = [["姚明"],["徐"]]
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('GO FROM 102 OVER serve yield $$.team.name')
        self.check_resp_succeeded(resp)
        expect_result = [["宇宙第一"]]
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('DROP TAG INDEX player_index_1')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESCRIBE TAG INDEX player_index_1')
        self.check_resp_failed(resp)

        resp = self.execute('DROP TAG INDEX team_index_1;')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESCRIBE TAG INDEX team_index_1;')
        self.check_resp_failed(resp)

        resp = self.execute('DROP EDGE INDEX serve_index_1;')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESCRIBE EDGE INDEX serve_index_1;')
        self.check_resp_failed(resp)

        resp = self.execute('DROP EDGE INDEX like_index_1')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESCRIBE EDGE INDEX like_index_1')
        self.check_resp_failed(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space nbaINDEX')
        self.check_resp_succeeded(resp)
