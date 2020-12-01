# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestUpdate(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS update_space(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('USE update_space')
        self.check_resp_succeeded(resp)

        resp = self.execute(
            'CREATE TAG IF NOT EXISTS person(name STRING, age INT, start TIMESTAMP DEFAULT 3600)')
        self.check_resp_succeeded(resp)

        resp = self.execute(
            'CREATE TAG IF NOT EXISTS student(name STRING  DEFAULT "", grade INT DEFAULT 1)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE IF NOT EXISTS study(start TIMESTAMP, '
                            'end TIMESTAMP DEFAULT 1577844000, name string DEFAULT "")')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    def test_update_vertex(self):
        # insert
        cmd = 'INSERT VERTEX person(name, age), student() VALUES 100:("AA", 10);'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON person 100;'
        resp = self.execute(cmd)

        expect_result = [[100, 'AA', 10, 3600]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # filter false
        cmd = 'UPDATE VERTEX 100 SET person.name = "aa" WHEN $^.person.age < 10;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON person 100;'
        resp = self.execute(cmd)

        expect_result = [[100, 'AA', 10, 3600]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # filter true
        cmd = 'UPDATE VERTEX 100 SET person.name = "aa" WHEN $^.person.age > 1;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON person 100;'
        resp = self.execute(cmd)

        expect_result = [[100, 'aa', 10, 3600]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # multi tags
        cmd = 'UPDATE VERTEX 100 SET person.name = "cc", student.name = "cc";'
        resp = self.execute(cmd)
        self.check_resp_failed(resp)

        # set YIELD
        cmd = 'UPDATE VERTEX 100 SET person.name = "cc", person.age = 15' \
              'YIELD $^.person.name as name , $^.person.age as age;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [['cc', 15]]
        self.check_out_of_order_result(resp.rows, expect_result)

    def test_upsert_vertex(self):
        # update success
        cmd = 'UPSERT VERTEX 100 SET person.name = "dd", person.age = 18 WHEN $^.person.age > 10;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON person 100;'
        resp = self.execute(cmd)

        expect_result = [[100, 'dd', 18, 3600]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # insert success: start has default
        cmd = 'UPSERT VERTEX 200 SET person.name = "aa", person.age = 8;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON person 200;'
        resp = self.execute(cmd)

        expect_result = [[200, 'aa', 8, 3600]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # success: all field
        cmd = 'UPSERT VERTEX 201 SET person.name = "aa", person.age = 8, ' \
              'person.start = $^.person.start + 1'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON person 201;'
        resp = self.execute(cmd)

        expect_result = [[201, 'aa', 8, 3601]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # success: oder update, use default value from start
        cmd = 'UPSERT VERTEX 202 SET person.name = "bb", person.age = $^.person.start + 8, ' \
              'person.start = 10;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON person 202;'
        resp = self.execute(cmd)

        expect_result = [[202, 'bb', 3608, 10]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # success: oder update, use the update value from start
        cmd = 'UPSERT VERTEX 202 SET person.name = "bb", person.start = 10, ' \
              'person.age = $^.person.start + 8;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON person 202;'
        resp = self.execute(cmd)
        expect_result = [[202, 'bb', 18, 10]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # insert failed: age without default value
        cmd = 'UPSERT VERTEX 203 SET person.name = "hh"'
        resp = self.execute(cmd)
        self.check_resp_failed(resp)

    def test_update_edge(self):
        # insert
        cmd = 'INSERT EDGE study(start) VALUES 100->101:(3600);'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON study 100->101;'
        resp = self.execute(cmd)

        expect_result = [[100, 101, 0, 3600, 1577844000, ""]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # filter false
        cmd = 'UPDATE EDGE 100->101 OF study SET start = 100 WHEN study.name == "aa";'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON study 100->101;'
        resp = self.execute(cmd)

        expect_result = [[100, 101, 0, 3600, 1577844000, ""]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # filter true
        cmd = 'UPDATE EDGE 100->101 OF study SET start = 100, end = 100 WHEN study.name == "";'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON study 100->101;'
        resp = self.execute(cmd)

        expect_result = [[100, 101, 0, 100, 100, ""]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # set YIELD
        cmd = 'UPDATE EDGE 100->101 OF study SET start = 200, end = 200' \
              'YIELD study.start as start, study.end as end'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[200, 200]]
        self.check_out_of_order_result(resp.rows, expect_result)

    def test_upsert_edge(self):
        # update
        cmd = 'UPSERT EDGE 100->101 OF study SET start = 3600, name = "dd";'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON study 100->101;'
        resp = self.execute(cmd)

        expect_result = [[100, 101, 0, 3600, 200, "dd"]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # insert success: end and name has default
        cmd = 'UPSERT EDGE 200->201 OF study SET start = 3600;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON study 200->201;'
        resp = self.execute(cmd)

        expect_result = [[200, 201, 0, 3600, 1577844000, ""]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # insert success: all field
        cmd = 'UPSERT EDGE 202->203 OF study SET name = "aa", start = 8, ' \
              'end = study.end + 1'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON study 202->203;'
        resp = self.execute(cmd)

        expect_result = [[202, 203, 0, 8, 1577844001, "aa"]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # success: oder update, use default value from start
        cmd = 'UPSERT EDGE 204->205 OF study SET name = "bb", start = study.end - 1000, ' \
              'end = 60000;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON study 204->205;'
        resp = self.execute(cmd)

        expect_result = [[204, 205, 0, 1577843000, 60000, "bb"]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # success: oder update, use the update value from start
        cmd = 'UPSERT EDGE 206->207 OF study SET end = 60000, start = study.end - 1000'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        cmd = 'FETCH PROP ON study 206->207;'
        resp = self.execute(cmd)

        expect_result = [[206, 207, 0, 59000, 60000, ""]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # insert failed: start without default value
        cmd = 'UPSERT EDGE 208->209 OF study SET name = "hh"'
        resp = self.execute(cmd)
        self.check_resp_failed(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space update_space')
        self.check_resp_succeeded(resp)
