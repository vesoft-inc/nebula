# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import sys
import time
import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


class TestSimpleQuery(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS simplequeryspace(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('USE simplequeryspace')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'CREATE TAG IF NOT EXISTS person(name string, age int)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE IF NOT EXISTS like(likeness double)')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    def test_schema(self):
        resp = self.execute('USE simplequeryspace')
        self.check_resp_succeeded(resp)

        resp = self.execute_query('SHOW TAGS')
        self.check_resp_succeeded(resp)
        # 2.0 enable to show tagID
        self.search_result(resp, [[re.compile(r'per')]], is_regex = True)

        resp = self.execute_query('SHOW EDGES')
        self.check_resp_succeeded(resp)
        self.check_result(resp, [[re.compile('lik')]], is_regex = True)

    def test_insert(self):
        resp = self.execute('USE simplequeryspace')
        self.check_resp_succeeded(resp)

        resp = self.execute(
            'INSERT VERTEX person(name, age) VALUES "1":(\'Bob\', 10)')
        self.check_resp_succeeded(resp)

        resp = self.execute(
            'INSERT VERTEX person(name, age) VALUES "2":(\'Lily\', 9)')
        self.check_resp_succeeded(resp)

        resp = self.execute(
            'INSERT VERTEX person(name, age) VALUES "3":(\'Tom\', 10)')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE like(likeness) VALUES "1"->"2":(80.0)')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE like(likeness) VALUES "1"->"3":(90.0)')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE like(likeness) VALUES "2"->"3":(100.0)')
        self.check_resp_succeeded(resp)

    @pytest.mark.skip(reason="does not support fetch and update")
    def test_upsert(self):
        resp = self.execute_query('FETCH PROP on person "1"')
        expect_result = [[1, 'Bob', 10]]
        self.check_result(resp, expect_result)
        resp = self.execute('UPSERT VERTEX "1" SET person.name = "BobUpsert"')
        self.check_resp_succeeded(resp)
        resp = self.execute_query('FETCH PROP on person "1"')
        expect_result = [["1", 'BobUpsert', 10]]
        self.check_result(resp, expect_result)
        resp = self.execute('UPSERT VERTEX 4 SET person.name = "NewBob", person.age=11')
        self.check_resp_succeeded(resp)
        resp = self.execute_query('FETCH PROP on person "4"')
        expect_result = [["4", 'NewBob', 11]]
        self.check_result(resp, expect_result)
        resp = self.execute('UPDATE VERTEX "4" SET person.age=20')
        self.check_resp_succeeded(resp)
        resp = self.execute_query('FETCH PROP on person "4"')
        expect_result = [["4", 'NewBob', 20]]
        self.check_result(resp, expect_result)

    def test_query(self):
        resp = self.execute('USE simplequeryspace')
        self.check_resp_succeeded(resp)

        resp = self.execute_query('GO FROM "1" OVER like YIELD $$.person.name, '
                                  '$$.person.age, like.likeness')
        self.check_resp_succeeded(resp)

        expect_result = [['Lily', 9, 80.0], ['Tom', 10, 90.0]]
        self.check_result(resp, expect_result)
        expect_result_OO = [['Tom', 10, 90.0], ['Lily', 9, 80.0]]
        self.check_out_of_order_result(resp, expect_result_OO)

    @pytest.mark.skip(reason="does not support FIND SHORTEST")
    def test_path(self):
        resp = self.execute('USE simplequeryspace')
        self.check_resp_succeeded(resp)

        resp = self.execute_query('FIND SHORTEST PATH FROM 1 to 3 OVER *')
        self.check_resp_succeeded(resp)
        expect_result = [[1, (b"like", 0), 3]]
        self.check_path_result(resp, expect_result)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space simplequeryspace')
        self.check_resp_succeeded(resp)
