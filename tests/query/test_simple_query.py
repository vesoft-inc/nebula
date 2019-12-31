# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
from tests.common.nebula_test_suite import NebulaTestSuite
import time
import re


class TestNebula(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.client.execute('CREATE SPACE IF NOT EXISTS space1')
        self.check_resp_successed(resp)

    def test_schema(self):
        resp = self.client.execute('USE space1')
        self.check_resp_successed(resp)
        resp = self.client.execute(
            'CREATE TAG IF NOT EXISTS person(name string, age int)')
        self.check_resp_successed(resp)

        self.client.execute('CREATE EDGE IF NOT EXISTS like(likeness double)')
        self.check_resp_successed(resp)

        resp = self.client.execute_query('SHOW TAGS')
        self.check_resp_successed(resp)
        self.search_result(resp.rows, [[re.compile(r'\d+'), 'person']])

        resp = self.client.execute_query('SHOW EDGES')
        self.check_resp_successed(resp)
        self.check_result(resp.rows, [[re.compile(r'\d+'), 'like']])

    def test_insert(self):
        resp = self.client.execute('USE space1')
        self.check_resp_successed(resp)
        time.sleep(3)

        resp = self.client.execute(
            'INSERT VERTEX person(name, age) VALUES 1:(\'Bob\', 10)')
        self.check_resp_successed(resp)

        resp = self.client.execute(
            'INSERT VERTEX person(name, age) VALUES 2:(\'Lily\', 9)')
        self.check_resp_successed(resp)

        resp = self.client.execute(
            'INSERT VERTEX person(name, age) VALUES 3:(\'Tom\', 10)')
        self.check_resp_successed(resp)

        resp = self.client.execute(
            'INSERT EDGE like(likeness) VALUES 1->2:(80.0)')
        self.check_resp_successed(resp)

        resp = self.client.execute(
            'INSERT EDGE like(likeness) VALUES 1->3:(90.0)')
        self.check_resp_successed(resp)

    def test_query(self):
        time.sleep(3)
        resp = self.client.execute('USE space1')
        assert resp.error_code == 0

        resp = self.client.execute_query(
            'GO FROM 1 OVER like YIELD $$.person.name, '
            '$$.person.age, like.likeness')
        assert resp.error_code == 0

        expect_result = [['Lily', 9, 80.0], ['Tom', 10, 90.0]]
        self.check_result(resp.rows, expect_result)

    @classmethod
    def cleanup(self):
        resp = self.client.execute('drop space space1')
        self.check_resp_successed(resp)
