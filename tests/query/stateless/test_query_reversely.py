# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import sys
import time

from graph import ttypes

from tests.common.nebula_test_suite import NebulaTestSuite


class TestQueryReversely(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS queryreversely(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)

    def test_query_reversely(self):
        time.sleep(self.delay)
        resp = self.execute('USE queryreversely')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            "CREATE TAG IF NOT EXISTS player(name string, age int)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE IF NOT EXISTS teammate(years int)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE IF NOT EXISTS like(degree int)")
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute(
            'INSERT VERTEX player(name, age) VALUES 100:(\"burning\", 31)')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'INSERT VERTEX player(name, age) VALUES 101:(\"longdd\", 31)')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'INSERT VERTEX player(name, age) VALUES 102:(\"sylar\", 27)')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'INSERT VERTEX player(name, age) VALUES 103:(\"rotk\", 28)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(degree) VALUES 100->101:(99)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(degree) VALUES 100->102:(80)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(degree) VALUES 100->103:(100)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(degree) VALUES 102->101:(10)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE like(degree) VALUES 102->103:(70)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE teammate(years) VALUES 100->101:(8)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE teammate(years) VALUES 100->102:(3)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE teammate(years) VALUES 100->103:(3)')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE teammate(years) VALUES 102->103:(5)')
        self.check_resp_succeeded(resp)
        resp = self.execute('go from 101 over * reversely')
        self.check_resp_succeeded(resp)
        expect_result = [[0, 100], [0, 102], [100, 0]]
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('go from 100 over * reversely')
        self.check_empty_result(resp.rows)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space queryreversely')
        self.check_resp_succeeded(resp)
