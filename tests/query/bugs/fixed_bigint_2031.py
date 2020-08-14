# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestBigInt(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS BigInt2031(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('USE BigInt2031')
        self.check_resp_succeeded(resp)

    def test_issue2031(self):
        time.sleep(self.delay)
        resp = self.execute(
            'CREATE TAG person1(name string, age bigint)')
        self.check_resp_failed(resp)

        resp = self.execute(
            'CREATE TAG person2(name string, age bigint DEFAULT 100)')
        self.check_resp_failed(resp)

        resp = self.execute(
            'CREATE TAG person3(name string, age Bigint)')
        self.check_resp_failed(resp)

        resp = self.execute(
            'CREATE TAG person4(name string, age BIGINT)')
        self.check_resp_failed(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space BigInt2031')
        self.check_resp_succeeded(resp)
