# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestNegativeTTL(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS NegativeTTL(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('USE NegativeTTL')
        self.check_resp_succeeded(resp)

    def test_NegativeTTL(self):
        time.sleep(self.delay)
        resp = self.execute(
            'CREATE TAG NegativeTTL111(name string, age int) TTL_DURATION=-111, TTL_COL="age"')
        self.check_resp_failed(resp)

        resp = self.execute(
            'CREATE TAG NegativeTTL112(name string, age int) TTL_DURATION=111, TTL_COL="age"')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

        resp = self.execute(
            'drop TAG NegativeTTL112')
        self.check_resp_succeeded(resp)

        resp = self.execute(
            'CREATE TAG NegativeTTL113(name string, age int) TTL_DURATION=0, TTL_COL="age"')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp = self.execute(
            'drop TAG NegativeTTL113')
        self.check_resp_succeeded(resp)


    @classmethod
    def cleanup(self):
        resp = self.execute('drop space NegativeTTL')
        self.check_resp_succeeded(resp)
