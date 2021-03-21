# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestSimpleQuery(NebulaTestSuite):

    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS fixed_pr_2042(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)

        resp = self.execute('USE fixed_pr_2042')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG IF NOT EXISTS person()')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE IF NOT EXISTS relation()')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

    def test_empty_input_in_fetch(self):
        resp = self.execute('GO FROM 11 over relation YIELD relation._dst as id | FETCH PROP ON person 11 YIELD $-.id')
        self.check_resp_failed(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space fixed_pr_2042')
        self.check_resp_succeeded(resp)
