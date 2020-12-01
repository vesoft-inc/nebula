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
            'CREATE SPACE IF NOT EXISTS fixed_issue_2020(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        resp = self.execute('USE fixed_issue_2020')
        self.check_resp_succeeded(resp)

    def test_default_value_timestamp(self):
        resp = self.execute('CREATE TAG IF NOT EXISTS person(name string, birthday timestamp DEFAULT "2020-02-02 10:00:00")')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE IF NOT EXISTS relation(start timestamp DEFAULT "2020-01-01 10:00:00",'
                    'end timestamp DEFAULT "2020-01-01 10:00:00")')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

        resp = self.execute('INSERT VERTEX person(name) VALUES hash("Laura"):("Laura");'
                'INSERT VERTEX person(name) VALUES hash("Lucy"):("Lucy")')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE relation() VALUES hash("Laura")->hash("Lucy"):();')
        self.check_resp_succeeded(resp)

        resp = self.execute('GO FROM hash("Laura") OVER relation '
                'YIELD relation.start as start, $$.person.birthday as birthday;')
        self.check_resp_succeeded(resp)

        expect_result = [[1577844000, 1580608800]]
        self.check_out_of_order_result(resp.rows, expect_result)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space fixed_issue_2020')
        self.check_resp_succeeded(resp)
