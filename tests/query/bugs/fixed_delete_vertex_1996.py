# --coding:utf-8--
#
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
            'CREATE SPACE IF NOT EXISTS fixed_delete_vertex_issue1996(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('USE fixed_delete_vertex_issue1996')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'CREATE TAG person(name string, age int)')
        self.check_resp_succeeded(resp)

    def test_issue1996(self):
        time.sleep(self.delay)
        resp = self.execute('INSERT VERTEX person(name, age) VALUES 101:("Tony Parker", 36)')
        self.check_resp_succeeded(resp)
        resp = self.execute('DELETE VERTEX 101')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space fixed_delete_vertex_issue1996')
        self.check_resp_succeeded(resp)
