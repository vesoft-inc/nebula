# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestFetchEmptyVertices(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute('CREATE SPACE empty(partition_num=1, replica_factor=1)')
        self.check_resp_succeeded(resp)

        # 2.0 use space get from cache
        time.sleep(self.delay)

        resp = self.execute('USE empty')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG empty_tag_0()')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG empty_tag_1()')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE empty_edge()')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

        resp = self.execute('INSERT VERTEX empty_tag_0() values "1":(), "2":();'
                            'INSERT VERTEX empty_tag_1() values "1":(), "2":();'
                            'INSERT EDGE empty_edge() values "1"->"2":();')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE empty')
        self.check_resp_succeeded(resp)

    def test_empty_props(self):
        # empty_tag_0
        resp = self.execute('FETCH PROP ON empty_tag_0 "1"')
        self.check_resp_succeeded(resp)
        expect_result = [['1']]
        self.check_result(resp, expect_result)

        # *
        resp = self.execute('FETCH PROP ON * "1"')
        self.check_resp_succeeded(resp)
        expect_result = [['1']]
        self.check_result(resp, expect_result)

        # edge
        resp = self.execute('FETCH PROP ON empty_edge "1"->"2"')
        self.check_resp_succeeded(resp)
        expect_result = [['1', '2', 0]]
        self.check_result(resp, expect_result)

    def test_input_with_empty_props(self):
        resp = self.execute('GO FROM "1" OVER empty_edge '
                                  'YIELD empty_edge._dst as id'
                                  '| FETCH PROP ON empty_tag_0 $-.id')
        self.check_resp_succeeded(resp)
        expect_result = [['2']]
        self.check_result(resp, expect_result)

        resp = self.execute('GO FROM "1" OVER empty_edge '
                                  'YIELD empty_edge._src as src, empty_edge._dst as dst'
                                  '| FETCH PROP ON empty_edge $-.src->$-.dst')
        self.check_resp_succeeded(resp)
        expect_result = [['1', '2', 0]]
        self.check_result(resp, expect_result)
