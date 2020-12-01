# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite


class TestFetchQuery(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_student_space()

    def test_fetch_vertex(self):
        # fetch *
        cmd = 'FETCH PROP ON * 2001;'
        resp = self.execute(cmd)

        self.check_resp_succeeded(resp)
        expect_result = [[2001, 'Mary', 25, 'female', 5, 'Math']]
        self.check_out_of_order_result(resp.rows, expect_result)

        # fetch with specified tag
        cmd = 'FETCH PROP ON teacher 2001;'
        resp = self.execute(cmd)

        expect_result = [[2001, 5, 'Math']]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # fetch with yield
        cmd = 'FETCH PROP ON teacher 2001 YIELD teacher.grade AS Grade, teacher.subject AS Subject;'
        resp = self.execute(cmd)

        expect_result = [[2001, 5, 'Math']]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

    def test_fetch_edge(self):
        # fetch edge
        cmd = 'FETCH PROP ON is_colleagues 2001 -> 2002'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[2001, 2002, 0, 2015, 0]]
        self.check_out_of_order_result(resp.rows, expect_result)
