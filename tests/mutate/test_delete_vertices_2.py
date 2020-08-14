# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestDeleteVertices(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.load_data()

    @classmethod
    def cleanup(self):
        pass

    def test_delete_with_pipe_wrong_vid_type(self):
        resp = self.execute_query('GO FROM "Boris Diaw" OVER like YIELD like._type as id | DELETE VERTEX $-.id')
        self.check_resp_failed(resp)

    def test_delete_with_pipe(self):
        resp = self.execute_query('GO FROM "Boris Diaw" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["Tony Parker"], ["Tim Duncan"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Tony Parker" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["LaMarcus Aldridge"],
                         ["Manu Ginobili"],
                         ["Tim Duncan"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Tim Duncan" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["Tony Parker"],
                         ["Manu Ginobili"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Boris Diaw" OVER like YIELD like._dst as id | DELETE VERTEX $-.id')
        self.check_resp_succeeded(resp)

        # check result
        resp = self.execute_query('GO FROM "Boris Diaw" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Tony Parker" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Tim Duncan" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

    def test_delete_with_var(self):
        resp = self.execute_query('GO FROM "Russell Westbrook" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["Paul George"], ["James Harden"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Paul George" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["Russell Westbrook"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "James Harden" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["Russell Westbrook"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('$var = GO FROM "Russell Westbrook" OVER like YIELD like._dst as id; DELETE VERTEX $var.id')
        self.check_resp_succeeded(resp)

        # check result
        resp = self.execute_query('GO FROM "Russell Westbrook" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Paul George" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute_query('GO FROM "Russell Westbrook" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)
