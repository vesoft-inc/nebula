# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestYield(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS test(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.graph_delay)
        resp = self.execute('USE test')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space test')
        self.check_resp_succeeded(resp)

    def test_yield(self):
        # test addition
        cmd = "yield 2+2"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[4]]
        self.check_result(resp.rows, expect_result)
        # test sub
        cmd = "yield 2-2"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[0]]
        self.check_result(resp.rows, expect_result)
        #test Unary delimiter
        cmd = "yield -3 as a"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[-3]]
        self.check_result(resp.rows, expect_result)
        #test Multiplication
        cmd = "yield 2*2"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[4]]
        self.check_result(resp.rows, expect_result)
        # test Division
        cmd = "yield 2/2"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[1]]
        self.check_result(resp.rows, expect_result)

        # test Modulo division
        cmd = "yield 3%2"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[1]]
        self.check_result(resp.rows, expect_result)
        # test Power
        cmd = "yield pow(3,4)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[81]]
        self.check_result(resp.rows, expect_result)
        # test abs
        cmd = "yield abs(-9)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[9]]
        self.check_result(resp.rows, expect_result)
        #test ceil
        cmd = "yield ceil(0.9)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[1.0000]]
        self.check_result(resp.rows, expect_result)
        # test floor
        cmd = "yield floor(0.9)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[0.0]]
        self.check_result(resp.rows, expect_result)
        #test rand
        cmd = "yield rand32()"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = "yield rand32(1000)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = "yield rand32(1000,2000)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = "yield rand64()"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = "yield rand64(1000)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = "yield rand64(1000,200000)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # test exp
        cmd = "yield exp(2)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        #test round
        cmd = "yield round(0.9)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[1.0000]]
        self.check_result(resp.rows, expect_result)
        # test floor
        cmd = "yield floor(0.9)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[0.0]]
        self.check_result(resp.rows, expect_result)
        # test log
        cmd = "yield log(0.9)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # test log10
        cmd = "yield log10(27)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # test sqrt
        cmd = "yield sqrt(256)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # test cos
        cmd = "yield cos(0.9)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # test substr
        cmd = "yield substr('hello',1,3)"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [["hel"]]
        self.check_result(resp.rows, expect_result)
        #test lower
        cmd = "yield lower('HellO')"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [["hello"]]
        self.check_result(resp.rows, expect_result)
        # test strcat
        cmd = "yield 'a'+'b'"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [["ab"]]
        self.check_result(resp.rows, expect_result)
        # test strlength
        cmd = "yield length('Hullo')"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        expect_result = [[5]]
        self.check_result(resp.rows, expect_result)
        # test strhash
        cmd = "yield hash('hello')"
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # test strcasecmp
        cmd = 'yield strcasecmp("hello", "world")'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # test strlpad
        cmd = 'yield lpad("Hello", 8, "1")'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'yield (timestamp)(now())'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
