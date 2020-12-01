# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite

class TestSetQuery(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_student_space()

    def test_union(self):
        # test UNION with distinct
        cmd = 'GO FROM 1004 OVER is_schoolmate YIELD $$.person.name as name, $$.person.age as age \
            UNION \
            GO FROM 1005 OVER is_schoolmate YIELD $$.person.name as name, $$.person.age as age;'
        resp = self.execute(cmd)

        expect_result = [['Lisa', 8], ['Peggy', 8], ['Kevin', 9], ['WangLe', 8]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # test UNION without distinct
        cmd = 'GO FROM 1005 OVER is_schoolmate YIELD $$.person.name as name, $$.person.age as age \
            UNION ALL \
            GO FROM 1004 OVER is_schoolmate YIELD $$.person.name as name, $$.person.age as age;'
        resp = self.execute(cmd)

        expect_result = [['Lisa', 8], ['Peggy', 8], ['Kevin', 9], ['WangLe', 8], ['WangLe', 8]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # type conversion succeeded
        cmd = 'GO FROM 1005 OVER is_schoolmate YIELD $$.person.name as name \
            UNION \
            GO FROM 1004 OVER is_schoolmate YIELD $$.person.age as name;'
        resp = self.execute(cmd)

        expect_result = [['8'], ['9'], ['Lisa'], ['WangLe']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # type conversion succeeded
        cmd = 'GO FROM 1005 OVER is_schoolmate YIELD $$.person.name as name \
            UNION \
            GO FROM 1004 OVER is_schoolmate YIELD (string)($$.person.age+2) as name;'
        resp = self.execute(cmd)

        expect_result = [['10'], ['11'], ['Lisa'], ['WangLe']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        #  type conversion failed
        try:
            cmd = 'GO FROM 1005 OVER is_schoolmate YIELD $$.person.name as name \
                        UNION \
                        GO FROM 1004 OVER is_schoolmate YIELD (string)($$.person.age+2) as name;'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed')

    def test_minus(self):
        # type conversion succeeded
        cmd = 'GO FROM 1004 OVER is_schoolmate YIELD $$.person.name as name, $$.person.age as age \
            MINUS \
            GO FROM 1005 OVER is_schoolmate YIELD $$.person.name as name, $$.person.age as age;'
        resp = self.execute(cmd)

        expect_result = [['Peggy', 8], ['Kevin', 9]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # type conversion failed
        try:
            cmd = 'GO FROM 1005 OVER is_schoolmate YIELD $$.person.age as name \
                MINUS \
                GO FROM 1004 OVER is_schoolmate YIELD $$.person.name as name;'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed')

    def test_intersect(self):
        # type conversion succeeded
        cmd = 'GO FROM 1004 OVER is_schoolmate YIELD $$.person.name as name, $$.person.age as age \
            INTERSECT \
            GO FROM 1005 OVER is_schoolmate YIELD $$.person.name as name, $$.person.age as age;'
        resp = self.execute(cmd)

        expect_result = [['WangLe', 8]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # type conversion failed
        try:
            cmd = 'GO FROM 1004 OVER is_schoolmate YIELD $$.person.name as name, $$.student.age as age \
                INTERSECT \
                GO FROM 1005 OVER is_schoolmate YIELD $$.person.name as name, $$.student.gender as gender;'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed')

    def test_all(self):
        # type conversion succeeded
        cmd = 'GO FROM 1004 OVER * YIELD $$.person.name as name, $$.student.grade as grade UNION ALL \
                GO FROM 1005 OVER * YIELD $$.person.name as name, $$.student.grade as grade MINUS \
                GO FROM 1003 OVER * YIELD $$.person.name as name, $$.student.grade as grade INTERSECT \
                GO FROM 1013 OVER * YIELD $$.person.name as name, $$.student.grade as grade;'
        resp = self.execute(cmd)

        expect_result = [['WangLe', 3]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # test with pipe
        cmd = '(GO FROM 1004 OVER * YIELD is_schoolmate._dst as id, $$.person.name as name \
                UNION ALL \
                GO FROM 1005 OVER * YIELD is_schoolmate._dst as id, $$.person.name as name) \
                | GO FROM $-.id OVER is_schoolmate YIELD DISTINCT $$.person.name as name, $$.person.age as age'
        resp = self.execute(cmd)

        expect_result = [['Peggy', 8], ['Kevin', 9], ['WangLe', 8], ['Lisa', 8]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # test with pipe
        cmd = '(GO FROM 1004 OVER * YIELD is_schoolmate._dst as id, $$.person.name as name \
                MINUS \
                GO FROM 1005 OVER * YIELD is_schoolmate._dst as id, $$.person.name as name \
                | GO FROM $-.id OVER is_schoolmate YIELD is_schoolmate._dst as id, $$.person.name as name) \
                UNION ALL \
                GO FROM 1004 OVER * YIELD is_schoolmate._dst as id, $$.person.name as name;'
        resp = self.execute(cmd)

        expect_result = [[1005, 'Peggy'], [1006, 'Kevin'], [1007, 'WangLe']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)
