# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


class TestGoQuery(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_student_space()

    def test_1_step(self):
        # 1 step
        cmd = 'GO FROM "2002" OVER is_teacher YIELD $$.person.name as name, $$.student.grade as grade;'
        resp = self.execute(cmd)

        expect_result = [['Lisa', 3], ['Peggy', 3], ['Kevin', 3], ['WangLe', 3],
                         ['Sandy', 4], ['Lynn', 5], ['Bonnie', 5], ['Peter', 5], ['XiaMei', 6]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # go 1 step with filter
        cmd = 'GO FROM "2002" OVER is_teacher WHERE $$.student.grade > 3 \
                YIELD $$.person.name as name, $$.student.grade as grade;'
        resp = self.execute(cmd)

        expect_result = [['Sandy', 4], ['Lynn', 5], ['Bonnie', 5], ['Peter', 5], ['XiaMei', 6]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        cmd = 'GO FROM "2002" OVER is_teacher WHERE strcasecmp($$.person.name, "Sandy") == 0 \
                YIELD $$.person.name as name, $$.student.grade as grade;'
        # go 1 step with string function filter
        resp = self.execute(cmd)

        expect_result = [['Sandy', 4]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # go 1 step with var
        cmd = '$var = GO FROM "2002" OVER is_teacher \
                WHERE strcasecmp($$.person.name, "Sandy") == 0 \
                YIELD is_teacher._dst as id; \
                GO FROM $var.id OVER is_schoolmate REVERSELY \
                YIELD is_schoolmate._src as src;'
        resp = self.execute(cmd)

        expect_result = [["1009"]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # go 1 step with pipe, TODO: to support over *
        # cmd = 'GO FROM "2002" OVER is_teacher WHERE $$.student.grade > 3 \
        #         YIELD is_teacher._dst as id \
        #         | GO FROM $-.id OVER * YIELD is_friend._dst, is_teacher._dst, \
        #         is_schoolmate._dst, is_colleagues._dst;'
        # resp = self.execute(cmd)
        # self.check_resp_succeeded(resp)

        # expect_result = [[1007, 0, 0, 0], [0, 0, 1012, 0], [0, 0, 1013, 0], [0, 0, 1014, 0],
        #                  [0, 0, 1015, 0], [0, 0, 1015, 0]]
        # self.check_out_of_order_result(resp, expect_result)

        # go 1 step with ORDER BY
        cmd = 'GO FROM "2002" OVER is_teacher \
                YIELD $$.person.name as name, $$.student.grade as grade \
                | ORDER BY $-.name;'
        resp = self.execute(cmd)

        expect_result = [['Bonnie', 5], ['Kevin', 3], ['Lisa', 3], ['Lynn', 5],
                         ['Peggy', 3], ['Peter', 5], ['Sandy', 4], ['WangLe', 3], ['XiaMei', 6]]
        self.check_resp_succeeded(resp)
        self.check_result(resp, expect_result)

        # go 1 step with ORDER BY DESC
        cmd = 'GO FROM "2002" OVER is_teacher \
                YIELD $$.person.name as name, $$.student.grade as grade \
                | ORDER BY $-.name DESC;'
        resp = self.execute(cmd)

        expect_result = [['XiaMei', 6], ['WangLe', 3], ['Sandy', 4], ['Peter', 5],
                         ['Peggy', 3], ['Lynn', 5], ['Lisa', 3], ['Kevin', 3], ['Bonnie', 5]]
        self.check_resp_succeeded(resp)
        self.check_result(resp, expect_result)

        # go 1 step with ORDER BY multi columns
        cmd = 'GO FROM "2002" OVER is_teacher \
                YIELD $$.person.name as name, $$.student.grade as grade \
                | ORDER BY  $-.grade, $-.name DESC;'
        resp = self.execute(cmd)

        expect_result = [['WangLe', 3], ['Peggy', 3], ['Lisa', 3], ['Kevin', 3],
                         ['Sandy', 4], ['Peter', 5], ['Lynn', 5], ['Bonnie', 5], ['XiaMei', 6]]
        self.check_resp_succeeded(resp)
        self.check_result(resp, expect_result)

        # go 1 step with ORDER BY and LIMIT
        cmd = 'GO FROM "2002" OVER is_teacher \
                YIELD $$.person.name as name, $$.student.grade as grade \
                | ORDER BY $-.grade, $-.name DESC | LIMIT 2,4;'
        resp = self.execute(cmd)

        expect_result = [['Lisa', 3], ['Kevin', 3], ['Sandy', 4], ['Peter', 5]]
        self.check_resp_succeeded(resp)
        self.check_result(resp, expect_result)

        # go 1 step with GROUP BY, TODO: to support GROUP BY
        # cmd = 'GO FROM "2002" OVER is_teacher \
        #         YIELD $$.person.name as name, $$.student.grade as grade \
        #         | GROUP BY $-.grade YIELD $-.grade as grade, COUNT(*) as count \
        #         | ORDER BY $-.grade DESC;'
        # resp = self.execute(cmd)
        #
        # expect_result = [[6, 1], [5, 3], [4, 1], [3, 4]]
        # self.check_resp_succeeded(resp)
        # self.check_result(resp, expect_result)

        # go 1 step with FETCH, TODO: to support GROUP BY
        # cmd = 'GO FROM "2002" OVER is_teacher \
        #         YIELD is_teacher._dst as id \
        #         | FETCH PROP ON person $-.id YIELD person.name, person.age, person.gender;'
        # resp = self.execute(cmd)
        #
        # expect_result = [[1013, 'Bonnie', 10, 'female'], [1006, 'Kevin', 9, 'male'], [1004, 'Lisa', 8, 'female'],
        #                  [1012, 'Lynn', 9, 'female'], [1005, 'Peggy', 8, 'male'], [1014, 'Peter', 10, 'male'],
        #                  [1009, 'Sandy', 9, 'female'], [1007, 'WangLe', 8, 'male'], [1019, 'XiaMei', 11, 'female']]
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

        # go 1 step over * REVERSELY, TODO: to support over *
        # cmd = 'GO FROM "2001" over * REVERSELY \
        #         YIELD $$.person.name as name, $$.teacher.subject as subject'
        # resp = self.execute(cmd)
        # expect_result = [['Ann', 'English'], ['Emma', 'Science']]
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, expect_result)

    def test_2_steps(self):
        # go 2 steps
        cmd = 'GO 2 STEPS FROM "1001" OVER is_schoolmate \
                YIELD $$.person.name as name, $$.person.age as age, \
                $$.student.grade as grade;'
        resp = self.execute(cmd)
        expect_result = [['Anne', 7, 2], ['Jane', 6, 2]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # go 2 steps without YIELD
        cmd = 'GO 2 STEPS FROM "1016" OVER is_friend;'
        resp = self.execute(cmd)
        expect_result = [["1016"], ["1020"]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

    @pytest.mark.skip(reason="does not support over *")
    def test_2_steps_with_over_all(self):
        # go 2 step over *
        cmd = 'GO 2 STEPS FROM "1016" over * YIELD is_friend._dst, is_teacher._dst, \
               is_schoolmate._dst, is_colleagues._dst'
        resp = self.execute(cmd)
        expect_result = [[0, 0, 1009, 0], [0, 0, 1010, 0], [0, 0, 1011, 0], [1016, 0, 0, 0],
                         [0, 0, 1016, 0], [0, 0, 1019, 0], [1020, 0, 0, 0], [0, 0, 1020, 0], [1020, 0, 0, 0]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # go 2 step over * YIELD
        cmd = 'GO 2 STEPS FROM "1016" over * \
                YIELD $$.person.name as name, \
                $$.person.age as age, $$.student.grade as grade;'
        resp = self.execute(cmd)
        expect_result = [['Sandy', 9, 4], ['Harry', 9, 4], ['Ada', 8, 4], ['Sonya', 11, 6],
                         ['Sonya', 11, 6], ['XiaMei', 11, 6], ['Lily', 10, 6], ['Lily', 10, 6], ['Lily', 10, 6]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # go 2 step over * YIELD with filter
        cmd = 'GO 2 STEPS FROM "1016" over * WHERE $$.student.grade > 4 AND $$.person.age > 10 \
                YIELD $$.person.name as name, $$.person.age as age, $$.student.grade as grade;'
        resp = self.execute(cmd)
        expect_result = [['Sonya', 11, 6], ['Sonya', 11, 6], ['XiaMei', 11, 6]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # go 2 step over * REVERSELY
        cmd = 'GO 2 STEPS FROM "1016" over * REVERSELY YIELD $^.person.name'
        resp = self.execute(cmd)
        expect_result = [['HeNa'], ['Tom'], ['Tom']]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # go 2 step over * REVERSELY with filter
        cmd = 'GO 2 STEPS FROM "1016" over * REVERSELY WHERE $^.person.age < 12 YIELD $^.person.name'
        resp = self.execute(cmd)
        expect_result = [['HeNa']]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

        # go 2 step over * REVERSELY with filter
        cmd = 'GO 2 STEPS FROM "1016" over * REVERSELY WHERE $^.person.age < 12 YIELD $^.person.name'
        resp = self.execute(cmd)
        expect_result = [['HeNa']]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)

    @pytest.mark.skip(reason="go has not checkout aggregation function")
    def test_mix_1(self):
        # yield field use agg_func
        cmd = 'GO FROM "2002" over is_teacher \
                YIELD $$.student.grade as grade, COUNT(*);'
        resp = self.execute(cmd)
        self.check_resp_failed(resp)

    @pytest.mark.skip(reason="does not support groupby")
    def test_mix_2(self):
        # use ORDER BY, LIMIT, UNION, GROUP BY
        cmd = '(GO FROM "1008" over * YIELD $$.person.name as name, $$.person.gender as gender, $$.student.grade as grade \
                | ORDER BY $-.name | LIMIT 2 \
                UNION \
                GO FROM "1016" over * YIELD $$.person.name as name, $$.person.gender as gender, $$.student.grade as grade \
                | ORDER BY $-.name | LIMIT 2) \
                | GROUP BY $-.gender YIELD $-.gender as gender, COUNT(*) AS count'
        resp = self.execute(cmd)
        expect_result = [['male', 1], ['female', 3]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expect_result)
