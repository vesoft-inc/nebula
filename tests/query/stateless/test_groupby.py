# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.query.stateless.prepare_data import PrepareData


class TestGroupby(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_student_space()

    def test_error(self):
        # group col without input field
        try:
            cmd = 'GO FROM 2002 over is_teacher \
                YIELD $$.student.grade as grade \
                | GROUP BY 1+1 \
                YIELD COUNT(1), 1+1;'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed')

        # input field is nonexistent
        try:
            cmd = 'GO FROM 2002 over is_teacher \
                YIELD $$.student.grade as grade \
                | GROUP BY $-.name YIELD COUNT(1);'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed')

        # TODO: if fix the bug, use it
        '''
        # aggfunc has input_ref
        try:
            cmd = 'GO FROM 2002 over is_teacher \
                YIELD $$.student.grade as grade, $$.person.name as name \
                | GROUP BY $-.grade \
                YIELD $-.grade as grade, upper($-.name) as len_name, COUNT(*) as count;'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed')
         '''

    def test_one_column(self):
        # one column on group column
        cmd = 'GO FROM 2002 over is_teacher \
            YIELD $$.student.grade as grade \
            | GROUP BY $-.grade \
            YIELD $-.grade as grade, COUNT(*) as count;'
        resp = self.execute(cmd)

        expect_result = [[6, 1], [5, 3], [4, 1], [3, 4]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # has alias col in group by column
        cmd = 'GO FROM 2002 over is_teacher \
                            YIELD $$.student.grade as grade \
                            | GROUP BY grade \
                            YIELD $-.grade as grade, COUNT(*) as count;'
        resp = self.execute(cmd)
        expect_result = [[6, 1], [5, 3], [4, 1], [3, 4]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # empty input
        cmd = 'GO FROM 3002 over is_teacher \
            YIELD $$.student.grade as grade \
            | GROUP BY $-.grade \
            YIELD $-.grade as grade, COUNT(*) as count;'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp.rows)

        # all aggfunc on group column
        cmd = 'GO FROM 2002 over is_teacher \
            YIELD $$.person.gender as gender, $$.student.grade as grade, $$.person.age as age \
            | GROUP BY $-.gender \
            YIELD $-.gender as gender, AVG($-.age) as avg_age, SUM($-.age) as sum_age, \
            MAX($-.age) as max_age, MIN($-.age) as min_age, STD($-.age) AS std_age, \
            BIT_AND($-.grade) AS and_grade, BIT_OR($-.grade) AS or_grade,\
            BIT_XOR($-.grade) AS xor_grade, COUNT(*) as count;'
        resp = self.execute(cmd)

        expect_result = [['male', 8.75, 35, 10, 8, 0.82915619758885, 1, 7, 6, 4],
                        ['female', 9.4, 47, 11, 8, 1.0198039027185568, 0, 7, 1, 5]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)


    def test_multi_columns(self):
        # one column on group column
        cmd = 'GO FROM 2002 over is_teacher \
            YIELD $$.person.gender as gender, $$.student.grade as grade, $$.person.age as age \
            | GROUP BY $-.grade, $-.gender \
            YIELD $-.grade as grade, $-.gender as gender, AVG($-.age) as age, COUNT(*) as count;'
        resp = self.execute(cmd)

        expect_result = [[6, 'female', 11.000000, 1],
                        [5, 'male', 10.000000, 1],
                        [5, 'female', 9.500000, 2],
                        [3, 'female', 8.000000, 1],
                        [4, 'female', 9.000000, 1],
                        [3, 'male', 8.333333333333334, 3]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        # has constant function call on agg column
        cmd = 'GO FROM 2002 over is_teacher \
                YIELD $$.student.grade as grade, $$.person.name as name \
                | GROUP BY $-.grade \
                YIELD $-.grade as grade, 2*3, COUNT(*) as count;'
        resp = self.execute(cmd)

        # all aggfunc on group column
        expect_result = [[6, 6, 1],
                        [5, 6, 3],
                        [4, 6, 1],
                        [3, 6, 4]]
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)
