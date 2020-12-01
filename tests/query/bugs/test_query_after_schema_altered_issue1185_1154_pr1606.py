# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestQuery(NebulaTestSuite):
    def test_add_prop(self):
        cmd = 'FETCH PROP ON person 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 'Lisa', 8, 'female']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'FETCH PROP ON is_teacher 2002->1004'
        resp = self.execute(cmd)
        expect_result = [[2002, 1004, 0, 2018, 2019]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute("ALTER TAG person ADD (birthplace string)")
        self.check_resp_succeeded(resp)

        resp = self.execute("ALTER EDGE is_teacher ADD (years int)")
        self.check_resp_succeeded(resp)

        time.sleep(self.delay);

        cmd = 'FETCH PROP ON person 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 'Lisa', 8, 'female', '']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'FETCH PROP ON is_teacher 2002->1004'
        resp = self.execute(cmd)
        expect_result = [[2002, 1004, 0, 2018, 2019, 0]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher '\
              'YIELD is_teacher.start_year, is_teacher.years, $$.person.name, $$.person.birthplace'
        resp = self.execute(cmd)
        expect_result = [[2018, 0, 'Lisa', ''], [2018, 0, 'Peggy', ''], [2018, 0, 'Kevin', ''],
            [2018, 0, 'WangLe', ''],[2017, 0, 'Sandy', ''], [2015, 0, 'Lynn', ''], [2015, 0, 'Bonnie', ''],
            [2015, 0, 'Peter', ''], [2014, 0, 'XiaMei', '']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

    def test_alter_prop_type(self):
        resp = self.execute('INSERT VERTEX person(name, age, gender, birthplace) VALUES\
                                1004:("Lisa", 8, "female", "Washington")')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE is_teacher(start_year, end_year, years) VALUES\
                                2002->1004:(2018, 2019, 1)')
        self.check_resp_succeeded(resp)

        cmd = 'FETCH PROP ON person 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 'Lisa', 8, 'female', 'Washington']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'FETCH PROP ON is_teacher 2002->1004'
        resp = self.execute(cmd)
        expect_result = [[2002, 1004, 0, 2018, 2019, 1]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher '\
              'YIELD is_teacher.start_year, is_teacher.years, $$.person.name, $$.person.birthplace'
        resp = self.execute(cmd)
        expect_result = [[2018, 1, 'Lisa', 'Washington'], [2018, 0, 'Peggy', ''], [2018, 0, 'Kevin', ''],
            [2018, 0, 'WangLe', ''],[2017, 0, 'Sandy', ''], [2015, 0, 'Lynn', ''], [2015, 0, 'Bonnie', ''],
            [2015, 0, 'Peter', ''], [2014, 0, 'XiaMei', '']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute("ALTER TAG person CHANGE (birthplace int)")
        self.check_resp_succeeded(resp)

        resp = self.execute("ALTER EDGE is_teacher CHANGE (years string)")
        self.check_resp_succeeded(resp)

        time.sleep(self.delay);

        cmd = 'FETCH PROP ON person 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 'Lisa', 8, 'female', 0]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'FETCH PROP ON is_teacher 2002->1004'
        resp = self.execute(cmd)
        expect_result = [[2002, 1004, 0, 2018, 2019, '']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher '\
              'YIELD is_teacher.start_year, is_teacher.years, $$.person.name, $$.person.birthplace'
        resp = self.execute(cmd)
        expect_result = [[2018, '', 'Lisa', 0], [2018, '', 'Peggy', 0], [2018, '', 'Kevin', 0],
            [2018, '', 'WangLe', 0],[2017, '', 'Sandy', 0], [2015, '', 'Lynn', 0], [2015, '', 'Bonnie', 0],
            [2015, '', 'Peter', 0], [2014, '', 'XiaMei', 0]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

    def test_delete_prop(self):
        resp = self.execute("ALTER TAG person DROP (birthplace)")
        self.check_resp_succeeded(resp)

        resp = self.execute("ALTER EDGE is_teacher DROP (years)")
        self.check_resp_succeeded(resp)

        time.sleep(self.delay);

        cmd = 'FETCH PROP ON person 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 'Lisa', 8, 'female']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'FETCH PROP ON is_teacher 2002->1004'
        resp = self.execute(cmd)
        expect_result = [[2002, 1004, 0, 2018, 2019]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher YIELD $$.person.name as name, $$.student.grade as grade;'
        resp = self.execute(cmd)

        expect_result = [['Lisa', 3], ['Peggy', 3], ['Kevin', 3], ['WangLe', 3],
                         ['Sandy', 4], ['Lynn', 5], ['Bonnie', 5], ['Peter', 5], ['XiaMei', 6]]
        print(cmd)
        self.check_resp_succeeded(resp)
