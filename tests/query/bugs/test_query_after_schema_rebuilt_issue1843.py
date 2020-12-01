# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestQuery(NebulaTestSuite):
    def test_rebuild_tag(self):
        cmd = 'FETCH PROP ON person 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 'Lisa', 8, 'female']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'FETCH PROP ON * 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 'Lisa', 8, 'female', 3, '']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher '\
              'YIELD is_teacher.start_year, $$.person.name'
        resp = self.execute(cmd)
        expect_result = [[2018, 'Lisa'], [2018, 'Peggy'], [2018, 'Kevin'],
            [2018, 'WangLe'],[2017, 'Sandy'], [2015, 'Lynn'], [2015, 'Bonnie'],
            [2015, 'Peter'], [2014, 'XiaMei']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'DROP TAG person'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        time.sleep(self.delay);

        cmd = 'CREATE TAG IF NOT EXISTS person(name string, age int, gender string);'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        time.sleep(self.delay);

        cmd = 'FETCH PROP ON person 1004'
        resp = self.execute(cmd)
        expect_result = []
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'FETCH PROP ON * 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 3, '']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher '\
              'YIELD is_teacher.start_year, $$.person.name'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        expect_result = [[2018, ''], [2018, ''], [2018, ''],
                        [2018, ''],[2017, ''], [2015, ''], [2015, ''],
                        [2015, ''], [2014, '']]
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('INSERT VERTEX person(name, age, gender), student(grade) VALUES \
                        1004:("Lisa", 8, "female", 3)')
        self.check_resp_succeeded(resp)

        cmd = 'FETCH PROP ON person 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 'Lisa', 8, 'female']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'FETCH PROP ON * 1004'
        resp = self.execute(cmd)
        expect_result = [[1004, 3, '', 'Lisa', 8, 'female']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher '\
              'YIELD is_teacher.start_year, $$.person.name'
        resp = self.execute(cmd)
        expect_result = [[2018, 'Lisa'], [2018, ''], [2018, ''],
            [2018, ''],[2017, ''], [2015, ''], [2015, ''],
            [2015, ''], [2014, '']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('INSERT VERTEX person(name, age, gender), teacher(grade, subject) VALUES \
                        2001:("Mary", 25, "female", 5, "Math"), \
                        2002:("Ann", 23, "female", 3, "English"), \
                        2003:("Julie", 33, "female", 6, "Math"), \
                        2004:("Kim", 30,"male",  5, "English"), \
                        2005:("Ellen", 27, "male", 4, "Art"), \
                        2006:("ZhangKai", 27, "male", 3, "Chinese"), \
                        2007:("Emma", 26, "female", 2, "Science"), \
                        2008:("Ben", 24, "male", 4, "Music"), \
                        2009:("Helen", 24, "male", 2, "Sports") ,\
                        2010:("Lilan", 32, "male", 5, "Chinese");')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT VERTEX person(name, age, gender), student(grade) VALUES \
                        1001:("Anne", 7, "female", 2), \
                        1002:("Cynthia", 7, "female", 2), \
                        1003:("Jane", 6, "male", 2), \
                        1004:("Lisa", 8, "female", 3), \
                        1005:("Peggy", 8, "male", 3), \
                        1006:("Kevin", 9, "male", 3), \
                        1007:("WangLe", 8, "male", 3), \
                        1008:("WuXiao", 9, "male", 4), \
                        1009:("Sandy", 9, "female", 4), \
                        1010:("Harry", 9, "female", 4), \
                        1011:("Ada", 8, "female", 4), \
                        1012:("Lynn", 9, "female", 5), \
                        1013:("Bonnie", 10, "female", 5), \
                        1014:("Peter", 10, "male", 5), \
                        1015:("Carl", 10, "female", 5), \
                        1016:("Sonya", 11, "male", 6), \
                        1017:("HeNa", 11, "female", 6), \
                        1018:("Tom", 12, "male", 6), \
                        1019:("XiaMei", 11, "female", 6), \
                        1020:("Lily", 10, "female", 6);')
        self.check_resp_succeeded(resp)

    def test_rebuild_edge(self):
        cmd = 'FETCH PROP ON is_teacher 2002->1004'
        resp = self.execute(cmd)
        expect_result = [[2002, 1004, 0, 2018, 2019]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher '\
              'YIELD is_teacher.start_year, $$.person.name'
        resp = self.execute(cmd)
        expect_result = [[2018, 'Lisa'], [2018, 'Peggy'], [2018, 'Kevin'],
            [2018, 'WangLe'],[2017, 'Sandy'], [2015, 'Lynn'], [2015, 'Bonnie'],
            [2015, 'Peter'], [2014, 'XiaMei']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'DROP EDGE is_teacher'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        time.sleep(self.delay);

        cmd = 'CREATE EDGE IF NOT EXISTS is_teacher(start_year int, end_year int);';
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        time.sleep(self.delay);

        cmd = 'FETCH PROP ON is_teacher 2002->1004'
        resp = self.execute(cmd)
        expect_result = []
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher '\
              'YIELD is_teacher.start_year, $$.person.name'
        resp = self.execute(cmd)
        expect_result = []
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        resp = self.execute('INSERT EDGE is_teacher(start_year, end_year) VALUES \
                        2002 -> 1004:(2018, 2019);')
        self.check_resp_succeeded(resp)

        cmd = 'FETCH PROP ON is_teacher 2002->1004'
        resp = self.execute(cmd)
        expect_result = [[2002, 1004, 0, 2018, 2019]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)

        cmd = 'GO FROM 2002 OVER is_teacher '\
              'YIELD is_teacher.start_year, $$.person.name'
        resp = self.execute(cmd)
        expect_result = [[2018, 'Lisa']]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp.rows, expect_result)
