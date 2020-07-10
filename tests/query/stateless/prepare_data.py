# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import sys
import time

from graph import ttypes

from nebula_test_common.nebula_test_suite import NebulaTestSuite


class PrepareData(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS test(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)

        # because need leader metad
        time.sleep(self.delay)

        resp = self.execute('USE test')
        self.check_resp_succeeded(resp)
        resp = self.execute('CREATE TAG IF NOT EXISTS person(name string, age int, gender string);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG IF NOT EXISTS teacher(grade int, subject string);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG IF NOT EXISTS student(grade int, hobby string DEFAULT "");')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE IF NOT EXISTS is_schoolmate(start_year int, end_year int);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE IF NOT EXISTS is_teacher(start_year int, end_year int);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE IF NOT EXISTS is_friend(start_year int, intimacy double);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE IF NOT EXISTS is_colleagues(start_year int, end_year int);')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp = self.execute_query('SHOW TAGS')
        self.check_resp_succeeded(resp)

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

        resp = self.execute('INSERT EDGE is_schoolmate(start_year, end_year) VALUES \
                        1001 -> 1002:(2018, 2019), \
                        1001 -> 1003:(2017, 2019), \
                        1002 -> 1003:(2017, 2018), \
                        1002 -> 1001:(2018, 2019);')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE is_schoolmate(start_year, end_year) VALUES \
                        1001 -> 1002:(2018, 2019), \
                        1001 -> 1003:(2017, 2019), \
                        1002 -> 1003:(2017, 2018), \
                        1002 -> 1001:(2018, 2019), \
                        1004 -> 1005:(2016, 2019), \
                        1004 -> 1006:(2017, 2019), \
                        1004 -> 1007:(2016, 2018), \
                        1005 -> 1004:(2017, 2018), \
                        1005 -> 1007:(2017, 2018), \
                        1006 -> 1004:(2017, 2018), \
                        1006 -> 1007:(2018, 2019), \
                        1008 -> 1009:(2015, 2019), \
                        1008 -> 1010:(2017, 2019), \
                        1008 -> 1011:(2018, 2019), \
                        1010 -> 1008:(2017, 2018), \
                        1011 -> 1008:(2018, 2019), \
                        1012 -> 1013:(2015, 2019), \
                        1012 -> 1014:(2017, 2019), \
                        1012 -> 1015:(2018, 2019), \
                        1013 -> 1012:(2017, 2018), \
                        1014 -> 1015:(2018, 2019), \
                        1016 -> 1017:(2015, 2019), \
                        1016 -> 1018:(2014, 2019), \
                        1018 -> 1019:(2018, 2019), \
                        1017 -> 1020:(2013, 2018), \
                        1017 -> 1016:(2018, 2019);')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE is_friend(start_year, intimacy) VALUES \
                        1003 -> 1004:(2017, 80.0), \
                        1013 -> 1007:(2018, 80.0), \
                        1016 -> 1008:(2015, 80.0), \
                        1016 -> 1018:(2014, 85.0), \
                        1017 -> 1020:(2018, 78.0), \
                        1018 -> 1016:(2013, 83.0), \
                        1018 -> 1020:(2018, 88.0);')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE is_colleagues(start_year, end_year) VALUES \
                        2001 -> 2002:(2015, 0), \
                        2001 -> 2007:(2014, 0), \
                        2001 -> 2003:(2018, 0), \
                        2003 -> 2004:(2013, 2017), \
                        2002 -> 2001:(2016, 2017), \
                        2007 -> 2001:(2013, 2018), \
                        2010 -> 2008:(2018, 0);')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE is_teacher(start_year, end_year) VALUES \
                        2002 -> 1004:(2018, 2019), \
                        2002 -> 1005:(2018, 2019), \
                        2002 -> 1006:(2018, 2019), \
                        2002 -> 1007:(2018, 2019), \
                        2002 -> 1009:(2017, 2018), \
                        2002 -> 1012:(2015, 2016), \
                        2002 -> 1013:(2015, 2016), \
                        2002 -> 1014:(2015, 2016), \
                        2002 -> 1019:(2014, 2015), \
                        2010 -> 1016:(2018,2019), \
                        2006 -> 1008:(2017, 2018);')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE test')
        self.check_resp_succeeded(resp)
