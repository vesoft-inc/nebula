# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


import time

from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config
from tests.common.configs import get_delay_time


class GlobalDataLoader(object):
    def __init__(self, data_dir, host, port, user, password):
        self.data_dir = data_dir
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.has_load_data = False

    def __enter__(self):
        config = Config()
        config.max_connection_pool_size = 2
        config.timeout = 0
        # init connection pool
        self.client_pool = ConnectionPool()
        assert self.client_pool.init([(self.host, self.port)], config)

        # get session from the pool
        self.client = self.client_pool.get_session(self.user, self.password)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.has_load_data:
            self.drop_data()
        self.client.release()
        self.client_pool.close()

    def load_all_test_data(self):
        if self.client is None:
            assert False, 'Connect to {}:{}'.format(self.host, self.port)
        self.load_nba()
        self.load_student()
        self.has_load_data = True

    # The whole test will load once, for the only read tests
    def load_nba(self):
        nba_file = self.data_dir + '/data/nba.ngql'
        print("open: ", nba_file)
        with open(nba_file, 'r') as data_file:
            resp = self.client.execute(
                'CREATE SPACE IF NOT EXISTS nba(partition_num=10, replica_factor=1, vid_type = fixed_string(30));USE nba;')
            assert resp.is_succeeded(), resp.error_msg()

            lines = data_file.readlines()
            ddl = False
            ngql_statement = ""
            for line in lines:
                strip_line = line.strip()
                if len(strip_line) == 0:
                    continue
                elif strip_line.startswith('--'):
                    comment = strip_line[2:]
                    if comment == 'DDL':
                        ddl = True
                    elif comment == 'END':
                        if ddl:
                            time.sleep(get_delay_time(self.client))
                            ddl = False
                else:
                    line = line.rstrip()
                    ngql_statement += " " + line
                    if line.endswith(';'):
                        resp = self.client.execute(ngql_statement)
                        assert resp.is_succeeded(), resp.error_msg()
                        ngql_statement = ""

    # The whole test will load once, for the only read tests
    def load_student(self):
        resp = self.client.execute(
            'CREATE SPACE IF NOT EXISTS student(partition_num=10, replica_factor=1, vid_type = fixed_string(8)); USE student;')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('CREATE TAG IF NOT EXISTS person(name string, age int, gender string);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('CREATE TAG IF NOT EXISTS teacher(grade int, subject string);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('CREATE TAG IF NOT EXISTS student(grade int, hobby string DEFAULT "");')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('CREATE EDGE IF NOT EXISTS is_schoolmate(start_year int, end_year int);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('CREATE EDGE IF NOT EXISTS is_teacher(start_year int, end_year int);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('CREATE EDGE IF NOT EXISTS is_friend(start_year int, intimacy double);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('CREATE EDGE IF NOT EXISTS is_colleagues(start_year int, end_year int);')
        assert resp.is_succeeded(), resp.error_msg()
        # TODO: update the time when config can use
        time.sleep(get_delay_time(self.client))

        resp = self.client.execute('INSERT VERTEX person(name, age, gender), teacher(grade, subject) VALUES \
                                "2001":("Mary", 25, "female", 5, "Math"), \
                                "2002":("Ann", 23, "female", 3, "English"), \
                                "2003":("Julie", 33, "female", 6, "Math"), \
                                "2004":("Kim", 30,"male",  5, "English"), \
                                "2005":("Ellen", 27, "male", 4, "Art"), \
                                "2006":("ZhangKai", 27, "male", 3, "Chinese"), \
                                "2007":("Emma", 26, "female", 2, "Science"), \
                                "2008":("Ben", 24, "male", 4, "Music"), \
                                "2009":("Helen", 24, "male", 2, "Sports") ,\
                                "2010":("Lilan", 32, "male", 5, "Chinese");')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('INSERT VERTEX person(name, age, gender), student(grade) VALUES \
                                "1001":("Anne", 7, "female", 2), \
                                "1002":("Cynthia", 7, "female", 2), \
                                "1003":("Jane", 6, "male", 2), \
                                "1004":("Lisa", 8, "female", 3), \
                                "1005":("Peggy", 8, "male", 3), \
                                "1006":("Kevin", 9, "male", 3), \
                                "1007":("WangLe", 8, "male", 3), \
                                "1008":("WuXiao", 9, "male", 4), \
                                "1009":("Sandy", 9, "female", 4), \
                                "1010":("Harry", 9, "female", 4), \
                                "1011":("Ada", 8, "female", 4), \
                                "1012":("Lynn", 9, "female", 5), \
                                "1013":("Bonnie", 10, "female", 5), \
                                "1014":("Peter", 10, "male", 5), \
                                "1015":("Carl", 10, "female", 5), \
                                "1016":("Sonya", 11, "male", 6), \
                                "1017":("HeNa", 11, "female", 6), \
                                "1018":("Tom", 12, "male", 6), \
                                "1019":("XiaMei", 11, "female", 6), \
                                "1020":("Lily", 10, "female", 6);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('INSERT EDGE is_schoolmate(start_year, end_year) VALUES \
                                "1001" -> "1002":(2018, 2019), \
                                "1001" -> "1003":(2017, 2019), \
                                "1002" -> "1003":(2017, 2018), \
                                "1002" -> "1001":(2018, 2019), \
                                "1004" -> "1005":(2016, 2019), \
                                "1004" -> "1006":(2017, 2019), \
                                "1004" -> "1007":(2016, 2018), \
                                "1005" -> "1004":(2017, 2018), \
                                "1005" -> "1007":(2017, 2018), \
                                "1006" -> "1004":(2017, 2018), \
                                "1006" -> "1007":(2018, 2019), \
                                "1008" -> "1009":(2015, 2019), \
                                "1008" -> "1010":(2017, 2019), \
                                "1008" -> "1011":(2018, 2019), \
                                "1010" -> "1008":(2017, 2018), \
                                "1011" -> "1008":(2018, 2019), \
                                "1012" -> "1013":(2015, 2019), \
                                "1012" -> "1014":(2017, 2019), \
                                "1012" -> "1015":(2018, 2019), \
                                "1013" -> "1012":(2017, 2018), \
                                "1014" -> "1015":(2018, 2019), \
                                "1016" -> "1017":(2015, 2019), \
                                "1016" -> "1018":(2014, 2019), \
                                "1018" -> "1019":(2018, 2019), \
                                "1017" -> "1020":(2013, 2018), \
                                "1017" -> "1016":(2018, 2019);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('INSERT EDGE is_friend(start_year, intimacy) VALUES \
                                "1003" -> "1004":(2017, 80.0), \
                                "1013" -> "1007":(2018, 80.0), \
                                "1016" -> "1008":(2015, 80.0), \
                                "1016" -> "1018":(2014, 85.0), \
                                "1017" -> "1020":(2018, 78.0), \
                                "1018" -> "1016":(2013, 83.0), \
                                "1018" -> "1020":(2018, 88.0);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('INSERT EDGE is_colleagues(start_year, end_year) VALUES \
                                "2001" -> "2002":(2015, 0), \
                                "2001" -> "2007":(2014, 0), \
                                "2001" -> "2003":(2018, 0), \
                                "2003" -> "2004":(2013, 2017), \
                                "2002" -> "2001":(2016, 2017), \
                                "2007" -> "2001":(2013, 2018), \
                                "2010" -> "2008":(2018, 0);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = self.client.execute('INSERT EDGE is_teacher(start_year, end_year) VALUES \
                                "2002" -> "1004":(2018, 2019), \
                                "2002" -> "1005":(2018, 2019), \
                                "2002" -> "1006":(2018, 2019), \
                                "2002" -> "1007":(2018, 2019), \
                                "2002" -> "1009":(2017, 2018), \
                                "2002" -> "1012":(2015, 2016), \
                                "2002" -> "1013":(2015, 2016), \
                                "2002" -> "1014":(2015, 2016), \
                                "2002" -> "1019":(2014, 2015), \
                                "2010" -> "1016":(2018,2019), \
                                "2006" -> "1008":(2017, 2018);')
        assert resp.is_succeeded(), resp.error_msg()

    def drop_data(self):
        resp = self.client.execute('DROP SPACE nba; DROP SPACE student;')
        assert resp.is_succeeded(), resp.error_msg()
