# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import re

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY

class TestSchema(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.client.execute('CREATE SPACE schema_test_space(partition_num=9)')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)
        resp = self.client.execute('USE schema_test_space')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE schema_test_space;')
        self.check_resp_succeeded(resp)

    # When turn on asan, it will lead to graphd Stack overflow
    def test_multi(self):
        # multi sentences
        cmd = ''
        for i in range(0, 100):
            cmd += 'CREATE TAG tag10' + str(i) + '(name string);'
        resp = self.client.execute(cmd)
        self.check_resp_succeeded(resp)

        # check result
        for i in range(0, 100):
            resp = self.client.execute('DESC TAG tag10' + str(i))
            self.check_resp_succeeded(resp)
            expect_result = [['name', 'string', 'YES', T_EMPTY]]
            self.check_result(resp, expect_result)

    def test_show_create_tag(self):
        # create tag succeed
        resp = self.client.execute('CREATE TAG person(name string, email string DEFAULT "NULL", '
                                   'age int, gender string, row_timestamp timestamp DEFAULT 2020)')
        self.check_resp_succeeded(resp)

        # test show create tag
        resp = self.client.execute('SHOW CREATE TAG person')
        self.check_resp_succeeded(resp)
        create_tag_str = 'CREATE TAG `person` (\n' \
                         ' `name` string NULL,\n' \
                         ' `email` string NULL DEFAULT "NULL",\n' \
                         ' `age` int64 NULL,\n' \
                         ' `gender` string NULL,\n' \
                         ' `row_timestamp` timestamp NULL DEFAULT 2020\n' \
                         ') ttl_duration = 0, ttl_col = ""'
        expect_result = [['person', create_tag_str]]
        self.check_result(resp, expect_result)

        # check result
        resp = self.client.execute('DROP TAG person')
        self.check_resp_succeeded(resp)

        resp = self.client.execute(create_tag_str)
        self.check_resp_succeeded(resp)

        # check result
        resp = self.client.execute('DESCRIBE TAG person')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', T_EMPTY],
                         ['email', 'string', 'YES', 'NULL'],
                         ['age', 'int64', 'YES', T_EMPTY],
                         ['gender', 'string', 'YES', T_EMPTY],
                         ['row_timestamp', 'timestamp', 'YES', 2020]]
        self.check_result(resp, expect_result)

    def test_show_create_edge(self):
        # create edge succeed
        resp = self.client.execute('CREATE EDGE education(id int, time_ timestamp, school string)')
        self.check_resp_succeeded(resp)

        # show create edge
        resp = self.client.execute('SHOW CREATE EDGE education')
        self.check_resp_succeeded(resp)
        create_edge_str = 'CREATE EDGE `education` (\n' \
                          ' `id` int64 NULL,\n' \
                          ' `time_` timestamp NULL,\n' \
                          ' `school` string NULL\n' \
                          ') ttl_duration = 0, ttl_col = "\"'
        expect_result = [['education', create_edge_str]]
        self.check_result(resp, expect_result)

        # check result from show create
        resp = self.client.execute('DROP EDGE education')
        self.check_resp_succeeded(resp)
        resp = self.client.execute(create_edge_str)
        self.check_resp_succeeded(resp)

        # DESC edge
        resp = self.client.execute('DESC EDGE education')
        self.check_resp_succeeded(resp)
        expect_result = [['id', 'int64', 'YES', T_EMPTY],
                         ['time_', 'timestamp', 'YES', T_EMPTY],
                         ['school', 'string', 'YES', T_EMPTY]]
        self.check_out_of_order_result(resp, expect_result)

    def test_create_tag_with_function_default_value(self):
        resp = self.client.execute('CREATE TAG tag_function(name string DEFAULT "N/A", '
                                   'birthday timestamp DEFAULT now())')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESC TAG tag_function;')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', 'N/A'],
                         ['birthday', 'timestamp', 'YES', 'now()']]
        self.check_result(resp, expect_result)

        resp = self.client.execute('SHOW CREATE TAG tag_function;')
        self.check_resp_succeeded(resp)
        create_tag_str = 'CREATE TAG `tag_function` (\n' \
                         ' `name` string NULL DEFAULT "N/A",\n' \
                         ' `birthday` timestamp NULL DEFAULT now()\n' \
                         ') ttl_duration = 0, ttl_col = ""'
        expect_result = [['tag_function', create_tag_str]]
        self.check_result(resp, expect_result)

        current_timestamp = int(time.time())
        time.sleep(self.delay)
        # insert successed
        resp = self.client.execute('INSERT VERTEX tag_function() VALUES "a":()')
        self.check_resp_succeeded(resp)

        # fetch
        resp = self.client.execute('FETCH PROP ON tag_function "a"')
        self.check_resp_succeeded(resp)
        expect_result = [[re.compile('a'), re.compile('N/A'), re.compile(r'\d+')]]
        self.check_result(resp, expect_result, is_regex = True)
        assert(resp.row_values(0)[2].as_int() > current_timestamp)

    def test_create_edge_with_function_default_value(self):
        resp = self.client.execute('CREATE EDGE edge_function(name string DEFAULT "N/A", '
                                   'birthday timestamp DEFAULT now())')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESC EDGE edge_function;')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', 'N/A'],
                         ['birthday', 'timestamp', 'YES', 'now()']]
        self.check_result(resp, expect_result)

        resp = self.client.execute('SHOW CREATE EDGE edge_function;')
        self.check_resp_succeeded(resp)
        create_tag_str = 'CREATE EDGE `edge_function` (\n' \
                         ' `name` string NULL DEFAULT "N/A",\n' \
                         ' `birthday` timestamp NULL DEFAULT now()\n' \
                         ') ttl_duration = 0, ttl_col = ""'
        expect_result = [['edge_function', create_tag_str]]
        self.check_result(resp, expect_result)

        current_timestamp = int(time.time())
        time.sleep(self.delay)
        # insert successed
        resp = self.client.execute('INSERT EDGE edge_function() VALUES "a"->"b":()')
        self.check_resp_succeeded(resp)

        # fetch
        resp = self.client.execute('FETCH PROP ON edge_function "a"->"b"')
        self.check_resp_succeeded(resp)
        expect_result = [[re.compile('a'), re.compile('b'), re.compile(r'\d+'), re.compile('N/A'), re.compile(r'\d+')]]
        self.check_result(resp, expect_result, is_regex = True)
        assert(resp.row_values(0)[4].as_int() > current_timestamp)


