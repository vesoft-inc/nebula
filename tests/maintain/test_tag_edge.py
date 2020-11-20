# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import re

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY, T_NULL

class TestSchema(NebulaTestSuite):

    @classmethod
    def prepare(self):
        resp = self.client.execute('CREATE SPACE tag_space(partition_num=9)')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)
        resp = self.client.execute('USE tag_space')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE tag_space;')
        self.check_resp_succeeded(resp)

    def test_tag(self):
        # empty prop
        resp = self.client.execute('CREATE TAG tag1()')
        self.check_resp_succeeded(resp)

        # if not exists
        resp = self.client.execute('CREATE TAG IF NOT EXISTS tag1()')
        self.check_resp_succeeded(resp)

        # check result
        resp = self.client.execute_query('DESCRIBE TAG tag1')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_result(resp, expect_result)

        # alter
        resp = self.client.execute('ALTER TAG tag1 ADD (id int, name string)')
        self.check_resp_succeeded(resp)

        # check result, 2.0 add null and DEFAULT
        resp = self.client.execute_query('DESCRIBE TAG tag1')
        self.check_resp_succeeded(resp)
        expect_result = [['id', 'int64', 'YES', T_EMPTY], ['name', 'string', 'YES', T_EMPTY]]
        self.check_result(resp, expect_result)

        # create tag succeed
        resp = self.client.execute('CREATE TAG person(name string, email string DEFAULT "NULL", '
                                   'age int, gender string, row_timestamp timestamp DEFAULT 2020)')
        self.check_resp_succeeded(resp)

        # Create Tag with duplicate field, failed
        resp = self.client.execute('CREATE TAG duplicate_tag(name string, name int)')
        self.check_resp_failed(resp)

        # Create Tag with duplicate field, failed
        resp = self.client.execute('CREATE TAG duplicate_tag(name string, name string)')
        self.check_resp_failed(resp)

        # Create Tag with DEFAULT value
        resp = self.client.execute('CREATE TAG person_with_default(name string, age int DEFAULT 18)')
        self.check_resp_succeeded(resp)

        # Create Tag with wrong type DEFAULT value, failed
        resp = self.client.execute('CREATE TAG person_type_mismatch'
                                   '(name string, age int DEFAULT "hello")')
        self.check_resp_failed(resp)

        # test DESCRIBE
        resp = self.client.execute_query('DESCRIBE TAG person')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', T_EMPTY],
                         ['email', 'string', 'YES', 'NULL'],
                         ['age', 'int64', 'YES', T_EMPTY],
                         ['gender', 'string', 'YES', T_EMPTY],
                         ['row_timestamp', 'timestamp', 'YES', 2020]]
        self.check_result(resp, expect_result)

        # test DESC
        resp = self.client.execute_query('DESCRIBE TAG person')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', T_EMPTY],
                         ['email', 'string', 'YES', 'NULL'],
                         ['age', 'int64', 'YES', T_EMPTY],
                         ['gender', 'string', 'YES', T_EMPTY],
                         ['row_timestamp', 'timestamp', 'YES', 2020]]
        self.check_result(resp, expect_result)

        # test show create tag
        resp = self.client.execute_query('SHOW CREATE TAG person')
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

        # describe tag not exist
        resp = self.client.execute_query('DESCRIBE TAG not_exist')
        self.check_resp_failed(resp)

        # unreserved keyword
        resp = self.client.execute('CREATE TAG upper(name string, ACCOUNT string, '
                                         'age int, gender string, row_timestamp timestamp DEFAULT 100)')
        self.check_resp_succeeded(resp)

        # check result
        resp = self.client.execute_query('DESCRIBE TAG upper')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', T_EMPTY],
                         ['account', 'string', 'YES', T_EMPTY],
                         ['age', 'int64', 'YES', T_EMPTY],
                         ['gender', 'string', 'YES', T_EMPTY],
                         ['row_timestamp', 'timestamp', 'YES', 100]]
        self.check_result(resp, expect_result)

        # existent tag
        resp = self.client.execute('CREATE TAG person(id int)')
        self.check_resp_failed(resp)

        # nonexistent tag
        resp = self.client.execute_query('DESCRIBE TAG not_exist')
        self.check_resp_failed(resp)

        # alter tag
        resp = self.client.execute('ALTER TAG person '
                                   'ADD (col1 int, col2 string), '
                                   'CHANGE (age string), '
                                   'DROP (gender)')
        self.check_resp_succeeded(resp)

        # drop not exist prop
        resp = self.client.execute('ALTER TAG person DROP (gender)')
        self.check_resp_failed(resp)

        # check result
        resp = self.client.execute_query('DESCRIBE TAG person')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', T_EMPTY],
                         ['email', 'string', 'YES', 'NULL'],
                         ['age', 'string', 'YES', T_EMPTY],
                         ['row_timestamp', 'timestamp', 'YES', 2020],
                         ['col1', 'int64', 'YES', T_EMPTY],
                         ['col2', 'string', 'YES', T_EMPTY]]
        self.check_result(resp, expect_result)

        # check result
        resp = self.client.execute_query('SHOW CREATE TAG person')
        self.check_resp_succeeded(resp)
        create_tag_str = 'CREATE TAG `person` (\n' \
                         ' `name` string NULL,\n' \
                         ' `email` string NULL DEFAULT "NULL",\n' \
                         ' `age` string NULL,\n' \
                         ' `row_timestamp` timestamp NULL DEFAULT 2020,\n' \
                         ' `col1` int64 NULL,\n' \
                         ' `col2` string NULL\n' \
                         ') ttl_duration = 0, ttl_col = ""';
        expect_result = [['person', create_tag_str]]
        self.check_result(resp, expect_result)

        # show tags
        resp = self.client.execute_query('SHOW TAGS')
        self.check_resp_succeeded(resp)
        expect_result = [['tag1'], ['person'], ['person_with_default'], ['upper']]
        self.check_out_of_order_result(resp, expect_result)

        # with negative DEFAULT value
        resp = self.client.execute('CREATE TAG default_tag_neg(id int DEFAULT -10, '
                                   'height double DEFAULT -176.0)')
        self.check_resp_succeeded(resp)

        # Tag with expression DEFAULT value
        resp = self.client.execute('CREATE TAG default_tag_expr'
                                   '(id int64 DEFAULT 3/2*4-5, '
                                   'male bool DEFAULT 3 > 2, '
                                   'height double DEFAULT abs(-176.0), '
                                   'adult bool DEFAULT true AND false)')
        self.check_resp_succeeded(resp)

    def test_drop_tag(self):
        resp = self.client.execute('DROP TAG person')
        self.check_resp_succeeded(resp)

        # drop not exist
        resp = self.client.execute('DROP TAG not_exist_tag')
        self.check_resp_failed(resp)

        # drop if exists
        resp = self.client.execute('DROP TAG IF EXISTS not_exist_tag')
        self.check_resp_succeeded(resp)

        # drop if exists
        resp = self.client.execute('CREATE TAG exist_tag(id int)')
        self.check_resp_succeeded(resp)
        resp = self.client.execute('DROP TAG IF EXISTS exist_tag')
        self.check_resp_succeeded(resp)

    def test_edge(self):
        # empty edge prop
        resp = self.client.execute('CREATE EDGE edge1()')
        self.check_resp_succeeded(resp)

        # IF NOT EXISTS
        resp = self.client.execute('CREATE EDGE IF NOT EXISTS edge1()')
        self.check_resp_succeeded(resp)

        # desc edge
        resp = self.client.execute_query('DESCRIBE EDGE edge1')
        self.check_resp_succeeded(resp)

        # alter edge
        resp = self.client.execute('ALTER EDGE edge1 ADD (id int, name string)')
        self.check_resp_succeeded(resp)

        # desc edge
        resp = self.client.execute_query('DESCRIBE EDGE edge1')
        self.check_resp_succeeded(resp)

        # 1.0 expect ['id', 'int', 'YES', T_EMPTY], 2.0 has int8, int16, int32 and int64
        expect_result = [['id', 'int64', 'YES', T_EMPTY],
                         ['name', 'string', 'YES', T_EMPTY]]
        self.check_out_of_order_result(resp, expect_result)

        # create edge succeeded
        resp = self.client.execute('CREATE EDGE buy(id int, time_ string)')
        self.check_resp_succeeded(resp)

        # create Edge with duplicate field
        resp = self.client.execute('CREATE EDGE duplicate_buy(time_ int, time_ string)')
        self.check_resp_failed(resp)

        # create Edge with duplicate field
        resp = self.client.execute('CREATE EDGE duplicate_buy(time_ int, time_ int)')
        self.check_resp_failed(resp)

        # create Edge with DEFAULT
        resp = self.client.execute('CREATE EDGE buy_with_default(id int, name string DEFAULT "NULL",'
                                   'time_ timestamp DEFAULT 2020)')
        self.check_resp_succeeded(resp)

        # DEFAULT value not match type
        resp = self.client.execute('CREATE EDGE buy_type_mismatch(id int, time_ string DEFAULT 0)')
        self.check_resp_failed(resp)

        # existent edge
        resp = self.client.execute('CREATE EDGE buy(id int, time_ string)')
        self.check_resp_failed(resp)

        # DESCRIBE edge
        resp = self.client.execute_query('DESCRIBE EDGE buy')
        self.check_resp_succeeded(resp)
        expect_result = [['id', 'int64', 'YES', T_EMPTY],
                         ['time_', 'string', 'YES', T_EMPTY]]
        self.check_out_of_order_result(resp, expect_result)

        # desc nonexistent edge
        resp = self.client.execute('DESCRIBE EDGE not_exist')
        self.check_resp_failed(resp)

        # DESC edge
        resp = self.client.execute_query('DESC EDGE buy')
        self.check_resp_succeeded(resp)
        expect_result = [['id', 'int64', 'YES', T_EMPTY],
                         ['time_', 'string', 'YES', T_EMPTY]]
        self.check_out_of_order_result(resp, expect_result)

        # show create edge
        resp = self.client.execute_query('SHOW CREATE EDGE buy_with_default')
        self.check_resp_succeeded(resp)
        create_edge_str = 'CREATE EDGE `buy_with_default` (\n' \
                          ' `id` int64 NULL,\n' \
                          ' `name` string NULL DEFAULT "NULL",\n' \
                          ' `time_` timestamp NULL DEFAULT 2020\n' \
                          ') ttl_duration = 0, ttl_col = "\"'
        expect_result = [['buy_with_default', create_edge_str]]
        self.check_result(resp, expect_result)

        # create edge succeed
        resp = self.client.execute('CREATE EDGE education(id int, time_ timestamp, school string)')
        self.check_resp_succeeded(resp)

        # DESC edge
        resp = self.client.execute_query('DESC EDGE education')
        self.check_resp_succeeded(resp)
        expect_result = [['id', 'int64', 'YES', T_EMPTY],
                         ['time_', 'timestamp', 'YES', T_EMPTY],
                         ['school', 'string', 'YES', T_EMPTY]]
        self.check_out_of_order_result(resp, expect_result)

        # show edges
        resp = self.client.execute_query('SHOW EDGES')
        self.check_resp_succeeded(resp)
        expect_result = [['edge1'], ['buy'], ['buy_with_default'], ['education']]
        self.check_out_of_order_result(resp, expect_result)

        # alter edge
        resp = self.client.execute('ALTER EDGE education '
                                   'ADD (col1 int, col2 string), '
                                   'CHANGE (school int), '
                                   'DROP (id, time_)')
        self.check_resp_succeeded(resp)

        # drop not exist prop, failed
        resp = self.client.execute('ALTER EDGE education DROP (id, time_)')
        self.check_resp_failed(resp)

        # check result
        resp = self.client.execute_query('DESC EDGE education')
        self.check_resp_succeeded(resp)
        expect_result = [['school', 'int64', 'YES', T_EMPTY],
                         ['col1', 'int64', 'YES', T_EMPTY],
                         ['col2', 'string', 'YES', T_EMPTY],]
        self.check_out_of_order_result(resp, expect_result)

        # show create edge
        resp = self.client.execute_query('SHOW CREATE EDGE education')
        self.check_resp_succeeded(resp)
        create_edge_str = 'CREATE EDGE `education` (\n' \
                          ' `school` int64 NULL,\n' \
                          ' `col1` int64 NULL,\n' \
                          ' `col2` string NULL\n' \
                          ') ttl_duration = 0, ttl_col = "\"'
        expect_result = [['education', create_edge_str]]
        self.check_result(resp, expect_result)

        # check result from show create
        resp = self.client.execute('DROP EDGE education')
        self.check_resp_succeeded(resp)
        resp = self.client.execute(create_edge_str)
        self.check_resp_succeeded(resp)

        # with negative DEFAULT value
        resp = self.client.execute('CREATE EDGE default_edge_neg(id int DEFAULT -10, '
                                   'height double DEFAULT -176.0)')
        self.check_resp_succeeded(resp)

        # Tag with expression DEFAULT value
        resp = self.client.execute('CREATE EDGE default_edge_expr'
                                   '(id int DEFAULT 3/2*4-5, '
                                   'male bool DEFAULT 3 > 2, '
                                   'height double DEFAULT abs(-176.0), '
                                   'adult bool DEFAULT true AND false)')
        self.check_resp_succeeded(resp)

    def test_drop_edge(self):
        resp = self.client.execute('DROP EDGE buy')
        self.check_resp_succeeded(resp)

        # drop not exist edge
        resp = self.client.execute('DROP EDGE not_exist_edge')
        self.check_resp_failed(resp)

        # drop if exists
        resp = self.client.execute('DROP EDGE IF EXISTS not_exist_edge')
        self.check_resp_succeeded(resp)

        # drop if exists
        resp = self.client.execute('CREATE EDGE exist_edge(id int)')
        self.check_resp_succeeded(resp)
        resp = self.client.execute('DROP EDGE IF EXISTS exist_edge')
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
            resp = self.client.execute_query('DESC TAG tag10' + str(i))
            self.check_resp_succeeded(resp)
            expect_result = [['name', 'string', 'YES', T_EMPTY]]
            self.check_result(resp, expect_result)

    def test_same_tag_in_different_space(self):
        resp = self.client.execute('CREATE SPACE my_space(partition_num=9, replica_factor=1)')
        self.check_resp_succeeded(resp)

        # 2.0 use space get from cache
        time.sleep(self.delay)

        resp = self.client.execute('USE my_space')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('CREATE TAG animal(name string, kind string)')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('DESCRIBE TAG animal')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', T_EMPTY],
                         ['kind', 'string', 'YES', T_EMPTY]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.client.execute('CREATE TAG person(name string, interest string)')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('SHOW TAGS')
        self.check_resp_succeeded(resp)
        expect_result = [['animal'],['person']]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.client.execute('CREATE SPACE test_multi')
        self.check_resp_succeeded(resp)

        # 2.0 use space get from cache
        time.sleep(self.delay)

        resp = self.client.execute_query('USE test_multi; CREATE Tag test_tag(); SHOW TAGS;')
        self.check_resp_succeeded(resp)
        expect_result = [['test_tag']]
        self.check_result(resp, expect_result)

        resp = self.client.execute_query('USE test_multi; CREATE TAG test_tag1(); USE my_space; SHOW TAGS;')
        self.check_resp_succeeded(resp)
        expect_result = [['animal'], ['person']]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.client.execute('DROP SPACE test_multi')
        self.check_resp_succeeded(resp)

    def test_reserved_keyword(self):
        resp = self.client.execute('USE my_space; CREATE TAG `tag` (`edge` string)')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('DESCRIBE TAG `tag`')
        self.check_resp_succeeded(resp)
        expect_result = [['edge', 'string', 'YES', T_EMPTY]]
        self.check_result(resp, expect_result)

    def test_drop_space(self):
        resp = self.client.execute_query('SHOW SPACES')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DROP SPACE my_space')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('SHOW SPACES')
        self.check_resp_succeeded(resp)
        expect_result = [['tag_space']]
        self.search_result(resp, expect_result)

        # checkout current session has clear the space info
        assert resp.space_name.decode('utf-8') == ""

    def test_alter_tag_with_default(self):
        resp = self.client.execute('USE tag_space; CREATE TAG t(name string DEFAULT "N/A", age int DEFAULT -1)')
        self.check_resp_succeeded(resp)

        # alter add
        resp = self.client.execute('ALTER TAG t ADD (description string DEFAULT "none")')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

        # insert
        resp = self.client.execute('INSERT VERTEX t() VALUES "1":()')
        self.check_resp_succeeded(resp)

        # fetch
        resp = self.client.execute_query('FETCH PROP ON t "1"')
        self.check_resp_succeeded(resp)
        expect_result = [['1', 'N/A', -1, 'none']]
        self.check_out_of_order_result(resp, expect_result)

        # alter drop
        resp = self.client.execute('ALTER TAG t CHANGE (description string NOT NULL)')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)
        # insert wrong tyoe
        resp = self.client.execute('INSERT VERTEX t(description) VALUES "1":("some one")')
        self.check_resp_succeeded(resp)

        # fetch
        resp = self.client.execute_query('FETCH PROP ON t "1"')
        self.check_resp_succeeded(resp)
        expect_result = [['1', 'N/A', -1, 'some one']]
        self.check_out_of_order_result(resp, expect_result)

        # insert without default prop, failed
        resp = self.client.execute('INSERT VERTEX t() VALUES "1":()')
        self.check_resp_failed(resp)

    def test_alter_edge_with_default(self):
        resp = self.client.execute('CREATE EDGE e(name string DEFAULT "N/A", age int DEFAULT -1)')
        self.check_resp_succeeded(resp)

        # alter add
        resp = self.client.execute('ALTER EDGE e ADD (description string DEFAULT "none")')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

        # insert
        resp = self.client.execute('INSERT EDGE e() VALUES "1"->"2":()')
        self.check_resp_succeeded(resp)

        # fetch
        resp = self.client.execute_query('FETCH PROP ON e "1"->"2"')
        self.check_resp_succeeded(resp)
        expect_result = [['1', '2', 0, 'N/A', -1, 'none']]
        self.check_out_of_order_result(resp, expect_result)

        # alter drop
        resp = self.client.execute('ALTER EDGE e CHANGE (description string NOT NULL)')
        self.check_resp_succeeded(resp)

        # sleep to get schema in cache
        time.sleep(self.delay)

        # insert successed
        resp = self.client.execute('INSERT EDGE e(description) VALUES "1"->"2":("some one")')
        self.check_resp_succeeded(resp)

        # fetch
        resp = self.client.execute_query('FETCH PROP ON e "1"->"2"')
        self.check_resp_succeeded(resp)
        expect_result = [['1', '2', 0, 'N/A', -1, 'some one']]
        self.check_out_of_order_result(resp, expect_result)

        # insert without default prop, failed
        resp = self.client.execute('INSERT EDGE e() VALUES "1"->"2":()')
        self.check_resp_failed(resp)

    def test_create_tag_with_function_default_value(self):
        resp = self.client.execute('CREATE TAG tag_function(name string DEFAULT "N/A", '
                                   'birthday timestamp DEFAULT now())')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('DESC TAG tag_function;')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', 'N/A'],
                         ['birthday', 'timestamp', 'YES', 'now()']]
        self.check_result(resp, expect_result)

        resp = self.client.execute_query('SHOW CREATE TAG tag_function;')
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
        resp = self.client.execute_query('FETCH PROP ON tag_function "a"')
        self.check_resp_succeeded(resp)
        expect_result = [[re.compile('a'), re.compile('N/A'), re.compile(r'\d+')]]
        self.check_result(resp, expect_result, is_regex = True)
        assert(resp.data.rows[0].values[2].get_iVal() > current_timestamp)

    def test_create_edge_with_function_default_value(self):
        resp = self.client.execute('CREATE EDGE edge_function(name string DEFAULT "N/A", '
                                   'birthday timestamp DEFAULT now())')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('DESC EDGE edge_function;')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', 'N/A'],
                         ['birthday', 'timestamp', 'YES', 'now()']]
        self.check_result(resp, expect_result)

        resp = self.client.execute_query('SHOW CREATE EDGE edge_function;')
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
        resp = self.client.execute_query('FETCH PROP ON edge_function "a"->"b"')
        self.check_resp_succeeded(resp)
        expect_result = [[re.compile('a'), re.compile('b'), re.compile(r'\d+'), re.compile('N/A'), re.compile(r'\d+')]]
        self.check_result(resp, expect_result, is_regex = True)
        assert(resp.data.rows[0].values[4].get_iVal() > current_timestamp)


    def test_alter_edge_with_timestamp_default(self):
        resp = self.client.execute('CREATE SPACE issue2009(vid_type = FIXED_STRING(20)); USE issue2009')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('CREATE EDGE IF NOT EXISTS relation'
                                   '(intimacy int DEFAULT 0, '
                                   'isReversible bool DEFAULT false, '
                                   'name string DEFAULT \"N/A\", '
                                   'startTime timestamp DEFAULT 0)')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)
        resp = self.client.execute('INSERT EDGE relation (intimacy) VALUES '
                                   '"person.Tom" -> "person.Marry"@0:(3)')
        self.check_resp_succeeded(resp)

    def test_create_tag_with_null_default_value(self):
        resp = self.client.execute('CREATE TAG tag_null_default1(name string NULL DEFAULT "N/A")')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('DESC TAG tag_null_default1;')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', 'N/A']]
        self.check_result(resp, expect_result)

        resp = self.client.execute('CREATE TAG tag_null_default2(name string DEFAULT "N/A" NULL)')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('DESC TAG tag_null_default2;')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'YES', 'N/A']]
        self.check_result(resp, expect_result)

    def test_create_tag_with_not_null_default_value(self):
        resp = self.client.execute('CREATE TAG tag_not_null_default1(name string NOT NULL DEFAULT "N/A")')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('DESC TAG tag_not_null_default1;')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'NO', 'N/A']]
        self.check_result(resp, expect_result)

        resp = self.client.execute('CREATE TAG tag_not_null_default2(name string DEFAULT "N/A" NOT NULL)')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('DESC TAG tag_not_null_default2;')
        self.check_resp_succeeded(resp)
        expect_result = [['name', 'string', 'NO', 'N/A']]
        self.check_result(resp, expect_result)

    def test_with_bad_null_default_value(self):
        # failed
        resp = self.execute('CREATE TAG bad_null_default_value(name string DEFAULT "N/A", age int DEFAULT 1%0)')
        self.check_resp_failed(resp)
        self.check_error_msg(resp, 'invalid parm!')

