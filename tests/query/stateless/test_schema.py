# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY


class TestSchema(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS schema_space(partition_num={partition_num}, replica_factor={replica_factor}, vid_type=FIXED_STRING(8))'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('USE schema_space')
        self.check_resp_succeeded(resp)

    def test_create_tag_succeed(self):
        # create tag without prop
        resp = self.execute('CREATE TAG TAG_empty()')
        self.check_resp_succeeded(resp)
        resp = self.execute('DESC TAG TAG_empty')
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        # create tag with all type
        resp = self.execute('CREATE TAG TAG_all_type(name string, age int, '
                                  'is_man bool, account double, birthday timestamp)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC TAG TAG_all_type')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['age', 'int64', 'YES', T_EMPTY, T_EMPTY],
                  ['is_man', 'bool', 'YES', T_EMPTY, T_EMPTY],
                  ['account', 'double', 'YES', T_EMPTY, T_EMPTY],
                  ['birthday', 'timestamp', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # create tag with default value
        resp = self.execute('CREATE TAG TAG_default(name string, age int, gender string DEFAULT "male")')
        self.check_resp_succeeded(resp)

        resp = self.execute('SHOW CREATE TAG TAG_default')
        self.check_resp_succeeded(resp)
        expect = [['TAG_default', 'CREATE TAG `TAG_default` (\n `name` string NULL,\n `age` int64 NULL,\n '
                                  '`gender` string NULL DEFAULT "male"\n) ttl_duration = 0, ttl_col = ""']]
        self.check_result(resp, expect)

        # create tag with ttl
        resp = self.execute('CREATE TAG person(name string, email string, '
                                  'age int, gender string, birthday timestamp) '
                                  'ttl_duration = 100, ttl_col = "birthday"')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC TAG person')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['email', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['age', 'int64', 'YES', T_EMPTY, T_EMPTY],
                  ['gender', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['birthday', 'timestamp', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # show all tags
        resp = self.execute('SHOW TAGS')
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, [['TAG_empty'], ['TAG_all_type'], ['TAG_default'], ['person']])

    def test_create_tag_failed(self):
        # create tag without prop
        try:
            resp = self.execute('CREATE TAG TAG_empty')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # create tag with wrong type
        try:
            resp = self.execute('CREATE TAG TAG_wrong_type(name list)')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # create tag with wrong default value type
        try:
            resp = self.execute('CREATE TAG TAG_wrong_default_type(name string, '
                                'gender string default false) ')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # create tag with wrong ttl type
        try:
            resp = self.execute('CREATE TAG TAG_wrong_ttl(name string, gender string) '
                                'ttl_duration = 100, ttl_col = "gender"')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

    def test_alter_tag_succeed(self):
        # create tag
        resp = self.execute('CREATE TAG student(name string, email string, '
                            'age int, gender string, birthday timestamp) '
                            'ttl_duration = 100, ttl_col = "birthday"')
        self.check_resp_succeeded(resp)

        # alter drop
        resp = self.execute('ALTER TAG student DROP (age, gender)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC TAG student')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['email', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['birthday', 'timestamp', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # alter add
        resp = self.execute('ALTER TAG student add (age string)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC TAG student')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['email', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['birthday', 'timestamp', 'YES', T_EMPTY, T_EMPTY],
                  ['age', 'string', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # alter change
        resp = self.execute('ALTER TAG student change (age int)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC TAG student')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['email', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['birthday', 'timestamp', 'YES', T_EMPTY, T_EMPTY],
                  ['age', 'int64', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # alter all
        resp = self.execute('ALTER TAG student drop (name),'
                            'ADD (gender string),'
                            'CHANGE (gender int)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC TAG student')
        self.check_resp_succeeded(resp)
        expect = [['email', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['birthday', 'timestamp', 'YES', T_EMPTY, T_EMPTY],
                  ['age', 'int64', 'YES', T_EMPTY, T_EMPTY],
                  ['gender', 'int64', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

    def test_alter_tag_failed(self):
        # alter ttl_col on wrong type
        try:
            resp = self.execute('ALTER TAG student ttl_col email')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # alter drop nonexistent col
        try:
            resp = self.execute('ALTER TAG student drop name')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # alter add existent col
        try:
            resp = self.execute('ALTER TAG student add (email, int)')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

    def test_create_edge_succeed(self):
        # create edge without prop
        resp = self.execute('CREATE EDGE EDGE_empty()')
        self.check_resp_succeeded(resp)
        resp = self.execute('DESC EDGE EDGE_empty')
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        # create tag with all type
        resp = self.execute('CREATE EDGE EDGE_all_type(name string, age int, '
                                  'is_man bool, account double, birthday timestamp)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC EDGE EDGE_all_type')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['age', 'int64', 'YES', T_EMPTY, T_EMPTY],
                  ['is_man', 'bool', 'YES', T_EMPTY, T_EMPTY],
                  ['account', 'double', 'YES', T_EMPTY, T_EMPTY],
                  ['birthday', 'timestamp', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # create tag with default value
        resp = self.execute('CREATE EDGE EDGE_default(name string, age int, gender string DEFAULT "male")')
        self.check_resp_succeeded(resp)

        resp = self.execute('SHOW CREATE EDGE EDGE_default')
        self.check_resp_succeeded(resp)
        expect = [['EDGE_default', 'CREATE EDGE `EDGE_default` (\n `name` string NULL,\n `age` int64 NULL,\n '
                                   '`gender` string NULL DEFAULT "male"\n) ttl_duration = 0, ttl_col = ""']]
        self.check_result(resp, expect)

        # create tag with ttl
        resp = self.execute('CREATE EDGE human(name string, email string, '
                                  'age int, gender string, birthday timestamp) '
                                  'ttl_duration = 100, ttl_col = "birthday"')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC EDGE human')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['email', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['age', 'int64', 'YES', T_EMPTY, T_EMPTY],
                  ['gender', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['birthday', 'timestamp', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # show all tags
        resp = self.execute('SHOW EDGES')
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, [['EDGE_empty'], ['EDGE_all_type'], ['EDGE_default'], ['human']])

    def test_create_edge_failed(self):
        # create edge without prop
        try:
            resp = self.execute('CREATE EDGE EDGE_empty')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # create edge with wrong type
        try:
            resp = self.execute('CREATE EDGE EDGE_wrong_type(name list)')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # create edge with wrong default value type
        try:
            resp = self.execute('CREATE EDGE EDGE_wrong_default_type(name string, '
                                'gender string default false) ')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # create edge with wrong ttl type
        try:
            resp = self.execute('CREATE edge EDGE_wrong_ttl(name string, gender string) '
                                'ttl_duration = 100, ttl_col = "gender"')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

    def test_alter_edge_succeed(self):
        # create edge
        resp = self.execute('CREATE EDGE relationship(name string, email string,'
                            'start_year int, end_year int) ')
        self.check_resp_succeeded(resp)

        # alter drop
        resp = self.execute('ALTER EDGE relationship DROP (start_year, end_year)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC EDGE relationship')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY], ['email', 'string', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # alter add
        resp = self.execute('ALTER EDGE relationship ADD (start_year string)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC EDGE relationship')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['email', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['start_year', 'string', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # alter change
        resp = self.execute('ALTER EDGE relationship change (start_year int)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC EDGE relationship')
        self.check_resp_succeeded(resp)
        expect = [['name', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['email', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['start_year', 'int64', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

        # alter all
        resp = self.execute('ALTER EDGE relationship drop (name),'
                            'ADD (end_year string),'
                            'CHANGE (end_year int)')
        self.check_resp_succeeded(resp)

        resp = self.execute('DESC EDGE relationship')
        self.check_resp_succeeded(resp)
        expect = [['email', 'string', 'YES', T_EMPTY, T_EMPTY],
                  ['start_year', 'int64', 'YES', T_EMPTY, T_EMPTY],
                  ['end_year', 'int64', 'YES', T_EMPTY, T_EMPTY]]
        self.check_result(resp, expect)

    def test_alter_edge_failed(self):
        # alter ttl_col on wrong type
        try:
            resp = self.execute('ALTER EDGE relationship ttl_col email')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # alter drop nonexistent col
        try:
            resp = self.execute('ALTER EDGE relationship drop name')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

        # alter add existent col
        try:
            resp = self.execute('ALTER EDGE relationship add (email, int)')
            self.check_resp_failed(resp)
        except Exception as x:
            print('failed', x)

    # Cover https://github.com/vesoft-inc/nebula/issues/1732
    def test_cover_fix_negative_default_value(self):
        # Tag
        resp = self.execute('CREATE TAG tag_default_neg(id int DEFAULT -3, '
                            'height double DEFAULT -176.0)')
        self.check_resp_succeeded(resp)
        # Edge
        resp = self.execute('CREATE EDGE edge_default_neg(id int DEFAULT -3, '
                            'height double DEFAULT -176.0)')
        self.check_resp_succeeded(resp)

    def test_default_expression_value(self):
        # Tag
        resp = self.execute('CREATE TAG default_tag_expr'
            '(id int DEFAULT 3/2*4-5, '  # arithmetic
            'male bool DEFAULT 3 > 2, '  # relation
            'height double DEFAULT abs(-176.0), '  # built-in function
            'adult bool DEFAULT true AND false)')  # logic
        self.check_resp_succeeded(resp)
        # Edge
        resp = self.execute('CREATE EDGE default_edge_expr'
            '(id int DEFAULT 3/2*4-5, '  # arithmetic
            'male bool DEFAULT 3 > 2, '  # relation
            'height double DEFAULT abs(-176.0), '  # built-in function
            'adult bool DEFAULT true AND false)')  # logic
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space schema_space')
        self.check_resp_succeeded(resp)
