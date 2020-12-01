# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import pytest

from tests.common.nebula_test_suite import NebulaTestSuite, T_NULL


class TestInsert2(NebulaTestSuite):
    '''
        The tests for nebula2
    '''
    @classmethod
    def prepare(self):
        resp = self.execute('CREATE SPACE IF NOT EXISTS mySpace2(partition_num=1, vid_type=FIXED_STRING(10));'
                            'USE mySpace2;'
                            'CREATE TAG student(name string NOT NULL, age int);'
                            'CREATE TAG course(name fixed_string(5) NOT NULL, introduce string DEFAULT NULL);')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE mySpace2')
        self.check_resp_succeeded(resp)

    def test_insert_out_of_range_id_size(self):
        resp = self.execute('INSERT VERTEX student(name, age) VALUES "12345678901":("Tom", 2)')
        self.check_resp_failed(resp)
        self.check_error_msg(resp, 'Storage Error: Invalid vid.')

    def test_insert_not_null_prop(self):
        resp = self.execute('INSERT VERTEX student(name, age) VALUES "Tom":(NULL, 12)')
        self.check_resp_failed(resp)
        self.check_error_msg(resp, "Storage Error: The not null field cannot be null.")

    def test_insert_with_fix_string(self):
        # succeeded
        resp = self.execute('INSERT VERTEX course(name) VALUES "Math":("Math")')
        self.check_resp_succeeded(resp)

        # out of range
        resp = self.execute('INSERT VERTEX course(name) VALUES "English":("English")')
        self.check_resp_succeeded(resp)

        # check
        resp = self.execute('FETCH PROP ON course "English"')
        self.check_resp_succeeded(resp)
        expect_result = [['English', 'Engli', T_NULL]]
        self.check_out_of_order_result(resp, expect_result)

    def test_insert_with_empty_str_vid(self):
        resp = self.execute('INSERT VERTEX student(name, age) VALUES "":("Tom", 12)')
        self.check_resp_succeeded(resp)

        # check
        resp = self.execute('FETCH PROP ON student ""')
        self.check_resp_succeeded(resp)
        expect_result = [['', 'Tom', 12]]
        self.check_out_of_order_result(resp, expect_result)
