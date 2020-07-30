# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import sys
import time
import string
import random

import pytest

from graph import ttypes

from tests.common.nebula_test_suite import NebulaTestSuite

class TestRangeChecking(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE test_range_checking(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp = self.execute('USE test_range_checking')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG test(id int)')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    @pytest.mark.parametrize('length', [1, 2048, 4096])
    def test_label_length_valid(self, length):
        query = 'CREATE TAG {}(id int)'.format(''.join(random.choice(string.ascii_letters) for i in range(length)))
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    @pytest.mark.parametrize('length', [4097, 4444, 10240])
    def test_label_length_invalid(self, length):
        query = 'CREATE TAG {}(id int)'.format(''.join(random.choice(string.ascii_letters) for i in range(length)))
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)

    @pytest.mark.parametrize('number', [-9223372036854775808, -1, 0, 1, 9223372036854775807])
    def test_integer_range_valid(self, number):
        # integer bound checking
        resp = self.execute("INSERT VERTEX test(id) VALUES '100':({})".format(number))
        self.check_resp_succeeded(resp)

        # hex bound checking
        resp = self.execute("INSERT VERTEX test(id) VALUES '100':({:#x})".format(number))
        self.check_resp_succeeded(resp)

        # oct bound checking
        # TODO(shylock) Trick to make oct number valid
        # I don't think the zero prefix oct number is good design
        # plan to change to `0o' prefix
        if (number < 0):
            query = "INSERT VERTEX test(id) VALUES '100':(-0{:o})".format(abs(number))
        else:
            query = "INSERT VERTEX test(id) VALUES '100':(0{:o})".format(abs(number))
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    @pytest.mark.parametrize('number', [-9223372036854999999, -9223372036854775809, 9223372036854775808, 9223372036899999999])
    def test_integer_range_invalid(self, number):
        # integer bound
        resp = self.execute("INSERT VERTEX test(id) VALUES '100':({})".format(number));
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)
        error = "SyntaxError: Out of range: near `{}'".format(number);
        assert resp.error_msg, error

        # hex bound
        resp = self.execute("INSERT VERTEX test(id) VALUES '100':({:#x})".format(number));
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)
        error = "SyntaxError: Out of range: near `{:#x}'".format(number);
        assert resp.error_msg, error

        # oct bound
        # TODO(shylock) Trick to make oct number valid
        # I don't think the zero prefix oct number is good design
        # plan to change to `0o' prefix
        if number < 0:
            query = "INSERT VERTEX test(id) VALUES '100':(-0{:o})".format(abs(number))
        else:
            query = "INSERT VERTEX test(id) VALUES '100':(0{:o})".format(abs(number))
        resp = self.execute(query);
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)
        if number < 0:
            error = "SyntaxError: Out of range: near `-0{:o}'".format(abs(number))
        else:
            error = "SyntaxError: Out of range: near `0{:o}'".format(abs(number))
        assert resp.error_msg, error

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE test_range_checking')
        self.check_resp_succeeded(resp)
