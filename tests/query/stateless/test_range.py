# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import string
import random

import pytest

from nebula2.common import ttypes

from tests.common.nebula_test_suite import NebulaTestSuite


class TestRangeChecking(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE test_range_checking(partition_num={partition_num}, replica_factor={replica_factor}, vid_type=FIXED_STRING(8))'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp = self.execute('USE test_range_checking')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG test(id int)')
        self.check_resp_succeeded(resp)

    def gen_name(self, length: int) -> str:
        return ''.join(random.choice(string.ascii_letters) for i in range(length))

    @pytest.mark.parametrize('length', [1, 2048, 4096])
    def test_label_length_valid(self, length):
        query = 'CREATE TAG {}(id int)'.format(self.gen_name(length))
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    @pytest.mark.parametrize('length', [4097, 4444, 10240])
    def test_label_length_invalid(self, length):
        query = 'CREATE TAG {}(id int)'.format(self.gen_name(length))
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE test_range_checking')
        self.check_resp_succeeded(resp)
