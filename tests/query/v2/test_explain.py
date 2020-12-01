# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

import pytest
from nebula2.graph import ttypes

from tests.common.nebula_test_suite import NebulaTestSuite


FORMAT = [
    ('', None),
    ('FORMAT="row"', None),
    ('FORMAT="dot"', None),
    ('FORMAT="dot:struct"', None),
    ('FORMAT="unknown"', ttypes.ErrorCode.E_SYNTAX_ERROR),
]


class TestExplain(NebulaTestSuite):
    @classmethod
    def prepare(cls):
        # cls.load_data()
        resp = cls.execute('CREATE SPACE IF NOT EXISTS explain_test')
        cls.check_resp_succeeded(resp)

        # 2.0 use space get from cache
        time.sleep(cls.delay)

        resp = cls.execute('USE explain_test')
        cls.check_resp_succeeded(resp)

    @classmethod
    def cleanup(cls):
        resp = cls.execute('DROP SPACE IF EXISTS explain_test')
        cls.check_resp_succeeded(resp)

    def check(self, query, err):
        resp = self.execute(query, profile=False)
        if err is None:
            self.check_resp_succeeded(resp)
        else:
            self.check_resp_failed(resp, err)

    @pytest.mark.parametrize('keyword', ['EXPLAIN', 'PROFILE'])
    @pytest.mark.parametrize('fmt,err', FORMAT)
    def test_success_cases(self, keyword, fmt, err):
        query = '{} {} YIELD 1;'.format(keyword, fmt)
        self.check(query, err)

        query = '{} {} {{$var = YIELD 1 AS a; YIELD $var.*;}};'.format(keyword, fmt)
        self.check(query, err)

        query = '{} {} {{YIELD 1 AS a;}};'.format(keyword, fmt)
        self.check(query, err)

    @pytest.mark.parametrize('keyword', ['PROFILE', 'EXPLAIN'])
    def test_failure_cases(self, keyword):
        query = '{} EXPLAIN YIELD 1'.format(keyword)
        self.check(query, ttypes.ErrorCode.E_SYNTAX_ERROR)

        query = '{} PROFILE YIELD 1'.format(keyword)
        self.check(query, ttypes.ErrorCode.E_SYNTAX_ERROR)
