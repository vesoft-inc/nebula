# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

import pytest
from nebula2.graph import ttypes

from tests.common.nebula_test_suite import NebulaTestSuite


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

    def test_explain(self):
        query = 'EXPLAIN YIELD 1 AS id;'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="row" YIELD 1;'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="dot" YIELD 1;'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="unknown" YIELD 1;'
        resp = self.execute_query(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

    def test_explain_stmts_block(self):
        query = 'EXPLAIN {$var = YIELD 1 AS a; YIELD $var.*;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="row" {$var = YIELD 1 AS a; YIELD $var.*;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="dot" {$var = YIELD 1 AS a; YIELD $var.*;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="dot" {YIELD 1 AS a;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = '''EXPLAIN FORMAT="unknown" \
            {$var = YIELD 1 AS a; YIELD $var.*;};'''
        resp = self.execute_query(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

    def test_profile(self):
        query = 'PROFILE YIELD 1 AS id;'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'PROFILE FORMAT="row" YIELD 1;'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'PROFILE FORMAT="dot" YIELD 1;'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'PROFILE FORMAT="unknown" YIELD 1;'
        resp = self.execute_query(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

    def test_profile_stmts_block(self):
        query = 'PROFILE {$var = YIELD 1 AS a; YIELD $var.*;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'PROFILE FORMAT="row" {$var = YIELD 1 AS a; YIELD $var.*;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'PROFILE FORMAT="dot" {$var = YIELD 1 AS a; YIELD $var.*;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'PROFILE FORMAT="dot" { YIELD 1 AS a };'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = '''PROFILE FORMAT="unknown" \
            {$var = YIELD 1 AS a; YIELD $var.*;};'''
        resp = self.execute_query(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)
