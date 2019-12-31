# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import pytest
import sys
import math
from graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient, AuthException, ExecutionException
from typing import Pattern

class NebulaTestSuite(object):
    @classmethod
    def setup_class(self):
        address = pytest.cmdline.address.split(':')
        self.host = address[0]
        self.port = address[1]
        self.user = pytest.cmdline.user
        self.password = pytest.cmdline.password
        self.create_nebula_clients()
        self.client.authenticate(self.user, self.password)
        self.prepare()
        self.check_format_str = 'result: {}, expect: {}'

    @classmethod
    def create_nebula_clients(self):
        self.client_pool = ConnectionPool(self.host, self.port)
        self.client = GraphClient(self.client_pool)

    @classmethod
    def teardown_class(self):
        if self.client != None:
            self.cleanup()
            self.client.sign_out()
            self.client_pool.close()

    @classmethod
    def prepare(self):
        self.prepare()

    @classmethod
    def cleanup(self):
        self.cleanup()

    @classmethod
    def check_resp_successed(self, resp):
        assert resp.error_code == 0, resp.error_msg

    @classmethod
    def check_resp_failed(self, resp):
        assert resp.error_code != 0

    @classmethod
    def check_value(self, col, expect):
        if col.getType() == ttypes.ColumnValue.__EMPTY__:
            msg = 'ERROR: type is empty'
            return False, msg

        if col.getType() == ttypes.ColumnValue.BOOL_VAL:
            msg = self.check_format_str.format(col.get_bool_val(), expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.get_bool_val())):
                    return False, msg
            else:
                if col.get_bool_val() != expect:
                    return False, msg
            return True, ''

        if col.getType() == ttypes.ColumnValue.INTEGER:
            msg = self.check_format_str.format(col.get_integer(), expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.get_integer())):
                    return False, msg
            else:
                if col.get_integer() != expect:
                    return False, msg
            return True, ''

        if col.getType() == ttypes.ColumnValue.ID:
            msg = self.check_format_str.format(col.getid(), expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.gettid())):
                    return False, msg
            else:
                if col.gettid() != expect:
                    return False, msg
            return True, ''

        if col.getType() == ttypes.ColumnValue.STR:
            msg = self.check_format_str.format(col.get_str().decode('utf-8'),
                                               expect)
            if isinstance(expect, Pattern):
                if not expect.match(col.get_str().decode('utf-8')):
                    return False, msg
            else:
                if col.get_str().decode('utf-8') != expect:
                    return False, msg
            return True, ''

        if col.getType() == ttypes.ColumnValue.DOUBLE_PRECISION:
            msg = self.check_format_str.format(col.get_double_precision(),
                                               expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.get_double_precision())):
                    return False, msg
            else:
                if not math.isclose(col.get_double_precision(), expect):
                    return False, msg
            return True, ''

        if col.getType() == ttypes.ColumnValue.TIMESTAMP:
            msg = self.check_format_str.format(col.get_timestamp(), expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.get_timestamp())):
                    return False, msg
            else:
                if col.get_timestamp() != expect:
                    return False, msg
            return True, ''
        return False, 'ERROR: Type unsupported'

    @classmethod
    def search_result(self, rows, expect):
        msg = 'len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect))
        assert len(rows) == len(expect), msg
        for row in rows:
            ok = False
            msg = ""
            for exp in expect:
                for col, i in zip(row.columns, range(0, len(exp))):
                    ok, msg = self.check_value(col, exp[i])
                    if not ok:
                        break
                if ok:
                    break
            assert ok, msg
            expect.remove(exp)

    @classmethod
    def check_result(self, rows, expect):
        msg = 'len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect))
        assert len(rows) == len(expect), msg
        for row, i in zip(rows, range(0, len(expect))):
            for col, j in zip(row.columns, range(0, len(expect[i]))):
                ok, msg = self.check_value(col, expect[i][j])
                assert ok, msg
