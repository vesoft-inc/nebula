# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import math
import sys
import time
from pathlib import Path
from typing import Pattern, Set

import pytest
from nebula2.Client import AuthException, ExecutionException, GraphClient
from nebula2.common import ttypes as CommonTtypes
from nebula2.Common import *
from nebula2.ConnectionPool import ConnectionPool
from nebula2.graph import ttypes


T_EMPTY = CommonTtypes.Value()
T_NULL = CommonTtypes.Value()
T_NULL.set_nVal(CommonTtypes.NullType.__NULL__)
T_NULL_NaN = CommonTtypes.Value()
T_NULL_NaN.set_nVal(CommonTtypes.NullType.NaN)
T_NULL_BAD_DATA = CommonTtypes.Value()
T_NULL_BAD_DATA.set_nVal(CommonTtypes.NullType.BAD_DATA)
T_NULL_BAD_TYPE = CommonTtypes.Value()
T_NULL_BAD_TYPE.set_nVal(CommonTtypes.NullType.BAD_TYPE)
T_NULL_UNKNOWN_PROP = CommonTtypes.Value()
T_NULL_BAD_TYPE.set_nVal(CommonTtypes.NullType.UNKNOWN_PROP)
T_NULL_UNKNOWN_DIV_BY_ZERO = CommonTtypes.Value()
T_NULL_UNKNOWN_DIV_BY_ZERO.set_nVal(CommonTtypes.NullType.DIV_BY_ZERO)

class NebulaTestSuite(object):
    @classmethod
    def set_delay(self):
        # resp = self.client.execute_query(
        #     'get configs GRAPH:heartbeat_interval_secs')
        # self.check_resp_succeeded(resp)
        # assert len(resp.rows) == 1, "invalid row size: {}".format(resp.rows)
        # self.graph_delay = int(resp.rows[0].values[4].get_sVal()) + 1
        self.graph_delay = 3

        # resp = self.client.execute_query(
        #     'get configs STORAGE:heartbeat_interval_secs')
        # self.check_resp_succeeded(resp)
        # assert len(resp.rows) == 1, "invalid row size: {}".format(resp.rows)
        # self.storage_delay = int(resp.rows[0].values[4].get_sVal()) + 1
        self.storage_delay = 3
        self.delay = max(self.graph_delay, self.storage_delay) * 2

    @classmethod
    def setup_class(self):
        address = pytest.cmdline.address.split(':')
        self.host = address[0]
        self.port = address[1]
        self.user = pytest.cmdline.user
        self.password = pytest.cmdline.password
        self.replica_factor = pytest.cmdline.replica_factor
        self.partition_num = pytest.cmdline.partition_num
        self.check_format_str = 'result: {}, expect: {}'
        self.data_dir = pytest.cmdline.data_dir
        self.data_loaded = False
        self.create_nebula_clients()
        self.set_delay()
        self.prepare()

    @classmethod
    def load_data(self):
        self.data_loaded = True
        pathlist = Path(self.data_dir).rglob('*.ngql')
        for path in pathlist:
            print("will open ", path)
            with open(path, 'r') as data_file:
                space_name = path.name.split('.')[0]
                resp = self.execute(
                'CREATE SPACE IF NOT EXISTS {space_name}(partition_num={partition_num}, replica_factor={replica_factor}, vid_size=30)'.format(partition_num=self.partition_num,
                        replica_factor=self.replica_factor,
                        space_name=space_name))
                self.check_resp_succeeded(resp)
                time.sleep(self.delay)
                resp = self.execute('USE {}'.format(space_name))
                self.check_resp_succeeded(resp)

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
                                time.sleep(self.delay)
                                ddl = False
                    else:
                        line = line.rstrip()
                        ngql_statement += " " + line
                        if line.endswith(';'):
                            resp = self.execute(ngql_statement)
                            self.check_resp_succeeded(resp)
                            ngql_statement = ""

    @classmethod
    def drop_data(self):
        if self.data_loaded:
            pathlist = Path(self.data_dir).rglob('*.ngql')
            drop_stmt = []
            for path in pathlist:
                space_name = path.name.split('.')[0]
                drop_stmt.append('DROP SPACE {}'.format(space_name))
            resp = self.execute(';'.join(drop_stmt))
            self.check_resp_succeeded(resp)

    @classmethod
    def create_nebula_clients(self):
        self.client_pool = ConnectionPool(ip=self.host,
                                          port=self.port,
                                          network_timeout=0)
        self.client = GraphClient(self.client_pool)
        self.client.authenticate(self.user, self.password)

    @classmethod
    def close_nebula_clients(self):
        self.client.sign_out()
        self.client_pool.close()

    @classmethod
    def teardown_class(self):
        if self.client is not None:
            self.cleanup()
            self.drop_data()
            self.close_nebula_clients()

    @classmethod
    def execute(self, ngql):
        return self.client.execute(ngql)

    @classmethod
    def execute_query(self, ngql):
        return self.client.execute_query(ngql)

    @classmethod
    def prepare(cls):
        if hasattr(cls.prepare, 'is_overridden'):
            cls.prepare()

    @classmethod
    def cleanup(cls):
        if hasattr(cls.cleanup, 'is_overridden'):
            cls.cleanup()

    @classmethod
    def check_resp_succeeded(self, resp):
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED \
               or resp.error_code == ttypes.ErrorCode.E_STATEMENT_EMTPY, resp.error_msg

    @classmethod
    def check_resp_failed(self, resp, error_code: ttypes.ErrorCode = ttypes.ErrorCode.SUCCEEDED):
        if error_code == ttypes.ErrorCode.SUCCEEDED:
            assert resp.error_code != error_code, '{} == {}, {}'.format(
                ttypes.ErrorCode._VALUES_TO_NAMES[resp.error_code],
                ttypes.ErrorCode._VALUES_TO_NAMES[error_code],
                resp.error_msg)
        else:
            assert resp.error_code == error_code, '{} != {}, {}'.format(
                ttypes.ErrorCode._VALUES_TO_NAMES[resp.error_code],
                ttypes.ErrorCode._VALUES_TO_NAMES[error_code],
                resp.error_msg)

    @classmethod
    def check_value(self, col, expect):
        if isinstance(expect, Pattern):
            if col.getType() == CommonTtypes.Value.BVAL:
                msg = self.check_format_str.format(col.get_bVal(), expect)
                if not expect.match(str(col.get_bVal())):
                    return False, msg
                return True, ''

            if col.getType() == CommonTtypes.Value.IVAL:
                msg = self.check_format_str.format(col.get_iVal(), expect)
                if not expect.match(str(col.get_iVal())):
                    return False, msg
                return True, ''

            if col.getType() == CommonTtypes.Value.SVAL:
                msg = self.check_format_str.format(col.get_sVal().decode('utf-8'),
                                                   expect)
                if not expect.match(col.get_sVal().decode('utf-8')):
                    return False, msg
                return True, ''

            if col.getType() == CommonTtypes.Value.FVAL:
                msg = self.check_format_str.format(col.get_fVal(),
                                                   expect)
                if not expect.match(str(col.get_fVal())):
                    return False, msg
                return True, ''

            return False, 'ERROR: Type unsupported'

        msg = self.check_format_str.format(col, expect)
        if col == expect:
            return True, ''
        else:
            return False, msg

    @classmethod
    def row_to_string(self, row):
        value_list = []
        for col in row.values:
            if col.getType() == CommonTtypes.Value.__EMPTY__:
                value_list.append('__EMPTY__')
            elif col.getType() == CommonTtypes.Value.NVAL:
                value_list.append('__NULL__')
            elif col.getType() == CommonTtypes.Value.BVAL:
                value_list.append(col.get_bVal())
            elif col.getType() == CommonTtypes.Value.IVAL:
                value_list.append(col.get_iVal())
            elif col.getType() == CommonTtypes.Value.FVAL:
                value_list.append(col.get_fVal())
            elif col.getType() == CommonTtypes.Value.SVAL:
                value_list.append(col.get_sVal().decode('utf-8'))
            elif col.getType() == CommonTtypes.Type.DVAL:
                value_list.append(col.get_dVal().decode('utf-8'))
            elif col.getType() == CommonTtypes.Type.DATETIME:
                value_list.append(col.get_datetime())
        return str(value_list)

    @classmethod
    def search_result(self, resp, expect, is_regex = False):
        if resp.data is None and len(expect) == 0:
            return

        if resp.data is None:
            assert False, 'resp.data is None'
        rows = resp.data.rows

        msg = 'len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect))
        assert len(rows) == len(expect), msg

        new_expect = expect
        if not is_regex:
            # convert expect to thrift value
            ok, new_expect, msg = self.convert_expect(expect)
            if not ok:
                assert ok, 'convert expect failed, error msg: {}'.format(msg)

        for row in rows:
            ok = False
            msg = ""
            for exp in new_expect:
                exp_values = []
                if not is_regex:
                    exp_values = exp.values
                else:
                    exp_values = exp
                for col, i in zip(row.values, range(0, len(exp_values))):
                    ok, msg = self.check_value(col, exp_values[i])
                    if not ok:
                        break
                if ok:
                    break
            assert ok, 'The returned row from nebula could not be found, row: {}, error message: {}'.format(
                self.row_to_string(row), msg)
            new_expect.remove(exp)

    @classmethod
    def check_column_names(self, resp, expect):
        for i in range(len(expect)):
            result = bytes.decode(resp.data.column_names[i])
            ok = (expect[i] == result)
            assert ok, "different column name, expect: {} vs. result: {}".format(expect[i], result)

    @classmethod
    def convert_expect(self, expect):
        result = []
        for row in expect:
            if not isinstance(row, list):
                return False, [], '{} is not list type'.format(str(row))
            new_row = CommonTtypes.Row()
            new_row.values = []
            for col in row:
                if isinstance(col, CommonTtypes.Value):
                    new_row.values.append(col)
                else:
                    value = CommonTtypes.Value()
                    if isinstance(col, bool):
                        value.set_bVal(col)
                    elif isinstance(col, int):
                        value.set_iVal(col)
                    elif isinstance(col, float):
                        value.set_fVal(col)
                    elif isinstance(col, str):
                        value.set_sVal(col.encode('utf-8'))
                    new_row.values.append(value)
            result.append(new_row)
        return True, result, ''

    @classmethod
    def check_result(self, resp, expect, ignore_col: Set[int] = set(), is_regex = False):
        if resp.data is None and len(expect) == 0:
            return

        if resp.data is None:
            assert False, 'resp.data is None'
        rows = resp.data.rows
        if not is_regex:
            msg = 'len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect))
            assert len(rows) == len(expect), msg

        new_expect = expect
        if not is_regex:
            # convert expect to thrift value
            ok, new_expect, msg = self.convert_expect(expect)
            if not ok:
                assert ok, 'convert expect failed, error msg: {}'.format(msg)

        for row, i in zip(rows, range(0, len(new_expect))):
            if isinstance(new_expect[i], CommonTtypes.Row):
                assert len(row.values) - len(ignore_col) == len(new_expect[i].values)
            else:
                assert len(row.values) - len(ignore_col) == len(new_expect[i])
            ignored_col_count = 0
            for j, col in enumerate(row.values):
                if j in ignore_col:
                    ignored_col_count += 1
                    continue
                exp_val = None
                expect_to_string = ''
                if isinstance(new_expect[i], CommonTtypes.Row):
                    exp_val = new_expect[i].values[j - ignored_col_count]
                    expect_to_string = self.row_to_string(new_expect[i])
                else:
                    exp_val = new_expect[i][j - ignored_col_count]
                    expect_to_string = str(new_expect[i])
                ok, msg = self.check_value(col, exp_val)
                assert ok, 'The returned row from nebula could not be found, row: {}, expect: {}, error message: {}'.format(
                        self.row_to_string(row), expect_to_string, msg)

    @classmethod
    def check_out_of_order_result(self, resp, expect, ignore_col: Set[int] = set()):
        if resp.data is None and len(expect) == 0:
            return

        if resp.data is None:
            assert False, 'resp.data is None'

        # convert expect to thrift value
        ok, new_expect, msg = self.convert_expect(expect)
        if not ok:
            assert ok, 'convert expect failed, error msg: {}'.format(msg)
        rows = resp.data.rows
        sorted_rows = sorted(rows, key=str)
        resp.data.rows = sorted_rows
        sorted_expect = sorted(new_expect, key=str)
        # has convert the expect, so set is_regex to True
        self.check_result(resp, sorted_expect, ignore_col, True)

    @classmethod
    def check_empty_result(self, resp):
        msg = 'the row was not empty {}'.format(resp)
        empty = False
        if resp.data is None:
            empty = True
        elif len(resp.data.rows) == 0:
            empty = True
        assert empty, msg

    @classmethod
    def check_path_result(self, rows, expect):
        msg = 'len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect))
        assert len(rows) == len(expect), msg
        for exp in expect:
            path = ttypes.Path()
            path.entry_list = []
            for ecol, j in zip(exp, range(len(exp))):
                if j % 2 == 0 or j == len(exp):
                    pathEntry = ttypes.PathEntry()
                    vertex = ttypes.Vertex()
                    vertex.id = ecol
                    pathEntry.set_vertex(vertex)
                    path.entry_list.append(pathEntry)
                else:
                    assert len(
                        ecol) == 2, "invalid values size in expect result"
                    pathEntry = ttypes.PathEntry()
                    edge = ttypes.Edge()
                    edge.type = ecol[0]
                    edge.ranking = ecol[1]
                    pathEntry.set_edge(edge)
                    path.entry_list.append(pathEntry)
            find = False
            for row in rows:
                assert len(
                    row.values
                ) == 1, "invalid values size in rows: {}".format(row)
                assert row.values[0].getType()(
                ) == ttypes.Value.PATH, "invalid column path type: {}".format(
                    row.values[0].getType()())
                if row.values[0].get_path() == path:
                    find = True
                    break
            msg = self.check_format_str.format(row.values[0].get_path(), path)
            assert find, msg
            rows.remove(row)

    @classmethod
    def check_error_msg(self, resp, expect):
        self.check_resp_failed(resp)
        msg = self.check_format_str.format(resp.error_msg, expect)
        if isinstance(expect, Pattern):
            if not expect.match(resp.error_msg.decode('utf-8')):
                assert False, msg
        else:
            assert resp.error_msg.decode('utf-8') == expect, msg