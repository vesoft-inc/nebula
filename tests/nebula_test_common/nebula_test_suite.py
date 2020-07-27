# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import math
import sys
from typing import Pattern, Set

import pytest
import time
from pathlib import Path
from nebula2.common import ttypes as CommonTtypes
from nebula2.graph import ttypes
from nebula2.ConnectionPool import ConnectionPool
from nebula2.Client import AuthException, ExecutionException, GraphClient
from nebula2.Common import *

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
        self.create_nebula_clients()
        self.set_delay()
        self.prepare()

    @classmethod
    def load_data(self):
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
        pass

    @classmethod
    def create_nebula_clients(self):
        self.client_pool = ConnectionPool(ip = self.host, port = self.port, network_timeout = 0)
        self.client = GraphClient(self.client_pool)
        self.client.authenticate(self.user, self.password)

    @classmethod
    def close_nebula_clients(self):
        self.client.sign_out()
        self.client_pool.close()

    @classmethod
    def teardown_class(self):
        if self.client != None:
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
    def prepare(self):
        self.prepare()

    @classmethod
    def cleanup(self):
        self.cleanup()

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
        if col.getType() == CommonTtypes.Value.__EMPTY__:
            msg = 'ERROR: type is empty'
            if isinstance(expect, Pattern):
                if not expect.match(str('EMPTY')):
                    return False, msg
            else:
                if 'EMPTY' != expect:
                    return False, msg
            return True, ''

        if col.getType() == CommonTtypes.Value.BVAL:
            msg = self.check_format_str.format(col.get_bVal(), expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.get_bool_val())):
                    return False, msg
            else:
                if col.get_bool_val() != expect:
                    return False, msg
            return True, ''

        if col.getType() == CommonTtypes.Value.IVAL:
            msg = self.check_format_str.format(col.get_iVal(), expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.get_iVal())):
                    return False, msg
            else:
                if col.get_iVal() != expect:
                    return False, msg
            return True, ''

        if col.getType() == CommonTtypes.Value.SVAL:
            msg = self.check_format_str.format(col.get_sVal().decode('utf-8'),
                                               expect)
            if isinstance(expect, Pattern):
                if not expect.match(col.get_sVal().decode('utf-8')):
                    return False, msg
            else:
                if col.get_sVal().decode('utf-8') != expect:
                    return False, msg
            return True, ''

        if col.getType() == CommonTtypes.Value.FVAL:
            msg = self.check_format_str.format(col.get_fVal(),
                                               expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.get_fVal())):
                    return False, msg
            else:
                if not math.isclose(col.get_fVal(), expect):
                    return False, msg
            return True, ''

        if col.getType() == CommonTtypes.Value.DVAL:
            msg = self.check_format_str.format(col.get_dVal(), expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.get_dVal())):
                    return False, msg
            else:
                if col.get_dVal() != expect:
                    return False, msg
            return True, ''

        if col.getType() == CommonTtypes.Value.TVAL:
            msg = self.check_format_str.format(col.get_tVal(), expect)
            if isinstance(expect, Pattern):
                if not expect.match(str(col.get_tVal())):
                    return False, msg
            else:
                if col.get_tVal() != expect:
                    return False, msg
            return True, ''

        return False, 'ERROR: Type unsupported'

    @classmethod
    def row_to_string(self, row):
        value_list = []
        for col in row.values:
            if col.getType() == CommonTtypes.Value.__EMPTY__:
                value_list.append('EMPTY')
            elif col.getType() == CommonTtypes.Value.NVAL:
                value_list.append('NULL')
            elif col.getType() == CommonTtypes.Value.BVAL:
                value_list.append(col.get_bVal())
            elif col.getType() == CommonTtypes.Value.IVAL:
                value_list.append(col.get_iVal())
            elif col.getType() == CommonTtypes.Value.FVAL:
                value_list.append(col.get_id())
            elif col.getType() == CommonTtypes.Value.SVAL:
                value_list.append(col.get_sVal().decode('utf-8'))
            elif col.getType() == CommonTtypes.Type.DVAL:
                value_list.append(col.get_dVal().decode('utf-8'))
            elif col.getType() == CommonTtypes.Type.DATETIME:
                value_list.append(col.get_datetime())
        return str(value_list)

    @classmethod
    def search_result(self, resp, expect):
        if resp.data is None and len(expect) == 0:
            return

        if resp.data is None:
            assert False, 'resp.data is None'
        rows = resp.data.rows

        msg = 'len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect))
        assert len(rows) == len(expect), msg
        for row in rows:
            ok = False
            msg = ""
            for exp in expect:
                for col, i in zip(row.values, range(0, len(exp))):
                    ok, msg = self.check_value(col, exp[i])
                    if not ok:
                        break
                if ok:
                    break
            assert ok, 'The returned row from nebula could not be found, row: {}, error message: {}'.format(
                self.row_to_string(row), msg)
            expect.remove(exp)

    @classmethod
    def check_column_names(self, resp, expect):
        for i in range(len(expect)):
            ok = (expect[i] == bytes.decode(resp.data.column_names[i]))
            assert ok, "different column name, expect: {} vs. result: {}".format(
                expect[i], resp.data.column_names[i])

    @classmethod
    def check_result(self, resp, expect, ignore_col: Set[int] = set()):
        if resp.data is None and len(expect) == 0:
            return

        if resp.data is None:
            assert False, 'resp.data is None'
        rows = resp.data.rows

        msg = 'len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect))
        assert len(rows) == len(expect), msg
        for row, i in zip(rows, range(0, len(expect))):
            print(row)
            print(expect[i])
            assert len(row.values) - len(ignore_col) == len(expect[i])
            ignored_col_count = 0
            for j, col in enumerate(row.values):
                if j in ignore_col:
                    ignored_col_count += 1
                    continue
                ok, msg = self.check_value(col, expect[i][j - ignored_col_count])
                assert ok, 'The returned row from nebula could not be found, row: {}, expect: {}, error message: {}'.format(
                    self.row_to_string(row), expect[i], msg)

    @classmethod
    def check_out_of_order_result(self, resp, expect, ignore_col: Set[int] = set()):
        if resp.data is None and len(expect) == 0:
            return

        if resp.data is None:
            assert False, 'resp.data is None'
        rows = resp.data.rows
        sorted_rows = sorted(rows, key=str)
        resp.data.rows = sorted_rows
        sorted_expect = sorted(expect)
        self.check_result(resp, sorted_expect, ignore_col)

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
