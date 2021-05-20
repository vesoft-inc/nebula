# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest
import time
import datetime

from pathlib import Path
from typing import Pattern, Set

from nebula2.common import ttypes as CommonTtypes
# from nebula2.gclient.net import ConnectionPool
# from nebula2.Config import Config
from nebula2.graph import ttypes
from tests.common.configs import get_delay_time
from tests.common.utils import (
    compare_value,
    row_to_string,
    to_value,
    value_to_string,
    find_in_rows,
)


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
T_NULL_UNKNOWN_PROP.set_nVal(CommonTtypes.NullType.UNKNOWN_PROP)
T_NULL_UNKNOWN_DIV_BY_ZERO = CommonTtypes.Value()
T_NULL_UNKNOWN_DIV_BY_ZERO.set_nVal(CommonTtypes.NullType.DIV_BY_ZERO)


@pytest.mark.usefixtures("workarround_for_class")
class NebulaTestSuite(object):
    @classmethod
    def set_delay(self):
        self.delay = get_delay_time(self.client)

    # @classmethod
    # def setup_class(self):
    #     self.spaces = []
    #     self.user = pytest.cmdline.user
    #     self.password = pytest.cmdline.password
    #     self.replica_factor = pytest.cmdline.replica_factor
    #     self.partition_num = pytest.cmdline.partition_num
    #     self.check_format_str = 'result: {}, expect: {}'
    #     self.data_dir = pytest.cmdline.data_dir
    #     self.data_loaded = False
    #     self.create_nebula_clients()
    #     self.set_delay()
    #     self.prepare()

    @classmethod
    def load_data(self):
        self.data_loaded = True
        # pathlist = Path(self.data_dir).rglob('*.ngql')
        pathlist = [Path(self.data_dir).joinpath("data/nba.ngql")]
        for path in pathlist:
            print("open: ", path)
            with open(path, 'r') as data_file:
                space_name = path.name.split('.')[0] + datetime.datetime.now().strftime('%H_%M_%S_%f')
                self.spaces.append(space_name)
                resp = self.execute(
                'CREATE SPACE IF NOT EXISTS {space_name}(partition_num={partition_num}, '
                'replica_factor={replica_factor}, vid_type=FIXED_STRING(30)); USE {space_name};'.format(
                        partition_num=self.partition_num,
                        replica_factor=self.replica_factor,
                        space_name=space_name))
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
            drop_stmt = []
            for space in self.spaces:
                drop_stmt.append('DROP SPACE {}'.format(space))
            resp = self.execute(';'.join(drop_stmt))
            self.check_resp_succeeded(resp)

    @classmethod
    def use_nba(self):
        resp = self.execute('USE nba;')
        self.check_resp_succeeded(resp)

    @classmethod
    def use_student_space(self):
        resp = self.execute('USE student;')
        self.check_resp_succeeded(resp)

    # @classmethod
    # def create_nebula_clients(self):
    #     config = Config()
    #     config.max_connection_pool_size = 20
    #     config.timeout = 60000
    #     # init connection pool
    #     self.client_pool = ConnectionPool()
    #     assert self.client_pool.init([(self.host, self.port)], config)

    #     # get session from the pool
    #     self.client = self.client_pool.get_session(self.user, self.password)

    @classmethod
    def spawn_nebula_client(self, user, password):
        return self.client_pool.get_session(user, password)

    @classmethod
    def release_nebula_client(self, client):
        client.release()

    # @classmethod
    # def close_nebula_clients(self):
    #     self.client_pool.close()

    # @classmethod
    # def teardown_class(self):
    #     if self.client is not None:
    #         self.cleanup()
    #         self.drop_data()
    #         self.client.release()
    #     self.close_nebula_clients()

    @classmethod
    def execute(self, ngql, profile=True):
        return self.client.execute(
            'PROFILE {{{}}}'.format(ngql) if profile else ngql)

    @classmethod
    def check_rows_with_header(cls, stmt: str, expected: dict):
        resp = cls.execute(stmt)
        cls.check_resp_succeeded(resp)
        if "column_names" in expected:
            cls.check_column_names(resp, expected['column_names'])
        if "rows" in expected:
            cls.check_out_of_order_result(resp, expected['rows'])

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
        assert (
            resp.is_succeeded()
            or resp.error_code() == CommonTtypes.ErrorCode.E_STATEMENT_EMPTY
        ), resp.error_msg()

    @classmethod
    def check_resp_failed(self, resp, error_code: CommonTtypes.ErrorCode = CommonTtypes.ErrorCode.SUCCEEDED):
        if error_code == CommonTtypes.ErrorCode.SUCCEEDED:
            assert resp.error_code() != error_code, '{} == {}, {}'.format(
                CommonTtypes.ErrorCode._VALUES_TO_NAMES[resp.error_code()],
                CommonTtypes.ErrorCode._VALUES_TO_NAMES[error_code], resp.error_msg()
            )
        else:
            assert resp.error_code() == error_code, '{} != {}, {}'.format(
                CommonTtypes.ErrorCode._VALUES_TO_NAMES[resp.error_code()],
                CommonTtypes.ErrorCode._VALUES_TO_NAMES[error_code], resp.error_msg()
            )

    @classmethod
    def search_result(self, resp, expect, is_regex=False):
        self.search(resp, expect, is_regex)

    @classmethod
    def search_not_exist(self, resp, expect, is_regex=False):
        self.search(resp, expect, is_regex, False)

    @classmethod
    def search(self, resp, expect, is_regex=False, exist=True):
        if resp.is_empty() and len(expect) == 0:
            return

        assert not resp.is_empty(), 'resp.data is None'

        rows = resp.rows()
        assert len(rows) >= len(expect), f'{len(rows)} < {len(expect)}'

        new_expect = expect
        if not is_regex:
            # convert expect to thrift value
            new_expect = self.convert_expect(expect)

        msg = 'Returned row from nebula could not be found, row: {}, resp: {}'
        for exp in new_expect:
            values, exp_str = (exp, str(exp)) if is_regex else (exp.values, row_to_string(exp))
            assert find_in_rows(values, rows) == exist, \
                msg.format(exp_str, value_to_string(rows))

    @classmethod
    def check_column_names(self, resp, expect):
        column_names = resp.keys()
        assert len(column_names) == len(expect), \
            f'Column names does not match, expected: {expect}, actual: {column_names}'
        for i in range(len(expect)):
            result = column_names[i]
            assert expect[i] == result, \
                f"different column name, expect: {expect[i]} vs. result: {result}"

    @classmethod
    def convert_expect(self, expect):
        result = []
        for row in expect:
            assert type(row) is list, f'{str(row)} is not list type'
            new_row = CommonTtypes.Row()
            new_row.values = list(map(to_value, row))
            result.append(new_row)
        return result

    @classmethod
    def check_result(self, resp, expect, ignore_col: Set[int] = set(), is_regex=False):
        if resp.is_empty() and len(expect) == 0:
            return

        assert not resp.is_empty(), 'resp.data is None'
        rows = resp.rows()

        assert len(rows) == len(expect), f'{len(rows)}!={len(expect)}'

        new_expect = expect
        if not is_regex:
            # convert expect to thrift value
            new_expect = self.convert_expect(expect)
        for row, i in zip(rows, range(0, len(new_expect))):
            columns = new_expect[i].values if isinstance(new_expect[i], CommonTtypes.Row) else new_expect[i]
            assert len(row.values) - len(ignore_col) == len(columns)
            ignored_col_count = 0
            for j, col in enumerate(row.values):
                if j in ignore_col:
                    ignored_col_count += 1
                    continue
                exp_val = columns[j - ignored_col_count]
                expect_to_string = row_to_string(columns)
                assert compare_value(col, exp_val), \
                    'The returned row from nebula could not be found, row: {}, expect: {}'.format(
                        row_to_string(row), expect_to_string)

    @classmethod
    def check_out_of_order_result(self, resp, expect, ignore_col: Set[int]=set()):
        if resp.is_empty() and len(expect) == 0:
            return

        assert not resp.is_empty()

        # convert expect to thrift value
        new_expect = self.convert_expect(expect)
        rows = resp.rows()
        sorted_rows = sorted(rows, key=row_to_string)
        resp._resp.data.rows = sorted_rows
        sorted_expect = sorted(new_expect, key=row_to_string)
        # has convert the expect, so set is_regex to True
        self.check_result(resp, sorted_expect, ignore_col, True)

    @classmethod
    def check_empty_result(self, resp):
        msg = 'the row was not empty {}'.format(resp)
        assert resp.is_empty(), msg

    @classmethod
    def check_path_result_without_prop(self, rows, expect):
        msg = 'len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect))
        assert len(rows) == len(expect), msg
        for exp in expect:
            path = CommonTtypes.Path()
            path.steps = []
            for col, j in zip(exp, range(len(exp))):
                if j == 0:
                    src = CommonTtypes.Vertex()
                    src.vid = bytes(col, encoding='utf-8')
                    src.tags = []
                    path.src = src
                else:
                    assert len(col) == 3, \
                        "{} invalid values size in expect result".format(exp.__repr__())
                    step = CommonTtypes.Step()
                    step.name = bytes(col[0], encoding='utf-8')
                    step.ranking = col[1]
                    step.type = 1
                    dst = CommonTtypes.Vertex()
                    dst.vid = bytes(col[2], encoding='utf-8')
                    dst.tags = []
                    step.dst = dst
                    step.props = {}
                    path.steps.append(step)
            find = False
            for row in rows:
                assert len(row.values) == 1, \
                    "invalid values size in rows: {}".format(row)
                assert row.values[0].getType() == CommonTtypes.Value.PVAL, \
                    "invalid column path type: {}".format(row.values[0].getType()())
                if row.values[0].get_pVal() == path:
                    find = True
                    break
            msg = self.check_format_str.format(row.values[0].get_pVal(), path)
            assert find, msg
            rows.remove(row)
        assert len(rows) == 0

    @classmethod
    def check_error_msg(self, resp, expect):
        self.check_resp_failed(resp)
        msg = self.check_format_str.format(resp.error_msg(), expect)
        err_msg = resp.error_msg()
        if isinstance(expect, Pattern):
            assert expect.match(err_msg), msg
        else:
            assert err_msg == expect, msg

    @classmethod
    def check_exec_plan(cls, resp, expect):
        cls.check_resp_succeeded(resp)
        if resp.plan_desc() is None:
            return
        cls.diff_plan_node(resp.plan_desc(), 0, expect, 0)

    @classmethod
    def diff_plan_node(cls, plan_desc, line_num, expect, expect_idx):
        plan_node_desc = plan_desc.plan_node_descs[line_num]
        expect_node = expect[expect_idx]
        name = bytes.decode(plan_node_desc.name)

        assert name.lower().startswith(expect_node[0].lower()), \
            "Different plan node: {} vs. {}".format(name, expect_node[0])

        if len(expect_node) > 2:
            descs = {bytes.decode(pair.value) for pair in plan_node_desc.description}
            assert set(expect_node[2]).issubset(descs), \
                'Invalid descriptions, expect: {} vs. resp: {}'.format(
                    '; '.join(map(str, expect_node[2])),
                    '; '.join(map(str, descs)))

        if plan_node_desc.dependencies is None:
            return

        assert len(expect_node[1]) == len(plan_node_desc.dependencies), \
            "Different plan node dependencies: {} vs. {}".format(
                len(plan_node_desc.dependencies), len(expect_node[1]))

        for i in range(len(plan_node_desc.dependencies)):
            line_num = plan_desc.node_index_map[plan_node_desc.dependencies[i]]
            cls.diff_plan_node(plan_desc, line_num, expect, expect_node[1][i])
