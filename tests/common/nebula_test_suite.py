# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import datetime
from pathlib import Path
from typing import Pattern, Set

import pytest
from nebula2.Client import GraphClient
from nebula2.common import ttypes as CommonTtypes
from nebula2.ConnectionPool import ConnectionPool
from nebula2.graph import ttypes
from tests.common.configs import get_delay_time
import re


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
        self.delay = get_delay_time(self.client)

    @classmethod
    def setup_class(self):
        address = pytest.cmdline.address.split(':')
        self.spaces = []
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
    def load_vertex_edge(self):
        self.VERTEXS = dict()
        self.EDGES = dict()
        nba_file = self.data_dir + '/data/nba.ngql'
        print("load will open ", nba_file)
        with open(nba_file, 'r') as data_file:
            lines = data_file.readlines()
            ddl = False
            dataType = ['none']
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
                            ddl = False
                else:
                    if not ddl:
                        self.parse_line(line.strip(), dataType)
                    if line.endswith(';'):
                        dataType[0] = 'none'

    @classmethod
    def load_data(self):
        self.data_loaded = True
        pathlist = Path(self.data_dir).rglob('*.ngql')
        for path in pathlist:
            print("open: ", path)
            with open(path, 'r') as data_file:
                space_name = path.name.split('.')[0] + datetime.datetime.now().strftime('%H_%M_%S_%f')
                self.spaces.append(space_name)
                resp = self.execute(
                'CREATE SPACE IF NOT EXISTS {space_name}(partition_num={partition_num}, '
                'replica_factor={replica_factor}, vid_size=30); USE {space_name};'.format(
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
        resp = self.execute('USE student_space;')
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
    def execute(self, ngql, profile=True):
        return self.client.execute(
            'PROFILE {{{}}}'.format(ngql) if profile else ngql)

    @classmethod
    def execute_query(self, ngql, profile=True):
        return self.client.execute_query(
            'PROFILE {{{}}}'.format(ngql) if profile else ngql)

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
            resp.error_code == ttypes.ErrorCode.SUCCEEDED
            or resp.error_code == ttypes.ErrorCode.E_STATEMENT_EMTPY
        ), bytes.decode(resp.error_msg) if resp.error_msg is not None else ''

    @classmethod
    def check_resp_failed(self, resp, error_code: ttypes.ErrorCode = ttypes.ErrorCode.SUCCEEDED):
        if error_code == ttypes.ErrorCode.SUCCEEDED:
            assert resp.error_code != error_code, '{} == {}, {}'.format(
                ttypes.ErrorCode._VALUES_TO_NAMES[resp.error_code],
                ttypes.ErrorCode._VALUES_TO_NAMES[error_code],
                bytes.decode(resp.error_msg) if resp.error_msg is not None else ''
            )
        else:
            assert resp.error_code == error_code, '{} != {}, {}'.format(
                ttypes.ErrorCode._VALUES_TO_NAMES[resp.error_code],
                ttypes.ErrorCode._VALUES_TO_NAMES[error_code],
                bytes.decode(resp.error_msg) if resp.error_msg is not None else ''
            )

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
    def date_to_string(self, date):
        return '{}/{}/{}'.format(date.year, date.month, date.day)

    @classmethod
    def date_time_to_string(self, date_time):
        zone = '+'
        if date_time.timezone < 0:
            zone = '-'
        return '{}/{}/{} {}:{}:{}.{} {}{}.{}'.format(date_time.year,
                                                     date_time.month,
                                                     date_time.day,
                                                     date_time.hour,
                                                     date_time.minute,
                                                     date_time.sec,
                                                     date_time.microsec,
                                                     zone,
                                                     date_time.timezone / 3600,
                                                     date_time.timezone % 3600)
    @classmethod
    def map_to_string(self, map):
        kvStrs = []
        if map.kvs is not None:
            for key in map.kvs:
                kvStrs.append('"{}":"{}"'.format(key.decode('utf-8'), self.value_to_string(map.kvs[key])))
            return '{' + ','.join(kvStrs) + '}'
        return ''

    @classmethod
    def value_to_string(self, value):
        if value.getType() == CommonTtypes.Value.__EMPTY__:
            return '__EMPTY__'
        elif value.getType() == CommonTtypes.Value.NVAL:
            return '__NULL__'
        elif value.getType() == CommonTtypes.Value.BVAL:
            return str(value.get_bVal())
        elif value.getType() == CommonTtypes.Value.IVAL:
            return str(value.get_iVal())
        elif value.getType() == CommonTtypes.Value.FVAL:
            return str(value.get_fVal())
        elif value.getType() == CommonTtypes.Value.SVAL:
            return value.get_sVal().decode('utf-8')
        elif value.getType() == CommonTtypes.Value.DVAL:
            return self.date_time_to_string(value.get_dVal())
        elif value.getType() == CommonTtypes.Value.TVAL:
            return self.date_time_to_string(value.get_tVal())
        elif value.getType() == CommonTtypes.Value.MVAL:
            return self.map_to_string(value.get_mVal())
        return 'Unsupported type'

    @classmethod
    def row_to_string(self, row):
        value_list = []
        for col in row.values:
            value_list.append(self.value_to_string(col))
        return str(value_list)

    @classmethod
    def search_result(self, resp, expect, is_regex=False):
        ok, msg = self.search(resp, expect, is_regex)
        assert ok, msg

    @classmethod
    def search_not_exist(self, resp, expect, is_regex=False):
        ok, msg = self.search(resp, expect, is_regex)
        assert not ok, 'expect "{}" has exist'.format(str(expect))

    @classmethod
    def search(self, resp, expect, is_regex=False):
        if resp.data is None and len(expect) == 0:
            return True

        if resp.data is None:
            return False, 'resp.data is None'
        rows = resp.data.rows

        msg = 'len(rows)[%d] < len(expect)[%d]' % (len(rows), len(expect))
        if len(rows) < len(expect):
            return False, msg

        new_expect = expect
        if not is_regex:
            # convert expect to thrift value
            ok, new_expect, msg = self.convert_expect(expect)
            if not ok:
                return ok, 'convert expect failed, error msg: {}'.format(msg)

        for exp in new_expect:
            exp_values = []
            exp_str = ''
            if not is_regex:
                exp_values = exp.values
                exp_str = self.row_to_string(exp)
            else:
                exp_values = exp
                exp_str = str(exp)
            has = False
            for row in rows:
                ok = True
                if len(row.values) != len(exp_values):
                    return False, 'len(row)[%d] != len(exp_values)[%d]' % (len(row.values), len(exp_values))
                for col1, col2 in zip(row.values, exp_values):
                    ok, msg = self.check_value(col1, col2)
                    if not ok:
                        ok = False
                        break
                if ok:
                    has = True
                    break
            if not has:
                return has, 'The returned row from nebula could not be found, row: {}'.format(exp_str)
        return True, ''

    @classmethod
    def check_column_names(self, resp, expect):
        for i in range(len(expect)):
            result = bytes.decode(resp.data.column_names[i])
            ok = (expect[i] == result)
            assert ok, "different column name, expect: {} vs. result: {}".format(expect[i], result)

    @classmethod
    def to_value(self, col):
        value = CommonTtypes.Value()
        if isinstance(col, bool):
            value.set_bVal(col)
        elif isinstance(col, int):
            value.set_iVal(col)
        elif isinstance(col, float):
            value.set_fVal(col)
        elif isinstance(col, str):
            value.set_sVal(col.encode('utf-8'))
        elif isinstance(col, dict):
            map_val = CommonTtypes.Map()
            map_val.kvs = dict()
            for key in col:
                ok, temp = self.to_value(col[key])
                if not ok:
                    return ok, temp
                map_val.kvs[key.encode('utf-8')] = temp
            value.set_mVal(map_val)
        elif isinstance(col, list):
            list_val = CommonTtypes.List()
            list_val.values = col
            value.set_lVal(list_val)
        else:
            return False, 'Wrong val type'
        return True, value

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
                    ok, value = self.to_value(col)
                    if not ok:
                        return ok, value, 'to_value:error'
                    new_row.values.append(value)
            result.append(new_row)
        return True, result, ''

    @classmethod
    def check_result(self, resp, expect, ignore_col: Set[int] = set(), is_regex=False):
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
                assert len(row.values) - len(ignore_col) == len(new_expect[i].values), \
                    '{}, {}, {}'.format(len(row.values), len(ignore_col), len(new_expect[i].values))
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
    def check_out_of_order_result(self, resp, expect, ignore_col: Set[int]=set()):
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
                    assert len(ecol) == 2, \
                        "invalid values size in expect result"
                    pathEntry = ttypes.PathEntry()
                    edge = ttypes.Edge()
                    edge.type = ecol[0]
                    edge.ranking = ecol[1]
                    pathEntry.set_edge(edge)
                    path.entry_list.append(pathEntry)
            find = False
            for row in rows:
                assert len(row.values) == 1, \
                    "invalid values size in rows: {}".format(row)
                assert row.values[0].getType()() == ttypes.Value.PATH, \
                    "invalid column path type: {}".format(row.values[0].getType()())
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
        err_msg = resp.error_msg.decode('utf-8')
        if isinstance(expect, Pattern):
            assert expect.match(err_msg), msg
        else:
            assert err_msg == expect, msg

    @classmethod
    def parse_line(self, line, dataType):
        if line.startswith('INSERT') or line.startswith('VALUES'):
            return ''

        if line.startswith('VERTEX player'):
            dataType[0] = 'player'
        elif line.startswith('VERTEX team'):
            dataType[0] = 'team'
        elif line.startswith('VERTEX bachelor'):
            dataType[0] = 'bachelor'
        elif line.startswith('EDGE serve'):
            dataType[0] = 'serve'
        elif line.startswith('EDGE like'):
            dataType[0] = 'like'
        elif line.startswith('EDGE teammate'):
            dataType[0] = 'teammate'
        else:
            line = re.split(':|,|->', line.strip(',; \t'))
            line = list(map(lambda i: i.strip(' ()"'), line))
            value = CommonTtypes.Value()
            if dataType[0] == 'none':
                assert False
            elif dataType[0] == 'player':
                vertex = self.create_vertex_player(line)
                key = str(vertex.vid, encoding='utf-8')
                if key in self.VERTEXS:
                    temp = self.VERTEXS[key].get_vVal()
                    temp.tags.append(vertex.tags[0])
                    temp.tags.sort(key=lambda x : x.name)
                    value.set_vVal(temp)
                    self.VERTEXS[key] = value
                else:
                    value.set_vVal(vertex)
                    self.VERTEXS[key] = value
            elif dataType[0] == 'team':
                vertex = self.create_vertex_team(line)
                value.set_vVal(vertex)
                key = str(vertex.vid, encoding = 'utf-8')
                self.VERTEXS[key] = value
            elif dataType[0] == 'bachelor':
                vertex = self.create_vertex_bachelor(line)
                key = str(vertex.vid, encoding = 'utf-8')
                if key in self.VERTEXS:
                    temp = self.VERTEXS[key].get_vVal()
                    temp.tags.append(vertex.tags[0])
                    temp.tags.sort(key=lambda x : x.name)
                    value.set_vVal(temp)
                    self.VERTEXS[key] = value
                else:
                    value.set_vVal(vertex)
                    self.VERTEXS[key] = value
            elif dataType[0] == 'serve':
                edge = self.create_edge_serve(line)
                value.set_eVal(edge)
                key = str(edge.src, encoding = 'utf-8') + str(edge.dst, encoding = 'utf-8') + str(edge.name, encoding = 'utf-8') + str(edge.ranking)
                self.EDGES[key] = value
            elif dataType[0] == 'like':
                edge = self.create_edge_like(line)
                value.set_eVal(edge)
                key = str(edge.src, encoding = 'utf-8') + str(edge.dst, encoding = 'utf-8') + str(edge.name, encoding = 'utf-8') + str(edge.ranking)
                self.EDGES[key] = value
            elif dataType[0] == 'teammate':
                edge = self.create_edge_teammate(line)
                value.set_eVal(edge)
                key = str(edge.src, encoding = 'utf-8') + str(edge.dst, encoding = 'utf-8') + str(edge.name, encoding = 'utf-8') + str(edge.ranking)
                self.EDGES[key] = value
            else:
                assert False

    @classmethod
    def create_vertex_player(self, line):
        if len(line) != 3:
            assert False

        vertex = CommonTtypes.Vertex()
        vertex.vid = bytes(line[0], encoding = 'utf-8')
        tags = []
        tag = CommonTtypes.Tag()
        tag.name = bytes('player', encoding = 'utf-8')

        props = dict()
        name = CommonTtypes.Value()
        name.set_sVal(bytes(line[1], encoding = 'utf-8'))
        props[bytes('name', encoding = 'utf-8')] = name
        age = CommonTtypes.Value()
        age.set_iVal(int(line[2]))
        props[bytes('age', encoding = 'utf-8')] = age
        tag.props = props
        tags.append(tag)
        vertex.tags = tags
        return vertex

    @classmethod
    def create_vertex_team(self, line):
        if len(line) != 2:
            assert False
        vertex = CommonTtypes.Vertex()
        vertex.vid = bytes(line[0], encoding = 'utf-8')
        tags = []
        tag = CommonTtypes.Tag()
        tag.name = bytes('team', encoding = 'utf-8')

        props = dict()
        name = CommonTtypes.Value()
        name.set_sVal(bytes(line[1], encoding = 'utf-8'))
        props[bytes('name', encoding = 'utf-8')] = name
        tag.props = props
        tags.append(tag)
        vertex.tags = tags
        return vertex

    @classmethod
    def create_vertex_bachelor(self, line):
        if len(line) != 3:
            assert False

        vertex = CommonTtypes.Vertex()
        vertex.vid = bytes(line[0], encoding = 'utf-8')
        tags = []
        tag = CommonTtypes.Tag()
        tag.name = bytes('bachelor', encoding = 'utf-8')

        props = dict()
        name = CommonTtypes.Value()
        name.set_sVal(bytes(line[1], encoding = 'utf-8'))
        props[bytes('name', encoding = 'utf-8')] = name
        speciality = CommonTtypes.Value()
        speciality.set_sVal(bytes(line[2], encoding = 'utf-8'))
        props[bytes('speciality', encoding = 'utf-8')] = speciality
        tag.props = props
        tags.append(tag)
        vertex.tags = tags
        return vertex

    @classmethod
    def create_edge_serve(self, line):
        if len(line) != 4:
            assert False
        edge = CommonTtypes.Edge()
        edge.src = bytes(line[0], encoding = 'utf-8')
        if '@' in line[1]:
            temp = list(map(lambda i: i.strip('"'), re.split('@', line[1])))
            edge.dst = bytes(temp[0], encoding = 'utf-8')
            edge.ranking = int(temp[1])
        else:
            edge.dst = bytes(line[1], encoding = 'utf-8')
            edge.ranking = 0
        edge.type = 0
        edge.name = bytes('serve', encoding = 'utf-8')
        props = dict()
        start_year = CommonTtypes.Value()
        start_year.set_iVal(int(line[2]))
        end_year = CommonTtypes.Value()
        end_year.set_iVal(int(line[3]))
        props[bytes('start_year', encoding = 'utf-8')] = start_year
        props[bytes('end_year', encoding = 'utf-8')] = end_year
        edge.props = props
        return edge

    @classmethod
    def create_edge_like(self, line):
        if len(line) != 3:
            assert False
        edge = CommonTtypes.Edge()

        edge.src = bytes(line[0], encoding = 'utf-8')
        edge.dst = bytes(line[1], encoding = 'utf-8')
        edge.type = 0
        edge.ranking = 0
        edge.name = bytes('like', encoding = 'utf-8')
        props = dict()
        likeness = CommonTtypes.Value()
        likeness.set_iVal(int(line[2]))
        props[bytes('likeness', encoding = 'utf-8')] = likeness
        edge.props = props
        return edge

    @classmethod
    def create_edge_teammate(self, line):
        if len(line) != 4:
            assert False
        edge = CommonTtypes.Edge()
        edge.src = bytes(line[0], encoding = 'utf-8')
        edge.dst = bytes(line[1], encoding = 'utf-8')
        edge.type = 0
        edge.ranking = 0
        edge.name = bytes('teammate', encoding = 'utf-8')
        props = dict()
        start_year = CommonTtypes.Value()
        start_year.set_iVal(int(line[2]))
        end_year = CommonTtypes.Value()
        end_year.set_iVal(int(line[3]))
        props[bytes('start_year', encoding = 'utf-8')] = start_year
        props[bytes('end_year', encoding = 'utf-8')] = end_year
        edge.props = props
        return edge

    @classmethod
    def check_exec_plan(cls, resp, expect):
        cls.check_resp_succeeded(resp)
        if resp.plan_desc is None:
            return
        cls.diff_plan_node(resp.plan_desc, 0, expect, 0)

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
