# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
import random
import string
import time
import yaml
from typing import Pattern

from nebula2.Config import Config
from nebula2.common import ttypes as CommonTtypes
from nebula2.gclient.net import Session
from nebula2.gclient.net import ConnectionPool

from tests.common.csv_import import CSVImporter
from tests.common.path_value import PathVal
from tests.common.types import SpaceDesc


def utf8b(s: str):
    return bytes(s, encoding='utf-8')


def utf8s(b):
    return str(b, encoding='utf-8')


def _compare_values_by_pattern(real, expect):
    if real.getType() == CommonTtypes.Value.BVAL:
        return expect.match(str(real.get_bVal()))
    if real.getType() == CommonTtypes.Value.IVAL:
        return expect.match(str(real.get_iVal()))
    if real.getType() == CommonTtypes.Value.SVAL:
        return expect.match(real.get_sVal().decode('utf-8'))
    if real.getType() == CommonTtypes.Value.FVAL:
        return expect.match(str(real.get_fVal()))
    return False


def _compare_list(rvalues, evalues):
    if len(rvalues) != len(evalues):
        return False

    for rval in rvalues:
        found = False
        for ev in evalues:
            if compare_value(rval, ev):
                found = True
                break
        if not found:
            return False
    return True


def _compare_map(rvalues, evalues):
    for key in rvalues:
        ev = evalues.get(key)
        if ev is None:
            return False
        rv = rvalues.get(key)
        if not compare_value(rv, ev):
            return False
    return True


def compare_value(real, expect):
    if isinstance(expect, Pattern):
        return _compare_values_by_pattern(real, expect)

    if real.getType() == CommonTtypes.Value.LVAL:
        rvalues = real.get_lVal().values
        if expect.getType() == CommonTtypes.Value.LVAL:
            evalues = expect.get_lVal().values
            return _compare_list(rvalues, evalues)
        if type(expect) is list:
            return _compare_list(rvalues, expect)
        return False

    if real.getType() == CommonTtypes.Value.UVAL:
        rvalues = real.get_uVal().values
        if expect.getType() == CommonTtypes.Value.UVAL:
            evalues = expect.get_uVal().values
            return _compare_list(rvalues, evalues)
        if type(expect) is set:
            return _compare_list(rvalues, expect)
        return False

    if real.getType() == CommonTtypes.Value.MVAL:
        rvalues = real.get_mVal().kvs
        if expect.getType() == CommonTtypes.Value.MVAL:
            evalues = expect.get_mVal().kvs
            return _compare_map(rvalues, evalues)
        if type(expect) is dict:
            return _compare_map(rvalues, expect)
        return False

    if real.getType() == CommonTtypes.Value.EVAL:
        if expect.getType() != CommonTtypes.Value.EVAL:
            return False
        redge = real.get_eVal()
        eedge = expect.get_eVal()
        rsrc, rdst = redge.src, redge.dst
        if redge.type < 0:
            rsrc, rdst = rdst, rsrc
        esrc, edst = eedge.src, eedge.dst
        if eedge.type < 0:
            esrc, edst = edst, esrc
        # ignore props comparation
        return rsrc == esrc and rdst == edst \
            and redge.ranking == eedge.ranking \
            and redge.name == eedge.name

    return real == expect


# Value to String conversation
def _value_to_string(value):
    if value.getType() == CommonTtypes.Value.__EMPTY__:
        return '__EMPTY__'
    elif value.getType() == CommonTtypes.Value.NVAL:
        nval = value.get_nVal()
        return f'__NULL__({CommonTtypes.NullType._VALUES_TO_NAMES[nval]})'
    elif value.getType() == CommonTtypes.Value.BVAL:
        return str(value.get_bVal())
    elif value.getType() == CommonTtypes.Value.IVAL:
        return str(value.get_iVal())
    elif value.getType() == CommonTtypes.Value.FVAL:
        return str(value.get_fVal())
    elif value.getType() == CommonTtypes.Value.SVAL:
        return '"' + value.get_sVal().decode('utf-8') + '"'
    elif value.getType() == CommonTtypes.Value.DVAL:
        return date_to_string(value.get_dVal())
    elif value.getType() == CommonTtypes.Value.TVAL:
        return time_to_string(value.get_tVal())
    elif value.getType() == CommonTtypes.Value.DTVAL:
        return date_time_to_string(value.get_dtVal())
    elif value.getType() == CommonTtypes.Value.EVAL:
        return edge_to_string(value.get_eVal())
    elif value.getType() == CommonTtypes.Value.LVAL:
        return list_to_string(value.get_lVal())
    elif value.getType() == CommonTtypes.Value.VVAL:
        return f'({value.get_vVal().vid})'
    elif value.getType() == CommonTtypes.Value.UVAL:
        return set_to_string(value.get_uVal())
    elif value.getType() == CommonTtypes.Value.MVAL:
        return map_to_string(value.get_mVal())
    elif value.getType() == CommonTtypes.Value.PVAL:
        return path_to_string(value.get_pVal())
    elif value.getType() == CommonTtypes.Value.GVAL:
        return dataset_to_string(value.get_gVal())
    else:
        raise ValueError(f"{value} is not a value")


def value_to_string(value):
    if isinstance(value, CommonTtypes.Value):
        return _value_to_string(value)
    elif type(value) is CommonTtypes.Tag:
        return tag_to_string(value)
    elif type(value) is CommonTtypes.Row:
        return row_to_string(value.values)
    elif type(value) is CommonTtypes.Step:
        return step_to_string(value)
    elif type(value) is list:
        return list_to_string(value)
    elif type(value) is dict:
        return map_to_string(value)
    elif type(value) is set:
        return set_to_string(value)
    else:
        return str(value)


def row_to_string(row):
    columns = row if type(row) is list else row.values
    value_list = map(value_to_string, columns)
    return '[' + ','.join(value_list) + ']'


def date_to_string(date):
    return f'{date.year}/{date.month}/{date.day}'


def time_to_string(time):
    return f'{time.hour}:{time.minute}:{time.sec}.{time.microsec}'


def date_time_to_string(date_time):
    return f'{date_to_string(date_time)}T{time_to_string(date_time)}'


def _kv_to_string(kv):
    dec = kv[0].decode('utf-8')
    return f'"{dec}": {value_to_string(kv[1])}'


def map_to_string(map_val):
    kvs = map_val if type(map_val) is dict else map_val.kvs
    values = list(map(_kv_to_string, kvs.items()))
    sorted_keys = sorted(values)
    return '{' + ", ".join(sorted_keys) + '}'


def list_to_string(lst):
    val_list = lst if type(lst) is list else lst.values
    values = map(value_to_string, val_list)
    sorted_values = sorted(values)
    return '[' + ', '.join(sorted_values) + ']'


def set_to_string(set_val):
    val_set = set_val if type(set_val) is set else set_val.values
    values = map(value_to_string, val_set)
    sorted_values = sorted(values)
    return '{' + ', '.join(sorted_values) + '}'


def edge_to_string(edge):
    name = edge.name.decode("utf-8")
    src, dst = (edge.src, edge.dst) if edge.type > 0 else (edge.dst, edge.src)
    return f'({src})-[:{name}@{edge.ranking} {map_to_string(edge.props)}]->({dst})'


def vertex_to_string(vertex):
    vid = vertex.vid.get_sVal().decode('utf-8')
    tags = list_to_string(vertex.tags)
    return f'({vid} {tags})'


def tag_to_string(tag):
    name = tag.name.decode('utf-8')
    props = map_to_string(tag.props)
    return f':{name} {props}'


def step_to_string(step):
    dst = vertex_to_string(step.dst)
    name = step.name.decode('utf-8')
    ranking = step.ranking
    props = map_to_string(step.props)
    if step.type > 0:
        return f'-[:{name}@{ranking} {props}]->{dst}'
    else:
        return f'<-[:{name}@{ranking} {props}]-{dst}'


def path_to_string(path):
    return vertex_to_string(path.src) \
        + ''.join(map(step_to_string, path.steps))


def dataset_to_string(dataset):
    column_names = ','.join(
        map(lambda x: x.decode('utf-8'), dataset.column_names))
    rows = '\n'.join(map(row_to_string, dataset.rows))
    return '\n'.join([column_names, rows])


# Conversation to Value
def to_value(col):
    if isinstance(col, CommonTtypes.Value):
        return col

    value = CommonTtypes.Value()
    if type(col) is bool:
        value.set_bVal(col)
    elif type(col) is int:
        value.set_iVal(col)
    elif type(col) is float:
        value.set_fVal(col)
    elif type(col) is str:
        value.set_sVal(col.encode('utf-8'))
    elif isinstance(col, CommonTtypes.Date):
        value.set_dVal(col)
    elif isinstance(col, CommonTtypes.Time):
        value.set_tVal(col)
    elif isinstance(col, CommonTtypes.DateTime):
        value.set_dtVal(col)
    elif type(col) is dict:
        map_val = CommonTtypes.NMap()
        map_val.kvs = {k.encode('utf-8'): to_value(v) for k, v in col.items()}
        value.set_mVal(map_val)
    elif type(col) is list:
        list_val = CommonTtypes.NList()
        list_val.values = list(map(to_value, col))
        value.set_lVal(list_val)
    elif type(col) is set:
        set_val = CommonTtypes.NSet()
        set_val.values = set(map(to_value, col))
        value.set_uVal(set_val)
    elif isinstance(col, CommonTtypes.Edge):
        value.set_eVal(col)
    elif isinstance(col, CommonTtypes.Vertex):
        value.set_vVal(col)
    elif isinstance(col, CommonTtypes.Path):
        value.set_pVal(col)
    elif isinstance(col, PathVal):
        value.set_pVal(col.to_value())
    else:
        raise ValueError(f'Wrong val type: {str(col)}')
    return value


def find_in_rows(row, rows):
    for r in rows:
        assert len(r.values) == len(row), f'{len(r.values)}!={len(row)}'
        found = True
        for col1, col2 in zip(r.values, row):
            if not compare_value(col1, col2):
                found = False
                break
        if found:
            return True
    return False


def space_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def check_resp(resp, stmt: str):
    assert resp is not None, f"response is None, stmt: {stmt}"
    msg = f"Fail to exec: {stmt}, error: {resp.error_msg()}"
    assert resp.is_succeeded(), msg


def retry(times: int, predicate=lambda x: x and x.is_succeeded()):
    def _retry(func):
        def wrapper(*args, **kwargs):
            resp = None
            for i in range(times):
                resp = func(*args, **kwargs)
                if predicate(resp):
                    return resp
                time.sleep(0.5)
            return resp

        return wrapper

    return _retry


@retry(30)
def try_execute(sess: Session, stmt: str):
    return sess.execute(stmt)


def return_if_not_leader_changed(resp) -> bool:
    if not resp:
        return True
    if resp.is_succeeded():
        return True

    err_msg = resp.error_msg()
    return err_msg.find('Storage Error: The leader has changed') < 0


@retry(30, return_if_not_leader_changed)
def process_leader_changed(sess: Session, stmt: str):
    return sess.execute(stmt)


def response(sess: Session, stmt: str, need_try: bool = False):
    try:
        if need_try:
            return try_execute(sess, stmt)
        return process_leader_changed(sess, stmt)
    except Exception as ex:
        assert not ex, f"Fail to exec: {stmt}, exception: {ex}"


def resp_ok(sess: Session, stmt: str, need_try: bool = False):
    resp = response(sess, stmt, need_try)
    check_resp(resp, stmt)
    return resp


def create_space(space_desc: SpaceDesc, sess: Session):
    resp_ok(sess, space_desc.drop_stmt(), True)
    resp_ok(sess, space_desc.create_stmt(), True)
    resp_ok(sess, space_desc.use_stmt(), True)


def _load_data_from_file(sess, data_dir, fd):
    for stmt in CSVImporter(fd, data_dir):
        response(sess, stmt, True)


def load_csv_data(
    sess: Session,
    data_dir: str,
    space_name: str = "",
):
    """
    Before loading CSV data files, you must create and select a graph
    space. The `config.yaml' file only create schema about tags and
    edges, not include space.
    """
    config_path = os.path.join(data_dir, 'config.yaml')

    with open(config_path, 'r') as f:
        config = yaml.full_load(f)

        space = config.get('space', None)
        assert space is not None
        if not space_name:
            space_name = space.get('name', "A" + space_generator())

        space_desc = SpaceDesc(
            name=space_name,
            vid_type=space.get('vidType', 'FIXED_STRING(32)'),
            partition_num=space.get('partitionNum', 7),
            replica_factor=space.get('replicaFactor', 1),
            charset=space.get('charset', 'utf8'),
            collate=space.get('collate', 'utf8_bin'),
        )

        create_space(space_desc, sess)

        schemas = config['schema']
        for line in schemas.splitlines():
            resp_ok(sess, line.strip(), True)

        for fd in config["files"]:
            _load_data_from_file(sess, data_dir, fd)

        return space_desc


def get_conn_pool(host: str, port: int):
    config = Config()
    config.max_connection_pool_size = 20
    config.timeout = 180000
    # init connection pool
    pool = ConnectionPool()
    if not pool.init([(host, port)], config):
        raise Exception("Fail to init connection pool.")
    return pool
