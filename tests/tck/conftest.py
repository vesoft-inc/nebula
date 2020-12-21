# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import functools
import os
import time
import pytest
import io
import csv
import re

from nebula2.graph.ttypes import ErrorCode
from pytest_bdd import given, parsers, then, when

from tests.common.dataset_printer import DataSetPrinter
from tests.common.comparator import DataSetComparator
from tests.common.configs import DATA_DIR
from tests.common.types import SpaceDesc
from tests.common.utils import create_space, load_csv_data, space_generator
from tests.tck.utils.table import dataset, table

parse = functools.partial(parsers.parse)
rparse = functools.partial(parsers.re)


@pytest.fixture
def graph_spaces():
    return dict(result_set=None)


@given(parse('a graph with space named "{space}"'))
def preload_space(space, load_nba_data, load_student_data, session,
                  graph_spaces):
    if space == "nba":
        graph_spaces["space_desc"] = load_nba_data
    elif space == "student":
        graph_spaces["space_desc"] = load_student_data
    else:
        raise ValueError(f"Invalid space name given: {space}")
    rs = session.execute(f'USE {space};')
    assert rs.is_succeeded(), f"Fail to use space `{space}': {rs.error_msg()}"


@given("an empty graph")
def empty_graph(session, graph_spaces):
    pass


@given(parse("having executed:\n{query}"))
def having_executed(query, session):
    ngql = " ".join(query.splitlines())
    resp = session.execute(ngql)
    assert resp.is_succeeded(), \
        f"Fail to execute {ngql}, error: {resp.error_msg()}"


@given(parse("create a space with following options:\n{options}"))
def new_space(options, session, graph_spaces):
    lines = csv.reader(io.StringIO(options), delimiter="|")
    opts = {line[1].strip(): line[2].strip() for line in lines}
    name = "EmptyGraph_" + space_generator()
    space_desc = SpaceDesc(
        name=name,
        partition_num=int(opts.get("partition_num", 7)),
        replica_factor=int(opts.get("replica_factor", 1)),
        vid_type=opts.get("vid_type", "FIXED_STRING(30)"),
        charset=opts.get("charset", "utf8"),
        collate=opts.get("collate", "utf8_bin"),
    )
    create_space(space_desc, session)
    graph_spaces["space_desc"] = space_desc
    graph_spaces["drop_space"] = True


@given(parse('import "{data}" csv data'))
def import_csv_data(data, graph_spaces, session, pytestconfig):
    data_dir = os.path.join(DATA_DIR, data)
    space_desc = graph_spaces["space_desc"]
    assert space_desc is not None
    resp = session.execute(space_desc.use_stmt())
    assert resp.is_succeeded(), \
        f"Fail to use {space_desc.name}, {resp.error_msg()}"
    load_csv_data(pytestconfig, session, data_dir)


@when(parse("executing query:\n{query}"))
def executing_query(query, graph_spaces, session):
    ngql = " ".join(query.splitlines())
    graph_spaces['result_set'] = session.execute(ngql)


@given(parse("wait {secs:d} seconds"))
@when(parse("wait {secs:d} seconds"))
def wait(secs):
    time.sleep(secs)


def cmp_dataset(graph_spaces,
                result,
                order: bool,
                strict: bool,
                included=False) -> None:
    rs = graph_spaces['result_set']
    assert rs.is_succeeded(), f"Response failed: {rs.error_msg()}"
    ds = dataset(table(result))
    dscmp = DataSetComparator(strict=strict,
                              order=order,
                              included=included,
                              decode_type=rs._decode_type)
    dsp = DataSetPrinter(rs._decode_type)
    resp_ds = rs._data_set_wrapper._data_set
    assert dscmp(resp_ds, ds), \
        f"Response: {dsp.ds_to_string(resp_ds)} vs. Expected: {dsp.ds_to_string(ds)}"


@then(parse("the result should be, in order:\n{result}"))
def result_should_be_in_order(result, graph_spaces):
    cmp_dataset(graph_spaces, result, order=True, strict=True)


@then(parse("the result should be, in order, with relax comparison:\n{result}"))
def result_should_be_in_order_relax_cmp(result, graph_spaces):
    cmp_dataset(graph_spaces, result, order=True, strict=False)


@then(parse("the result should be, in any order:\n{result}"))
def result_should_be(result, graph_spaces):
    cmp_dataset(graph_spaces, result, order=False, strict=True)


@then(parse("the result should be, in any order, with relax comparison:\n{result}"))
def result_should_be_relax_cmp(result, graph_spaces):
    cmp_dataset(graph_spaces, result, order=False, strict=False)


@then(parse("the result should include:\n{result}"))
def result_should_include(result, graph_spaces):
    cmp_dataset(graph_spaces, result, order=False, strict=True, included=True)


@then("no side effects")
def no_side_effects():
    pass


@then("the execution should be successful")
def execution_should_be_succ(graph_spaces):
    rs = graph_spaces["result_set"]
    assert rs is not None, "Please execute a query at first"
    assert rs.is_succeeded(), f"Response failed: {rs.error_msg()}"


@then(rparse(r"a (?P<err_type>\w+) should be raised at (?P<time>.*):(?P<msg>.*)"))
def raised_type_error(err_type, time, msg, graph_spaces):
    res = graph_spaces["result_set"]
    assert not res.is_succeeded(), "Response should be failed"
    err_type = err_type.strip()
    msg = msg.strip().replace('$', r'\$')
    res_msg = res.error_msg()
    if res.error_code() == ErrorCode.E_EXECUTION_ERROR:
        assert err_type == "ExecutionError"
        expect_msg = r"{}".format(msg)
    else:
        expect_msg = r"{}: {}".format(err_type, msg)
    m = re.search(expect_msg, res_msg)
    assert m, f'Could not find "{expect_msg}" in "{res_msg}"'


@then("drop the used space")
def drop_used_space(session, graph_spaces):
    drop_space = graph_spaces.get("drop_space", False)
    if not drop_space:
        return
    space_desc = graph_spaces["space_desc"]
    resp = session.execute(space_desc.drop_stmt())
    assert resp.is_succeeded(), f"Fail to drop space {space_desc.name}"
