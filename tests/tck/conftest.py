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

from nebula2.common.ttypes import Value, ErrorCode
from pytest_bdd import given, parsers, then, when

from tests.common.dataset_printer import DataSetPrinter
from tests.common.comparator import DataSetComparator, CmpType
from tests.common.plan_differ import PlanDiffer
from tests.common.configs import DATA_DIR
from tests.common.types import SpaceDesc
from tests.common.utils import (
    create_space,
    load_csv_data,
    space_generator,
    check_resp,
    response,
    resp_ok,
)
from tests.tck.utils.table import dataset, table
from tests.tck.utils.nbv import murmurhash2

parse = functools.partial(parsers.parse)
rparse = functools.partial(parsers.re)
example_pattern = re.compile(r"<(\w+)>")


def normalize_outline_scenario(request, name):
    for group in example_pattern.findall(name):
        fixval = request.getfixturevalue(group)
        name = name.replace(f"<{group}>", fixval)
    return name


def combine_query(query: str) -> str:
    return " ".join(line.strip() for line in query.splitlines())


def is_job_finished(sess, job):
    rsp = resp_ok(sess, f"SHOW JOB {job}")
    assert rsp.row_size() > 0

    def is_finished(val) -> bool:
        return val.is_string() and "FINISHED" == val.as_string()

    return any(is_finished(val) for val in rsp.row_values(0))


def wait_all_jobs_finished(sess, jobs=[]):
    times = 60
    while jobs and times > 0:
        jobs = [job for job in jobs if not is_job_finished(sess, job)]
        time.sleep(0.5)
        times -= 1
    return len(jobs) == 0


def job_id(resp):
    for key in resp.keys():
        for job in resp.column_values(key):
            assert job.is_int(), f"job id is not int: {job}"
            return job.as_int()


def wait_tag_or_edge_indexes_ready(sess, schema: str = "TAG"):
    resp = resp_ok(sess, f"SHOW {schema} INDEXES")
    jobs = []
    for val in resp.column_values("Index Name"):
        job = val.as_string()
        resp = resp_ok(sess, f"REBUILD {schema} INDEX {job}", True)
        jobs.append(job_id(resp))
    wait_all_jobs_finished(sess, jobs)


def wait_indexes_ready(sess):
    wait_tag_or_edge_indexes_ready(sess, "TAG")
    wait_tag_or_edge_indexes_ready(sess, "EDGE")


@pytest.fixture
def graph_spaces():
    return dict(result_set=None)


@given(parse('a graph with space named "{space}"'))
def preload_space(
    request,
    space,
    load_nba_data,
    load_nba_int_vid_data,
    load_student_data,
    session,
    graph_spaces,
):
    space = normalize_outline_scenario(request, space)
    if space == "nba":
        graph_spaces["space_desc"] = load_nba_data
    elif space == "nba_int_vid":
        graph_spaces["space_desc"] = load_nba_int_vid_data
    elif space == "student":
        graph_spaces["space_desc"] = load_student_data
    else:
        raise ValueError(f"Invalid space name given: {space}")
    resp_ok(session, f'USE {space};', True)
    wait_indexes_ready(session)


@given("an empty graph")
def empty_graph(session, graph_spaces):
    pass


@given(parse("having executed:\n{query}"))
def having_executed(query, session, request):
    ngql = combine_query(query)
    ngql = normalize_outline_scenario(request, ngql)
    for stmt in ngql.split(';'):
        stmt and resp_ok(session, stmt, True)


@given(parse("create a space with following options:\n{options}"))
def new_space(request, options, session, graph_spaces):
    lines = csv.reader(io.StringIO(options), delimiter="|")
    opts = {
        line[1].strip(): normalize_outline_scenario(request, line[2].strip())
        for line in lines
    }
    name = "EmptyGraph_" + space_generator()
    space_desc = SpaceDesc(
        name=opts.get("name", name),
        partition_num=int(opts.get("partition_num", 7)),
        replica_factor=int(opts.get("replica_factor", 1)),
        vid_type=opts.get("vid_type", "FIXED_STRING(30)"),
        charset=opts.get("charset", "utf8"),
        collate=opts.get("collate", "utf8_bin"),
    )
    create_space(space_desc, session)
    graph_spaces["space_desc"] = space_desc
    graph_spaces["drop_space"] = True

@given(parse("Any graph"))
def new_space(request, session, graph_spaces):
    name = "EmptyGraph_" + space_generator()
    space_desc = SpaceDesc(
        name=name,
        partition_num=9,
        replica_factor=1,
        vid_type="FIXED_STRING(30)",
        charset="utf8",
        collate="utf8_bin",
    )
    create_space(space_desc, session)
    graph_spaces["space_desc"] = space_desc
    graph_spaces["drop_space"] = True

@given(parse('load "{data}" csv data to a new space'))
def import_csv_data(request, data, graph_spaces, session, pytestconfig):
    data_dir = os.path.join(DATA_DIR, normalize_outline_scenario(request, data))
    space_desc = load_csv_data(
        session,
        data_dir,
        "I" + space_generator(),
    )
    wait_indexes_ready(session)
    assert space_desc is not None
    graph_spaces["space_desc"] = space_desc
    graph_spaces["drop_space"] = True


def exec_query(request, ngql, session, graph_spaces, need_try: bool = False):
    if not ngql:
        return
    ngql = normalize_outline_scenario(request, ngql)
    graph_spaces['result_set'] = response(session, ngql, need_try)
    graph_spaces['ngql'] = ngql


@when(parse("executing query:\n{query}"))
def executing_query(query, graph_spaces, session, request):
    ngql = combine_query(query)
    exec_query(request, ngql, session, graph_spaces)


@when(parse("profiling query:\n{query}"))
def profiling_query(query, graph_spaces, session, request):
    ngql = "PROFILE {" + combine_query(query) + "}"
    exec_query(request, ngql, session, graph_spaces)


@when(parse("try to execute query:\n{query}"))
def try_to_execute_query(query, graph_spaces, session, request):
    ngql = normalize_outline_scenario(request, combine_query(query))
    for stmt in ngql.split(';'):
        exec_query(request, stmt, session, graph_spaces, True)


@given("wait all indexes ready")
@when("wait all indexes ready")
@then("wait all indexes ready")
def wait_index_ready(graph_spaces, session):
    space_desc = graph_spaces.get("space_desc", None)
    assert space_desc is not None
    space = space_desc.name
    resp_ok(session, f"USE {space}", True)
    wait_indexes_ready(session)


@when(parse("submit a job:\n{query}"))
def submit_job(query, graph_spaces, session, request):
    ngql = normalize_outline_scenario(request, combine_query(query))
    exec_query(request, ngql, session, graph_spaces, True)


@then("wait the job to finish")
def wait_job_to_finish(graph_spaces, session):
    resp = graph_spaces['result_set']
    jid = job_id(resp)
    is_finished = wait_all_jobs_finished(session, [jid])
    assert is_finished, f"Fail to finish job {jid}"


@given(parse("wait {secs:d} seconds"))
@when(parse("wait {secs:d} seconds"))
@then(parse("wait {secs:d} seconds"))
def wait(secs):
    time.sleep(secs)


def line_number(steps, result):
    for step in steps:
        res_lines = result.split('\n')
        if all(l in r for (l, r) in zip(res_lines, step.lines)):
            return step.line_number
    return -1


# IN literal `1, 2, 3...'
def parse_list(s: str):
    return [int(num) for num in s.split(',')]



def hash_columns(ds, hashed_columns):
    if len(hashed_columns) == 0:
        return ds
    assert all(col < len(ds.column_names) for col in hashed_columns)
    for row in ds.rows:
        for col in hashed_columns:
            val = row.values[col]
            if val.getType() not in [Value.NVAL, Value.__EMPTY__]:
                row.values[col] = Value(iVal=murmurhash2(val))
    return ds


def cmp_dataset(
        request,
        graph_spaces,
        result,
        order: bool,
        strict: bool,
        contains=CmpType.EQUAL,
        hashed_columns=[],
) -> None:
    rs = graph_spaces['result_set']
    ngql = graph_spaces['ngql']
    check_resp(rs, ngql)
    space_desc = graph_spaces.get('space_desc', None)
    vid_fn = murmurhash2 if space_desc and space_desc.is_int_vid() else None
    ds = dataset(
        table(result, lambda x: normalize_outline_scenario(request, x)),
        graph_spaces.get("variables", {}),
    )
    ds = hash_columns(ds, hashed_columns)
    dscmp = DataSetComparator(strict=strict,
                              order=order,
                              contains=contains,
                              decode_type=rs._decode_type,
                              vid_fn=vid_fn)

    def dsp(ds):
        printer = DataSetPrinter(rs._decode_type, vid_fn=vid_fn)
        return printer.ds_to_string(ds)

    def rowp(ds, i):
        if i is None or i < 0:
            return "" if i != -2 else "Invalid column names"
        assert i < len(ds.rows), f"{i} out of range {len(ds.rows)}"
        row = ds.rows[i].values
        printer = DataSetPrinter(rs._decode_type, vid_fn=vid_fn)
        ss = printer.list_to_string(row, delimiter='|')
        return f'{i}: |' + ss + '|'

    if rs._data_set_wrapper is None:
        assert not ds.column_names and not ds.rows, f"Expected result must be empty table: ||"

    rds = rs._data_set_wrapper._data_set
    res, i = dscmp(rds, ds)
    if not res:
        scen = request.function.__scenario__
        feature = scen.feature.rel_filename
        location = f"{feature}:{line_number(scen._steps, result)}"
        msg = [
            f"Fail to exec: {ngql}",
            f"Response: {dsp(rds)}",
            f"Expected: {dsp(ds)}",
            f"NotFoundRow: {rowp(ds, i)}",
            f"Location: {location}",
            f"Space: {str(space_desc)}",
            f"vid_fn: {vid_fn}",
        ]
        assert res, "\n".join(msg)


@then(parse("define some list variables:\n{text}"))
def define_list_var_alias(text, graph_spaces):
    tbl = table(text)
    graph_spaces["variables"] = {
        column: "[" + ",".join(row[i] for row in tbl['rows'] if row[i]) + "]"
        for i, column in enumerate(tbl['column_names'])
    }


@then(parse("the result should be, in order:\n{result}"))
def result_should_be_in_order(request, result, graph_spaces):
    cmp_dataset(request, graph_spaces, result, order=True, strict=True)


@then(parse("the result should be, in order, and the columns {hashed_columns} should be hashed:\n{result}"))
def result_should_be_in_order_and_hash(request, result, graph_spaces, hashed_columns):
    cmp_dataset(request, graph_spaces, result, order=True, strict=True, hashed_columns=parse_list(hashed_columns))


@then(parse("the result should be, in order, with relax comparison:\n{result}"))
def result_should_be_in_order_relax_cmp(request, result, graph_spaces):
    cmp_dataset(request, graph_spaces, result, order=True, strict=False)


@then(parse("the result should be, in order, with relax comparison, and the columns {hashed_columns} should be hashed:\n{result}"))
def result_should_be_in_order_relax_cmp_and_hash(request, result, graph_spaces, hashed_columns):
    cmp_dataset(request, graph_spaces, result, order=True, strict=False, hashed_columns=parse_list(hashed_columns))


@then(parse("the result should be, in any order:\n{result}"))
def result_should_be(request, result, graph_spaces):
    cmp_dataset(request, graph_spaces, result, order=False, strict=True)


@then(parse("the result should be, in any order, and the columns {hashed_columns} should be hashed:\n{result}"))
def result_should_be_and_hash(request, result, graph_spaces, hashed_columns):
    cmp_dataset(request, graph_spaces, result, order=False, strict=True, hashed_columns=parse_list(hashed_columns))


@then(parse("the result should be, in any order, with relax comparison:\n{result}"))
def result_should_be_relax_cmp(request, result, graph_spaces):
    cmp_dataset(request, graph_spaces, result, order=False, strict=False)


@then(parse("the result should be, in any order, with relax comparison, and the columns {hashed_columns} should be hashed:\n{result}"))
def result_should_be_relax_cmp_and_hash(request, result, graph_spaces, hashed_columns):
    cmp_dataset(request, graph_spaces, result, order=False, strict=False, hashed_columns=parse_list(hashed_columns))


@then(parse("the result should contain:\n{result}"))
def result_should_contain(request, result, graph_spaces):
    cmp_dataset(request,
                graph_spaces,
                result,
                order=False,
                strict=True,
                contains=CmpType.CONTAINS)


@then(parse("the result should not contain:\n{result}"))
def result_should_not_contain(request, result, graph_spaces):
    cmp_dataset(request,
                graph_spaces,
                result,
                order=False,
                strict=True,
                contains=CmpType.NOT_CONTAINS)


@then(parse("the result should contain, and the columns {hashed_columns} should be hashed:\n{result}"))
def result_should_contain_and_hash(request, result, graph_spaces, hashed_columns):
    cmp_dataset(request,
                graph_spaces,
                result,
                order=False,
                strict=True,
                contains=True,
                hashed_columns=parse_list(hashed_columns))


@then("no side effects")
def no_side_effects():
    pass


@then("the execution should be successful")
def execution_should_be_succ(graph_spaces):
    rs = graph_spaces["result_set"]
    stmt = graph_spaces["ngql"]
    check_resp(rs, stmt)


@then(rparse(r"(?P<unit>a|an) (?P<err_type>\w+) should be raised at (?P<time>runtime|compile time)(?P<sym>:|.)(?P<msg>.*)"))
def raised_type_error(unit, err_type, time, sym, msg, graph_spaces):
    res = graph_spaces["result_set"]
    ngql = graph_spaces['ngql']
    assert not res.is_succeeded(), f"Response should be failed: ngql:{ngql}"
    err_type = err_type.strip()
    msg = msg.strip()
    res_msg = res.error_msg()
    if res.error_code() == ErrorCode.E_EXECUTION_ERROR:
        assert err_type == "ExecutionError", f'Error code mismatch, ngql:{ngql}"'
        expect_msg = "{}".format(msg)
    else:
        expect_msg = "{}: {}".format(err_type, msg)
    m = res_msg.startswith(expect_msg)
    assert m, f'Could not find "{expect_msg}" in "{res_msg}" when execute query: "{ngql}"'


@then("drop the used space")
def drop_used_space(session, graph_spaces):
    drop_space = graph_spaces.get("drop_space", False)
    if not drop_space:
        return
    space_desc = graph_spaces.get("space_desc", None)
    if space_desc is not None:
        stmt = space_desc.drop_stmt()
        response(session, stmt)


@then(parse("the execution plan should be:\n{plan}"))
def check_plan(plan, graph_spaces):
    resp = graph_spaces["result_set"]
    expect = table(plan)
    column_names = expect.get('column_names', [])
    idx = column_names.index('dependencies')
    rows = expect.get("rows", [])
    for i, row in enumerate(rows):
        row[idx] = [
            int(cell.strip()) for cell in row[idx].split(",") if len(cell) > 0
        ]
        rows[i] = row
    differ = PlanDiffer(resp.plan_desc(), expect)
    assert differ.diff(), differ.err_msg()

@when(parse("executing query via graph {index:d}:\n{query}"))
def executing_query(query, index, graph_spaces, session_from_first_conn_pool, session_from_second_conn_pool, request):
    assert index < 2, "There exists only 0,1 graph: {}".format(index)
    ngql = combine_query(query)
    if index == 0:
        exec_query(request, ngql, session_from_first_conn_pool, graph_spaces)
    else:
        exec_query(request, ngql, session_from_second_conn_pool, graph_spaces)
