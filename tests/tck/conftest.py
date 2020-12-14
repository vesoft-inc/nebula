# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import functools

from pytest_bdd import given, when, then, parsers
from nebula2.data.DataObject import DataSetWrapper
from tests.tck.utils.table import table, dataset
from tests.tck.utils.comparator import DataSetWrapperComparator

parse = functools.partial(parsers.parse)


@given(
    parse('a graph with space named "nba"'),
    target_fixture="nba_space",
)
def nba_space(load_nba_data, session):
    rs = session.execute('USE nba;')
    assert rs.is_succeeded()
    return {"result_set": None}


@when(parse("executing query:\n{query}"))
def executing_query(query, nba_space, session):
    ngql = " ".join(query.splitlines())
    nba_space['result_set'] = session.execute(ngql)


@then(parse("the result should be, in any order:\n{result}"))
def result_should_be(result, nba_space):
    rs = nba_space['result_set']
    assert rs.is_succeeded()
    ds = DataSetWrapper(dataset(table(result)))
    dscmp = DataSetWrapperComparator(strict=True, order=False)
    assert dscmp(rs._data_set_wrapper, ds)


@then(
    parse(
        "the result should be, in any order, with relax comparision:\n{result}"
    ))
def result_should_be_relax_cmp(result, nba_space):
    rs = nba_space['result_set']
    assert rs.is_succeeded()
    ds = dataset(table(result))
    dsw = DataSetWrapper(ds)
    dscmp = DataSetWrapperComparator(strict=False, order=False)
    assert dscmp(rs._data_set_wrapper, dsw), \
        f"Response: {str(rs)} vs. Expected: {str(dsw)}"


@then("no side effects")
def no_side_effects():
    pass


@then(parse("a TypeError should be raised at runtime: InvalidArgumentValue"))
def raised_type_error():
    pass
