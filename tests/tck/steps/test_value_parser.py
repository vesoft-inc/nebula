# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

import os
import sys
import pytest

from pytest_bdd import (
    scenarios,
    given,
    when,
    then,
    parsers,
)

from nebula2.common.ttypes import Value, NullType
from tests.tck.utils.nbv import register_function, parse
from tests.tck.utils.table import table

# You could register functions that can be invoked from the parsing text
register_function('len', len)

scenarios('../features')


@given(parsers.parse("A set of string:\n{text}"),
       target_fixture="string_table")
def string_table(text):
    return table(text)


@when('They are parsed as Nebula Value')
def parsed_as_values(string_table):
    values = []
    column_names = string_table['column_names']
    for row in string_table['rows']:
        cell = row[column_names[0]]
        v = parse(cell)
        assert v is not None, f"Failed to parse `{cell}'"
        values.append(v)
    string_table['values'] = values


@then('The type of the parsed value should be as expected')
def parsed_as_expected(string_table):
    nvalues = string_table['values']
    column_names = string_table['column_names']
    for i, val in enumerate(nvalues):
        type = val.getType()
        if type == 0:
            actual = 'EMPTY'
        elif type == 1:
            null = val.get_nVal()
            if null == 0:
                actual = 'NULL'
            else:
                actual = NullType._VALUES_TO_NAMES[val.get_nVal()]
        else:
            actual = Value.thrift_spec[val.getType()][2]
        expected = string_table['rows'][i][column_names[1]]
        assert actual == expected, f"expected: {expected}, actual: {actual}"
