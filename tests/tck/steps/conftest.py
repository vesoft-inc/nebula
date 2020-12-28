# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from pytest_bdd import (
    given,
    when,
    then,
)

from nebula2.common.ttypes import Value, NullType
from tests.tck.utils.nbv import register_function, parse

# You could register functions that can be invoked from the parsing text
register_function('len', len)


@given("a string: <format>", target_fixture="string_table")
def string_table(format):
    return dict(text=format)


@when('They are parsed as Nebula Value')
def parsed_as_values(string_table):
    cell = string_table['text']
    v = parse(cell)
    assert v is not None, f"Failed to parse `{cell}'"
    string_table['value'] = v


@then('The type of the parsed value should be <_type>')
def parsed_as_expected(string_table, _type):
    val = string_table['value']
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
    assert actual == _type, f"expected: {_type}, actual: {actual}"
