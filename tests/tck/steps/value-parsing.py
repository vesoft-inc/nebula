# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

import os
import sys
from behave import *

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(this_dir + '/../utils/')
import nbv
from nebula2.common.ttypes import Value,NullType

# You could register functions that can be invoked from the parsing text
nbv.register_function('len', len)

@given('A set of string')
def step_impl(context):
    context.saved = context.table

@when('They are parsed as Nebula Value')
def step_impl(context):
    pass

@then('It must succeed')
def step_impl(context):
    values = []
    saved = context.saved.rows
    for row in saved:
        v = nbv.parse(row['format'])
        assert v != None, "Failed to parse `%s'" % row['format']
        values.append(v)
    context.values = values

@then('The type of the parsed value should be as expected')
def step_impl(context):
    n = len(context.values)
    saved = context.saved
    for i in range(n):
        type = context.values[i].getType()
        if type == 0:
            actual = 'EMPTY'
        elif type == 1:
            null = context.values[i].get_nVal()
            if null == 0:
                actual = 'NULL'
            else:
                actual = NullType._VALUES_TO_NAMES[context.values[i].get_nVal()]
        else:
            actual = Value.thrift_spec[context.values[i].getType()][2]
        expected = saved[i]['type']
        assert actual == expected, \
                       "expected: %s, actual: %s" % (expected, actual)
