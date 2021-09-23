# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import functools
import logging
import pytest
from pytest_bdd import scenarios
from pytest_bdd import given, when, then, parsers

parse = functools.partial(parsers.parse)


@pytest.fixture
def class_variables():
    return dict(session=None)


scenarios('cluster/zone/create.feature')
