# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME parser_fuzzer
    FUZZER ON
    SOURCES ParserFuzzer.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} wangle
)
