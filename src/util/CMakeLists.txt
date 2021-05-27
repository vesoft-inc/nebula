# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

nebula_add_library(
    util_obj OBJECT
    ExpressionUtils.cpp
    SchemaUtil.cpp
    IndexUtil.cpp
    FTIndexUtils.cpp
    ZoneUtil.cpp
    ToJson.cpp
    ParserUtil.cpp
    QueryUtil.cpp
)

nebula_add_library(
    idgenerator_obj OBJECT
    IdGenerator.cpp
)

nebula_add_subdirectory(test)
