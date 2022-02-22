# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
#

nebula_add_library(
    util_obj SHARED
    ExpressionUtils.cpp
    SchemaUtil.cpp
    IndexUtil.cpp
    FTIndexUtils.cpp
    ZoneUtil.cpp
    ToJson.cpp
    ParserUtil.cpp
    PlannerUtil.cpp
    ValidateUtil.cpp
)

nebula_add_library(
    idgenerator_obj SHARED
    IdGenerator.cpp
)

nebula_add_subdirectory(test)
