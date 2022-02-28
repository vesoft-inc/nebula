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
target_link_libraries(util_obj PRIVATE ft_es_graph_adapter_obj expression_obj expr_visitor_obj  meta_obj graph_context_obj )

nebula_add_library(
    idgenerator_obj SHARED
    IdGenerator.cpp
)

nebula_add_subdirectory(test)
