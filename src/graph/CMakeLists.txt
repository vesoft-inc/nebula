# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_subdirectory(context)
nebula_add_subdirectory(executor)
nebula_add_subdirectory(optimizer)
nebula_add_subdirectory(planner)
nebula_add_subdirectory(scheduler)
nebula_add_subdirectory(service)
nebula_add_subdirectory(session)
nebula_add_subdirectory(stats)
nebula_add_subdirectory(util)
nebula_add_subdirectory(validator)
nebula_add_subdirectory(visitor)
nebula_add_library(util_obj SHARED
$<TARGET_OBJECTS:util_obj_impl>
$<TARGET_OBJECTS:graph_context_obj_impl>
$<TARGET_OBJECTS:plan_obj_impl>
)
target_link_libraries(util_obj PUBLIC graph_session_obj meta_client_obj graph_flags_obj memory_obj  ft_es_graph_adapter_obj expression_obj expr_visitor_obj  meta_obj idgenerator_obj )
add_library(graph_context_obj ALIAS util_obj)
add_library(plan_obj ALIAS util_obj)
