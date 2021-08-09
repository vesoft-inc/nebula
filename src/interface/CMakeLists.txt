# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-Wno-maybe-uninitialized)
endif()

set(THRIFT1 ${Fbthrift_BIN})
set(THRIFT_TEMPLATES ${Fbthrift_INCLUDE_DIR}/thrift/templates)
include(ThriftGenerate)

# Target object name : common_thrift_obj
thrift_generate("common" "" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "common/interface")

# Target object name : graph_thrift_obj
thrift_generate("graph" "GraphService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "common/interface")

# Target object name : raftex_thrift_obj
thrift_generate("raftex" "RaftexService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "common/interface")

# Target object name : storage_thrift_obj
thrift_generate(
    "storage"
    "GraphStorageService;StorageAdminService;GeneralStorageService;InternalStorageService"
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
    "common/interface"
)

# Target object name : meta_thrift_obj
thrift_generate("meta" "MetaService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "common/interface")

add_custom_target(
    clean-interface
    COMMAND "rm" "-fr" "gen-cpp2" "gen-java" "gen-go" "gen-py" "gen-rust"
)
