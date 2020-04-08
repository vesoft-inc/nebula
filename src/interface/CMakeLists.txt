
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-Wno-maybe-uninitialized)
endif()

set(THRIFT1 ${Fbthrift_BIN})
set(THRIFT_TEMPLATES ${Fbthrift_INCLUDE_DIR}/thrift/templates)
include(ThriftGenerate)

set(type "OBJECT")
if (ENABLE_TESTING AND (NOT ENABLE_ONLY_SHARED))
    set(type "ALL")
elseif (ENABLE_TESTING OR ENABLE_ONLY_SHARED)
    set(type "SHARED")
elseif (NOT ENABLE_ONLY_SHARED)
    set(type "OBJECT")
endif()

thrift_generate("common" "" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "interface" ${type})

# Target object name : graph_thrift_obj
thrift_generate("graph" "GraphService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "interface" ${type})

# Target object name : raftex_thrift_obj
thrift_generate("raftex" "RaftexService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "interface" ${type})

# Target object name : storage_thrift_obj
thrift_generate("storage" "StorageService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "interface" ${type})

# Target object name : meta_thrift_obj
thrift_generate("meta" "MetaService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "interface" ${type})

add_custom_target(
    clean-interface
    COMMAND "rm" "-fr" "gen-cpp2" "gen-java" "gen-go" "gen-py"
)
