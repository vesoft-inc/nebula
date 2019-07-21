add_library(
    base_obj OBJECT
    Base.cpp
    Cord.cpp
    Configuration.cpp
    Status.cpp
    SanitizerOptions.cpp
    SignalHandler.cpp
    NebulaKeyUtils.cpp
)

add_dependencies(base_obj common_thrift_obj graph_thrift_obj raftex_thrift_obj storage_thrift_obj meta_thrift_obj hbase_thrift_obj)
IF(${PCHSupport_FOUND})
    add_precompiled_header(base_obj Base.h common_thrift_obj graph_thrift_obj raftex_thrift_obj storage_thrift_obj meta_thrift_obj hbase_thrift_obj)
ENDIF(${PCHSupport_FOUND})

add_custom_target(
    clean-pch
    COMMAND "rm" "-fr" "Base.h.gch"
)

add_subdirectory(test)


