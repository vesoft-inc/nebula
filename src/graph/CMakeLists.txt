add_library(
    graph_obj
    OBJECT
    GraphFlags.cpp
    GraphService.cpp
    GraphHttpHandler.cpp
    ClientSession.cpp
    SessionManager.cpp
    ExecutionEngine.cpp
    ExecutionContext.cpp
    ExecutionPlan.cpp
    Executor.cpp
    TraverseExecutor.cpp
    SequentialExecutor.cpp
    UseExecutor.cpp
    GoExecutor.cpp
    PipeExecutor.cpp
    # DefineEdgeExecutor.cpp
    # DefineTagExecutor.cpp
    AlterEdgeExecutor.cpp
    AlterTagExecutor.cpp
    # DescribeTagExecutor.cpp
    # DescribeEdgeExecutor.cpp
    InsertVertexExecutor.cpp
    InsertEdgeExecutor.cpp
    AssignmentExecutor.cpp
    InterimResult.cpp
    VariableHolder.cpp
    AddHostsExecutor.cpp
    RemoveHostsExecutor.cpp
    CreateSpaceExecutor.cpp
    DropSpaceExecutor.cpp
    ShowExecutor.cpp
    mock/PropertiesSchema.cpp
    mock/EdgeSchema.cpp
    mock/TagSchema.cpp
    mock/SchemaManager.cpp
    mock/StorageService.cpp
)
add_dependencies(
    graph_obj
    base_obj
    parser_obj
    graph_thrift_obj
    ws_obj
    meta_thrift_obj
    meta_client
    schema_obj
)

add_subdirectory(test)
