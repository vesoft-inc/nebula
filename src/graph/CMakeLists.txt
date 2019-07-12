nebula_add_library(
    graph_obj OBJECT
    GraphFlags.cpp
    GraphService.cpp
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
    CreateEdgeExecutor.cpp
    CreateTagExecutor.cpp
    AlterEdgeExecutor.cpp
    AlterTagExecutor.cpp
    DropTagExecutor.cpp
    DropEdgeExecutor.cpp
    DescribeTagExecutor.cpp
    DescribeEdgeExecutor.cpp
    InsertVertexExecutor.cpp
    InsertEdgeExecutor.cpp
    AssignmentExecutor.cpp
    InterimResult.cpp
    VariableHolder.cpp
    AddHostsExecutor.cpp
    RemoveHostsExecutor.cpp
    CreateSpaceExecutor.cpp
    DropSpaceExecutor.cpp
    DescribeSpaceExecutor.cpp
    ShowExecutor.cpp
    YieldExecutor.cpp
    DownloadExecutor.cpp
    OrderByExecutor.cpp
    SchemaHelper.cpp
    ConfigExecutor.cpp
)
nebula_add_base_obj(graph_obj)

nebula_add_library(
    graph_http_handler OBJECT
    GraphHttpHandler.cpp
)

add_subdirectory(test)
