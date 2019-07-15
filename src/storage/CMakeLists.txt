add_library(
    storage_service_handler OBJECT
    StorageServiceHandler.cpp
    QueryBaseProcessor.cpp
    AddVerticesProcessor.cpp
    AddEdgesProcessor.cpp
    QueryBoundProcessor.cpp
    QueryVertexPropsProcessor.cpp
    QueryEdgePropsProcessor.cpp
    QueryStatsProcessor.cpp
)
add_dependencies(
    storage_service_handler
    base_obj
    storage_thrift_obj
    kvstore_obj
    dataman_obj
    thread_obj
    stats_obj
)

add_library(
    storage_http_handler OBJECT
    StorageHttpIngestHandler.cpp
    StorageHttpStatusHandler.cpp
    StorageHttpDownloadHandler.cpp
)
add_dependencies(
    storage_http_handler
    base_obj
)

add_library(
    storage_client OBJECT
    client/StorageClient.cpp
)
add_dependencies(
    storage_client
    base_obj
    storage_thrift_obj
    dataman_obj
)

add_subdirectory(test)
