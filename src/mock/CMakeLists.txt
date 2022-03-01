
nebula_add_library(
    mock_obj SHARED
    MockCluster.cpp
    MockData.cpp
    AdHocIndexManager.cpp
    AdHocSchemaManager.cpp
)
target_link_libraries(mock_obj PUBLIC expression_obj meta_obj graph_storage_service_handler )

