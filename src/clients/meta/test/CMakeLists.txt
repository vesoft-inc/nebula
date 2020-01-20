nebula_add_test(
    NAME file_based_cluster_id_man_test
    SOURCES FileBasedClusterIdManTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_client_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:meta_obj>
        $<TARGET_OBJECTS:meta_thrift_obj>
        $<TARGET_OBJECTS:thrift_obj>
        $<TARGET_OBJECTS:network_obj>
    LIBRARIES gtest
)

