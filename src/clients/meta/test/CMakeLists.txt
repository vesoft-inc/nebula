nebula_add_test(
    NAME file_based_cluster_id_man_test
    SOURCES FileBasedClusterIdManTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:file_based_cluster_id_man_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES gtest
)

