add_executable(
    file_based_part_manager_test
    FileBasedPartManagerTest.cpp
    $<TARGET_OBJECTS:meta_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:network_obj>
)
nebula_link_libraries(
    file_based_part_manager_test
    ${THRIFT_LIBRARIES}
    gtest
)
add_test(NAME file_based_part_manager_test COMMAND file_based_part_manager_test)


add_executable(
    file_based_schema_manager_test
    FileBasedSchemaManagerTest.cpp
    $<TARGET_OBJECTS:meta_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:network_obj>
)
nebula_link_libraries(
    file_based_schema_manager_test
    ${THRIFT_LIBRARIES}
    gtest
)
add_test(NAME file_based_schema_manager_test COMMAND file_based_schema_manager_test)

