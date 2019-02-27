add_executable(
    flags_access_test
    FlagsAccessTest.cpp
    $<TARGET_OBJECTS:ws_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:fs_obj>
)
nebula_link_libraries(
    flags_access_test
    proxygenhttpserver
    proxygenlib
    wangle
    gtest
)
nebula_add_test(flags_access_test)

