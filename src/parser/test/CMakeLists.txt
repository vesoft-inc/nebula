add_executable(
    parser_test
    ParserTest.cpp
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:base_obj>
)
target_link_libraries(
    parser_test
    gtest
    gtest_main
    folly
    boost_system
    boost_context
    ${OPENSSL_LIBRARIES}
    ${KRB5_LIBRARIES}
    glog
    gflags
    event
    ${COMPRESSION_LIBRARIES}
    resolv
    double-conversion
    dl
    -pthread
)
target_include_directories(parser_test SYSTEM BEFORE PUBLIC ${FLEX_INCLUDE_DIRS})
add_test(NAME parser_test COMMAND parser_test)

add_executable(
    scanner_test
    ScannerTest.cpp
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:base_obj>
)
target_link_libraries(
    scanner_test
    gtest
    gtest_main
    glog
    gflags
    folly
    boost_system
    boost_context
    ${OPENSSL_LIBRARIES}
    ${KRB5_LIBRARIES}
    glog
    gflags
    event
    ${COMPRESSION_LIBRARIES}
    resolv
    double-conversion
    dl
    -pthread
)
target_include_directories(scanner_test SYSTEM BEFORE PUBLIC ${FLEX_INCLUDE_DIRS})
add_test(NAME scanner_test COMMAND scanner_test)
