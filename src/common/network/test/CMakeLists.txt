nebula_add_test(
    NAME
        network_utils_test
    SOURCES
        NetworkUtilsTest.cpp
    LIBRARIES
        base_lib
        network_lib
        fs_lib
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_executable(
    NAME
        network_utils_bm
    SOURCES
        NetworkUtilsBenchmark.cpp
    LIBRARIES
        base_lib
        network_lib
        fs_lib
        wangle
        follybenchmark
        boost_regex
)
