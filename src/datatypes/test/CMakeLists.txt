nebula_add_test(
    NAME
        host_addr_test
    SOURCES
        HostAddrTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        gtest
)


nebula_add_test(
    NAME
        date_test
    SOURCES
        DateTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        gtest
)


nebula_add_test(
    NAME
        value_test
    SOURCES
        ValueTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        gtest
)
