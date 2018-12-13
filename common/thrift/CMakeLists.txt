get_property(
    thrift_channel
    GLOBAL
    PROPERTY fbthrift_src_file
)

if ("${thrift_channel}" STREQUAL "")
    message(
        FATAL_ERROR
        "Cannot find ReconnectingRequestChannel.cpp in fbthrift"
    )
endif()

set_property(
    SOURCE ${thrift_channel}
    PROPERTY GENERATED true
)

add_library(
    thrift_obj OBJECT
    ThriftClientManager.cpp
    ${thrift_channel}
)

add_dependencies(thrift_obj base_obj)

# add_subdirectory(test)

