find_file(
    thrift_reconnecting_channel_cpp
    ReconnectingRequestChannel.cpp
    PATHS ${VGRAPH_HOME}/third-party/fbthrift/_build
    PATH_SUFFIXES *
    NO_DEFAULT_PATH
)
if (${thrift_reconnecting_channel_cpp}
    STREQUAL
    "thrift_reconnecting_channel_cpp-NOTFOUND")
    message(
        FATAL_ERROR
        "Cannot find ReconnectingRequestChannel.cpp in fbthrift"
    )
endif()

add_library(
    thrift_obj OBJECT
    ThriftClientManager.cpp
    ${thrift_reconnecting_channel_cpp}
)

add_dependencies(thrift_obj base_obj)

# add_subdirectory(test)

