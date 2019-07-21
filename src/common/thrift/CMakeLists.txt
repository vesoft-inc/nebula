
add_library(
    thrift_obj OBJECT
    ReconnectingRequestChannel.cpp
    ThriftClientManager.cpp
)
nebula_add_base_obj(thrift_obj)

# add_subdirectory(test)

