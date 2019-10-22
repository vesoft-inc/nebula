nebula_add_library(
    client_cpp_obj OBJECT
    NebulaClientImpl.cpp
    ExecuteResponse.cpp
)

if (NOT ENABLE_CPPCLIENT_LIB)
    return()
endif()

nebula_add_library(
    nebulaClient SHARED
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:schema_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:gflags_man_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:graph_thrift_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    ExecuteResponse.cpp
    NebulaClientImpl.cpp
    NebulaClient.cpp
)


target_link_libraries(
    nebulaClient
    ${OPENSSL_SSL_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${THRIFT_LIBRARIES}
    -lwangle
    -lfolly
    -lboost_context
    -lboost_system
    -lglog
    -lgflags
    -lsnappy
    -lunwind
    -lz
    -lzstd
    -levent
    -ldouble-conversion
    -static-libgcc
    -static-libstdc++
    -lpthread
    -ldl
)
