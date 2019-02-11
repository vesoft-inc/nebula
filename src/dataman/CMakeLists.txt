add_library(
    dataman_obj OBJECT
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
    RowSetReader.cpp
    RowSetWriter.cpp
    RowReader.cpp
    RowUpdater.cpp
    RowWriter.cpp
    NebulaCodecImpl.cpp
)
add_dependencies(dataman_obj base_obj graph_thrift_obj)

add_subdirectory(test)

add_library(
    dataman_static STATIC
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
    RowSetReader.cpp
    RowSetWriter.cpp
    RowSetReader.cpp
    RowUpdater.cpp
    RowWriter.cpp
    NebulaCodecImpl.cpp
)

add_dependencies(dataman_static base_obj graph_thrift_obj)

set(DATAMAN_OUT ${PROJECT_SOURCE_DIR}/src/dataman/_build)
add_custom_command(TARGET dataman_static
    POST_BUILD
    COMMAND rm ARGS      ${DATAMAN_OUT}/libdataman.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${DATAMAN_OUT}/libdataman_static.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/bzip2/_install/lib/libbz2.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/double-conversion/_install/lib/libdouble-conversion.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/folly/_install/lib/libfolly.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/rocksdb/_install/lib/librocksdb.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/fbthrift/_install/lib/libthrift.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/gflags/_install/lib/libgflags.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/glog/_install/lib/libglog.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/googletest/_install/lib/libgtest.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/jemalloc/_install/lib/libjemalloc.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/libevent/_install/lib/libevent.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/mstch/_install/lib/libmstch.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/proxygen/_install/lib/libproxygenlib.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/snappy/_install/lib/libsnappy.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/wangle/_install/lib/libwangle.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/zlib/_install/lib/libz.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a ${PROJECT_SOURCE_DIR}/third-party/zstd/_install/lib/libzstd.a
    COMMAND ar ARGS crsT ${DATAMAN_OUT}/libdataman.a /home/engshare/boost/lib/*.a
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
