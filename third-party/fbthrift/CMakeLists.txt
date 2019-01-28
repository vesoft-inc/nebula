execute_process(
    COMMAND ./find_lib_name.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE fbthrift_ver_name
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

set_property(
    GLOBAL
    PROPERTY
        fbthrift_src_file
        ${CMAKE_CURRENT_SOURCE_DIR}/_build/${fbthrift_ver_name}/thrift/lib/cpp2/async/ReconnectingRequestChannel.cpp)

build_external(
    tgt_fbthrift
    tgt_glog tgt_gflags tgt_folly tgt_wangle tgt_fatal tgt_mstch tgt_zlib tgt_zstd
)

add_custom_target(
    clean-fbthrift
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

