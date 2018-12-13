set(ADDITIONAL_MAKE_CLEAN_FILES "_build/")

SET(build_command
    VGRAPH_C_COMPILER=${CMAKE_C_COMPILER}
    VGRAPH_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    VGRAPH_CMAKE=${CMAKE_COMMAND}
    VGRAPH_FLEX=${FLEX_EXECUTABLE}
    VGRAPH_KRB5_ROOT=${VGRAPH_KRB5_ROOT}
    VGRAPH_LIBUNWIND_ROOT=${VGRAPH_LIBUNWIND_ROOT}
    VGRAPH_OPENSSL_ROOT=${VGRAPH_OPENSSL_ROOT}
    VGRAPH_BOOST_ROOT=${VGRAPH_BOOST_ROOT}
    VGRAPH_GPERF_BIN_DIR=${VGRAPH_GPERF_BIN_DIR}
    "./build.sh" "-j2" ">" "./_build.log" "2>&1"
)

macro(build_external tgtName)
    add_custom_target(
        ${tgtName}
        COMMAND
            VGRAPH_C_COMPILER=${CMAKE_C_COMPILER}
            VGRAPH_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            VGRAPH_CMAKE=${CMAKE_COMMAND}
            VGRAPH_FLEX=${FLEX_EXECUTABLE}
            VGRAPH_KRB5_ROOT=${VGRAPH_KRB5_ROOT}
            VGRAPH_LIBUNWIND_ROOT=${VGRAPH_LIBUNWIND_ROOT}
            VGRAPH_OPENSSL_ROOT=${VGRAPH_OPENSSL_ROOT}
            VGRAPH_BOOST_ROOT=${VGRAPH_BOOST_ROOT}
            VGRAPH_GPERF_BIN_DIR=${VGRAPH_GPERF_BIN_DIR}
            "./build.sh" "-j2" ">" "./_build.log" "2>&1"
        DEPENDS ${ARGN}
    )
endmacro(build_external)

add_subdirectory(bzip2)
add_subdirectory(double-conversion)
add_subdirectory(fatal)
add_subdirectory(fbthrift)
add_subdirectory(folly)
add_subdirectory(gflags)
add_subdirectory(glog)
add_subdirectory(googletest)
add_subdirectory(jemalloc)
add_subdirectory(libevent)
add_subdirectory(mstch)
add_subdirectory(proxygen)
add_subdirectory(rocksdb)
add_subdirectory(snappy)
add_subdirectory(wangle)
add_subdirectory(zlib)
add_subdirectory(zstd)

add_custom_target(
    third-party ALL
    DEPENDS tgt_fbthrift tgt_rocksdb tgt_proxygen tgt_googletest tgt_jemalloc
)

add_custom_target(
    clean-third-party
    DEPENDS clean-bzip2 clean-double-conversion clean-fatal clean-fbthrift clean-folly clean-gflags clean-glog clean-googletest clean-jemalloc clean-libevent clean-mstch clean-proxygen clean-rocksdb clean-snappy clean-wangle clean-zlib clean-zstd
)
