set(ADDITIONAL_MAKE_CLEAN_FILES "_build/")

macro(build_external tgtName)
    if (GLIBC_VERSION VERSION_LESS "2.17")
        set(GETTIME_NEEDS_POSIX_MACRO "true")
        set(GETTIME_LDFLAGS "-lrt")
    else()
        set(GETTIME_NEEDS_POSIX_MACRO)
        set(GETTIME_LDFLAGS)
    endif()
    add_custom_target(
        ${tgtName}
        COMMAND
            NEBULA_C_COMPILER=${CMAKE_C_COMPILER}
            NEBULA_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            NEBULA_CMAKE=${CMAKE_COMMAND}
            NEBULA_FLEX=${FLEX_EXECUTABLE}
            NEBULA_KRB5_ROOT=${NEBULA_KRB5_ROOT}
            NEBULA_LIBUNWIND_ROOT=${NEBULA_LIBUNWIND_ROOT}
            NEBULA_OPENSSL_ROOT=${NEBULA_OPENSSL_ROOT}
            NEBULA_BOOST_ROOT=${NEBULA_BOOST_ROOT}
            NEBULA_GPERF_BIN_DIR=${NEBULA_GPERF_BIN_DIR}
            NEBULA_GETTIME_NEEDS_POSIX_MACRO=${GETTIME_NEEDS_POSIX_MACRO}
            NEBULA_GETTIME_LDFLAGS=${GETTIME_LDFLAGS}
            "./build.sh" "-j${THIRD_PARTY_JOBS}" ">" "./_build.log" "2>&1"
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
