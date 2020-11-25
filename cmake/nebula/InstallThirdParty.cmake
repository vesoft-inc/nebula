set(third_party_install_prefix ${NEBULA_THIRDPARTY_ROOT})
message(STATUS "Downloading prebuilt third party automatically...")
if(${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang")
    if(NOT ${NEBULA_CLANG_USED_GCC_TOOLCHAIN} STREQUAL "")
        set(cxx_cmd ${NEBULA_CLANG_USED_GCC_TOOLCHAIN}/bin/g++)
    else()
        set(cxx_cmd ${CMAKE_CXX_COMPILER})
    endif()
else()
    set(cxx_cmd ${CMAKE_CXX_COMPILER})
endif()
if(${DISABLE_CXX11_ABI})
    set(cxx_cmd "${cxx_cmd} -D_GLIBCXX_USE_CXX11_ABI=0")
else()
    set(cxx_cmd "${cxx_cmd} -D_GLIBCXX_USE_CXX11_ABI=1")
endif()
message(STATUS "cxx_cmd: ${cxx_cmd}")
execute_process(
    COMMAND
        env CXX=${cxx_cmd} ${nebula_common_source_dir}/third-party/install-third-party.sh --prefix=${third_party_install_prefix}
    WORKING_DIRECTORY
        ${CMAKE_BINARY_DIR}
)
unset(third_party_install_prefix)
