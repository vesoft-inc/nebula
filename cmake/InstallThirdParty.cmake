set(third_party_install_prefix ${CMAKE_BINARY_DIR}/third-party/install)
message(STATUS "Downloading prebuilt third party automatically...")
execute_process(
    COMMAND
        env CXX=${CMAKE_CXX_COMPILER} ${CMAKE_SOURCE_DIR}/third-party/install-third-party.sh --prefix=${third_party_install_prefix}
    WORKING_DIRECTORY
        ${CMAKE_BINARY_DIR}
)
if(EXISTS ${third_party_install_prefix})
    set(NEBULA_THIRDPARTY_ROOT ${third_party_install_prefix})
endif()
unset(third_party_install_prefix)
