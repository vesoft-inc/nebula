set(BISON_EXECUTE_ENV "")
execute_process(
    COMMAND ${BISON_EXECUTABLE} --print-datadir
    OUTPUT_VARIABLE bison_encoded_data_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT EXISTS ${bison_encoded_data_dir})
    get_filename_component(bison_prefix ${BISON_EXECUTABLE} DIRECTORY)
    get_filename_component(bison_prefix ${bison_prefix} DIRECTORY)
    if(EXISTS ${bison_prefix}/share/bison)
        set(BISON_EXECUTE_ENV "BISON_PKGDATADIR=${bison_prefix}/share/bison")
    endif()
endif()
if(NOT ${BISON_EXECUTE_ENV} STREQUAL "")
    set(BISON_EXECUTABLE ${CMAKE_COMMAND} -E env ${BISON_EXECUTE_ENV} ${BISON_EXECUTABLE})
endif()

