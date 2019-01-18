# This code is based on
#   http://rosegarden.svn.sourceforge.net/viewvc/rosegarden/trunk/rosegarden/cmake_admin/FindPCHSupport.cmake?revision=7699&view=markup
#
# - Try to find precompiled headers support for GCC 3.4 and 4.x
# Once done this will define:
#
# Variable:
#   PCHSupport_FOUND
#
# Macro:
#   ADD_PRECOMPILED_HEADER

IF(CMAKE_COMPILER_IS_GNUCXX)
    EXEC_PROGRAM(
        ${CMAKE_CXX_COMPILER}
        ARGS 			--version
        OUTPUT_VARIABLE _compiler_output)
    STRING(REGEX REPLACE ".* ([0-9]\\.[0-9]\\.[0-9]) .*" "\\1"
           gcc_compiler_version ${_compiler_output})
    IF(gcc_compiler_version MATCHES "[4-9]\\.[0-9]\\.[0-9]")
        SET(PCHSupport_FOUND TRUE)
        MESSAGE("-- Found PCH Support: gcc compiler version is " ${gcc_compiler_version})
    ELSE(gcc_compiler_version MATCHES "[4-9]\\.[0-9]\\.[0-9]")
        IF(gcc_compiler_version MATCHES "3\\.4\\.[0-9]")
            SET(PCHSupport_FOUND TRUE)
            MESSAGE("-- Found PCH Support: gcc compiler version is " ${gcc_compiler_version})
        ENDIF(gcc_compiler_version MATCHES "3\\.4\\.[0-9]")
    ENDIF(gcc_compiler_version MATCHES "[4-9]\\.[0-9]\\.[0-9]")
ENDIF(CMAKE_COMPILER_IS_GNUCXX)



MACRO(ADD_PRECOMPILED_HEADER _targetName _input _dep)

    GET_FILENAME_COMPONENT(_name ${_input} NAME)
    SET(_source "${CMAKE_CURRENT_SOURCE_DIR}/${_input}")
    SET(_output "${CMAKE_CURRENT_BINARY_DIR}/${_name}.gch")
    STRING(TOUPPER "CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE}" _flags_var_name)
    SET(_compiler_FLAGS ${${_flags_var_name}})

    GET_DIRECTORY_PROPERTY(_directory_flags INCLUDE_DIRECTORIES)
    SET(_system_INCLUDE_DIRS "/usr/include" "/usr/local/include")
    FOREACH(item ${_directory_flags})
        # Exclude standard paths
        LIST(FIND _system_INCLUDE_DIRS ${item} _index)
        IF(NOT ${_index} EQUAL -1)
            continue()
        ENDIF(NOT ${_index} EQUAL -1)

        IF(item MATCHES "^${PROJECT_SOURCE_DIR}.*")
            # Directories in this project
            IF(item MATCHES "^${PROJECT_SOURCE_DIR}/third-party.*")
                # third-party
                LIST(APPEND _compiler_FLAGS "-isystem ${item}")
            ELSE(item MATCHES "^${PROJECT_SOURCE_DIR}/third-party.*")
                # Our own directories
                LIST(APPEND _compiler_FLAGS "-I ${item}")
            ENDIF(item MATCHES "^${PROJECT_SOURCE_DIR}/third-party.*")
        ELSE(item MATCHES "^${PROJECT_SOURCE_DIR}.*")
            # All of others
            LIST(APPEND _compiler_FLAGS "-isystem ${item}")
        ENDIF(item MATCHES "^${PROJECT_SOURCE_DIR}.*")
    ENDFOREACH(item)

	GET_DIRECTORY_PROPERTY(_directory_flags COMPILE_DEFINITIONS)
    FOREACH(item ${_directory_flags})
        LIST(APPEND _compiler_FLAGS "-D${item}")
    ENDFOREACH(item)

    SEPARATE_ARGUMENTS(_compiler_FLAGS)
    MESSAGE("Precompile header file " ${_source} " into " ${_output})
    ADD_CUSTOM_COMMAND(
        OUTPUT ${_output}
        COMMAND ${CMAKE_CXX_COMPILER}
				${_compiler_FLAGS}
				-x c++-header
				-o ${_output} ${_source}
        MAIN_DEPENDENCY ${_source})
    ADD_CUSTOM_TARGET(${_targetName}_gch DEPENDS ${_output})
    ADD_DEPENDENCIES(${_targetName}_gch ${_dep})
    ADD_DEPENDENCIES(${_targetName} ${_targetName}_gch)

ENDMACRO(ADD_PRECOMPILED_HEADER)
