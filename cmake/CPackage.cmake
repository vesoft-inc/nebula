# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
#


# Used to package into deb or RPM files
macro(package to_one name home_page scripts_dir)
    set(CPACK_PACKAGE_DESCRIPTION ${name})
    set(CPACK_PACKAGE_CONTACT ${name})
    set(CPACK_PACKAGE_VERSION ${NEBULA_BUILD_VERSION})
    set(CPACK_RPM_PACKAGE_LICENSE "Apache 2.0")
    set(CPACK_PACKAGE_NAME ${name})
    # set(CPACK_SET_DESTDIR TRUE)
    set(CPACK_PACKAGE_RELOCATABLE ON)
    set(CPACK_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
    set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
    if (EXISTS "/etc/redhat-release")
        if(ENABLE_PACKAGE_TAR)
            set(CPACK_GENERATOR "TGZ")
        else()
            set(CPACK_GENERATOR "RPM")
        endif()
        file (STRINGS "/etc/redhat-release" SYSTEM_NAME)
        if (${SYSTEM_NAME} MATCHES "CentOS")
	    set(HOST_SYSTEM_NAME "el")
            execute_process(
                COMMAND echo ${SYSTEM_NAME}
                COMMAND tr -dc "0-9."
                COMMAND cut -d "." -f1
                OUTPUT_VARIABLE HOST_SYSTEM_VER
                OUTPUT_STRIP_TRAILING_WHITESPACE
            )
            string(CONCAT HOST_SYSTEM_VER ${HOST_SYSTEM_NAME} ${HOST_SYSTEM_VER})
        elseif (${SYSTEM_NAME} MATCHES "Fedora")
	    set(HOST_SYSTEM_NAME "fc")
            execute_process(
                COMMAND echo ${SYSTEM_NAME}
                COMMAND cut -d " " -f3
                OUTPUT_VARIABLE HOST_SYSTEM_VER
                OUTPUT_STRIP_TRAILING_WHITESPACE
            )
            string(CONCAT HOST_SYSTEM_VER ${HOST_SYSTEM_NAME} ${HOST_SYSTEM_VER})
        else()
            set(HOST_SYSTEM_VER "Unknown")
        endif()
    elseif (EXISTS "/etc/lsb-release")
        if(ENABLE_PACKAGE_TAR)
            set(CPACK_GENERATOR "TGZ")
        else()
            set(CPACK_GENERATOR "DEB")
        endif()
        file (STRINGS "/etc/lsb-release" SYSTEM_NAME)
        execute_process(
            COMMAND echo "${SYSTEM_NAME}"
            COMMAND cut -d ";" -f 1
            COMMAND cut -d "=" -f 2
            OUTPUT_VARIABLE HOST_SYSTEM_NAME
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        execute_process(
            COMMAND echo "${SYSTEM_NAME}"
            COMMAND cut -d ";" -f 2
            COMMAND cut -d "=" -f 2
            OUTPUT_VARIABLE HOST_SYSTEM_VER
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        string(REPLACE "." "" HOST_SYSTEM_VER ${HOST_SYSTEM_VER})
        if (${HOST_SYSTEM_NAME} MATCHES "Ubuntu")
            string(CONCAT HOST_SYSTEM_VER "ubuntu" ${HOST_SYSTEM_VER})
            # the ubuntu need to modify the architecture name
            if (${CMAKE_HOST_SYSTEM_PROCESSOR} MATCHES "x86_64")
                set(CMAKE_HOST_SYSTEM_PROCESSOR "amd64")
            endif()
        # Adapt the Kylin system
        elseif (${HOST_SYSTEM_NAME} MATCHES "Kylin" AND ${CMAKE_HOST_SYSTEM_PROCESSOR} MATCHES "aarch64")
            string(CONCAT HOST_SYSTEM_VER ${HOST_SYSTEM_NAME} ${HOST_SYSTEM_VER})
            set(CMAKE_HOST_SYSTEM_PROCESSOR "arm64")
        endif()
    elseif (EXISTS "/etc/issue")
        file (STRINGS "/etc/issue" SYSTEM_NAME)
        execute_process(
            COMMAND echo "${SYSTEM_NAME}"
            COMMAND sed -n "1p"
            COMMAND cut -d " " -f 1
            OUTPUT_VARIABLE HOST_SYSTEM_NAME
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        execute_process(
            COMMAND echo "${SYSTEM_NAME}"
            COMMAND sed -n "1p"
            COMMAND cut -d " " -f 3
            OUTPUT_VARIABLE HOST_SYSTEM_VER
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        if (${HOST_SYSTEM_NAME} MATCHES "Debian")
            execute_process(
                COMMAND echo "${SYSTEM_NAME}"
                COMMAND sed -n "1p"
                COMMAND cut -d " " -f 3
                OUTPUT_VARIABLE HOST_SYSTEM_VER
                OUTPUT_STRIP_TRAILING_WHITESPACE
            )
            if(ENABLE_PACKAGE_TAR)
                set(CPACK_GENERATOR "TGZ")
            else()
                set(CPACK_GENERATOR "DEB")
            endif()
        # Adapt the NeoKylin system
        elseif (${HOST_SYSTEM_NAME} MATCHES "NeoKylin" AND ${CMAKE_HOST_SYSTEM_PROCESSOR} MATCHES "mips64")
            execute_process(
                COMMAND echo "${SYSTEM_NAME}"
                COMMAND sed -n "1p"
                COMMAND cut -d " " -f 4
                OUTPUT_VARIABLE HOST_SYSTEM_VER
                OUTPUT_STRIP_TRAILING_WHITESPACE
            )
            set(CMAKE_HOST_SYSTEM_PROCESSOR "mips64el")
            if(ENABLE_PACKAGE_TAR)
                set(CPACK_GENERATOR "TGZ")
            else()
                set(CPACK_GENERATOR "RPM")
            endif()
        endif()
        string(CONCAT HOST_SYSTEM_VER ${HOST_SYSTEM_NAME} ${HOST_SYSTEM_VER})
        set(CMAKE_HOST_SYSTEM_PROCESSOR "mips64el")
    else()
        set(HOST_SYSTEM_VER "Unknown")
    endif()

    print_config(NEBULA_BUILD_VERSION)
    print_config(HOST_SYSTEM_NAME)
    print_config(HOST_SYSTEM_VER)
    print_config(CPACK_GENERATOR)
    print_config(CMAKE_HOST_SYSTEM_PROCESSOR)

    set(CPACK_PACKAGE_FILE_NAME ${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}.${HOST_SYSTEM_VER}.${CMAKE_HOST_SYSTEM_PROCESSOR})
    if (${to_one})
        set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)
    else ()
        set(CPACK_COMPONENTS_GROUPING ONE_PER_GROUP)
    endif()

    set(CPACK_DEB_COMPONENT_INSTALL YES)
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE ${CMAKE_HOST_SYSTEM_PROCESSOR})
    set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${home_page})
    set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${scripts_dir}/postinst)

    set(CPACK_RPM_SPEC_MORE_DEFINE "%define debug_package %{nil}
            %define __os_install_post %{nil}")
    set(CPACK_RPM_COMPONENT_INSTALL YES)
    set(CPACK_RPM_PACKAGE_ARCHITECTURE ${CMAKE_HOST_SYSTEM_PROCESSOR})
    set(CPACK_RPM_PACKAGE_URL ${home_page})
    set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${scripts_dir}/rpm_postinst)
    set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION /usr/local)
    set(CPACK_RPM_PACKAGE_RELOCATABLE ON)

    include(CPack)

    cpack_add_component(common GROUP common)
    cpack_add_component(graph GROUP graph DEPENDS common)
    cpack_add_component(storage GROUP storage DEPENDS common)
    cpack_add_component(meta GROUP meta DEPENDS common)
    cpack_add_component(tool GROUP tool)
    cpack_add_component(nebula-console GROUP nebula-console)
    cpack_add_component_group(common)
    cpack_add_component_group(graph)
    cpack_add_component_group(storage)
    cpack_add_component_group(meta)
    cpack_add_component_group(tool)
    cpack_add_component_group(nebula-console)
endmacro()
