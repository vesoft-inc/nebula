# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#


# Used to package into deb or RPM files
macro(package to_one name home_page scripts_dir)
    set(CPACK_PACKAGE_DESCRIPTION ${name})
    set(CPACK_PACKAGE_CONTACT ${name})
    set(CPACK_PACKAGE_VERSION ${NEBULA_BUILD_VERSION})
    set(CPACK_RPM_PACKAGE_LICENSE "Apache 2.0 + Common Clause 1.0")
    set(CPACK_PACKAGE_NAME ${name})
    # set(CPACK_SET_DESTDIR TRUE)
    set(CPACK_PACKAGE_RELOCATABLE ON)
    set(CPACK_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
    set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
    if (${to_one})
        set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)
    else ()
        set(CPACK_COMPONENTS_GROUPING ONE_PER_GROUP)
    endif()

    set(CPACK_DEB_COMPONENT_INSTALL YES)
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
    set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${home_page})
    set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${scripts_dir}/postinst)

    set(CPACK_RPM_SPEC_MORE_DEFINE "%define debug_package %{nil}
            %define __os_install_post %{nil}")
    set(CPACK_RPM_COMPONENT_INSTALL YES)
    set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
    set(CPACK_RPM_PACKAGE_URL ${home_page})
    set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${scripts_dir}/rpm_postinst)
    set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION /usr/local)
    set(CPACK_RPM_PACKAGE_RELOCATABLE ON)

    include(CPack)

    cpack_add_component(common GROUP common)
    cpack_add_component(graph GROUP graph DEPENDS common)
    cpack_add_component(storage GROUP storage DEPENDS common)
    cpack_add_component(meta GROUP meta DEPENDS common)
    cpack_add_component(console GROUP console)
    cpack_add_component(tool GROUP tool)
    cpack_add_component_group(common)
    cpack_add_component_group(graph)
    cpack_add_component_group(storage)
    cpack_add_component_group(meta)
    cpack_add_component_group(console)
    cpack_add_component_group(tool)
endmacro()
