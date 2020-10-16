# Used to package into deb or RPM files

macro(package is_one)
    message(STATUS "is_one is ${is_one}")
    set(CPACK_PACKAGE_DESCRIPTION "Nebula Graph")
    set(CPACK_PACKAGE_CONTACT "Nebula Graph")
    set(CPACK_PACKAGE_VERSION ${NEBULA_BUILD_VERSION})
    set(CPACK_RPM_PACKAGE_LICENSE "Apache 2.0 + Common Clause 1.0")
    set(CPACK_PACKAGE_NAME nebula)
    set(CPACK_PACKAGE_VENDOR "vesoft inc")
    # set(CPACK_SET_DESTDIR TRUE)
    set(CPACK_PACKAGE_RELOCATABLE ON)
    set(CPACK_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
    set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
    if (${is_one})
        set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)
    else ()
        set(CPACK_COMPONENTS_GROUPING ONE_PER_GROUP)
    endif()

    set(CPACK_DEBIAN_PACKAGE_MAINTAINER "vesoft inc")
    set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "A distributed, scalable, lightning-fast graph database.")
    set(CPACK_DEB_COMPONENT_INSTALL YES)
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
    set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "https://github.com/vesoft-inc/nebula/releases")
    set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${CMAKE_CURRENT_SOURCE_DIR}/package/postinst)

    set(CPACK_RPM_PACKAGE_GROUP "vesoft inc")
    set(CPACK_RPM_PACKAGE_DESCRIPTION "A distributed, scalable, lightning-fast graph database.")
    set(CPACK_RPM_SPEC_MORE_DEFINE "%define debug_package %{nil}
            %define __os_install_post %{nil}")
    set(CPACK_RPM_COMPONENT_INSTALL YES)
    set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
    set(CPACK_RPM_PACKAGE_URL "https://github.com/vesoft-inc/nebula/releases")
    set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/package/rpm_postinst)
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
