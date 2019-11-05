# package nebula to a deb/rpm file
set(CPACK_PACKAGE_DESCRIPTION "nebula-graph")
set(CPACK_PACKAGE_CONTACT "nebula-graph")
set(CPACK_PACKAGE_VERSION ${NEBULA_BUILD_VERSION})
set(CPACK_RPM_PACKAGE_LICENSE "Apache 2.0 + Common Clause 1.0")
set(CPACK_PACKAGE_NAME nebula-graph)
set(CPACK_SET_DESTDIR TRUE)
set(CPACK_PACKAGE_RELOCATABLE FALSE)
set(CPACK_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "https://github.com/vesoft-inc/nebula/releases")
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${CMAKE_CURRENT_SOURCE_DIR}/package/postinst)
set(CPACK_RPM_PACKAGE_REQUIRES "ncurses, readline")

set(CPACK_RPM_SPEC_MORE_DEFINE "%define debug_package %{nil}
        %define __os_install_post %{nil}")
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
set(CPACK_RPM_PACKAGE_URL "https://github.com/vesoft-inc/nebula/releases")
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/package/postinst)
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION /usr/local)

include(CPack)
