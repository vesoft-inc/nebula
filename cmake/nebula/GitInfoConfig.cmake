if (GIT_INFO_SHA)
    string(REGEX REPLACE "[^0-9a-f]+" "" GIT_INFO_SHA "${GIT_INFO_SHA}")
    add_definitions(-DGIT_INFO_SHA=${GIT_INFO_SHA})
endif()

if (NEBULA_BUILD_VERSION)
    add_definitions(-DNEBULA_BUILD_VERSION=${NEBULA_BUILD_VERSION})
endif()
