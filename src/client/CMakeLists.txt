nebula_add_subdirectory(cpp)

if (NOT SKIP_JAVA_CLIENT)
    nebula_add_subdirectory(java)
else()
    message(STATUS "Skip building the java client")
endif()
