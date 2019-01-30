add_subdirectory(cpp)

if (NOT SKIP_JAVA_CLIENT)
    add_subdirectory(java)
else()
    message(STATUS "Skip building the java client")
endif()
