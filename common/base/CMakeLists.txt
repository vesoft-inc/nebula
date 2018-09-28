add_library(base_obj OBJECT Cord.cpp Configuration.cpp)
IF(${PCHSupport_FOUND})
    add_precompiled_header(base_obj Base.h third-party)
ENDIF(${PCHSupport_FOUND})

add_custom_target(
    clean-pch
    COMMAND "rm" "-fr" "Base.h.gch"
)

add_subdirectory(test)


