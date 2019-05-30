add_library(
    base_obj OBJECT
    Base.cpp
    Cord.cpp
    Configuration.cpp
    Status.cpp
    SanitizerOptions.cpp
    SignalHandler.cpp
    NebulaKeyUtils.cpp
)
add_dependencies(base_obj third-party)
IF(${PCHSupport_FOUND})
    add_precompiled_header(base_obj Base.h third-party)
ENDIF(${PCHSupport_FOUND})

add_custom_target(
    clean-pch
    COMMAND "rm" "-fr" "Base.h.gch"
)

add_subdirectory(test)


