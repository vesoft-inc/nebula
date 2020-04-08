
# The base is a basic library. Other libraries are not allowed to depend on it.

if (NOT ENABLE_ONLY_SHARED)
    add_library(
        base_obj OBJECT
        Base.cpp
        Cord.cpp
        Configuration.cpp
        Status.cpp
        SanitizerOptions.cpp
        SignalHandler.cpp
        SlowOpTracker.cpp
    )
endif()

if (ENABLE_TESTING OR ENABLE_ONLY_SHARED)
    add_library(
        base_lib SHARED
        Base.cpp
        Cord.cpp
        Configuration.cpp
        Status.cpp
        SanitizerOptions.cpp
        SignalHandler.cpp
        SlowOpTracker.cpp
    )
endif()

IF(${PCHSupport_FOUND})
    if (NOT ENABLE_ONLY_SHARED)
        add_precompiled_header(base_obj Base.h)
    endif()
    if (ENABLE_TESTING OR ENABLE_ONLY_SHARED)
        add_precompiled_header(base_lib Base.h)
    endif()
ENDIF(${PCHSupport_FOUND})

add_custom_target(
    clean-pch
    COMMAND "rm" "-fr" "Base.h.gch"
)

nebula_add_subdirectory(test)


