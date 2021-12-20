# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    function_manager_obj OBJECT
    FunctionManager.cpp
    ../geo/GeoFunction.cpp
    WasmFunctionManager.cpp
)


nebula_add_library(
    agg_function_manager_obj OBJECT
    AggFunctionManager.cpp
)

#about wasm .....
nebula_add_executable(
        NAME TestWasm
        SOURCES TestWasm.cpp
        OBJECTS $<TARGET_OBJECTS:base_obj>
        LIBRARIES
            ${WASMTIME_LIBRARYS}
            ${WASMEDGE_LIBRARYS}
)

nebula_add_library(
        wasm_function_obj OBJECT
        WasmFunctionManager.cpp
        HTTPRequest.hpp
)

target_link_libraries(wasm_function_obj ${WASMTIME_LIBRARYS} ${WASMEDGE_LIBRARYS})


nebula_add_subdirectory(test)
