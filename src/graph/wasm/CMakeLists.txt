#include_directories(wasmtime-c-api/include)
#link_directories(wasmtime-c-api/lib)

#TestWasm
#add_executable(TestWasm TestWasm.cpp)
#target_include_directories(TestWasm INTERFACE wasmtime-c-api/include)
#target_link_libraries(TestWasm libwasmtime.a libwasmtime.so stdc++ pthread dl pthread dl)

nebula_add_executable(
        NAME TestWasm
        SOURCES TestWasm.cpp
        OBJECTS $<TARGET_OBJECTS:base_obj>
        LIBRARIES ${WASMTIME_LIBRARYS}
)
#WasmFunction
nebula_add_library(
        wasm_function_obj OBJECT
        WasmFunction.cpp
)

target_link_libraries(wasm_function_obj ${WASMTIME_LIBRARYS})

nebula_add_subdirectory(test)