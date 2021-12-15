nebula_add_test(
        NAME wasm_function_test
        SOURCES
        WasmFunctionTest.cpp
        OBJECTS
        $<TARGET_OBJECTS:wasm_function_obj>
        LIBRARIES
        gtest
        gtest_main
        ${WASMTIME_LIBRARYS}
)
