# Summary

Provide the build facility to expose objects of nebula for outside project.

# Motivation

Some outside project will use some components of nebula directly, such as cpp client will reference the `common` and `interface` code. And also useful to `UDF` and `extension` system etc. developing in future.

# Usage explanation

For the module user, just try these steps:

1. Include the nebula project, build the objects you need, such as `cmake --build --target base_obj` will build the object contains all code in `base` source folder.
2. Find the nebula as package.
3. Include the reference headers, link with used objects. And the object will add prefix `nebula_` to name when exposed to outside, so user need link to `nebula_base_obj` instead of `base_obj`.

# Design explanation

There is a cmake macro to simplify expose object:

```cmake
macro(nebula_add_export_library name type)
    nebula_add_library(${name} ${type} ${ARGN})
    export(
        TARGETS ${name}
        NAMESPACE "nebula_"
        APPEND
        FILE ${CMAKE_BINARY_DIR}/${PACKAGE_NAME}-config.cmake
    )
endmacro()
```

# Rationale and alternatives

# Drawbacks

# Prior art

# Unresolved questions

# Future possibilities

Maybe we should provide shared library too. And the shared library could reduce the size of test binaries.
