# Summary

Provide the build facility to expose objects of nebula for outside project.

# Motivation

Some outside projects will use some components of nebula directly, such as CPP client will reference the `common` and `interface` code. And also useful to `UDF` and `extension` systems etc. developing in future.

# Usage explanation

For the module user, just try these steps:

1. Include the nebula project, build the objects you need, such as `cmake --build --target base_obj` which will build the object containing all code in `base` source folder.
2. Find the nebula as package.
3. Include the reference headers, link with used objects. A prefix `nebula_` will be added to the object's name when exposed to outside, so user should link to `nebula_base_obj` instead of `base_obj`.

# Design explanation

There is a cmake macro to simplify exposing object:

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
