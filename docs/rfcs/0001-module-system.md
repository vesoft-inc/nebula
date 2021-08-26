# Summary

Provide the modular build system to abstract the component of objects for outside project.

# Motivation

Some outside project will use some components directly, such as cpp client will reference the `common` and `interface` code. And useful to `UDF` and `extension` system etc. developing in future.

User could only build used modules instead of build the whole project or build many objects directly.

# Usage explanation

For the module user, just try these steps:

1. Include the nebula project, build the module you need, such as `cmake --build --target module_common` will build the module contains all objects in common source folder.
2. Find the nebula as package.
3. Include the reference headers, link with used objects. And the object will add prefix `nebula_` to name, so user need link to `nebula_base_obj` instead of `base_obj`.

# Design explanation

The module is a target which depends the whole objects belong to itself.
And define a macro to simplify add object to module:

```cmake
macro(nebula_add_module_library name type module)
    nebula_add_library(${name} ${type} ${ARGN})
    export(
        TARGETS ${name}
        NAMESPACE "nebula_"
        APPEND
        FILE ${CMAKE_BINARY_DIR}/${PACKAGE_NAME}-config.cmake
    )
    add_dependencies(${module} ${name})
endmacro()
```

# Rationale and alternatives

# Drawbacks

# Prior art

# Unresolved questions

# Future possibilities

Maybe could use in inner too. Replace link to objects to avoid write so many objects. And make the relationship between build components more clear.
