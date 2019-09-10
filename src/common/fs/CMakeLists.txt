nebula_add_library(
    fs_obj OBJECT
    FileUtils.cpp
    TempDir.cpp
    TempFile.cpp
)

nebula_add_subdirectory(test)


