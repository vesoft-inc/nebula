add_library(fs_obj OBJECT FileUtils.cpp TempDir.cpp TempFile.cpp)
nebula_add_base_obj(fs_obj)

add_subdirectory(test)


