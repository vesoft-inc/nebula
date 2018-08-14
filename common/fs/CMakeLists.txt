add_library(fs_obj OBJECT FileUtils.cpp TempDir.cpp)

add_dependencies(fs_obj base_obj)

add_subdirectory(test)


