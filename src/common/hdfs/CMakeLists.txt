add_library(hdfs_helper_obj OBJECT HdfsCommandHelper.cpp)

add_dependencies(process_obj fs_obj base_obj)

add_subdirectory(test)
