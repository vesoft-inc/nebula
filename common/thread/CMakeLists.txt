add_library(
        thread_obj OBJECT
        NamedThread.cpp
        GenericWorker.cpp
        GenericThreadPool.cpp
)
add_dependencies(thread_obj base_obj)

add_subdirectory(test)
