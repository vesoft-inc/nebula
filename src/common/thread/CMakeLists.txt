add_library(
        thread_obj OBJECT
        NamedThread.cpp
        GenericWorker.cpp
        GenericThreadPool.cpp
)
nebula_add_base_obj(thread_obj)

add_subdirectory(test)
