nebula_add_library(
    thread_obj OBJECT
    NamedThread.cpp
    GenericWorker.cpp
    GenericThreadPool.cpp
)

add_subdirectory(test)
