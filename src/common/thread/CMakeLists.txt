nebula_add_library(
    thread_obj OBJECT
    NamedThread.cpp
    GenericWorker.cpp
    GenericThreadPool.cpp
)

nebula_add_library(
    thread_manager_obj OBJECT
    ThreadManager.cpp
)

nebula_add_subdirectory(test)
