nebula_add_all_library(
    thread
    NamedThread.cpp
    GenericWorker.cpp
    GenericThreadPool.cpp
)

nebula_add_subdirectory(test)
