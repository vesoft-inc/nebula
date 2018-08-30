add_library(concurrent_obj OBJECT thread/NamedThread.cpp
        thread/GenericWorker.cpp
        thread/GenericThreadPool.cpp
        sync/Barrier.cpp
        sync/Latch.cpp
)
add_subdirectory(test)
