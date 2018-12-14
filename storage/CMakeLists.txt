add_library(
    storage_obj OBJECT
    Part.cpp
    RocksdbEngine.cpp
    PartManager.cpp
    KVStoreImpl.cpp
)
add_dependencies(storage_obj common)

add_subdirectory(test)

