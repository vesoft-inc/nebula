add_library(
    kvstore_obj OBJECT
    Part.cpp
    RocksdbEngine.cpp
    PartManager.cpp
    KVStoreImpl.cpp
)
add_dependencies(kvstore_obj base_obj)

add_subdirectory(test)

