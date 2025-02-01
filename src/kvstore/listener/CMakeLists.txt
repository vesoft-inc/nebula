# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    listener_obj OBJECT
    Listener.cpp
    elasticsearch/ESListener.cpp
)

target_link_libraries(listener_obj
    PUBLIC
        base_obj
        kvstore_obj
        es_adapter_obj
        http_client_obj
        embedding_client_obj
        ${NEBULA_GFLAGS_LIBS}
)

nebula_add_subdirectory(test)
