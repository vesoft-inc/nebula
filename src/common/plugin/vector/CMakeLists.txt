# Copyright (c) 2024 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    embedding_client_obj
    OBJECT
    HttpEmbeddingClient.cpp
)

target_link_libraries(embedding_client_obj PUBLIC curl)

nebula_add_subdirectory(test) 