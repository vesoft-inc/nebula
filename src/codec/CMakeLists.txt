# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    codec_obj OBJECT
    RowReaderV2.cpp
    RowWriterV2.cpp
    RowReaderWrapper.cpp
)

nebula_add_subdirectory(test)
