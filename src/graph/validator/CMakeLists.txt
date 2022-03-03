# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    validator_obj SHARED
    Validator.cpp
    AssignmentValidator.cpp
    GoValidator.cpp
    PipeValidator.cpp
    SequentialValidator.cpp
    SetValidator.cpp
    UseValidator.cpp
    GetSubgraphValidator.cpp
    AdminValidator.cpp
    AdminJobValidator.cpp
    MaintainValidator.cpp
    MutateValidator.cpp
    ACLValidator.cpp
    FetchEdgesValidator.cpp
    FetchVerticesValidator.cpp
    LimitValidator.cpp
    OrderByValidator.cpp
    YieldValidator.cpp
    ExplainValidator.cpp
    GroupByValidator.cpp
    FindPathValidator.cpp
    LookupValidator.cpp
    MatchValidator.cpp
    DownloadValidator.cpp
    IngestValidator.cpp
)

target_link_libraries(validator_obj PUBLIC charset_obj "${Glog_LIBRARY}" )
nebula_add_subdirectory(test)
