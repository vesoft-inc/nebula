/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_THRIFTTYPES_H_
#define COMMON_BASE_THRIFTTYPES_H_

#include <cstdint>

namespace nebula {

// Raft related types
using ClusterID = int64_t;
using GraphSpaceID = int32_t;
using PartitionID = int32_t;
using TermID = int64_t;
using LogID = int64_t;
using IPv4 = int32_t;
using Port = int32_t;

using VertexID = int64_t;
using TagID = int32_t;
using TagVersion = int64_t;
using EdgeType = int32_t;
using EdgeRanking = int64_t;
using EdgeVersion = int64_t;
using SchemaVer = int64_t;
using IndexID = int32_t;
using UserID = int32_t;
using Timestamp = int64_t;


struct Date {
    int16_t year;
    int8_t month;
    int8_t day;
};


struct DateTime {
    int16_t year;
    int8_t month;
    int8_t day;
    int8_t hour;
    int8_t minute;
    int8_t sec;
    int32_t microsec;
    int32_t timezone;
};

}  // namespace nebula
#endif  // COMMON_BASE_THRIFTTYPES_H_

