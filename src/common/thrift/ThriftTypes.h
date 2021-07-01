/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_THRIFT_THRIFTTYPES_H_
#define COMMON_THRIFT_THRIFTTYPES_H_

#include <cstdint>
#include <string>

namespace nebula {
// Raft related types
using ClusterID = int64_t;
using GraphSpaceID = int32_t;
using PartitionID = int32_t;
using TermID = int64_t;
using LogID = int64_t;
using Port = int32_t;

// Starting from 2.0, both string and int64 vertex ids will be supported.
//
// The string id must be fixed-length (the length of the id will be specified
// as a Graph Space property). So the int64 id will be treated as 8-byte string
//
// If the length of a given id is shorter than the specified length, '\0' will
// be appended to the end
using VertexID = std::string;
using TagID = int32_t;
using TagVersion = int64_t;
using EdgeType = int32_t;
using EdgeRanking = int64_t;
using EdgeVersion = int64_t;
using EdgeVerPlaceHolder = char;
using SchemaVer = int64_t;
using IndexID = int32_t;
using Timestamp = int64_t;

using JobID  = int32_t;
using TaskID = int32_t;
using BalanceID = int64_t;

using GroupID = int32_t;
using ZoneID = int32_t;

using SessionID = int64_t;

using ExecutionPlanID = int64_t;
}  // namespace nebula
#endif  // COMMON_THRIFT_THRIFTTYPES_H_
