/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_BASE_THRIFTTYPES_H_
#define COMMON_BASE_THRIFTTYPES_H_

namespace nebula {

using ClusterID = int8_t;
using GraphSpaceID = int32_t;
using PartitionID = int32_t;
using TermID = int64_t;
using LogID = int64_t;
using IPv4 = int32_t;
using Port = int32_t;

// Host address type
using HostAddr = std::pair<IPv4, Port>;

}  // namespace nebula
#endif  // COMMON_BASE_THRIFTTYPES_H_

