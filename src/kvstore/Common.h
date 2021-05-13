/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_COMMON_H_
#define KVSTORE_COMMON_H_

#include <folly/Function.h>
#include <rocksdb/slice.h>
#include "common/base/Base.h"
#include "common/datatypes/HostAddr.h"
#include "common/thrift/ThriftTypes.h"
#include "common/interface/gen-cpp2/common_types.h"

namespace nebula {
namespace kvstore {

class KVFilter {
public:
    KVFilter() = default;
    virtual ~KVFilter() = default;

    /**
     * Remove the key in background compaction if return true, otherwise return false.
     * */
    virtual bool filter(GraphSpaceID spaceId,
                        const folly::StringPiece& key,
                        const folly::StringPiece& val) const = 0;
};

using KV = std::pair<std::string, std::string>;
using KVCallback = folly::Function<void(nebula::cpp2::ErrorCode code)>;
using NewLeaderCallback = folly::Function<void(HostAddr nLeader)>;

inline rocksdb::Slice toSlice(const folly::StringPiece& str) {
    return rocksdb::Slice(str.begin(), str.size());
}

using KVMap = std::unordered_map<std::string, std::string>;
using KVArrayIterator = std::vector<KV>::const_iterator;

}   // namespace kvstore
}   // namespace nebula

#endif   // KVSTORE_COMMON_H_
