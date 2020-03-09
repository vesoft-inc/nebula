/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_COMMON_H_
#define KVSTORE_COMMON_H_

#include "base/Base.h"
#include "rocksdb/slice.h"
#include <folly/Function.h>

namespace nebula {
namespace kvstore {

enum ResultCode {
    SUCCEEDED               = 0,
    ERR_SPACE_NOT_FOUND     = -1,
    ERR_PART_NOT_FOUND      = -2,
    ERR_KEY_NOT_FOUND       = -3,
    ERR_CONSENSUS_ERROR     = -4,
    ERR_LEADER_CHANGED      = -5,
    ERR_INVALID_ARGUMENT    = -6,
    ERR_IO_ERROR            = -7,
    ERR_UNSUPPORTED         = -8,
    ERR_CHECKPOINT_ERROR    = -9,
    ERR_WRITE_BLOCK_ERROR   = -10,
    ERR_TAG_NOT_FOUND       = -11,
    ERR_EDGE_NOT_FOUND      = -12,
    ERR_PARTIAL_RESULT      = -99,
    ERR_UNKNOWN             = -100,
};

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
using KVCallback = folly::Function<void(ResultCode code)>;
using NewLeaderCallback = folly::Function<void(HostAddr nLeader)>;

inline rocksdb::Slice toSlice(const folly::StringPiece& str) {
    return rocksdb::Slice(str.begin(), str.size());
}

using KVMap = std::unordered_map<std::string, std::string>;
using KVArrayIterator = std::vector<KV>::const_iterator;

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_COMMON_H_
