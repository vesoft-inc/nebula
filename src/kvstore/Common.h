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

#define X_ERROR \
    X(SUCCEEDED               , 0) \
    X(ERR_SPACE_NOT_FOUND     , -1) \
    X(ERR_PART_NOT_FOUND      , -2) \
    X(ERR_KEY_NOT_FOUND       , -3) \
    X(ERR_CONSENSUS_ERROR     , -4) \
    X(ERR_LEADER_CHANGED      , -5) \
    X(ERR_INVALID_ARGUMENT    , -6) \
    X(ERR_IO_ERROR            , -7) \
    X(ERR_UNSUPPORTED         , -8) \
    X(ERR_CHECKPOINT_ERROR    , -9) \
    X(ERR_WRITE_BLOCK_ERROR   , -10) \
    X(ERR_UNKNOWN             , -100) \



/// X(enumerate, value)
#define X(enumerate, ...) enumerate = __VA_ARGS__,
enum ResultCode {
    X_ERROR
};
#undef X

#define X(enumerate, ...) case ResultCode::enumerate : return #enumerate;
static inline
const char* errMsg(ResultCode code) {
    switch (code) {
        X_ERROR
    }
    return "INVALID ERROR";
}
#undef X

#undef X_ERROR


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
