/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_COMMON_H_
#define KVSTORE_COMMON_H_

#include "base/Base.h"
#include "rocksdb/slice.h"

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
    ERR_UNKNOWN             = -8,
};

#define KV_DATA_PATH_FORMAT(path, spaceId) \
     folly::stringPrintf("%s/nebula/%d/data", path, spaceId)

#define KV_WAL_PATH_FORMAT(path, spaceId, partId) \
     folly::stringPrintf("%s/nebula/%d/wals/%d", \
        path, spaceId, partId)

using KVCallback = std::function<void(ResultCode code)>;
using KV = std::pair<std::string, std::string>;

inline rocksdb::Slice toSlice(const folly::StringPiece& str) {
    return rocksdb::Slice(str.begin(), str.size());
}

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_COMMON_H_
