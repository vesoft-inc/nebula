/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_SHARD_H_
#define STORAGE_SHARD_H_

#include "base/Base.h"
#include "storage/ResultCode.h"

namespace vesoft {
namespace vgraph {
namespace storage {

class Shard final {
public:
    Shard(const std::string& group,
          int32_t shardId,
          const std::string& dataPath,
          const std::string& walPath)
        : group_(group)
        , shardId_(shardId)
        , dataPath_(dataPath)
        , walPath_(walPath)  {}

    ResultCode get(const std::string& key, std::string& value);

    ResultCode put(std::string key, std::string value);

private:
    std::string group_;
    int32_t shardId_;
    std::string dataPath_;
    std::string walPath_;
};

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft
#endif  // STORAGE_SHARD_H_

