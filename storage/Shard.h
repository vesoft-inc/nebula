/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_SHARD_H_
#define STORAGE_SHARD_H_

#include "base/Base.h"
#include "storage/ResultCode.h"
#include "storage/StorageEngine.h"

namespace vesoft {
namespace vgraph {
namespace storage {

class Shard final {
public:
    Shard(GraphSpaceID spaceId,
          PartitionID partId,
          const std::string& walPath)
        : spaceId_(spaceId)
        , partId_(partId)
        , walPath_(walPath)  {}

    ResultCode get(const std::string& key, std::string& value);

    ResultCode put(std::string key, std::string value);

private:
    GraphSpaceID spaceId_;
    PartitionID partId_;
    std::string walPath_;
    StorageEngine* engine_ = nullptr;
};

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft
#endif  // STORAGE_SHARD_H_

