/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SHARDMANAGER_H_
#define META_SHARDMANAGER_H_

#include "base/Base.h"

namespace vesoft {
namespace meta {

class ShardManager final {
public:
    static const ShardManager& get();

    // Return the total number of shards
    int32_t numShards() const;

    // Calculate the shard id for the given vertex id
    int32_t shardId(int64_t id) const;

    // Figure out which host that the given shard belongs to
    HostAddr hostAddr(int32_t shard) const;

    // Cluster given ids into the shard they belong to
    // The method returns a map
    //  shard_id => [ids that belong to the shard]
    std::unordered_map<int32_t, std::vector<int64_t>> clusterIdsToShards(
        std::vector<int64_t> ids) const;

private:
    ShardManager() = default;

private:
    std::unordered_map<int32_t, HostAddr> shardHostMap_;
};

}  // namespace meta
}  // namespace vesoft
#endif  // META_SHARDMANAGER_H_
