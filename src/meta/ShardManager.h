/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SHARDMANAGER_H_
#define META_SHARDMANAGER_H_

#include "base/Base.h"

namespace nebula {
namespace meta {

class ShardManager final {
    friend class HostManager;
public:
    static const ShardManager& get();

    // Return the total number of shards
    size_t numShards(GraphSpaceID space) const;

    // Calculate the shard id for the given vertex id
    PartitionID shardId(GraphSpaceID space, int64_t id) const;

    // Figure out which host that the given shard belongs to
    const std::vector<HostAddr>& hostsForShard(GraphSpaceID space,
                                               PartitionID shard) const;

    const std::vector<PartitionID>& shardsOnHost(GraphSpaceID space,
                                                 HostAddr host) const;

    std::vector<GraphSpaceID> allGraphSpaces() const;

    std::vector<GraphSpaceID> graphSpacesOnHost(HostAddr host) const;

    // Cluster given ids into the shard they belong to
    // The method returns a map
    //  shard_id => [ids that belong to the shard]
    std::unordered_map<PartitionID, std::vector<int64_t>> clusterIdsToShards(
        GraphSpaceID space, std::vector<int64_t>& ids) const;

private:
    ShardManager() = default;

private:
    // A map from space => host
    std::unordered_map<GraphSpaceID, std::vector<HostAddr>> spaceHostMap_;

    // A map from space/shard => host
    std::unordered_map<
        GraphSpaceID,
        std::unordered_map<PartitionID, std::vector<HostAddr>>
    > shardHostMap_;

    // A reversed map from host => space/shard
    std::unordered_map<
        HostAddr,
        std::unordered_map<GraphSpaceID, std::vector<PartitionID>>
    > hostShardMap_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SHARDMANAGER_H_
