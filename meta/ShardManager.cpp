/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/ShardManager.h"

#define ID_HASH(id, numShards) \
    ((static_cast<uint64_t>(id)) % numShards)

namespace vesoft {
namespace meta {

// static
const ShardManager& ShardManager::get() {
    static ShardManager shardMgr;
    return shardMgr;
}


size_t ShardManager::numShards(GraphSpaceID space) const {
    auto it = shardHostMap_.find(space);
    CHECK(it != shardHostMap_.end());
    return it->second.size();
}


PartitionID ShardManager::shardId(GraphSpaceID space, int64_t id) const {
    auto shards = numShards(space);
    auto s = ID_HASH(id, shards);
    CHECK_GE(s, 0U);
    return s;
}


const std::vector<HostAddr>& ShardManager::hostsForShard(
        GraphSpaceID space,
        PartitionID shard) const {
    auto spaceIt = shardHostMap_.find(space);
    CHECK(spaceIt != shardHostMap_.end());

    auto shardIt = spaceIt->second.find(shard);
    CHECK(shardIt != spaceIt->second.end());
    return shardIt->second;
}


std::unordered_map<PartitionID, std::vector<int64_t>>
ShardManager::clusterIdsToShards(GraphSpaceID space,
                                 std::vector<int64_t>& ids) const {
    std::unordered_map<PartitionID, std::vector<int64_t>> clusters;
    auto shards = numShards(space);

    for (auto id : ids) {
        auto s = ID_HASH(id, shards);
        CHECK_GE(s, 0U);
        clusters[s].push_back(id);
    }

    return clusters;
}


const std::vector<PartitionID>& ShardManager::shardsOnHost(GraphSpaceID space,
                                                           HostAddr host) const {
    static const std::vector<PartitionID> emptyShardList;

    auto hostIt = hostShardMap_.find(host);
    if (hostIt == hostShardMap_.end()) {
        VLOG(2) << "The given host does not exist";
        return emptyShardList;
    }

    auto spaceIt = hostIt->second.find(space);
    if (spaceIt == hostIt->second.end()) {
        VLOG(2) << "The given graph space does not exist";
        return emptyShardList;
    }

    return spaceIt->second;
}


std::vector<GraphSpaceID> ShardManager::allGraphSpaces() const {
    std::vector<GraphSpaceID> spaces;
    for (auto& gs : shardHostMap_) {
        spaces.push_back(gs.first);
    }
    return spaces;
}


std::vector<GraphSpaceID> ShardManager::graphSpacesOnHost(HostAddr host) const {
    std::vector<GraphSpaceID> spaces;
    auto it = hostShardMap_.find(host);
    if (it == hostShardMap_.end()) {
        VLOG(2) << "The given host does not exist";
        return spaces;
    }

    for (auto& gs : it->second) {
        spaces.push_back(gs.first);
    }
    return spaces;
}

}  // namespace meta
}  // namespace vesoft
