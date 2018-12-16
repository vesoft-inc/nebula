/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/HostManager.h"
#include "meta/ShardManager.h"

namespace nebula {
namespace meta {

// static
const HostManager& HostManager::get() {
    static HostManager hostMgr;
    return hostMgr;
}


size_t HostManager::numHosts(GraphSpaceID space) const {
    auto sm = ShardManager::get();
    auto it = sm.spaceHostMap_.find(space);
    if (it == sm.spaceHostMap_.end()) {
        return 0;
    } else {
        return it->second.size();
    }
}


const std::vector<HostAddr>& HostManager::allHosts(GraphSpaceID space) const {
    static const std::vector<HostAddr> emptyHostList;

    auto sm = ShardManager::get();
    auto it = sm.spaceHostMap_.find(space);
    if (it == sm.spaceHostMap_.end()) {
        return emptyHostList;
    } else {
        return it->second;
    }
}


HostAddr HostManager::hostForId(GraphSpaceID space,
                                int64_t id,
                                PartitionID& shard) const {
    // TODO Now always return the first host. We need to return the leader
    // when we know it
    auto sm = ShardManager::get();
    shard = sm.shardId(space, id);
    auto hosts = sm.hostsForShard(space, shard);
    CHECK_GT(hosts.size(), 0U);
    return hosts.front();
}


std::unordered_map<HostAddr, std::unordered_map<PartitionID, std::vector<int64_t>>>
HostManager::clusterIdsToHosts(GraphSpaceID space,
                               std::vector<int64_t>& ids) const {
    std::unordered_map<HostAddr, std::unordered_map<PartitionID, std::vector<int64_t>>>
        clusters;

    auto sm = ShardManager::get();
    for (auto id : ids) {
        PartitionID shard = sm.shardId(space, id);
        auto hosts = sm.hostsForShard(space, shard);
        CHECK_GT(hosts.size(), 0U);
        // TODO We need to use the leader here
        clusters[hosts.front()][shard].push_back(id);
    }

    return clusters;
}

}  // namespace meta
}  // namespace nebula
