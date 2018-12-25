/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/PartManager.h"

#define ID_HASH(id, numShards) \
    ((static_cast<uint64_t>(id)) % numShards)

namespace nebula {
namespace meta {

std::unordered_map<GraphSpaceID, std::shared_ptr<const PartManager>>
    PartManager::partManagers_;


// static
std::shared_ptr<const PartManager> PartManager::get(GraphSpaceID space) {
    auto it = partManagers_.find(space);
    if (it != partManagers_.end()) {
        return it->second;
    } else {
        return std::shared_ptr<const PartManager>();
    }
}


size_t PartManager::numParts() const {
    return partHostMap_.size();
}


size_t PartManager::numHosts() const {
    return hosts_.size();
}


const std::vector<HostAddr>& PartManager::allHosts() const {
    return hosts_;
}


PartitionID PartManager::partId(int64_t id) const {
    auto parts = numParts();
    auto s = ID_HASH(id, parts);
    CHECK_GE(s, 0U);
    return s;
}


const std::vector<HostAddr>& PartManager::hostsForPart(PartitionID part) const {
    auto partIt = partHostMap_.find(part);
    CHECK(partIt != partHostMap_.end());
    return partIt->second;
}


const std::vector<PartitionID>& PartManager::partsOnHost(HostAddr host) const {
    static const std::vector<PartitionID> emptyShardList;

    auto hostIt = hostPartMap_.find(host);
    if (hostIt == hostPartMap_.end()) {
        VLOG(2) << "The given host does not exist";
        return emptyShardList;
    }

    return hostIt->second;
}


std::unordered_map<PartitionID, std::vector<int64_t>>
PartManager::clusterIdsToParts(std::vector<int64_t>& ids) const {
    std::unordered_map<PartitionID, std::vector<int64_t>> clusters;
    auto parts = numParts();

    for (auto id : ids) {
        auto s = ID_HASH(id, parts);
        CHECK_GE(s, 0U);
        clusters[s].push_back(id);
    }

    return clusters;
}

}  // namespace meta
}  // namespace nebula
