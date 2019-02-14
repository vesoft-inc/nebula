/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/PartManager.h"
#include "meta/FileBasedPartManager.h"

#define ID_HASH(id, numShards) \
    ((static_cast<uint64_t>(id)) % numShards)

DECLARE_string(partition_conf_file);
DECLARE_string(meta_server);

namespace nebula {
namespace meta {

folly::RWSpinLock PartManager::accessLock_;
std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>>
    PartManager::partManagers_;
std::once_flag initFlag;


// static
std::shared_ptr<const PartManager> PartManager::get(GraphSpaceID space) {
    std::call_once(initFlag, [] () {
        if (!FLAGS_partition_conf_file.empty()) {
            partManagers_ = FileBasedPartManager::init();
        } else {
            partManagers_ = AdHocPartManagersBuilder::get();
        }
    });

    folly::RWSpinLock::ReadHolder rh(accessLock_);

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

std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>>
    AdHocPartManagersBuilder::partManagers_;
// static
std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>> AdHocPartManagersBuilder::get() {
    return partManagers_;
}

// static
void AdHocPartManagersBuilder::add(GraphSpaceID spaceId,
                                   HostAddr host,
                                   std::vector<PartitionID> parts) {
    auto it = partManagers_.find(spaceId);
    if (it != partManagers_.end()) {
        LOG(FATAL) << "PartManager for space " << spaceId << " already existed!";
    }
    auto* pmPtr = new PartManager();
    std::shared_ptr<PartManager> pm(pmPtr);
    pm->hosts_.emplace_back(host);
    for (auto partId : parts) {
        pm->partHostMap_[partId].emplace_back(host);
    }
    pm->hostPartMap_.emplace(host, std::move(parts));
    partManagers_.emplace(spaceId, pm);
}

}  // namespace meta
}  // namespace nebula
