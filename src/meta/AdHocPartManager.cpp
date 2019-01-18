/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/AdHocPartManager.h"

namespace nebula {
namespace meta {

// static
std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>> AdHocPartManager::init() {
    std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>> pms;
    return pms;
}


// static
void AdHocPartManager::addPartition(GraphSpaceID space, PartitionID part, HostAddr host) {
    folly::RWSpinLock::WriteHolder wh(PartManager::accessLock_);

    auto& pm = partManagers_[space];
    if (!pm) {
        pm.reset(new AdHocPartManager());
    }

    auto hostIt = pm->hostPartMap_.find(host);
    if (hostIt == pm->hostPartMap_.end()) {
        // New host
        pm->hosts_.push_back(host);
    }
    pm->hostPartMap_[host].push_back(part);
    pm->partHostMap_[part].push_back(host);
}

}  // namespace meta
}  // namespace nebula


