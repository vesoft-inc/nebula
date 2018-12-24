/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/HostManager.h"

namespace nebula {
namespace meta {

std::unordered_map<GraphSpaceID, std::shared_ptr<HostManager>> HostManager::hostManagers_;


// static
std::shared_ptr<const HostManager> HostManager::get(GraphSpaceID space) {
    auto it = hostManagers_.find(space);
    if (it != hostManagers_.end()) {
        return it->second;
    } else {
        return std::shared_ptr<const HostManager>();
    }
}


size_t HostManager::numHosts() const {
    auto sm = PartManager::get(space_);
    if (!sm) {
        LOG(ERROR) << "Cannot find PartManager for the graph space " << space_;
        return 0;
    } else {
        return sm->numHosts();
    }
}


const std::vector<HostAddr>& HostManager::allHosts() const {
    static const std::vector<HostAddr> emptyHostList;

    auto sm = PartManager::get(space_);
    if (!sm) {
        LOG(ERROR) << "Cannot find PartManager for the graph space " << space_;
        return emptyHostList;
    } else {
        return sm->allHosts();
    }
}


HostAddr HostManager::hostForId(int64_t id, PartitionID& part) const {
    // TODO Now always return the first host. We need to return the leader
    // when we know it
    auto sm = PartManager::get(space_);
    CHECK_NE(!sm, true);
    part = sm->partId(id);
    auto hosts = sm->hostsForPart(part);
    CHECK_GT(hosts.size(), 0U);
    // TODO We need to use the leader here
    return hosts.front();
}

}  // namespace meta
}  // namespace nebula
