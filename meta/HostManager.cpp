/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/HostManager.h"
#include "meta/ShardManager.h"

namespace vesoft {
namespace meta {

// static
const HostManager& HostManager::get() {
    static HostManager hostMgr;
    return hostMgr;
}


int32_t HostManager::numHosts() const {
    return hosts_.size();
}


const std::vector<HostAddr>& HostManager::allHosts() const {
    return hosts_;
}


HostAddr HostManager::host(int64_t id) const {
    auto sm = ShardManager::get();
    return sm.hostAddr(sm.shardId(id));
}


std::unordered_map<HostAddr, std::vector<int64_t>>
HostManager::clusterIdsToHosts(std::vector<int64_t> ids) const {
    std::unordered_map<HostAddr, std::vector<int64_t>> clusters;
    auto sm = ShardManager::get();
    for (auto id : ids) {
        auto host = sm.hostAddr(sm.shardId(id));
        auto it = clusters.find(host);
        if (it == clusters.end()) {
            clusters.insert(
                std::make_pair(host, std::vector<int64_t>(1, id)));
        } else {
            it->second.push_back(id);
        }
    }

    return std::move(clusters);
}

}  // namespace meta
}  // namespace vesoft
