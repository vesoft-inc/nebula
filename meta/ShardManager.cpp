/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/ShardManager.h"

namespace vesoft {
namespace meta {

// static
const ShardManager& ShardManager::get() {
    static ShardManager shardMgr;
    return shardMgr;
}


int32_t ShardManager::numShards() const {
    // TODO To implement this
    return 100;
}


int32_t ShardManager::shardId(int64_t id) const {
    auto s = id % numShards();
    return s >= 0 ? s : numShards() + s;
}


HostAddr ShardManager::hostAddr(int32_t shard) const {
    auto it = shardHostMap_.find(shard);
    CHECK(it != shardHostMap_.end());
    return it->second;
}


std::unordered_map<int32_t, std::vector<int64_t>>
ShardManager::clusterIdsToShards(std::vector<int64_t> ids) const {
    std::unordered_map<int32_t, std::vector<int64_t>> clusters;
    for (auto id : ids) {
        auto s = shardId(id);
        auto it = clusters.find(s);
        if (it == clusters.end()) {
            clusters.insert(
                std::make_pair(s, std::vector<int64_t>(1, id)));
        } else {
            it->second.push_back(id);
        }
    }

    return std::move(clusters);
}

}  // namespace meta
}  // namespace vesoft
