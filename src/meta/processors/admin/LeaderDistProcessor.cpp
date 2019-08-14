/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/LeaderDistProcessor.h"
#include "meta/processors/admin/Balancer.h"
#include "network/NetworkUtils.h"

namespace nebula {
namespace meta {

void LeaderDistProcessor::process(const cpp2::LeaderDistReq& req) {
    UNUSED(req);
    auto ret = Balancer::instance(kvstore_)->leaderDist();
    if (!ret.ok()) {
        LOG(ERROR) << "Error occurs when get leader";
        resp_.set_code(cpp2::ErrorCode::E_RPC_FAILURE);
        onFinished();
        return;
    }
    std::unordered_map<std::string,
                       std::unordered_map<GraphSpaceID, std::vector<PartitionID>>> dist;
    auto hostLeaderMap = ret.value();
    for (auto& entry : hostLeaderMap) {
        std::string host = network::NetworkUtils::intToIPv4(entry.first.first) + ":" +
                           folly::to<std::string>(entry.first.second);
        dist[host] = std::move(entry.second);
    }

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_leader_dist(dist);
    onFinished();
}


}  // namespace meta
}  // namespace nebula

