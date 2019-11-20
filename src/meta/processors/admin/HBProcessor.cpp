/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/HBProcessor.h"
#include "time/WallClock.h"
#include "meta/ActiveHostsMan.h"
#include "meta/ClusterIdMan.h"

DEFINE_bool(hosts_whitelist_enabled, false, "Check host whether in whitelist when received hb");

namespace nebula {
namespace meta {


void HBProcessor::process(const cpp2::HBReq& req) {
    HostAddr host(req.host.ip, req.host.port);
    if (FLAGS_hosts_whitelist_enabled
            && hostExist(MetaServiceUtils::hostKey(host.first, host.second))
                == Status::HostNotFound()) {
        LOG(INFO) << "Reject unregistered host " << host << "!";
        resp_.set_code(cpp2::ErrorCode::E_INVALID_HOST);
        onFinished();
        return;
    }

    ClusterID peerCluserId = req.get_cluster_id();
    if (peerCluserId == 0) {
        LOG(INFO) << "Set clusterId for new host " << host << "!";
        resp_.set_cluster_id(clusterId_);
    } else if (peerCluserId != clusterId_) {
        LOG(ERROR) << "Reject wrong cluster host " << host << "!";
        resp_.set_code(cpp2::ErrorCode::E_WRONGCLUSTER);
        onFinished();
        return;
    }

    LOG(INFO) << "Receive heartbeat from " << host;
    HostInfo info(time::WallClock::fastNowInMilliSec());
    auto ret = ActiveHostsMan::updateHostInfo(kvstore_, host, info);
    resp_.set_code(to(ret));
    if (ret == kvstore::ResultCode::ERR_LEADER_CHANGED) {
        auto leaderRet = kvstore_->partLeader(kDefaultSpaceId, kDefaultPartId);
        if (nebula::ok(leaderRet)) {
            resp_.set_leader(toThriftHost(nebula::value(leaderRet)));
        }
    }
    onFinished();
}

}  // namespace meta
}  // namespace nebula

