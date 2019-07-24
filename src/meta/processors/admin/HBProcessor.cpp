/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/HBProcessor.h"
#include "time/WallClock.h"
#include "meta/ActiveHostsMan.h"

DEFINE_int32(expired_hosts_check_interval_sec, 20,
             "Check the expired hosts at the interval");
DEFINE_int32(expired_threshold_sec, 10 * 60,
             "Hosts will be expired in this time if no heartbeat received");
DEFINE_bool(hosts_whitelist_enabled, true, "Check host whether in whitelist when received hb");

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

    if (!clusterMan_) {
        LOG(ERROR) << "Meta server init error!";
        resp_.set_code(cpp2::ErrorCode::E_WRONGCLUSTER);
        onFinished();
        return;
    }

    ClusterID peerCluserId = req.get_clusterId();
    if (peerCluserId == 0) {
        LOG(INFO) << "Set clusterId for new host " << host << "!";
        resp_.set_clusterId(clusterMan_->getClusterId());
    } else if (peerCluserId != clusterMan_->getClusterId()) {
        LOG(ERROR) << "Reject wrong cluster host " << host << "!";
        resp_.set_code(cpp2::ErrorCode::E_WRONGCLUSTER);
        onFinished();
        return;
    }

    LOG(INFO) << "Receive heartbeat from " << host;
    HostInfo info;
    info.lastHBTimeInSec_ = time::WallClock::fastNowInSec();
    if (!ActiveHostsMan::instance()->updateHostInfo(host, info)) {
        resp_.set_code(cpp2::ErrorCode::E_LEADER_CHANGED);
    }
    onFinished();
}

}  // namespace meta
}  // namespace nebula

