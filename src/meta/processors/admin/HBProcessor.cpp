/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/HBProcessor.h"
#include "common/time/WallClock.h"
#include "meta/ActiveHostsMan.h"
#include "meta/KVBasedClusterIdMan.h"

DEFINE_bool(hosts_whitelist_enabled, false, "Check host whether in whitelist when received hb");

namespace nebula {
namespace meta {

void HBProcessor::process(const cpp2::HBReq& req) {
    HostAddr host(req.host.host, req.host.port);
    if (FLAGS_hosts_whitelist_enabled
            && hostExist(MetaServiceUtils::hostKey(host.host, host.port))
                == Status::HostNotFound()) {
        LOG(INFO) << "Reject unregistered host " << host << "!";
        handleErrorCode(cpp2::ErrorCode::E_INVALID_HOST);
        onFinished();
        return;
    }

    LOG(INFO) << "Receive heartbeat from " << host
              << ", role = " << meta::cpp2::_HostRole_VALUES_TO_NAMES.at(req.get_role());
    auto ret = kvstore::ResultCode::SUCCEEDED;
    if (req.get_role() == cpp2::HostRole::STORAGE) {
        ClusterID peerCluserId = req.get_cluster_id();
        if (peerCluserId == 0) {
            LOG(INFO) << "Set clusterId for new host " << host << "!";
            resp_.set_cluster_id(clusterId_);
        } else if (peerCluserId != clusterId_) {
            LOG(ERROR) << "Reject wrong cluster host " << host << "!";
            handleErrorCode(cpp2::ErrorCode::E_WRONGCLUSTER);
            onFinished();
            return;
        }
    }

    HostInfo info(time::WallClock::fastNowInMilliSec(),
                  req.get_role(), req.get_git_info_sha());
    if (req.__isset.leader_partIds) {
        ret = ActiveHostsMan::updateHostInfo(kvstore_, host, info,
                                             req.get_leader_partIds());
    } else {
        ret = ActiveHostsMan::updateHostInfo(kvstore_, host, info);
    }
    if (ret == kvstore::ResultCode::ERR_LEADER_CHANGED) {
        auto leaderRet = kvstore_->partLeader(kDefaultSpaceId, kDefaultPartId);
        if (nebula::ok(leaderRet)) {
            resp_.set_leader(toThriftHost(nebula::value(leaderRet)));
        }
    }

    handleErrorCode(MetaCommon::to(ret));
    int64_t lastUpdateTime = LastUpdateTimeMan::get(this->kvstore_);
    resp_.set_last_update_time_in_ms(lastUpdateTime);
    onFinished();
}

}  // namespace meta
}  // namespace nebula

