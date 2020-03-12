/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "network/NetworkUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"
#include "meta/processors/jobMan/RebuildJobExecutor.h"

namespace nebula {
namespace meta {

ErrorOr<nebula::kvstore::ResultCode, std::map<HostAddr, Status>>
RebuildJobExecutor::execute() {
    if (paras_.empty()) {
        LOG(ERROR) << "SimpleConcurrentJob should have a para";
        return nebula::kvstore::ResultCode::ERR_INVALID_ARGUMENT;
    }

    std::string spaceName = paras_[0];
    std::string indexName = paras_[1];
    LOG(INFO) << "Rebuild Index Space " << spaceName << ", Index Name " << indexName;

    auto spaceKey = MetaServiceUtils::indexSpaceKey(spaceName);
    std::string val;
    auto rc = kvstore_->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &val);
    if (rc != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "KVStore get indexKey error spaceName: " << spaceName;
        return rc;
    }
    int space = *reinterpret_cast<const int*>(val.c_str());
    bool isOffline = false;

    const auto& hostPrefix = MetaServiceUtils::leaderPrefix();
    std::unique_ptr<kvstore::KVIterator> leaderIter;
    auto leaderRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &leaderIter);
    if (leaderRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get space " << space << "'s part failed";
    }

    std::string indexValue;
    auto indexKey = MetaServiceUtils::indexIndexKey(space, indexName);
    auto indexCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &indexValue);
    if (indexCode != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "KVStore get indexKey error indexName: " << indexName;
        return indexCode;
    }

    auto indexID = *reinterpret_cast<const IndexID*>(indexValue.c_str());
    auto statusKey = MetaServiceUtils::rebuildIndexStatus(space, category_, indexName);
    if (!MetaCommon::saveRebuildStatus(kvstore_, statusKey, "RUNNING")) {
        LOG(ERROR) << "Save rebuild status failed";
    }

    std::vector<folly::Future<Status>> results;
    auto activeHosts = ActiveHostsMan::getActiveHosts(kvstore_, FLAGS_heartbeat_interval_secs + 1);
    while (leaderIter->valid()) {
        auto host = MetaServiceUtils::parseLeaderKey(leaderIter->key());
        auto ip = NetworkUtils::intToIPv4(host.get_ip());
        auto port = host.get_port();
        auto hostAddrRet = NetworkUtils::toHostAddr(ip, port);
        if (!hostAddrRet.ok()) {
            LOG(ERROR) << "Can't cast to host " << ip + ":" << port;
        }

        auto hostAddr = hostAddrRet.value();
        if (std::find(activeHosts.begin(), activeHosts.end(),
                      HostAddr(host.ip, host.port)) != activeHosts.end()) {
            auto leaderParts = MetaServiceUtils::parseLeaderVal(leaderIter->val());
            auto& partIds = leaderParts[space];
            auto future = caller(hostAddr, space, indexID, std::move(partIds), isOffline);
            results.emplace_back(std::move(future));
        }
        leaderIter->next();
    }

    handleRebuildIndexResult(std::move(results), kvstore_, std::move(statusKey));

    std::map<HostAddr, Status> ret;
    return ret;
}

void RebuildJobExecutor::stop() {
}

}  // namespace meta
}  // namespace nebula
