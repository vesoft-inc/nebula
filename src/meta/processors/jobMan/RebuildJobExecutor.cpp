/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/network/NetworkUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/common/MetaCommon.h"
#include "meta/processors/Common.h"
#include "meta/processors/jobMan/RebuildJobExecutor.h"
#include "utils/Utils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

bool RebuildJobExecutor::check() {
    return paras_.size() >= 1;
}

nebula::cpp2::ErrorCode RebuildJobExecutor::prepare() {
    // the last value of paras_ is the space name, others are index name
    auto spaceRet = getSpaceIdFromName(paras_.back());
    if (!nebula::ok(spaceRet)) {
        LOG(ERROR) << "Can't find the space: " << paras_.back();
        return nebula::error(spaceRet);
    }
    space_ = nebula::value(spaceRet);

    std::string indexValue;
    IndexID indexId = -1;
    for (auto i = 0u; i < paras_.size() - 1; i++) {
        auto indexKey = MetaServiceUtils::indexIndexKey(space_, paras_[i]);
        auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &indexValue);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Get indexKey error indexName: " << paras_[i] << " error: "
                       << apache::thrift::util::enumNameSafe(retCode);
            return retCode;
        }

        indexId = *reinterpret_cast<const IndexID*>(indexValue.c_str());
        LOG(INFO) << "Rebuild Index Space " << space_ << ", Index " << indexId;
        taskParameters_.emplace_back(folly::to<std::string>(indexId));
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode RebuildJobExecutor::stop() {
    auto errOrTargetHost = getTargetHost(space_);
    if (!nebula::ok(errOrTargetHost)) {
        LOG(ERROR) << "Get target host failed";
        auto retCode = nebula::error(errOrTargetHost);
        if (retCode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            retCode = nebula::cpp2::ErrorCode::E_NO_HOSTS;
        }
        return retCode;
    }

    auto& hosts = nebula::value(errOrTargetHost);
    std::vector<folly::Future<Status>> futures;
    for (auto& host : hosts) {
        auto future = adminClient_->stopTask({Utils::getAdminAddrFromStoreAddr(host.first)},
                                             jobId_, 0);
        futures.emplace_back(std::move(future));
    }

    auto tries = folly::collectAll(std::move(futures)).get();
    if (std::any_of(tries.begin(), tries.end(), [](auto& t){ return t.hasException(); })) {
        LOG(ERROR) << "RebuildJobExecutor::stop() RPC failure.";
        return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
    }
    for (const auto& t : tries) {
        if (!t.value().ok()) {
            LOG(ERROR) << "Stop Build Index Failed";
            return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
