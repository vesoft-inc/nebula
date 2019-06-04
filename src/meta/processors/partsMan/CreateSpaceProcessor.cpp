/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/CreateSpaceProcessor.h"
#include "meta/ActiveHostsMan.h"

namespace nebula {
namespace meta {

void CreateSpaceProcessor::process(const cpp2::CreateSpaceReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    auto& properties = req.get_properties();
    auto spaceRet = getSpaceId(properties.get_space_name());
    if (spaceRet.ok()) {
        LOG(ERROR) << "Create Space Failed : Space " << properties.get_space_name()
                   << " have existed!";
        resp_.set_id(to(spaceRet.value(), EntryType::SPACE));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }
    CHECK_EQ(Status::SpaceNotFound(), spaceRet.status());
    auto hosts = ActiveHostsMan::instance()->getActiveHosts();
    if (hosts.empty()) {
        LOG(ERROR) << "Create Space Failed : No Hosts!";
        resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }

    auto spaceId = autoIncrementId();
    auto spaceName = properties.get_space_name();
    auto partitionNum = properties.get_partition_num();
    auto replicaFactor = properties.get_replica_factor();
    VLOG(3) << "Create space " << spaceName << ", id " << spaceId;
    if ((int32_t)hosts.size() < replicaFactor) {
        LOG(ERROR) << "Not enough hosts existed for replica "
                   << replicaFactor << ", hosts num " << hosts.size();
        resp_.set_code(cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexSpaceKey(spaceName),
                      std::string(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId)));
    data.emplace_back(MetaServiceUtils::spaceKey(spaceId),
                      MetaServiceUtils::spaceVal(properties));
    for (auto partId = 1; partId <= partitionNum; partId++) {
        auto partHosts = pickHosts(partId, hosts, replicaFactor);
        data.emplace_back(MetaServiceUtils::partKey(spaceId, partId),
                          MetaServiceUtils::partVal(partHosts));
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(spaceId, EntryType::SPACE));
    doPut(std::move(data));
}


std::vector<nebula::cpp2::HostAddr>
CreateSpaceProcessor::pickHosts(PartitionID partId,
                                const std::vector<HostAddr>& hosts,
                                int32_t replicaFactor) {
    if (hosts.size() == 0) {
        return std::vector<nebula::cpp2::HostAddr>();
    }
    auto startIndex = partId;
    std::vector<nebula::cpp2::HostAddr> pickedHosts;
    for (decltype(replicaFactor) i = 0; i < replicaFactor; i++) {
        pickedHosts.emplace_back(toThriftHost(hosts[startIndex++ % hosts.size()]));
    }
    return pickedHosts;
}

}  // namespace meta
}  // namespace nebula

