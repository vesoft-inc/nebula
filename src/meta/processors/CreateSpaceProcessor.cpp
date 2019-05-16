/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/CreateSpaceProcessor.h"

namespace nebula {
namespace meta {

void CreateSpaceProcessor::process(const cpp2::CreateSpaceReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    auto spaceRet = getSpaceId(req.get_space_name());
    if (spaceRet.ok()) {
        LOG(ERROR) << "Create Space Failed : Space " << req.get_space_name() << " have existed!";
        resp_.set_id(to(spaceRet.value(), EntryType::SPACE));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }
    CHECK_EQ(Status::SpaceNotFound(), spaceRet.status());
    auto ret = allHosts();
    if (!ret.ok()) {
        LOG(ERROR) << "Create Space Failed : No Hosts!";
        resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }
    auto spaceId = autoIncrementId();
    auto hosts = ret.value();
    auto spaceName = req.get_space_name();
    auto partitionNum = req.get_partition_num();
    auto replicaFactor = req.get_replica_factor();
    cpp2::SpaceProperties properties;
    properties.set_space_name(spaceName);
    properties.set_partition_num(partitionNum);
    properties.set_replica_factor(replicaFactor);
    VLOG(3) << "Create space " << spaceName << ", id " << spaceId;

    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexSpaceKey(spaceName),
                      std::string(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId)));
    data.emplace_back(MetaServiceUtils::spaceKey(spaceId),
                      MetaServiceUtils::spaceVal(properties));
    for (auto partId = 1; partId <= req.get_partition_num(); partId++) {
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
                                const std::vector<nebula::cpp2::HostAddr>& hosts,
                                int32_t replicaFactor) {
    if (hosts.size() == 0) {
        return std::vector<nebula::cpp2::HostAddr>();
    }
    auto startIndex = partId;
    std::vector<nebula::cpp2::HostAddr> pickedHosts;
    for (decltype(replicaFactor) i = 0; i < replicaFactor; i++) {
        pickedHosts.emplace_back(hosts[startIndex++ % hosts.size()]);
    }
    return pickedHosts;
}

}  // namespace meta
}  // namespace nebula

