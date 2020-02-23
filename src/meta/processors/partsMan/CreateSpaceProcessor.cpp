/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/CreateSpaceProcessor.h"
#include "meta/ActiveHostsMan.h"

DEFINE_int32(default_parts_num, 100, "The default number of parts when a space is created");
DEFINE_int32(default_replica_factor, 1, "The default replica factor when a space is created");

namespace nebula {
namespace meta {

void CreateSpaceProcessor::process(const cpp2::CreateSpaceReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    auto properties = req.get_properties();
    auto spaceRet = getSpaceId(properties.get_space_name());
    if (spaceRet.ok()) {
        cpp2::ErrorCode ret;
        if (req.get_if_not_exists()) {
            ret = cpp2::ErrorCode::SUCCEEDED;
        } else {
            LOG(ERROR) << "Create Space Failed : Space " << properties.get_space_name()
                       << " have existed!";
            ret = cpp2::ErrorCode::E_EXISTED;
        }

        resp_.set_id(to(spaceRet.value(), EntryType::SPACE));
        handleErrorCode(ret);
        onFinished();
        return;
    }
    CHECK_EQ(Status::SpaceNotFound(), spaceRet.status());
    auto hosts = ActiveHostsMan::getActiveHosts(kvstore_);
    if (hosts.empty()) {
        LOG(ERROR) << "Create Space Failed : No Hosts!";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }

    auto idRet = autoIncrementId();
    if (!nebula::ok(idRet)) {
        LOG(ERROR) << "Create Space Failed : Get space id failed";
        handleErrorCode(nebula::error(idRet));
        onFinished();
        return;
    }
    auto spaceId = nebula::value(idRet);
    auto spaceName = properties.get_space_name();
    auto partitionNum = properties.get_partition_num();
    auto replicaFactor = properties.get_replica_factor();
    if (partitionNum == 0) {
        partitionNum = FLAGS_default_parts_num;
        // Set the default value back to the struct, which will be written to storage
        properties.set_partition_num(partitionNum);
    }
    if (replicaFactor == 0) {
        replicaFactor = FLAGS_default_replica_factor;
        // Set the default value back to the struct, which will be written to storage
        properties.set_replica_factor(replicaFactor);
    }
    VLOG(3) << "Create space " << spaceName << ", id " << spaceId;
    if ((int32_t)hosts.size() < replicaFactor) {
        LOG(ERROR) << "Not enough hosts existed for replica "
                   << replicaFactor << ", hosts num " << hosts.size();
        handleErrorCode(cpp2::ErrorCode::E_UNSUPPORTED);
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
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(spaceId, EntryType::SPACE));
    doSyncPutAndUpdate(std::move(data));
}


std::vector<nebula::cpp2::HostAddr>
CreateSpaceProcessor::pickHosts(PartitionID partId,
                                const std::vector<HostAddr>& hosts,
                                int32_t replicaFactor) {
    if (hosts.empty()) {
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

