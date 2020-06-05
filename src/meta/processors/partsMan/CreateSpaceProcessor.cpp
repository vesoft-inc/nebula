/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/CreateSpaceProcessor.h"
#include "meta/ActiveHostsMan.h"

DEFINE_int32(default_parts_num, 100, "The default number of parts when a space is created");
DEFINE_int32(default_replica_factor, 1, "The default replica factor when a space is created");
DEFINE_int32(default_max_parts_num, 100, "The default max partitions number");

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
    auto charsetName = properties.get_charset_name();
    auto collateName = properties.get_collate_name();
    auto count = GetPartsNumbers();
    if (count == -1) {
        handleErrorCode(cpp2::ErrorCode::E_UNKNOWN);
        onFinished();
        return;
    }

    if (count + partitionNum >= FLAGS_default_max_parts_num) {
        LOG(ERROR) << "Create Space Failed : TOO MANY PARTITIONS!";
        handleErrorCode(cpp2::ErrorCode::E_TOO_MANY_PARTS);
        onFinished();
        return;
    }

    // Use default values or values from meta's configuration file
    if (partitionNum == 0) {
        partitionNum = FLAGS_default_parts_num;
        if (partitionNum <= 0) {
            LOG(ERROR) << "Create Space Failed : partition_num is illegal!";
              resp_.set_code(cpp2::ErrorCode::E_INVALID_PARTITION_NUM);
              onFinished();
              return;
        }
        // Set the default value back to the struct, which will be written to storage
        properties.set_partition_num(partitionNum);
    }

    if (replicaFactor == 0) {
        replicaFactor = FLAGS_default_replica_factor;
        if (replicaFactor <= 0) {
            LOG(ERROR) << "Create Space Failed : replicaFactor is illegal!";
              resp_.set_code(cpp2::ErrorCode::E_INVALID_REPLICA_FACTOR);
              onFinished();
              return;
        }
        // Set the default value back to the struct, which will be written to storage
        properties.set_replica_factor(replicaFactor);
    }

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
    LOG(INFO) << "Create space " << spaceName << ", id " << spaceId;
}


std::vector<nebula::cpp2::HostAddr>
CreateSpaceProcessor::pickHosts(PartitionID partId,
                                const std::vector<network::InetAddress>& hosts,
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

int32_t CreateSpaceProcessor::GetPartsNumbers() {
    int32_t count = 0;
    auto prefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Spaces Failed";
        return -1;
    }

    while (iter->valid()) {
        auto properties = MetaServiceUtils::parseSpace(iter->val());
        count += properties.get_partition_num();
        iter->next();
    }

    return count;
}
}  // namespace meta
}  // namespace nebula

