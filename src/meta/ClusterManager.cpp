/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "base/Base.h"
#include "fs/FileUtils.h"
#include "ClusterManager.h"
#include "kvstore/KVStore.h"
#include "meta/processors/Common.h"
#include "folly/synchronization/Baton.h"

namespace nebula {
namespace meta {

using KVStore = nebula::kvstore::KVStore;
using ResultCode = nebula::kvstore::ResultCode;
const char* ClusterManager::kClusterIdKey = "metaClusterIdKey";

bool ClusterManager::loadOrCreateCluId(KVStore* kvstore) {
    std::string strClusterId;
    kvstore->get(kDefaultSpaceId, kDefaultPartId, kClusterIdKey, &strClusterId);

    bool ret = false;
    if (!strClusterId.empty()) {
        clusterId_ = folly::to<ClusterID>(strClusterId);
        clusterIdIsSet_ = true;
        return true;
    } else {
        createClusterId();
        strClusterId = folly::stringPrintf("%ld", clusterId_);
        folly::Baton<true, std::atomic> baton;
        kvstore->asyncMultiPut(kDefaultSpaceId,
                               kDefaultPartId,
                               {{kClusterIdKey, strClusterId}},
                               [&](ResultCode code) {
                                   ret = code == ResultCode::SUCCEEDED;
                                   baton.post();
                               });
        baton.wait();
    }
    return ret;
}


bool ClusterManager::loadClusterId() {
    int fd = ::open(clusterIdPath_.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(WARNING) << "loadClusterId failed!";
        return false;
    }
    char buff[32] = {};
    int len = ::read(fd, buff, sizeof(buff));
    if (len >= 0) {
        buff[len] = '\0';
    } else {
        buff[0] = '\0';
    }

    ClusterID clusterId = folly::to<ClusterID>(buff);
    LOG(INFO) << "int clusterId: " << clusterId;
    if (clusterId == 0) {
        ::close(fd);
        return false;
    } else {
        clusterId_ = clusterId;
        clusterIdIsSet_ = true;
        clusterIdDumped_ = true;
        LOG(INFO) << "loadClusterId succeed! clusterId_: " << clusterId_;
        ::close(fd);
        return true;
    }
}


void ClusterManager::createClusterId() {
    std::hash<std::string> hash_fn;
    clusterId_ = hash_fn(clusterHosts_);
    uint64_t mask = 0x7FFFFFFFFFFFFFFF;
    clusterId_ &= mask;
    LOG(INFO) << "ClusterManager::createClusterId clusterId_: " << clusterId_;
    clusterIdIsSet_ = true;
}


bool ClusterManager::dumpClusterId() {
    if (clusterIdPath_.empty()) {
        LOG(WARNING) << "ClusterManager::loadClusterId path to clusterId empty!";
        return false;
    }
    auto dirname = fs::FileUtils::dirname(clusterIdPath_.c_str());
    if (!fs::FileUtils::makeDir(dirname)) {
        LOG(ERROR) << "ClusterManager::dumpClusterId makeDir error!";
        return false;
    }

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    int fd = ::open(clusterIdPath_.c_str(), flags, 0644);
    if (fd < 0) {
        LOG(ERROR) << "ClusterManager::dumpClusterId open file error!";
        return false;
    }
    std::string strClusterId = folly::to<std::string>(clusterId_);
    LOG(INFO) << "dump strClusterId: " << strClusterId;
    int bytes = ::write(fd, strClusterId.c_str(), strClusterId.size());
    ::close(fd);
    if (bytes < static_cast<int>(strClusterId.size())) {
        return false;
    } else {
        clusterIdDumped_ = true;
        return true;
    }
}

}  // namespace meta
}  // namespace nebula

