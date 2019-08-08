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


DEFINE_int32(putTryNum, 10, "try num of store clusterId to kvstore");

namespace nebula {
namespace meta {

const char* ClusterManager::kClusterIdKey = "__meta_cluster_id_key__";

ResultCode ClusterManager::kvPut(KVStore* kvstore, std::string& strClusterId) {
    ResultCode code;
    folly::Baton<true, std::atomic> baton;
    kvstore->asyncMultiPut(kDefaultSpaceId,
                           kDefaultPartId,
                           {{kClusterIdKey, strClusterId}},
                           [&](ResultCode innerCode) {
                               code = innerCode;
                               baton.post();
                           });
    baton.wait();
    return code;
}

bool ClusterManager::loadOrCreateCluId(KVStore* kvstore) {
    std::string strClusterId;

    ResultCode getCode = kvstore->get(kDefaultSpaceId,
                                      kDefaultPartId,
                                      kClusterIdKey,
                                      &strClusterId);
    // TODO: check if leader election success
    if ((getCode != ResultCode::SUCCEEDED) && (getCode != ResultCode::ERR_KEY_NOT_FOUND)) {
        LOG(ERROR) << "ClusterManager::loadOrCreateCluId get error!";
        return false;
    }

    if (!strClusterId.empty()) {
        clusterId_ = folly::to<ClusterID>(strClusterId);
        LOG(INFO) << "ClusterManager::loadOrCreateCluId load from kvstore, clusterId: "
                  << clusterId_;
        clusterIdIsSet_ = true;
        return true;
    } else {
        createClusterId();
        strClusterId = folly::stringPrintf("%ld", clusterId_);
        while (FLAGS_putTryNum > 0) {
            ResultCode code = kvPut(kvstore, strClusterId);
            if (code == ResultCode::SUCCEEDED) {
                LOG(INFO) << "ClusterManager::loadOrCreateCluId kvPut success, clusterId: "
                          << clusterId_;
                return true;
            } else if (code != ResultCode::ERR_LEADER_CHANGED) {
                LOG(ERROR) << "ClusterManager::loadOrCreateCluId put error!";
                return false;
            }
            auto leaderAddrRet = kvstore->partLeader(kDefaultSpaceId, kDefaultPartId);
            if (!ok(leaderAddrRet)) {
                LOG(ERROR) << "ClusterManager::loadOrCreateCluId partLeader() error!";
                return false;
            }
            auto leaderAddr = value(leaderAddrRet);
            bool elecCompleted = (leaderAddr != HostAddr(0, 0));
            if (elecCompleted) {
                LOG(INFO) << "ClusterManager::loadOrCreateCluId elecCompleted";
                return true;
            } else {
                LOG(INFO) << "ClusterManager::loadOrCreateCluId not elecCompleted";
                sleep(1);
            }
            --FLAGS_putTryNum;
        }  // end while
    }  // end else
    LOG(INFO) << "ClusterManager::loadOrCreateCluId after load, clusterId: " << clusterId_;

    return true;
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

