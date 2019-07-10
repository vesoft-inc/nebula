/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLUSTER_MANAGER_H_
#define CLUSTER_MANAGER_H_

#include "base/Base.h"
#include "kvstore/Common.h"
#include <gtest/gtest_prod.h>
#include <folly/RWSpinLock.h>

namespace nebula {
namespace kvstore {


/**
 * This class manages clusterId used for meta server and storage server.
 * */
class ClusterManager {
public:
    ClusterManager(HostAddr storeSvcAddr,
                   const std::string& dataPath = "data")
            : storeSvcAddr_(storeSvcAddr)
            , dataPath_(dataPath) {}

    virtual ~ClusterManager() = default;

    ResultCode init();

    ResultCode loadClusterId();

    ResultCode createClusterId();

    ResultCode dumpClusterId();

    ClusterID getClusterId() {
        return clusterId_;
    }

    void setClusterId(ClusterID clusterId) {
        clusterId_ = clusterId;
        clusterIsSet = true;
    }

    bool isClusterIdSet() {
        return clusterIsSet;
    }

private:
    ResultCode mkdirsMultiLevel(std::string& path);

    void splitPath(const std::string& path,
                   std::vector<std::string>& pieces,
                   char delim = '/');


private:
    bool clusterIsSet{false};
    ClusterID clusterId_{0};
    HostAddr storeSvcAddr_;
    std::string dataPath_;
    static const char* fileName_;
};



}  // namespace kvstore
}  // namespace nebula
#endif  // CLUSTER_MANAGER_H_

